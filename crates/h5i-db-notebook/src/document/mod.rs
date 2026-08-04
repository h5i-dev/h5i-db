//! The notebook document: nbformat v4 parsing, editing, and lossless writing.
//!
//! Round-trip contract, in decreasing order of strength:
//!
//! 1. A file written by Jupyter's own writer reloads and rewrites **byte for
//!    byte identical**.
//! 2. Fields this build does not model (JupyterLab metadata, Colab metadata,
//!    unknown cell or output types) survive a load/save cycle untouched.
//! 3. A file in a non-canonical shape (source as one string instead of a line
//!    array, keys unsorted) is normalized to canonical form on save, which is
//!    exactly what Jupyter does to it too.

pub mod cell;
pub mod json;
pub mod output;

pub use cell::{Cell, CellType, CodeCell, TextCell, new_cell_id};
pub use output::{
    DisplayDataOutput, ErrorOutput, ExecuteResultOutput, MimeBundle, Output, StreamName,
    StreamOutput,
};

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::error::{Error, Result};

/// The nbformat major version this build reads and writes.
pub const NBFORMAT_MAJOR: i64 = 4;
/// The nbformat minor version new notebooks are created at.
pub const NBFORMAT_MINOR: i64 = 5;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notebook {
    #[serde(default)]
    pub cells: Vec<Cell>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    pub nbformat: i64,
    pub nbformat_minor: i64,
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

impl Default for Notebook {
    fn default() -> Self {
        Notebook {
            cells: Vec::new(),
            metadata: Map::new(),
            nbformat: NBFORMAT_MAJOR,
            nbformat_minor: NBFORMAT_MINOR,
            extra: Map::new(),
        }
    }
}

impl Notebook {
    /// A new empty notebook wired to a kernel.
    ///
    /// `kernelspec.name` is what `nb kernel start` resolves later, and
    /// `language_info.name` is what the TUI highlights with, so both are
    /// written even though only the former is strictly required.
    pub fn new(kernel_name: &str, display_name: &str, language: &str) -> Self {
        let mut metadata = Map::new();
        metadata.insert(
            "kernelspec".into(),
            json!({
                "display_name": display_name,
                "language": language,
                "name": kernel_name,
            }),
        );
        metadata.insert("language_info".into(), json!({ "name": language }));
        Notebook {
            metadata,
            ..Default::default()
        }
    }

    // ---- io -------------------------------------------------------------

    pub fn from_json_str(text: &str, path_for_errors: &str) -> Result<Self> {
        let value = json::parse_to_value(text).map_err(|e| Error::NotebookParse {
            path: path_for_errors.to_string(),
            detail: match non_json_token(text) {
                Some(token) => format!(
                    "{e}; the file contains a bare `{token}`, which Python writes but JSON does \
                     not allow. Re-saving it from Jupyter with `allow_nan=False`, or replacing \
                     the value with null, makes it readable"
                ),
                None => e.to_string(),
            },
        })?;
        let nb: Notebook = serde_json::from_value(value).map_err(|e| Error::NotebookParse {
            path: path_for_errors.to_string(),
            detail: e.to_string(),
        })?;
        // Only the major version is a compatibility barrier. A future *minor*
        // adds fields, and this build preserves the fields it does not model,
        // so refusing one would reject notebooks that Jupyter itself opens and
        // that would survive a round trip here intact.
        if nb.nbformat != NBFORMAT_MAJOR || nb.nbformat_minor < 0 {
            return Err(Error::UnsupportedNbformat {
                path: path_for_errors.to_string(),
                major: nb.nbformat,
                minor: nb.nbformat_minor,
            });
        }
        Ok(nb)
    }

    pub fn to_json_string(&self) -> Result<String> {
        let value = serde_json::to_value(self).map_err(Error::internal)?;
        Ok(json::to_canonical_string(value))
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| Error::io(path.display(), e))?;
        Self::from_json_str(&text, &path.display().to_string())
    }

    /// Write atomically: temp file in the same directory, fsync, rename.
    ///
    /// Same-directory temp matters because `rename` is only atomic within a
    /// filesystem. A crash mid-write must never leave a human's notebook
    /// truncated, which is what a plain `File::create` would risk.
    pub fn write(&self, path: impl AsRef<Path>) -> Result<()> {
        use std::io::Write;

        let path = path.as_ref();
        let text = self.to_json_string()?;
        let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
        let dir: PathBuf = dir
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        std::fs::create_dir_all(&dir).map_err(|e| Error::io(dir.display(), e))?;

        // The temp name carries a counter as well as the pid: two saves of one
        // notebook from the same process (an autosave racing an explicit save)
        // would otherwise share a temp path, and the second `create` would
        // truncate what the first was still writing.
        static WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let ticket = WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = dir.join(format!(
            ".{}.{}.{ticket}.tmp",
            path.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "notebook".into()),
            std::process::id()
        ));

        let mut file = std::fs::File::create(&tmp).map_err(|e| Error::io(tmp.display(), e))?;
        file.write_all(text.as_bytes())
            .map_err(|e| Error::io(tmp.display(), e))?;
        file.sync_all().map_err(|e| Error::io(tmp.display(), e))?;
        drop(file);

        std::fs::rename(&tmp, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            Error::io(path.display(), e)
        })?;

        // Fsync the directory so the rename itself is durable, not just the
        // file contents. Best-effort: some filesystems reject O_RDONLY fsync
        // on a directory, and failing the save over that would be worse than
        // the lost durability guarantee.
        if let Ok(d) = std::fs::File::open(&dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    // ---- normalization --------------------------------------------------

    /// Bring the document to a valid nbformat 4.5 state.
    ///
    /// Real-world files violate the id rules routinely (missing ids from 4.4,
    /// duplicated ids from careless copy-paste tooling), and a duplicate id
    /// silently breaks any id-addressed operation, so this runs on load.
    /// Returns the number of ids assigned or rewritten.
    pub fn normalize(&mut self) -> usize {
        if self.nbformat_minor < 5 {
            self.nbformat_minor = NBFORMAT_MINOR;
        }
        let mut seen: HashSet<String> = HashSet::new();
        let mut fixed = 0;
        for cell in &mut self.cells {
            let needs = match cell.id() {
                None => true,
                Some(id) => id.is_empty() || id.len() > 64 || seen.contains(id),
            };
            if needs {
                let mut id = new_cell_id();
                while seen.contains(&id) {
                    id = new_cell_id();
                }
                seen.insert(id.clone());
                cell.set_id(id);
                fixed += 1;
            } else if let Some(id) = cell.id() {
                seen.insert(id.to_string());
            }
        }
        fixed
    }

    // ---- accessors ------------------------------------------------------

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn kernel_name(&self) -> Option<&str> {
        self.metadata.get("kernelspec")?.get("name")?.as_str()
    }

    pub fn language(&self) -> Option<&str> {
        self.metadata
            .get("language_info")
            .and_then(|l| l.get("name"))
            .and_then(Value::as_str)
            .or_else(|| {
                self.metadata
                    .get("kernelspec")
                    .and_then(|k| k.get("language"))
                    .and_then(Value::as_str)
            })
    }

    pub fn set_kernelspec(&mut self, name: &str, display_name: &str, language: &str) {
        self.metadata.insert(
            "kernelspec".into(),
            json!({"display_name": display_name, "language": language, "name": name}),
        );
        self.metadata
            .entry("language_info")
            .or_insert_with(|| json!({"name": language}));
    }

    pub fn get(&self, index: usize) -> Result<&Cell> {
        self.cells.get(index).ok_or(Error::CellIndexOutOfRange {
            index,
            len: self.cells.len(),
        })
    }

    pub fn get_mut(&mut self, index: usize) -> Result<&mut Cell> {
        let len = self.cells.len();
        self.cells
            .get_mut(index)
            .ok_or(Error::CellIndexOutOfRange { index, len })
    }

    pub fn index_of_id(&self, id: &str) -> Option<usize> {
        self.cells.iter().position(|c| c.id() == Some(id))
    }

    /// Resolve a cell reference that is either a numeric index or a cell id.
    ///
    /// Numeric-looking ids are possible in principle, so an all-digit
    /// reference is tried as an index first and falls back to an id lookup,
    /// which is the behaviour that surprises least.
    pub fn resolve(&self, reference: &str) -> Result<usize> {
        if let Ok(i) = reference.parse::<usize>()
            && i < self.cells.len()
        {
            return Ok(i);
        }
        self.index_of_id(reference)
            .ok_or_else(|| match reference.parse::<usize>() {
                Ok(i) => Error::CellIndexOutOfRange {
                    index: i,
                    len: self.cells.len(),
                },
                Err(_) => Error::CellIdNotFound {
                    id: reference.to_string(),
                },
            })
    }

    // ---- editing --------------------------------------------------------

    /// Append a cell and return its index.
    pub fn push(&mut self, cell: Cell) -> usize {
        self.cells.push(cell);
        self.cells.len() - 1
    }

    /// Insert at `index`, clamped to the end.
    pub fn insert(&mut self, index: usize, cell: Cell) -> usize {
        let index = index.min(self.cells.len());
        self.cells.insert(index, cell);
        index
    }

    pub fn remove(&mut self, index: usize) -> Result<Cell> {
        if index >= self.cells.len() {
            return Err(Error::CellIndexOutOfRange {
                index,
                len: self.cells.len(),
            });
        }
        Ok(self.cells.remove(index))
    }

    /// Move the cell at `from` to `to`, shifting the rest.
    pub fn move_cell(&mut self, from: usize, to: usize) -> Result<()> {
        let cell = self.remove(from)?;
        let to = to.min(self.cells.len());
        self.cells.insert(to, cell);
        Ok(())
    }

    /// Clear every code cell's outputs and execution counts.
    pub fn clear_all_outputs(&mut self) {
        for cell in &mut self.cells {
            cell.clear_outputs();
        }
    }

    /// Highest execution count present, which is where a resumed session
    /// continues numbering from.
    pub fn max_execution_count(&self) -> i64 {
        self.cells
            .iter()
            .filter_map(|c| c.as_code().and_then(|c| c.execution_count))
            .max()
            .unwrap_or(0)
    }
}

/// A bare `NaN` or `Infinity` in `text`, outside any string literal.
///
/// Python's `json.dumps` writes these by default and nbformat does not turn
/// that off, so they do occur in files Jupyter wrote and can reopen. JSON has
/// no syntax for them, so serde rejects the whole document with "expected
/// value", which says nothing about what is wrong or what would fix it. This
/// finds the token so the error can.
fn non_json_token(text: &str) -> Option<&'static str> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        let byte = bytes[i];
        if in_string {
            match byte {
                b'\\' => i += 2,
                b'"' => {
                    in_string = false;
                    i += 1;
                }
                _ => i += 1,
            }
            continue;
        }
        if byte == b'"' {
            in_string = true;
            i += 1;
            continue;
        }
        // Only at the start of a token: a key or an identifier cannot appear
        // here unquoted anyway, and this keeps `Infinity` inside a longer run
        // of letters from matching.
        for token in ["NaN", "Infinity"] {
            if bytes[i..].starts_with(token.as_bytes())
                && !bytes
                    .get(i + token.len())
                    .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
            {
                return Some(token);
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_future_minor_version_is_readable() {
        // nbformat treats a minor bump as read-compatible, and this build
        // preserves fields it does not model, so refusing one would reject
        // notebooks that Jupyter opens and that round-trip here intact.
        let text = r#"{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 9}"#;
        let nb = Notebook::from_json_str(text, "t.ipynb").expect("a 4.9 notebook should open");
        assert_eq!(nb.nbformat_minor, 9);
        // A different major is still refused: that is a real barrier.
        let text = r#"{"cells": [], "metadata": {}, "nbformat": 3, "nbformat_minor": 0}"#;
        assert!(Notebook::from_json_str(text, "t.ipynb").is_err());
    }

    #[test]
    fn a_bare_nan_is_explained_rather_than_just_rejected() {
        // Python writes these by default, so they occur in real files; the
        // bare serde message ("expected value") says nothing useful.
        let text = r#"{"cells": [], "metadata": {"x": NaN}, "nbformat": 4, "nbformat_minor": 5}"#;
        let error = Notebook::from_json_str(text, "t.ipynb").unwrap_err();
        let message = error.to_string();
        assert!(message.contains("NaN"), "unhelpful: {message}");

        // A `NaN` inside a string is data, not a syntax error, and must not
        // be blamed for an unrelated parse failure.
        assert_eq!(non_json_token(r#"{"a": "NaN"}"#), None);
        assert_eq!(non_json_token(r#"{"a": "NaNo\""}"#), None);
        assert_eq!(non_json_token(r#"{"a": Infinity}"#), Some("Infinity"));
    }

    /// What `nbformat.write` produces for a two-cell notebook. Written out in
    /// full rather than generated, so the test fails if our writer drifts.
    const CANONICAL: &str = r##"{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "m1",
   "metadata": {},
   "source": [
    "# Title\n",
    "text"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "c1",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "hello\n"
     ]
    }
   ],
   "source": [
    "print(\"hello\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
"##;

    #[test]
    fn canonical_notebooks_rewrite_byte_identically() {
        let nb = Notebook::from_json_str(CANONICAL, "t.ipynb").unwrap();
        assert_eq!(nb.to_json_string().unwrap(), CANONICAL);
    }

    #[test]
    fn in_memory_source_is_joined() {
        let nb = Notebook::from_json_str(CANONICAL, "t.ipynb").unwrap();
        assert_eq!(nb.cells[0].source(), "# Title\ntext");
        assert_eq!(nb.cells[1].source(), "print(\"hello\")");
        match &nb.cells[1].outputs()[0] {
            Output::Stream(s) => assert_eq!(s.text, "hello\n"),
            other => panic!("expected stream, got {other:?}"),
        }
    }

    #[test]
    fn unmodelled_notebook_metadata_survives() {
        let src = r#"{
 "cells": [],
 "metadata": {
  "colab": {
   "provenance": []
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5,
 "some_top_level_extension": 7
}
"#;
        let nb = Notebook::from_json_str(src, "t.ipynb").unwrap();
        assert_eq!(nb.to_json_string().unwrap(), src);
    }

    #[test]
    fn nbformat_3_is_rejected_with_an_actionable_error() {
        let src = r#"{"cells": [], "metadata": {}, "nbformat": 3, "nbformat_minor": 0}"#;
        let err = Notebook::from_json_str(src, "old.ipynb").unwrap_err();
        assert_eq!(err.code(), "unsupported_nbformat");
        assert!(err.hint().is_some());
    }

    #[test]
    fn normalize_assigns_missing_and_deduplicates_repeated_ids() {
        let src = r#"{
 "cells": [
  {"cell_type": "code", "execution_count": null, "metadata": {}, "outputs": [], "source": "a"},
  {"cell_type": "code", "execution_count": null, "id": "dup", "metadata": {}, "outputs": [], "source": "b"},
  {"cell_type": "code", "execution_count": null, "id": "dup", "metadata": {}, "outputs": [], "source": "c"}
 ],
 "metadata": {},
 "nbformat": 4,
 "nbformat_minor": 5
}
"#;
        let mut nb = Notebook::from_json_str(src, "t.ipynb").unwrap();
        assert_eq!(nb.normalize(), 2);
        let ids: Vec<&str> = nb.cells.iter().filter_map(|c| c.id()).collect();
        assert_eq!(ids.len(), 3);
        let unique: HashSet<&&str> = ids.iter().collect();
        assert_eq!(unique.len(), 3, "ids still collide: {ids:?}");
    }

    #[test]
    fn resolve_accepts_index_or_id() {
        let mut nb = Notebook::new("python3", "Python 3", "python");
        nb.push(Cell::new_code("a"));
        nb.push(Cell::new_code("b"));
        let id = nb.cells[1].id().unwrap().to_string();
        assert_eq!(nb.resolve("1").unwrap(), 1);
        assert_eq!(nb.resolve(&id).unwrap(), 1);
        assert_eq!(nb.resolve("nope").unwrap_err().code(), "cell_id_not_found");
        assert_eq!(
            nb.resolve("9").unwrap_err().code(),
            "cell_index_out_of_range"
        );
    }

    #[test]
    fn write_is_atomic_and_leaves_no_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nb.ipynb");
        let mut nb = Notebook::new("python3", "Python 3", "python");
        nb.push(Cell::new_code("1 + 1"));
        nb.write(&path).unwrap();

        let reloaded = Notebook::read(&path).unwrap();
        assert_eq!(reloaded, nb);

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n != "nb.ipynb")
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
    }

    #[test]
    fn write_replaces_rather_than_truncates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nb.ipynb");
        let mut nb = Notebook::new("python3", "Python 3", "python");
        nb.push(Cell::new_code("first"));
        nb.write(&path).unwrap();
        nb.cells.clear();
        nb.push(Cell::new_code("second"));
        nb.write(&path).unwrap();
        assert_eq!(Notebook::read(&path).unwrap().cells[0].source(), "second");
    }

    #[test]
    fn editing_operations_keep_the_cell_list_consistent() {
        let mut nb = Notebook::new("python3", "Python 3", "python");
        nb.push(Cell::new_code("a"));
        nb.push(Cell::new_code("b"));
        nb.insert(1, Cell::new_code("mid"));
        assert_eq!(
            nb.cells.iter().map(Cell::source).collect::<Vec<_>>(),
            ["a", "mid", "b"]
        );
        nb.move_cell(2, 0).unwrap();
        assert_eq!(
            nb.cells.iter().map(Cell::source).collect::<Vec<_>>(),
            ["b", "a", "mid"]
        );
        nb.remove(1).unwrap();
        assert_eq!(
            nb.cells.iter().map(Cell::source).collect::<Vec<_>>(),
            ["b", "mid"]
        );
        assert_eq!(nb.remove(9).unwrap_err().code(), "cell_index_out_of_range");
    }
}
