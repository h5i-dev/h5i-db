//! Notebook cells (nbformat v4).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::document::output::Output;
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellType {
    Code,
    Markdown,
    Raw,
    Unknown,
}

impl CellType {
    pub fn as_str(self) -> &'static str {
        match self {
            CellType::Code => "code",
            CellType::Markdown => "markdown",
            CellType::Raw => "raw",
            CellType::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CodeCell {
    /// Required from nbformat 4.5; absent in older files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default)]
    pub source: String,
    /// Always serialized (JSON `null` when unset), as nbformat requires.
    pub execution_count: Option<i64>,
    #[serde(default)]
    pub outputs: Vec<Output>,
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextCell {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default)]
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Map<String, Value>>,
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A notebook cell.
///
/// `Unknown` holds any cell whose `cell_type` this build does not model, or
/// whose payload does not match its declared type, verbatim. See
/// [`Output::Unknown`](crate::document::Output) for the reasoning.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Code(CodeCell),
    Markdown(TextCell),
    Raw(TextCell),
    Unknown(Map<String, Value>),
}

impl Cell {
    pub fn new_code(source: impl Into<String>) -> Self {
        Cell::Code(CodeCell {
            id: Some(new_cell_id()),
            metadata: Map::new(),
            source: source.into(),
            execution_count: None,
            outputs: Vec::new(),
            extra: Map::new(),
        })
    }

    pub fn new_markdown(source: impl Into<String>) -> Self {
        Cell::Markdown(TextCell {
            id: Some(new_cell_id()),
            metadata: Map::new(),
            source: source.into(),
            attachments: None,
            extra: Map::new(),
        })
    }

    pub fn new_raw(source: impl Into<String>) -> Self {
        Cell::Raw(TextCell {
            id: Some(new_cell_id()),
            metadata: Map::new(),
            source: source.into(),
            attachments: None,
            extra: Map::new(),
        })
    }

    pub fn cell_type(&self) -> CellType {
        match self {
            Cell::Code(_) => CellType::Code,
            Cell::Markdown(_) => CellType::Markdown,
            Cell::Raw(_) => CellType::Raw,
            Cell::Unknown(_) => CellType::Unknown,
        }
    }

    /// The literal `cell_type` string, which for `Unknown` is whatever the
    /// file said rather than the placeholder `CellType::Unknown` name.
    pub fn cell_type_str(&self) -> &str {
        match self {
            Cell::Unknown(map) => map
                .get("cell_type")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            other => other.cell_type().as_str(),
        }
    }

    pub fn id(&self) -> Option<&str> {
        match self {
            Cell::Code(c) => c.id.as_deref(),
            Cell::Markdown(c) | Cell::Raw(c) => c.id.as_deref(),
            Cell::Unknown(m) => m.get("id").and_then(Value::as_str),
        }
    }

    pub fn set_id(&mut self, id: String) {
        match self {
            Cell::Code(c) => c.id = Some(id),
            Cell::Markdown(c) | Cell::Raw(c) => c.id = Some(id),
            Cell::Unknown(m) => {
                m.insert("id".into(), Value::String(id));
            }
        }
    }

    pub fn source(&self) -> &str {
        match self {
            Cell::Code(c) => &c.source,
            Cell::Markdown(c) | Cell::Raw(c) => &c.source,
            Cell::Unknown(m) => m.get("source").and_then(Value::as_str).unwrap_or(""),
        }
    }

    pub fn set_source(&mut self, source: impl Into<String>) {
        let source = source.into();
        match self {
            Cell::Code(c) => c.source = source,
            Cell::Markdown(c) | Cell::Raw(c) => c.source = source,
            Cell::Unknown(m) => {
                m.insert("source".into(), Value::String(source));
            }
        }
    }

    pub fn metadata(&self) -> Option<&Map<String, Value>> {
        match self {
            Cell::Code(c) => Some(&c.metadata),
            Cell::Markdown(c) | Cell::Raw(c) => Some(&c.metadata),
            Cell::Unknown(_) => None,
        }
    }

    /// Cell tags from `metadata.tags`, used by `nb run --tag`.
    pub fn tags(&self) -> Vec<&str> {
        self.metadata()
            .and_then(|m| m.get("tags"))
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default()
    }

    pub fn as_code(&self) -> Option<&CodeCell> {
        match self {
            Cell::Code(c) => Some(c),
            _ => None,
        }
    }

    pub fn as_code_mut(&mut self) -> Option<&mut CodeCell> {
        match self {
            Cell::Code(c) => Some(c),
            _ => None,
        }
    }

    pub fn outputs(&self) -> &[Output] {
        match self {
            Cell::Code(c) => &c.outputs,
            _ => &[],
        }
    }

    /// Drops outputs and the execution count. No-op on non-code cells.
    pub fn clear_outputs(&mut self) {
        if let Cell::Code(c) = self {
            c.outputs.clear();
            c.execution_count = None;
        }
    }

    fn from_map(mut map: Map<String, Value>) -> Result<Self> {
        let Some(tag) = map
            .get("cell_type")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return Ok(Cell::Unknown(map));
        };
        map.remove("cell_type");
        let rest = Value::Object(map);
        let recover = |v: Value| match v {
            Value::Object(mut m) => {
                m.insert("cell_type".into(), Value::String(tag.clone()));
                Cell::Unknown(m)
            }
            other => Cell::Unknown(
                [
                    ("cell_type".to_string(), Value::String(tag.clone())),
                    ("value".to_string(), other),
                ]
                .into_iter()
                .collect(),
            ),
        };
        Ok(match tag.as_str() {
            "code" => serde_json::from_value(rest.clone())
                .map(Cell::Code)
                .unwrap_or_else(|_| recover(rest)),
            "markdown" => serde_json::from_value(rest.clone())
                .map(Cell::Markdown)
                .unwrap_or_else(|_| recover(rest)),
            "raw" => serde_json::from_value(rest.clone())
                .map(Cell::Raw)
                .unwrap_or_else(|_| recover(rest)),
            _ => recover(rest),
        })
    }

    fn to_map(&self) -> Result<Map<String, Value>> {
        if let Cell::Unknown(map) = self {
            return Ok(map.clone());
        }
        let body = match self {
            Cell::Code(c) => serde_json::to_value(c),
            Cell::Markdown(c) | Cell::Raw(c) => serde_json::to_value(c),
            Cell::Unknown(_) => unreachable!("handled above"),
        }
        .map_err(Error::internal)?;
        let mut map = match body {
            Value::Object(m) => m,
            other => {
                return Err(Error::internal(format!(
                    "cell serialized to a non-object: {other}"
                )));
            }
        };
        map.insert(
            "cell_type".into(),
            Value::String(self.cell_type_str().to_string()),
        );
        Ok(map)
    }
}

impl Serialize for Cell {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.to_map()
            .map_err(serde::ser::Error::custom)?
            .serialize(s)
    }
}

impl<'de> Deserialize<'de> for Cell {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let map = Map::deserialize(d)?;
        Cell::from_map(map).map_err(serde::de::Error::custom)
    }
}

/// nbformat 4.5 cell id: 1-64 chars from `[a-zA-Z0-9-_]`.
///
/// Jupyter itself uses a short random string rather than a full UUID, and
/// short ids keep `nb cells` output readable, so we take the first 8
/// hex characters of a v4 UUID.
pub fn new_cell_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn code_cell_round_trips_with_the_required_keys() {
        let v = json!({
            "cell_type": "code",
            "execution_count": null,
            "id": "abc123",
            "metadata": {},
            "outputs": [],
            "source": "1 + 1"
        });
        let c: Cell = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(c.cell_type(), CellType::Code);
        assert_eq!(serde_json::to_value(&c).unwrap(), v);
    }

    #[test]
    fn markdown_cells_do_not_grow_code_only_keys() {
        let v = json!({
            "cell_type": "markdown",
            "id": "m1",
            "metadata": {},
            "source": "# hi"
        });
        let c: Cell = serde_json::from_value(v.clone()).unwrap();
        let back = serde_json::to_value(&c).unwrap();
        assert!(
            back.get("outputs").is_none(),
            "markdown grew an outputs key"
        );
        assert!(back.get("execution_count").is_none());
        assert_eq!(back, v);
    }

    #[test]
    fn unmodelled_cell_keys_survive() {
        let v = json!({
            "cell_type": "code",
            "execution_count": 3,
            "id": "abc",
            "metadata": {"collapsed": true},
            "outputs": [],
            "source": "x",
            "some_future_key": {"a": 1}
        });
        let c: Cell = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(serde_json::to_value(&c).unwrap(), v);
    }

    #[test]
    fn unknown_cell_types_survive() {
        let v = json!({"cell_type": "heading", "level": 2, "source": "Title"});
        let c: Cell = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(c.cell_type(), CellType::Unknown);
        assert_eq!(c.cell_type_str(), "heading");
        assert_eq!(c.source(), "Title");
        assert_eq!(serde_json::to_value(&c).unwrap(), v);
    }

    #[test]
    fn generated_ids_match_the_nbformat_45_charset() {
        for _ in 0..100 {
            let id = new_cell_id();
            assert!((1..=64).contains(&id.len()));
            assert!(
                id.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
                "bad id {id}"
            );
        }
    }

    #[test]
    fn tags_read_from_metadata() {
        let v = json!({
            "cell_type": "code", "execution_count": null, "id": "a",
            "metadata": {"tags": ["setup", "slow"]}, "outputs": [], "source": ""
        });
        let c: Cell = serde_json::from_value(v).unwrap();
        assert_eq!(c.tags(), vec!["setup", "slow"]);
    }
}
