//! Kernelspec discovery.
//!
//! We implement the search rather than delegating it, because the obvious
//! delegation targets are both incomplete for the case that matters most here:
//! an agent in a fresh container with a project virtualenv and no `jupyter` on
//! `PATH`. `jupyter --paths` cannot run, and `jupyter-zmq-client`'s
//! `data_dirs()` reads `JUPYTER_PATH` as a single path (it is `PATH`-style,
//! separator-delimited) and carries an explicit TODO for the `sys.prefix`
//! directory that every venv install writes into.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{Error, Result};

/// How to interrupt a running cell, from the spec's `interrupt_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum InterruptMode {
    /// SIGINT to the kernel's process group. Jupyter's default when the key is
    /// absent, and what ipykernel uses.
    #[default]
    Signal,
    /// `interrupt_request` on the control channel, for kernels that cannot
    /// take a signal (Windows-native kernels, some remote ones).
    Message,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelSpecInfo {
    /// Directory name under `kernels/`, which is what the notebook's
    /// `metadata.kernelspec.name` refers to.
    pub name: String,
    pub display_name: String,
    pub language: String,
    /// The `kernel.json` this came from, so errors can point at a file.
    pub path: PathBuf,
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub interrupt_mode: InterruptMode,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

/// `kernel.json` as written on disk.
#[derive(Debug, Deserialize)]
struct KernelJson {
    #[serde(default)]
    argv: Vec<String>,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    interrupt_mode: Option<String>,
    #[serde(default)]
    metadata: Map<String, Value>,
}

impl KernelSpecInfo {
    fn from_json(name: &str, path: &Path, raw: KernelJson) -> Self {
        KernelSpecInfo {
            name: name.to_string(),
            display_name: if raw.display_name.is_empty() {
                name.to_string()
            } else {
                raw.display_name
            },
            language: raw.language,
            path: path.to_path_buf(),
            argv: raw.argv,
            env: raw.env,
            interrupt_mode: match raw.interrupt_mode.as_deref() {
                Some("message") => InterruptMode::Message,
                _ => InterruptMode::Signal,
            },
            metadata: raw.metadata,
        }
    }
}

/// Load one `kernel.json` directly, bypassing the search path.
///
/// Used when the caller already knows where the spec is (a pinned test
/// kernel, a `--kernel-json` flag), and by `discover` for each hit.
pub fn load(kernel_json: impl AsRef<Path>) -> Result<KernelSpecInfo> {
    let path = kernel_json.as_ref();
    let text = std::fs::read_to_string(path).map_err(|e| Error::io(path.display(), e))?;
    let raw: KernelJson = serde_json::from_str(&text)
        .map_err(|e| Error::invalid(format!("{}: not a valid kernel.json: {e}", path.display())))?;
    if raw.argv.is_empty() {
        return Err(Error::invalid(format!(
            "{}: kernel.json has an empty argv, so there is nothing to launch",
            path.display()
        )));
    }
    let name = path
        .parent()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "kernel".to_string());
    Ok(KernelSpecInfo::from_json(&name, path, raw))
}

/// Jupyter's data directories, highest precedence first.
///
/// Mirrors `jupyter_core.paths.jupyter_path()`: `JUPYTER_PATH`, then the user
/// directory, then the environment (venv/conda) prefix, then system paths.
pub fn data_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut push = |p: PathBuf| {
        if !dirs.contains(&p) {
            dirs.push(p);
        }
    };

    if let Ok(raw) = std::env::var("JUPYTER_PATH") {
        for entry in std::env::split_paths(&raw) {
            if !entry.as_os_str().is_empty() {
                push(entry);
            }
        }
    }

    if let Ok(dir) = std::env::var("JUPYTER_DATA_DIR") {
        push(PathBuf::from(dir));
    } else if let Some(home) = home_dir() {
        // macOS puts it under Library, every other unix under XDG.
        if cfg!(target_os = "macos") {
            push(home.join("Library/Jupyter"));
        } else if let Ok(xdg) = std::env::var("XDG_DATA_HOME").filter_non_empty() {
            push(PathBuf::from(xdg).join("jupyter"));
        } else {
            push(home.join(".local/share/jupyter"));
        }
    }

    for prefix in env_prefixes() {
        push(prefix.join("share/jupyter"));
    }

    push(PathBuf::from("/usr/local/share/jupyter"));
    push(PathBuf::from("/usr/share/jupyter"));

    dirs
}

/// Candidate `sys.prefix` values, without paying for a Python subprocess.
///
/// An activated venv exports `VIRTUAL_ENV`; conda exports `CONDA_PREFIX`. When
/// neither is set we derive the prefix from whichever `python3` is on `PATH`,
/// which covers the common case of a venv's `bin` being prepended to `PATH`
/// without the variable being exported.
fn env_prefixes() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for var in ["VIRTUAL_ENV", "CONDA_PREFIX"] {
        if let Ok(p) = std::env::var(var).filter_non_empty() {
            out.push(PathBuf::from(p));
        }
    }
    if let Some(python) = which("python3").or_else(|| which("python"))
        && let Some(prefix) = python.parent().and_then(Path::parent)
    {
        let prefix = prefix.to_path_buf();
        if !out.contains(&prefix) {
            out.push(prefix);
        }
    }
    out
}

fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME")
        .filter_non_empty()
        .ok()
        .map(PathBuf::from)
}

fn which(program: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(program))
        .find(|candidate| candidate.is_file())
}

/// Every kernelspec on this machine, highest precedence first.
///
/// A name found in an earlier directory shadows the same name later, which is
/// how Jupyter resolves a venv kernel that shares a name with a system one.
pub fn discover() -> Vec<KernelSpecInfo> {
    let mut found: Vec<KernelSpecInfo> = Vec::new();
    for dir in data_dirs() {
        let kernels = dir.join("kernels");
        let Ok(entries) = std::fs::read_dir(&kernels) else {
            continue;
        };
        let mut names: Vec<(String, PathBuf)> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .map(|e| (e.file_name().to_string_lossy().to_string(), e.path()))
            .collect();
        // read_dir order is filesystem-dependent; sort so `nb kernel list` is
        // stable between runs.
        names.sort_by(|a, b| a.0.cmp(&b.0));

        for (name, dir) in names {
            if found.iter().any(|k| k.name == name) {
                continue;
            }
            let json_path = dir.join("kernel.json");
            let Ok(text) = std::fs::read_to_string(&json_path) else {
                continue;
            };
            let Ok(raw) = serde_json::from_str::<KernelJson>(&text) else {
                // A malformed kernel.json shadows nothing: skip it and keep
                // looking, the way Jupyter does.
                tracing::debug!(path = %json_path.display(), "skipping unparseable kernel.json");
                continue;
            };
            if raw.argv.is_empty() {
                tracing::debug!(path = %json_path.display(), "skipping kernelspec with empty argv");
                continue;
            }
            found.push(KernelSpecInfo::from_json(&name, &json_path, raw));
        }
    }
    found
}

/// Resolve a kernelspec by name, erroring with what *is* available.
///
/// The listing goes in the error message because the consumer replans from the
/// text: an agent told only "not found" retries the same name.
pub fn find(name: &str) -> Result<KernelSpecInfo> {
    let available = discover();
    if let Some(spec) = available.iter().find(|k| k.name == name) {
        return Ok(spec.clone());
    }
    // Kernel names are conventionally lowercase, but display names are not,
    // and people pass what they saw.
    if let Some(spec) = available
        .iter()
        .find(|k| k.name.eq_ignore_ascii_case(name) || k.display_name == name)
    {
        return Ok(spec.clone());
    }
    let listed = if available.is_empty() {
        String::new()
    } else {
        format!(
            "; available: {}",
            available
                .iter()
                .map(|k| k.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    Err(Error::KernelNotFound {
        name: name.to_string(),
        listed,
    })
}

/// Where connection files live, matching `jupyter_core.paths.jupyter_runtime_dir`
/// so `jupyter console --existing` can attach to a kernel we started.
pub fn runtime_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("JUPYTER_RUNTIME_DIR").filter_non_empty() {
        return PathBuf::from(dir);
    }
    if let Ok(dir) = std::env::var("JUPYTER_DATA_DIR").filter_non_empty() {
        return PathBuf::from(dir).join("runtime");
    }
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR").filter_non_empty() {
        return PathBuf::from(dir).join("jupyter");
    }
    match home_dir() {
        Some(home) if cfg!(target_os = "macos") => home.join("Library/Jupyter/runtime"),
        Some(home) => home.join(".local/share/jupyter/runtime"),
        None => std::env::temp_dir().join("jupyter-runtime"),
    }
}

/// `std::env::var` returning `Err` for an empty value.
///
/// An exported-but-empty `XDG_DATA_HOME` is common in minimal containers and
/// would otherwise resolve to a path of `"/jupyter"`.
trait FilterNonEmpty {
    fn filter_non_empty(self) -> std::result::Result<String, std::env::VarError>;
}

impl FilterNonEmpty for std::result::Result<String, std::env::VarError> {
    fn filter_non_empty(self) -> std::result::Result<String, std::env::VarError> {
        match self {
            Ok(v) if v.is_empty() => Err(std::env::VarError::NotPresent),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupt_mode_defaults_to_signal_when_absent() {
        // ipykernel's own kernel.json omits the key, so getting this default
        // wrong breaks Ctrl-C for the most common kernel there is.
        let raw: KernelJson = serde_json::from_str(
            r#"{"argv": ["python"], "display_name": "P", "language": "python"}"#,
        )
        .unwrap();
        let spec = KernelSpecInfo::from_json("python3", Path::new("/x/kernel.json"), raw);
        assert_eq!(spec.interrupt_mode, InterruptMode::Signal);
    }

    #[test]
    fn interrupt_mode_message_is_read() {
        let raw: KernelJson = serde_json::from_str(
            r#"{"argv": ["k"], "display_name": "K", "language": "l", "interrupt_mode": "message"}"#,
        )
        .unwrap();
        let spec = KernelSpecInfo::from_json("k", Path::new("/x/kernel.json"), raw);
        assert_eq!(spec.interrupt_mode, InterruptMode::Message);
    }

    #[test]
    fn display_name_falls_back_to_the_directory_name() {
        let raw: KernelJson = serde_json::from_str(r#"{"argv": ["k"]}"#).unwrap();
        let spec = KernelSpecInfo::from_json("mykernel", Path::new("/x/kernel.json"), raw);
        assert_eq!(spec.display_name, "mykernel");
    }

    #[test]
    fn unmodelled_kernel_json_keys_do_not_break_parsing() {
        let raw: KernelJson = serde_json::from_str(
            r#"{"argv": ["k"], "display_name": "K", "language": "l",
                "kernel_protocol_version": "5.5", "future": {"x": 1}}"#,
        )
        .unwrap();
        assert_eq!(raw.argv, ["k"]);
    }

    #[test]
    fn jupyter_path_is_split_on_the_platform_separator() {
        // The bug this guards: treating JUPYTER_PATH as one path silently
        // hides every kernel after the first entry.
        let sep = if cfg!(windows) { ";" } else { ":" };
        let value = format!("/tmp/one{sep}/tmp/two");
        // SAFETY: single-threaded test, and the variable is read back below.
        unsafe { std::env::set_var("JUPYTER_PATH", &value) };
        let dirs = data_dirs();
        unsafe { std::env::remove_var("JUPYTER_PATH") };
        assert!(dirs.contains(&PathBuf::from("/tmp/one")), "{dirs:?}");
        assert!(dirs.contains(&PathBuf::from("/tmp/two")), "{dirs:?}");
    }

    #[test]
    fn data_dirs_are_deduplicated_and_ordered() {
        let dirs = data_dirs();
        let mut seen = std::collections::HashSet::new();
        for d in &dirs {
            assert!(seen.insert(d.clone()), "duplicate search path {d:?}");
        }
        let system = dirs
            .iter()
            .position(|d| d == Path::new("/usr/share/jupyter"));
        if let (Some(sys), Some(user)) = (
            system,
            dirs.iter()
                .position(|d| d.ends_with(".local/share/jupyter")),
        ) {
            assert!(user < sys, "user dir must outrank system dirs: {dirs:?}");
        }
    }

    #[test]
    fn missing_kernel_lists_what_is_available() {
        let err = find("definitely-not-installed-kernel").unwrap_err();
        assert_eq!(err.code(), "kernel_not_found");
        assert!(err.hint().is_some());
        assert!(!err.retryable(), "installing a kernel is not a retry");
    }

    #[test]
    fn discovery_is_stable_across_calls() {
        let a: Vec<String> = discover().into_iter().map(|k| k.name).collect();
        let b: Vec<String> = discover().into_iter().map(|k| k.name).collect();
        assert_eq!(a, b, "kernel listing order is not deterministic");
    }
}
