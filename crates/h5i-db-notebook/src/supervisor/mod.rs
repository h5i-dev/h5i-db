//! Session persistence across CLI invocations.
//!
//! `h5i-nb exec` is a fresh process every time an agent calls it, so something
//! has to hold the kernel in between. That something is a supervisor: a
//! detached process owning one notebook's [`Session`](crate::Session), reachable
//! over a unix socket, auto-started on first use and idling out after a TTL.
//!
//! The simpler alternative, reconnecting to the kernel's connection file per
//! invocation the way `jupyter console --existing` does, was rejected for one
//! reason: ZeroMQ's PUB sockets drop messages when no subscriber is attached,
//! so any output produced while no client was connected is gone for good. That
//! breaks detached execution, breaks recovery from an interrupted call, and
//! loses exactly the long-running cell output that matters most. A supervisor
//! subscribed to iopub continuously is what makes those cases work.

pub mod client;
pub mod protocol;
pub mod server;

pub use client::{SessionClient, set_command_prefix};
pub use protocol::{Request, Response, SessionInfo};

use std::path::{Path, PathBuf};

/// Per-user directory holding session sockets and registry files.
///
/// `XDG_RUNTIME_DIR` is preferred because it is user-private (0700) and wiped
/// on logout, which is what we want for a socket that grants code execution.
pub fn session_dir() -> PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            // Fall back to a uid-qualified temp path so two users on one host
            // cannot collide, and one cannot pre-create the other's directory.
            std::env::temp_dir().join(format!("h5i-db-{}", current_uid()))
        });
    base.join("h5i-db/nb")
}

fn current_uid() -> u32 {
    // SAFETY: getuid is always safe and cannot fail.
    unsafe { libc::getuid() }
}

/// Stable, filesystem-safe identifier for a notebook path.
///
/// Derived from the canonical path so that `./nb.ipynb` and `/abs/nb.ipynb`
/// reach the same session, and hashed so the socket name stays short enough
/// for the 108-byte `sun_path` limit regardless of how deep the notebook is.
pub fn session_id(notebook: &Path) -> String {
    let canonical = notebook
        .canonicalize()
        .unwrap_or_else(|_| absolutize(notebook));
    let digest = blake3_hex(canonical.to_string_lossy().as_bytes());
    let stem = notebook
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "notebook".into());
    let stem: String = stem
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(24)
        .collect();
    format!("{stem}-{}", &digest[..16])
}

/// `canonicalize` fails for a path that does not exist yet; this still gives a
/// stable absolute form so a session id can be computed before creation.
fn absolutize(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

pub fn socket_path(notebook: &Path) -> PathBuf {
    session_dir().join(format!("{}.sock", session_id(notebook)))
}

/// Lock file whose holder owns this notebook's supervisor.
///
/// Kept beside the socket rather than derived from it, so a caller that passes
/// `--socket` explicitly still gets one supervisor per notebook: the notebook
/// is what a session is about, and two supervisors on two sockets would still
/// be two kernels writing one file.
pub fn lock_path(notebook: &Path) -> PathBuf {
    session_dir().join(format!("{}.lock", session_id(notebook)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ids_are_stable_and_path_shaped() {
        let a = session_id(Path::new("/tmp/research.ipynb"));
        let b = session_id(Path::new("/tmp/research.ipynb"));
        assert_eq!(a, b);
        assert!(a.starts_with("research-"), "{a}");
        assert!(
            a.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "{a} is not filesystem-safe"
        );
    }

    #[test]
    fn different_notebooks_get_different_sessions() {
        assert_ne!(
            session_id(Path::new("/tmp/a.ipynb")),
            session_id(Path::new("/tmp/b.ipynb"))
        );
        // Same file name in different directories must not share a kernel.
        assert_ne!(
            session_id(Path::new("/tmp/one/nb.ipynb")),
            session_id(Path::new("/tmp/two/nb.ipynb"))
        );
    }

    #[test]
    fn relative_and_absolute_paths_reach_the_same_session() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nb.ipynb");
        std::fs::write(&path, "{}").unwrap();
        let previous = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let relative = session_id(Path::new("nb.ipynb"));
        std::env::set_current_dir(&previous).unwrap();
        assert_eq!(relative, session_id(&path));
    }

    #[test]
    fn socket_paths_fit_the_unix_socket_length_limit() {
        // sun_path is 108 bytes; a deep notebook path must not overflow it,
        // which is the reason the id is hashed rather than escaped.
        let deep = PathBuf::from(format!("/{}/deep.ipynb", "nested/".repeat(40)));
        let socket = socket_path(&deep);
        assert!(
            socket.as_os_str().len() < 100,
            "socket path too long ({}): {}",
            socket.as_os_str().len(),
            socket.display()
        );
    }
}
