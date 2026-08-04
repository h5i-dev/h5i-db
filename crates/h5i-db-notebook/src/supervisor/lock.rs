//! Who owns a notebook's supervisor.
//!
//! One file per session, locked for the owner's whole lifetime. Every question
//! about whether a session is alive is answered by whether this lock can be
//! taken, and by nothing else: `ls` uses it to tell a crashed supervisor's
//! leftovers from a live one's socket, and a starting supervisor uses it to
//! refuse to become a second writer to one notebook.

use std::path::Path;
use std::time::Duration;

use crate::error::{Error, Result};

/// Exclusive claim on one notebook's supervisor role.
///
/// An advisory `flock` rather than a pid file: the kernel drops it when the
/// process dies however it dies, so a supervisor that is SIGKILLed or OOM-
/// killed leaves nothing stale behind. A pid file would need a liveness check,
/// and checking a recycled pid is exactly the mistake that lets one supervisor
/// declare another one dead.
pub struct SupervisorLock {
    /// Held, not read: dropping the file is what releases the lock.
    #[allow(dead_code)]
    file: std::fs::File,
}

impl SupervisorLock {
    /// Claim the lock, waiting up to `limit` for the current holder to exit.
    ///
    /// Polled rather than blocking on `flock`, so the wait has a bound: a
    /// holder that is wedged rather than exiting must not turn every later
    /// start into a hang.
    pub async fn acquire_within(path: &Path, limit: Duration) -> Result<Option<Self>> {
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            if let Some(held) = Self::try_acquire(path)? {
                return Ok(Some(held));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Claim the lock, or return `None` if another process holds it.
    pub fn try_acquire(path: &Path) -> Result<Option<Self>> {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::io::AsRawFd;

        // The lock lives in the session directory, which an explicit
        // `--socket` elsewhere would not have created.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.display(), e))?;
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .map_err(|e| Error::io(path.display(), e))?;

        // SAFETY: `flock` on a descriptor we own. LOCK_NB makes it answer
        // rather than wait, which is what turns "somebody else is serving
        // this notebook" into a decision instead of a hang.
        let taken = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if taken != 0 {
            let error = std::io::Error::last_os_error();
            return match error.kind() {
                std::io::ErrorKind::WouldBlock => Ok(None),
                _ => Err(Error::io(path.display(), error)),
            };
        }

        // Recorded for whoever has to work out which process is holding a
        // session; nothing reads it back, because the lock itself is the
        // authority on liveness.
        use std::io::Write;
        let mut file = file;
        let _ = file.set_len(0);
        let _ = writeln!(file, "{}", std::process::id());
        let _ = file.flush();
        Ok(Some(SupervisorLock { file }))
    }

    /// Whether nobody owns the session this lock file belongs to.
    ///
    /// Taking the lock and letting it go is the test: a lock we could take is
    /// a lock nobody was holding. There is no window to worry about, because a
    /// supervisor that starts afterwards takes it before it touches anything.
    pub fn is_unowned(path: &Path) -> bool {
        matches!(Self::try_acquire(path), Ok(Some(_)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_lock_is_refused_and_a_free_one_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.lock");

        let held = SupervisorLock::try_acquire(&path)
            .unwrap()
            .expect("a fresh lock should be free");
        // Same process, second descriptor: flock is per open file description,
        // so this is a real second claim rather than a re-entrant one.
        assert!(
            SupervisorLock::try_acquire(&path).unwrap().is_none(),
            "a held lock was handed out twice"
        );
        assert!(!SupervisorLock::is_unowned(&path));

        drop(held);
        assert!(
            SupervisorLock::is_unowned(&path),
            "the lock outlived its holder"
        );
    }

    #[test]
    fn the_holders_pid_is_recorded_for_whoever_has_to_look() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.lock");
        let _held = SupervisorLock::try_acquire(&path).unwrap().unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(written.trim(), std::process::id().to_string());
    }
}
