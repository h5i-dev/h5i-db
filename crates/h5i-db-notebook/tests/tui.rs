//! End-to-end tests that drive the real TUI through a pty.
//!
//! The `TestBackend` tests in `src/tui/render.rs` cover what is drawn; these
//! cover everything underneath it that only exists in a real terminal: raw
//! mode, the alternate screen, the crossterm key decoder, the async event
//! loop, and terminal restoration on exit. The overlay-centring panic that
//! reached a running UI was invisible to every unit test and would have been
//! caught here.
//!
//! Ignored by default because they need a Jupyter kernel. Run with:
//!
//! ```bash
//! H5I_TEST_KERNEL_JSON=… cargo test -p h5i-db-notebook --test tui -- --ignored --test-threads=1
//! ```

#![cfg(unix)]

use std::io::{Read, Write};
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tempfile::TempDir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-nb")
}

fn test_kernel() -> Option<(String, PathBuf)> {
    let json = PathBuf::from(std::env::var("H5I_TEST_KERNEL_JSON").ok()?);
    let name = json.parent()?.file_name()?.to_string_lossy().to_string();
    let jupyter_path = json.parent()?.parent()?.parent()?.to_path_buf();
    Some((name, jupyter_path))
}

const ROWS: u16 = 30;
const COLUMNS: u16 = 100;

/// A child process attached to a pseudo-terminal.
struct Pty {
    master: std::fs::File,
    pid: libc::pid_t,
}

impl Pty {
    /// Spawn `program` with `args` on a pty sized [`ROWS`] x [`COLUMNS`].
    fn spawn(program: &str, args: &[&str], env: &[(&str, &str)], cwd: &std::path::Path) -> Pty {
        let mut master: libc::c_int = 0;
        let winsize = libc::winsize {
            ws_row: ROWS,
            ws_col: COLUMNS,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: standard forkpty use; the size is set at fork time so the
        // child never observes a zero-sized terminal, which is what makes
        // ratatui draw nothing.
        let pid = unsafe {
            libc::forkpty(
                &mut master,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &winsize as *const _ as *mut _,
            )
        };
        assert!(pid >= 0, "forkpty failed");

        if pid == 0 {
            // Child: exec the binary, and abort if that fails, because
            // returning here would resume the test harness in a fork.
            std::env::set_current_dir(cwd).unwrap();
            unsafe { std::env::set_var("TERM", "xterm-256color") };
            for (key, value) in env {
                unsafe { std::env::set_var(key, value) };
            }
            let error = std::process::Command::new(program).args(args).exec_error();
            eprintln!("exec failed: {error}");
            std::process::exit(127);
        }

        // SAFETY: forkpty hands us ownership of the master fd.
        let master = unsafe { std::fs::File::from(OwnedFd::from_raw_fd(master)) };
        Pty { master, pid }
    }

    fn send(&mut self, keys: &str) {
        self.master.write_all(keys.as_bytes()).unwrap();
        self.master.flush().unwrap();
    }

    /// Read whatever is available, for up to `duration`.
    fn drain(&mut self, duration: Duration) -> String {
        let deadline = Instant::now() + duration;
        let mut out = Vec::new();
        set_nonblocking(&self.master, true);
        while Instant::now() < deadline {
            let mut buffer = [0u8; 8192];
            match self.master.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&buffer[..n]),
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
        String::from_utf8_lossy(&out).to_string()
    }

    /// Wait for the child to exit, killing it if it overstays.
    fn finish(mut self, grace: Duration) -> i32 {
        let deadline = Instant::now() + grace;
        loop {
            let mut status = 0;
            // SAFETY: waiting on our own child.
            let result = unsafe { libc::waitpid(self.pid, &mut status, libc::WNOHANG) };
            if result == self.pid {
                return if libc::WIFEXITED(status) {
                    libc::WEXITSTATUS(status)
                } else {
                    -1
                };
            }
            if Instant::now() >= deadline {
                // SAFETY: killing our own child.
                unsafe { libc::kill(self.pid, libc::SIGKILL) };
                let mut status = 0;
                unsafe { libc::waitpid(self.pid, &mut status, 0) };
                return -1;
            }
            let _ = self.drain(Duration::from_millis(50));
        }
    }
}

fn set_nonblocking(file: &std::fs::File, on: bool) {
    use std::os::fd::AsRawFd;
    // SAFETY: plain fcntl on a fd we own.
    unsafe {
        let flags = libc::fcntl(file.as_raw_fd(), libc::F_GETFL);
        let flags = if on {
            flags | libc::O_NONBLOCK
        } else {
            flags & !libc::O_NONBLOCK
        };
        libc::fcntl(file.as_raw_fd(), libc::F_SETFL, flags);
    }
}

/// `Command::exec` returns only on failure; this names that.
trait ExecError {
    fn exec_error(&mut self) -> std::io::Error;
}

impl ExecError for std::process::Command {
    fn exec_error(&mut self) -> std::io::Error {
        use std::os::unix::process::CommandExt;
        self.exec()
    }
}

/// A minimal terminal screen, so assertions read what a user would see.
///
/// Stripping escapes and concatenating what is left does not work: ratatui
/// positions the cursor between every run of text, so `x = 1` comes back as
/// `x=1` and every assertion about spacing becomes a lie. Honouring cursor
/// movement into a grid is only a few more lines and makes the assertions
/// mean what they say.
struct Screen {
    grid: Vec<Vec<char>>,
    row: usize,
    column: usize,
}

impl Screen {
    fn new() -> Self {
        Screen {
            grid: vec![vec![' '; COLUMNS as usize]; ROWS as usize],
            row: 0,
            column: 0,
        }
    }

    fn feed(&mut self, raw: &str) {
        let mut chars = raw.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\u{1b}' => self.escape(&mut chars),
                '\n' => {
                    self.row = (self.row + 1).min(ROWS as usize - 1);
                    self.column = 0;
                }
                '\r' => self.column = 0,
                c if (c as u32) < 0x20 => {}
                c => self.put(c),
            }
        }
    }

    fn put(&mut self, c: char) {
        if self.row < self.grid.len() && self.column < self.grid[self.row].len() {
            self.grid[self.row][self.column] = c;
        }
        self.column += 1;
    }

    fn escape(&mut self, chars: &mut std::iter::Peekable<std::str::Chars>) {
        match chars.peek() {
            Some('[') => {
                chars.next();
                let mut parameters = String::new();
                let mut final_byte = ' ';
                for c in chars.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&c) {
                        final_byte = c;
                        break;
                    }
                    parameters.push(c);
                }
                match final_byte {
                    // Cursor position, 1-based, defaulting to the origin.
                    'H' | 'f' => {
                        let mut parts = parameters.split(';');
                        let row: usize = parts.next().unwrap_or("1").parse().unwrap_or(1);
                        let column: usize = parts.next().unwrap_or("1").parse().unwrap_or(1);
                        self.row = row.saturating_sub(1).min(ROWS as usize - 1);
                        self.column = column.saturating_sub(1);
                    }
                    'J' => {
                        self.grid = vec![vec![' '; COLUMNS as usize]; ROWS as usize];
                        self.row = 0;
                        self.column = 0;
                    }
                    _ => {}
                }
            }
            // OSC runs to BEL or ST.
            Some(']') => {
                chars.next();
                while let Some(c) = chars.next() {
                    if c == '\u{7}' {
                        break;
                    }
                    if c == '\u{1b}' && chars.peek() == Some(&'\\') {
                        chars.next();
                        break;
                    }
                }
            }
            Some(_) => {
                chars.next();
            }
            None => {}
        }
    }

    fn text(&self) -> String {
        self.grid
            .iter()
            .map(|row| row.iter().collect::<String>().trim_end().to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// The screen as a user would see it after `raw` has been drawn.
fn visible(raw: &str) -> String {
    let mut screen = Screen::new();
    screen.feed(raw);
    screen.text()
}

struct Fixture {
    _dir: TempDir,
    notebook: PathBuf,
    jupyter_path: PathBuf,
    kernel: String,
}

fn fixture(cells: &[&str]) -> Fixture {
    let (kernel, jupyter_path) = test_kernel().expect("set H5I_TEST_KERNEL_JSON");
    let dir = tempfile::tempdir().unwrap();
    let notebook = dir.path().join("tui.ipynb");

    let run = |args: &[&str]| {
        let output = std::process::Command::new(binary())
            .args(args)
            .env("JUPYTER_PATH", &jupyter_path)
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    run(&["new", notebook.to_str().unwrap(), "--kernel", &kernel]);
    for source in cells {
        run(&[
            "edit",
            notebook.to_str().unwrap(),
            "insert",
            "--code",
            source,
        ]);
    }
    Fixture {
        _dir: dir,
        notebook,
        jupyter_path,
        kernel,
    }
}

impl Fixture {
    fn view(&self) -> Pty {
        Pty::spawn(
            binary(),
            &["view", self.notebook.to_str().unwrap()],
            &[("JUPYTER_PATH", self.jupyter_path.to_str().unwrap())],
            self._dir.path(),
        )
    }
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn the_notebook_is_painted_and_quitting_restores_the_terminal() {
    let fixture = fixture(&["x = 1"]);
    let mut pty = fixture.view();
    let painted = visible(&pty.drain(Duration::from_secs(3)));
    assert!(painted.contains("tui.ipynb"), "{painted}");
    assert!(painted.contains("x = 1"), "{painted}");
    assert!(painted.contains("COMMAND"), "{painted}");
    assert!(painted.contains(&fixture.kernel), "{painted}");

    pty.send("q");
    let raw = pty.drain(Duration::from_secs(2));
    // Leaving the alternate screen is what puts the user's shell back.
    assert!(raw.contains("\u{1b}[?1049l"), "alternate screen not left");
    assert_eq!(pty.finish(Duration::from_secs(5)), 0);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn running_a_cell_shows_its_output_and_execution_count() {
    let fixture = fixture(&["print('the answer is', 6 * 7)"]);
    let mut pty = fixture.view();
    pty.drain(Duration::from_secs(2));

    // `e` is the binding that works without the kitty keyboard protocol.
    pty.send("e");
    let raw = pty.drain(Duration::from_secs(20));
    let text = visible(&raw);
    assert!(text.contains("the answer is 42"), "{text}");
    assert!(text.contains("[  1]"), "no execution count: {text}");

    pty.send("q");
    pty.finish(Duration::from_secs(5));

    // And it reached the file, not just the screen.
    let saved = std::fs::read_to_string(&fixture.notebook).unwrap();
    assert!(saved.contains("the answer is 42"), "{saved}");
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn editing_a_cell_writes_through_to_the_file() {
    let fixture = fixture(&["original"]);
    let mut pty = fixture.view();
    pty.drain(Duration::from_secs(2));

    // Enter arrives as CR from a terminal.
    pty.send("\r");
    pty.drain(Duration::from_millis(500));
    pty.send("edited_");
    pty.drain(Duration::from_millis(500));
    pty.send("\u{1b}"); // Esc commits
    pty.drain(Duration::from_secs(1));
    pty.send("q");
    pty.finish(Duration::from_secs(5));

    let saved = std::fs::read_to_string(&fixture.notebook).unwrap();
    assert!(saved.contains("edited_original"), "{saved}");
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn the_help_overlay_opens_and_closes() {
    let fixture = fixture(&["x = 1"]);
    let mut pty = fixture.view();
    pty.drain(Duration::from_secs(2));

    pty.send("?");
    let text = visible(&pty.drain(Duration::from_secs(2)));
    assert!(text.contains("run and advance"), "{text}");

    pty.send("x");
    pty.drain(Duration::from_millis(500));
    pty.send("q");
    assert_eq!(
        pty.finish(Duration::from_secs(5)),
        0,
        "the UI did not exit cleanly after the overlay"
    );
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn a_sql_cell_runs_through_the_native_engine() {
    let fixture = fixture(&["%%sql\nSELECT 1 AS answer"]);
    // The SQL cell needs a database; point the notebook at one.
    let database = fixture._dir.path().join("market.db");
    let created = std::process::Command::new(binary())
        .args(["kernel", "list"])
        .output()
        .unwrap();
    assert!(created.status.success());
    // Build a database directly rather than through the CLI.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        h5i_db_core::Database::create(&database).await.unwrap();
    });
    let mut notebook = h5i_db_notebook::Notebook::read(&fixture.notebook).unwrap();
    h5i_db_notebook::session::set_notebook_database(&mut notebook, "market.db");
    notebook.write(&fixture.notebook).unwrap();

    let mut pty = fixture.view();
    pty.drain(Duration::from_secs(2));
    pty.send("e");
    let text = visible(&pty.drain(Duration::from_secs(15)));
    assert!(text.contains("answer"), "{text}");
    assert!(text.contains("[1 rows x 1 columns]"), "{text}");

    pty.send("q");
    pty.finish(Duration::from_secs(5));
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn ctrl_c_interrupts_the_cell_instead_of_killing_the_ui() {
    // Losing an unsaved notebook to a reflex Ctrl-C would be unforgivable.
    let fixture = fixture(&["import time\ntime.sleep(120)"]);
    let mut pty = fixture.view();
    pty.drain(Duration::from_secs(2));

    pty.send("E");
    pty.drain(Duration::from_secs(6));
    pty.send("\u{3}"); // Ctrl-C
    let text = visible(&pty.drain(Duration::from_secs(15)));
    assert!(
        text.contains("KeyboardInterrupt") || text.contains("interrupting"),
        "the interrupt did not reach the kernel: {text}"
    );

    // The UI is still alive and still quits normally.
    pty.send("q");
    assert_eq!(
        pty.finish(Duration::from_secs(10)),
        0,
        "Ctrl-C killed the UI"
    );
}
