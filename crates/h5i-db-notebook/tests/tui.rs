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

    /// Read until `wanted` is on screen, or `limit` elapses; returns the screen.
    ///
    /// Bytes accumulate across reads and the whole run is replayed into one
    /// screen, because ratatui writes only the cells that changed: rendering a
    /// later chunk on its own would show a mostly blank terminal. Waiting for
    /// the content rather than draining for a fixed time is what keeps these
    /// tests honest on a slow or loaded machine, where a fixed wait turns into
    /// a flake rather than a failure.
    fn wait_for(&mut self, wanted: &str, limit: Duration) -> String {
        let deadline = Instant::now() + limit;
        let mut raw = String::new();
        loop {
            raw.push_str(&self.drain(Duration::from_millis(250)));
            let screen = visible(&raw);
            if screen.contains(wanted) || Instant::now() >= deadline {
                return screen;
            }
        }
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

/// A notebook with no kernelspec anyone has to have installed.
///
/// Watching never starts a kernel, so its tests must not need one either:
/// they are the part of this suite that can run everywhere.
fn bare_fixture(cells: &[&str]) -> BareFixture {
    let dir = tempfile::tempdir().unwrap();
    let notebook = dir.path().join("watch.ipynb");
    std::fs::write(
        &notebook,
        r#"{"cells": [], "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}}, "nbformat": 4, "nbformat_minor": 5}"#,
    )
    .unwrap();
    let fixture = BareFixture {
        _dir: dir,
        notebook,
    };
    for source in cells {
        fixture.insert(source);
    }
    fixture
}

struct BareFixture {
    _dir: TempDir,
    notebook: PathBuf,
}

impl BareFixture {
    fn path(&self) -> &str {
        self.notebook.to_str().unwrap()
    }

    /// Change the notebook from outside, the way an agent's `exec` would.
    fn insert(&self, source: &str) {
        let output = std::process::Command::new(binary())
            .args(["edit", self.path(), "insert", "--code", source])
            .current_dir(self._dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "insert failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn watch(&self) -> Pty {
        Pty::spawn(binary(), &["watch", self.path()], &[], self._dir.path())
    }

    fn bytes(&self) -> Vec<u8> {
        std::fs::read(&self.notebook).unwrap()
    }
}

impl Fixture {
    fn watch(&self) -> Pty {
        Pty::spawn(
            binary(),
            &["watch", self.notebook.to_str().unwrap()],
            &[("JUPYTER_PATH", self.jupyter_path.to_str().unwrap())],
            self._dir.path(),
        )
    }

    /// Run a cell the way an agent does: a separate process, through the
    /// supervisor, with nobody attached to the UI.
    fn exec(&self, code: &str) {
        let output = std::process::Command::new(binary())
            .args(["exec", self.notebook.to_str().unwrap(), "--code", code])
            .env("JUPYTER_PATH", &self.jupyter_path)
            .current_dir(self._dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "exec failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn stop(&self) {
        let _ = std::process::Command::new(binary())
            .args(["kernel", "stop", self.notebook.to_str().unwrap()])
            .env("JUPYTER_PATH", &self.jupyter_path)
            .current_dir(self._dir.path())
            .output();
    }

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
    // The opening frame is kept rather than drained away: it carries the cells
    // that later frames only touch, and a terminal that answers nothing now
    // finishes starting up well inside this wait.
    let mut raw = pty.drain(Duration::from_secs(2));

    // `e` is the binding that works without the kitty keyboard protocol.
    pty.send("e");
    raw.push_str(&pty.drain(Duration::from_secs(20)));
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

// ---------------------------------------------------------------------------
// watch: no kernel needed, because watching never starts one
// ---------------------------------------------------------------------------

#[test]
fn watch_paints_the_notebook_and_says_it_is_read_only() {
    let fixture = bare_fixture(&["x = 41"]);
    let mut pty = fixture.watch();
    let painted = pty.wait_for("WATCH", Duration::from_secs(20));

    assert!(painted.contains("watch.ipynb"), "{painted}");
    assert!(painted.contains("x = 41"), "{painted}");
    assert!(
        painted.contains("WATCH"),
        "no badge to say this pane cannot edit: {painted}"
    );

    pty.send("q");
    assert_eq!(pty.finish(Duration::from_secs(5)), 0);
}

#[test]
fn watch_ignores_every_key_that_would_change_the_notebook() {
    let fixture = bare_fixture(&["keep me"]);
    let before = fixture.bytes();
    let mut pty = fixture.watch();
    pty.wait_for("keep me", Duration::from_secs(20));

    // Delete a cell, open the editor, type into it, run it, save it: the whole
    // editing vocabulary, none of which a watcher may use.
    pty.send("dd");
    pty.send("\r");
    pty.send("junk");
    pty.send("\x1b");
    pty.send("es");
    let after_keys = pty.wait_for("read-only", Duration::from_secs(20));
    assert!(
        after_keys.contains("read-only"),
        "a rejected key said nothing: {after_keys}"
    );

    pty.send("q");
    assert_eq!(pty.finish(Duration::from_secs(5)), 0);
    assert_eq!(
        fixture.bytes(),
        before,
        "a read-only pane wrote to the notebook"
    );
}

#[test]
fn watch_shows_a_change_made_by_another_process_without_a_keypress() {
    // The whole point: an agent runs a cell, and the human's pane updates.
    let fixture = bare_fixture(&["first"]);
    let mut pty = fixture.watch();
    let painted = pty.wait_for("first", Duration::from_secs(20));
    assert!(painted.contains("first"), "{painted}");
    assert!(
        !painted.contains("second"),
        "the cell existed before it was written: {painted}"
    );

    fixture.insert("second");

    // Nothing is typed here. The pane has to notice on its own.
    let updated = pty.wait_for("second", Duration::from_secs(20));
    assert!(
        updated.contains("second"),
        "the change never reached the pane: {updated}"
    );

    pty.send("q");
    assert_eq!(pty.finish(Duration::from_secs(5)), 0);
}

/// A 240x120 PNG, big enough to be worth several rows of cells.
const RED_PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAPAAAAB4CAIAAABD1OhwAAABXElEQVR4nO3SQQkAIADAQDWI/aMYyxKCMO4S7LF59h5QsX4HwEuGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE2KoUkxNCmGJsXQpBiaFEOTYmhSDE3KBRozAfSFclCgAAAAAElFTkSuQmCC";

/// A plot in an unfocused tmux pane is drawn into that pane, not the focused one.
///
/// tmux hands passthrough bytes to the terminal without moving the real cursor
/// first, so an image that does not say where it goes lands wherever tmux last
/// drew: the pane with the focus. Watching a notebook beside an agent that is
/// working in the neighbouring pane is exactly that case, and the plot used to
/// appear in the agent's pane.
///
/// Ignored because it needs tmux; the assertion is on what the notebook writes
/// into its own pane, so it does not depend on how tmux then forwards it.
#[test]
#[ignore]
fn a_plot_in_an_unfocused_tmux_pane_says_where_it_goes() {
    if std::process::Command::new("tmux")
        .arg("-V")
        .output()
        .is_err()
    {
        eprintln!("skipping: no tmux");
        return;
    }
    let socket = "h5i-nb-tmux-test";
    let tmux = |args: &[&str]| {
        std::process::Command::new("tmux")
            .args(["-L", socket])
            .args(args)
            .output()
            .unwrap()
    };
    tmux(&["kill-server"]);

    let dir = tempfile::tempdir().unwrap();
    let notebook = dir.path().join("plot.ipynb");
    // Written the way the supervisor writes: a rename, so a watcher polling
    // the file can never read a half-written document and give up on it.
    //
    // Each round adds a line of source above the plot, which moves it down a
    // row. That is what makes the image redraw at all: only cells that differ
    // from the last frame are sent, and an image whose position has not
    // changed is left alone on the screen.
    let write_notebook = |round: usize| {
        let source = "# pad\\n".repeat(round) + "plot()";
        let scratch = notebook.with_extension("tmp");
        std::fs::write(
            &scratch,
            format!(
                r#"{{"cells": [{{"cell_type": "code", "execution_count": 1, "id": "a1",
                   "metadata": {{}}, "source": "{source}",
                   "outputs": [{{"output_type": "display_data", "metadata": {{}},
                     "data": {{"image/png": "{RED_PNG}", "text/plain": "<Figure>"}}}}]}}],
                   "metadata": {{"kernelspec": {{"display_name": "Python 3",
                   "language": "python", "name": "python3"}}}},
                   "nbformat": 4, "nbformat_minor": 5}}"#
            ),
        )
        .unwrap();
        std::fs::rename(&scratch, &notebook).unwrap();
    };
    write_notebook(0);

    // default-terminal matters: it is what tells the image layer underneath us
    // that it is inside tmux and has to use a passthrough at all.
    let conf = dir.path().join("tmux.conf");
    std::fs::write(
        &conf,
        "set -g status off\nset -g default-terminal \"tmux-256color\"\n",
    )
    .unwrap();
    let transcript = dir.path().join("pane.out");

    let pty = Pty::spawn(
        "tmux",
        &[
            "-L",
            socket,
            "-f",
            conf.to_str().unwrap(),
            "new-session",
            "-x",
            &COLUMNS.to_string(),
            "-y",
            &ROWS.to_string(),
            "--",
            "sh",
        ],
        &[("TERM", "xterm-256color")],
        dir.path(),
    );
    // A real terminal reads continuously, and tmux needs one: a server blocked
    // writing to a pty nobody is draining stops serving its panes, and then
    // nothing is ever drawn. Reading on a thread is the only way to hold that
    // up while this test also talks to tmux and waits on files.
    let watcher = pty.master.try_clone().unwrap();
    set_nonblocking(&watcher, false);
    let reading = std::thread::spawn(move || {
        let mut watcher = watcher;
        let mut buffer = [0u8; 8192];
        loop {
            match watcher.read(&mut buffer) {
                Ok(0) => return,
                Ok(_) => {}
                // A signal interrupting the read must not end the draining:
                // that is how the whole session stalls.
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return,
            }
        }
    });
    std::thread::sleep(Duration::from_millis(800));

    tmux(&[
        "split-window",
        "-h",
        "-e",
        "H5I_NB_IMAGES=sixel",
        "--",
        binary(),
        "watch",
        notebook.to_str().unwrap(),
    ]);
    std::thread::sleep(Duration::from_millis(500));
    let geometry = tmux(&[
        "display-message",
        "-p",
        "-t",
        "1",
        "#{pane_left} #{pane_top} #{pane_width} #{pane_height}",
    ]);
    let geometry = String::from_utf8_lossy(&geometry.stdout);
    let numbers: Vec<u16> = geometry
        .split_whitespace()
        .filter_map(|n| n.parse().ok())
        .collect();
    let [left, top, width, height] = numbers[..] else {
        panic!("tmux did not describe the pane: {geometry:?}");
    };

    // The focus goes to the other pane: this is the case that used to break.
    tmux(&["select-pane", "-t", "0"]);
    // Record what the pane writes from the start, so the first frame — the one
    // with the plot in it — is in the transcript rather than raced against.
    // Unbuffered, or `cat` holds a frame in its own 4K buffer and the plot
    // looks like it was never drawn.
    tmux(&[
        "pipe-pane",
        "-o",
        "-t",
        "1",
        &format!("stdbuf -o0 cat > {}", transcript.display()),
    ]);

    // Each round moves the plot down a line, so a frame that missed the first
    // paint still has a reason to redraw the image: only cells that differ
    // from the last frame are sent.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut written = String::new();
    for round in 1..20 {
        std::thread::sleep(Duration::from_millis(500));
        written = std::fs::read_to_string(&transcript).unwrap_or_default();
        if written.contains("\x1bPtmux;") || Instant::now() >= deadline {
            break;
        }
        write_notebook(round);
    }
    let state = tmux(&[
        "display-message",
        "-p",
        "-t",
        "1",
        "cmd=#{pane_current_command} dead=#{pane_dead} size=#{pane_width}x#{pane_height}",
    ]);
    let state = String::from_utf8_lossy(&state.stdout).into_owned();
    let state = format!(
        "{state} showing:\n{}",
        String::from_utf8_lossy(&tmux(&["capture-pane", "-p", "-t", "1"]).stdout)
    );
    tmux(&["kill-server"]);
    pty.finish(Duration::from_secs(5));
    let _ = reading.join();

    assert!(
        written.contains("\x1bPtmux;"),
        "the pane never drew an image at all, so there is nothing to place. \
         Pane: {state}It wrote {} bytes: {:?}",
        written.len(),
        visible(&written),
    );
    // Escapes are doubled inside a passthrough: save cursor, move, restore.
    let marker = "\x1bPtmux;\x1b\x1b7\x1b\x1b[";
    let at = written
        .find(marker)
        .unwrap_or_else(|| panic!("the image carries no cursor move of its own"));
    let rest = &written[at + marker.len()..];
    let end = rest.find('H').expect("unterminated cursor move");
    let (row, column) = rest[..end].split_once(';').expect("malformed cursor move");
    let (row, column): (u16, u16) = (row.parse().unwrap(), column.parse().unwrap());

    // One-based, and inside this pane rather than the focused one next door.
    assert!(
        column > left && column <= left + width,
        "image aimed at column {column}, outside a pane spanning {}..={}",
        left + 1,
        left + width
    );
    assert!(
        row > top && row <= top + height,
        "image aimed at row {row}, outside a pane spanning {}..={}",
        top + 1,
        top + height
    );
    assert!(
        written[at..].contains("\x1b\x1b8"),
        "the cursor is never put back, so tmux's next write starts from ours"
    );
}

#[test]
fn watch_help_offers_only_what_a_watcher_can_do() {
    let fixture = bare_fixture(&["x = 1"]);
    let mut pty = fixture.watch();
    pty.wait_for("WATCH", Duration::from_secs(20));

    pty.send("?");
    let help = pty.wait_for("follow", Duration::from_secs(20));
    assert!(help.contains("follow"), "{help}");
    assert!(help.contains("interrupt"), "{help}");
    assert!(
        !help.contains("delete cell"),
        "the watcher was offered an edit it cannot make: {help}"
    );

    pty.send("q");
    pty.send("q");
    assert_eq!(pty.finish(Duration::from_secs(5)), 0);
}

#[test]
fn ctrl_c_closes_a_watch_pane_rather_than_stopping_someone_elses_cell() {
    // The opposite of `view`, deliberately: the cell running behind a watch
    // pane belongs to whoever started it, and a reflex Ctrl-C must not be the
    // thing that kills an agent's work. `ii` is the way to do that on purpose.
    let fixture = bare_fixture(&["x = 1"]);
    let mut pty = fixture.watch();
    pty.wait_for("WATCH", Duration::from_secs(20));

    pty.send("\x03");
    assert_eq!(
        pty.finish(Duration::from_secs(5)),
        0,
        "Ctrl-C did not close the watch pane"
    );
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn a_watch_pane_shows_cells_an_agent_runs_through_the_supervisor() {
    // The workflow the whole feature exists for: the agent drives the session
    // from another process, and the human's pane keeps up on its own.
    let fixture = fixture(&["x = 1"]);
    let mut pty = fixture.watch();
    pty.wait_for("WATCH", Duration::from_secs(20));

    fixture.exec("print('hello from the agent')");

    let seen = pty.wait_for("hello from the agent", Duration::from_secs(30));
    assert!(
        seen.contains("hello from the agent"),
        "the agent's output never reached the watch pane: {seen}"
    );
    // And the pane learned there is a live kernel behind the file, which the
    // notebook alone cannot say.
    assert!(
        seen.contains("idle") || seen.contains("busy"),
        "no kernel state in a watched session: {seen}"
    );

    pty.send("q");
    assert_eq!(pty.finish(Duration::from_secs(5)), 0);
    fixture.stop();
}

#[test]
#[ignore = "spawns a real tmux server"]
fn split_opens_a_pane_in_tmux() {
    // The argv construction is unit-tested; this is the one test that proves
    // a real multiplexer accepts what we build.
    if std::process::Command::new("tmux")
        .arg("-V")
        .output()
        .is_err()
    {
        eprintln!("tmux is not installed; skipping");
        return;
    }
    let fixture = bare_fixture(&["x = 1"]);
    // Its own server socket, so the test cannot disturb a human's tmux.
    let socket = format!("h5i-nb-test-{}", std::process::id());
    let tmux = |args: &[&str]| {
        let mut full = vec!["-L", &socket];
        full.extend_from_slice(args);
        std::process::Command::new("tmux")
            .args(&full)
            .output()
            .unwrap()
    };

    // A session whose first pane just waits, so there is something to split.
    let started = tmux(&["new-session", "-d", "-s", "w", "sleep", "60"]);
    assert!(started.status.success());

    // $TMUX is how tmux finds its server, and $TMUX_PANE which pane to split.
    // Both have to name the server this test started, or the split lands
    // somewhere else or nowhere at all.
    let ask = |format: &str| {
        let out = tmux(&["display-message", "-p", "-t", "w", format]);
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    let tmux_env = format!("{},{},0", ask("#{socket_path}"), ask("#{pid}"));
    let pane = ask("#{pane_id}");

    let split = std::process::Command::new(binary())
        .args(["watch", fixture.path(), "--split", "right"])
        .env("TMUX", tmux_env)
        .env("TMUX_PANE", pane)
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .current_dir(fixture._dir.path())
        .output()
        .unwrap();

    // The split must have been asked for through tmux; if this machine's tmux
    // refuses the injected $TMUX, the command says so rather than silently
    // taking over the current terminal.
    let stderr = String::from_utf8_lossy(&split.stderr).to_string();
    let panes = tmux(&["list-panes", "-t", "w"]);
    let listed = String::from_utf8_lossy(&panes.stdout).lines().count();

    let _ = tmux(&["kill-session", "-t", "w"]);
    let _ = tmux(&["kill-server"]);

    assert!(
        split.status.success(),
        "--split failed inside tmux: {stderr}"
    );
    assert_eq!(listed, 2, "no second pane was created");
}
