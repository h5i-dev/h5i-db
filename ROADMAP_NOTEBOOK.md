# In-Terminal Notebook — Design & Roadmap

`crates/h5i-db-notebook`, exposed as the `h5i-nb` command line and a TUI.

---

## 1. Thesis

An agent doing quant research today writes a Python file, runs it through bash,
and reads stdout. Every iteration re-imports, re-opens the database, re-loads
the frame, and re-computes the intermediate that the previous iteration already
had in memory. A 40-second load times thirty exploratory probes is twenty
minutes of pure waste, and the agent's context fills with the same boilerplate
thirty times over.

A notebook session changes four things, and all four matter more for an agent
than for a human:

1. **State persists.** Load once, probe many times. Iteration cost drops from
   tens of seconds to milliseconds, which is the difference between an agent
   that tries three hypotheses and one that tries thirty.
2. **Failure is local.** A traceback in cell 7 leaves cells 1-6 alive. A script
   restarts from zero; a notebook edits one cell and re-runs it.
3. **Output is structured and attributed.** Each output is a typed mime bundle
   bound to the code that produced it, not an undifferentiated stdout blob that
   the agent has to re-parse.
4. **The session is a durable, reviewable artifact.** The `.ipynb` is what a
   human opens to check what the agent actually did, in the tool humans already
   use for exactly that. For an engine whose thesis is auditability, notebook
   files are the natural provenance object for the exploration phase, the way
   forks are for the execution phase.

Point 4 is why this belongs in h5i-db and not in a generic notebook tool. The
engine already records what an agent *ran*: versions, forks, trial ledgers. It
does not record what the agent *looked at* on the way to deciding what to run.
The notebook closes that gap, and it does it in a file format a human already
knows how to read.

### Non-goals

- Not a Jupyter server. No HTTP, no tokens, no multi-user, no JupyterLab.
- Not ipywidgets. The comm protocol is plumbed so widget-producing libraries do
  not crash the session, but nothing interactive renders in v1.
- Not a Python reimplementation. We are a *client*; ipykernel stays ipykernel.
- Not a replacement for `h5i-db query` for one-shot queries.

---

## 2. What the reference implementation taught us

`euporie` (~86k LOC Python) is the mature in-terminal notebook. The breakdown is
instructive: ~49k LOC is `apptk`, a fork of prompt-toolkit, and ~31k is
`euporie-core`, overwhelmingly widget, layout, and input machinery. The kernel
client, which is the part carrying the actual value, is ~3.6k LOC.

The lesson is not "rewrite euporie in Rust". It is that a terminal notebook is
mostly a terminal *widget toolkit*, and that ratatui already is one. What we
port is the kernel client's hard-won lifecycle knowledge (startup handshake
retries, death detection, interrupt mode dispatch, restart sequencing), not
the UI.

The second lesson is negative: euporie's design centre is a human at a
keyboard, so every output is rendered for eyes and every byte of a 10,000-row
DataFrame reaches the screen. An agent-facing renderer has the opposite
requirement, and that is a first-class subsystem here (§6), not a flag.

---

## 3. Dependencies

The wire protocol is reused, everything above it is ours.

| Layer | Source | Why |
| --- | --- | --- |
| msgspec v5.3 message types, HMAC-SHA256 signing | `jupyter-protocol` 2.0.2 | Correctness here is invisible until it corrupts a session. Re-implementing multipart framing, the `<IDS\|MSG>` delimiter, and signing over four blobs is pure risk with no differentiation. |
| Five ZMQ channels, connection info, kernelspec discovery | `jupyter-zmq-client` 1.0.1 | Same. Gives `Connection<S>` send/read, port probing, kernelspec search paths. |
| ZeroMQ | `zeromq` 0.6 (transitive) | Pure Rust, no `libzmq` C dependency, so the single-static-binary distribution story is unchanged. |

All three are BSD-3/MIT from the `runtimed` org, the lineage behind Zed's REPL.
They stop at the transport: process spawning, lifecycle, request/reply
correlation, and everything above are ours.

Explicitly *not* reused: the `nbformat` crate. It parses notebooks; we need
byte-faithful round-tripping of files a human may also edit in JupyterLab
(§5), which is a stricter contract than that crate offers.

---

## 4. Kernel abstraction

Two backends behind one trait. The trait is what the session layer, the CLI,
and the TUI all speak.

```rust
#[async_trait]
pub trait Kernel: Send {
    fn spec(&self) -> &KernelSpecInfo;
    fn status(&self) -> KernelStatus;   // Starting|Idle|Busy|Dead|Restarting
    async fn execute(&mut self, code: &str, opts: ExecOptions,
                     sink: &mut dyn OutputSink) -> Result<ExecOutcome>;
    async fn complete(&mut self, code: &str, cursor: usize) -> Result<Completions>;
    async fn inspect(&mut self, code: &str, cursor: usize, detail: u8) -> Result<Option<Media>>;
    async fn is_complete(&mut self, code: &str) -> Result<Completeness>;
    async fn interrupt(&mut self) -> Result<()>;
    async fn restart(&mut self) -> Result<()>;
    async fn shutdown(self: Box<Self>) -> Result<()>;
}
```

`OutputSink` receives outputs as they arrive rather than at cell end, so the TUI
paints incrementally, `nb exec --stream` emits jsonl live, and a cell that
prints for ten minutes is observable while it runs.

### 4.1 `JupyterKernel` (ZMQ)

Lifecycle, in the order the failures actually happen:

- **Discovery.** `find_kernelspec`, plus `$JUPYTER_PATH`, plus a venv-relative
  `share/jupyter/kernels` probe so a project venv's kernel resolves without
  `jupyter` on `PATH`. (On this machine `jupyter` is not on `PATH` at all, which
  is the common case for an agent in a fresh container.)
- **Launch.** Probe five free ports, generate an HMAC key, write the connection
  file into the runtime dir, substitute `{connection_file}` into the spec's
  `argv`, spawn in its own process group.
- **Handshake.** ipykernel drops messages published before its iopub socket has
  a subscriber, so a single `kernel_info_request` after connect is the classic
  hang. We wait for the iopub welcome where offered, then retry
  `kernel_info_request` on a backoff until a reply or a deadline.
- **Liveness.** A monitor task owns the child handle and waits on it, so a
  segfault or an `os._exit` is observed the instant it happens. The heartbeat
  channel is deliberately *not* used: for a locally launched kernel it adds no
  information, because ipykernel answers heartbeats from a separate thread that
  keeps replying while the main thread is wedged, so it cannot distinguish
  "hung" from "busy" any better than we already do, and it can only add false
  positives under load. Remote kernels would change that calculus; we do not
  have them.
- **Dispatch.** One reader task per channel feeds a routing table keyed by
  `parent_header.msg_id`. Orphan iopub traffic (messages whose parent is not
  ours, or kernel-side prints from a background thread) is surfaced rather than
  dropped, because silently swallowing it is how output goes mysteriously
  missing.
- **Interrupt.** Honours the spec's `interrupt_mode`: `signal` sends SIGINT to
  the process group, `message` sends `interrupt_request` on the control channel.
- **Restart.** `shutdown_request(restart=true)` with a grace period, then
  SIGTERM, then SIGKILL, then relaunch.
- **Reaping.** On drop, always. A notebook tool that leaks kernel processes is
  a notebook tool that eats a laptop, and this is the single most common defect
  in hand-rolled clients. `Drop` covers every ordinary path but nothing runs on
  SIGKILL, so an OOM-killed or `kill -9`ed owner used to leave its kernel
  reparented to init holding an interpreter's worth of memory. The owning pid
  is now recorded beside the connection file, and each start sweeps sidecars
  whose owner is gone. The kernel is only killed after its command line
  confirms it is still the process recorded, so a recycled pid can never be
  hit.

### 4.2 `SqlKernel` (native, in-process)

Runs on the existing `h5i-db-query` DataFusion context against an open
`Database`. No subprocess, no Python, no startup cost.

- Honours the same limits the CLI already exposes: `--max-rows`, `--max-bytes`,
  `--timeout`, `--memory-limit`, `--read-only`.
- Emits a mime bundle with `text/plain` (aligned table), `text/html` (so the
  notebook renders in JupyterLab too), and
  `application/vnd.h5i.arrow+base64` for lossless downstream use.
- `%%sql --into df` hands the result to the Python kernel as Arrow IPC through a
  temp file, so the crossing is zero-copy-ish and lossless rather than a CSV
  round-trip. Requires `pyarrow` or the `h5i_db` wheel in the kernel env; the
  error says which when neither is present.

The SQL kernel is what makes this an h5i-db notebook rather than a generic one:
schema discovery and the first twenty exploratory queries never pay for a Python
interpreter at all.

---

## 5. Document model

nbformat v4.5, strict, with a round-trip contract:

- Every node carries `#[serde(flatten)] extra: Map<String, Value>` so fields we
  do not model (JupyterLab metadata, Colab metadata, custom cell attachments)
  survive a load/save cycle untouched.
- Writing matches `nbformat`'s own writer byte for byte: sorted keys, one-space
  indent, `source`/`text` split into line arrays with trailing newlines, single
  trailing newline at EOF. Anything else churns the git diff every time a human
  opens the file in JupyterLab, which in a repo that versions everything is a
  real cost rather than an aesthetic one.
- Cell ids are generated and uniqueness is enforced on load (v4.5 requires them
  and real-world files violate it).
- Saves are atomic: temp file in the same directory, fsync, rename. A crash
  mid-save must never truncate a human's notebook.

---

## 6. Rendering: two audiences, one mime bundle

**Human (TUI).** Priority: image > html table > markdown > `text/plain`, ANSI
colour preserved. Images go through kitty graphics, then iTerm2, then sixel,
then unicode half-blocks, selected by terminal probe rather than by guessing
from `$TERM` alone.

**Agent (CLI).** Token-budgeted by default, because the whole point is to spend
fewer tokens than `python script.py` did, not more:

- `text/plain`: head N lines, tail M lines, `… 4,912 lines elided …` between.
- DataFrames: re-rendered as a compact aligned table, first and last rows only,
  with a `[10,000 rows x 12 columns]` shape line. Shape is usually the entire
  information content of the output for the agent's next decision.
- `image/png`: never inlined as base64. Written to
  `<notebook>_files/cell-<id>-<n>.png`, and the *path* is returned. A multimodal
  agent that wants the pixels reads the file; one that does not, pays nothing.
- Errors: full traceback, ANSI stripped. Agents need every frame.

The untruncated output always stays in the `.ipynb` and is retrievable with
`h5i-db nb output <cell> --raw`. This is deliberately the same
summarise-then-rehydrate contract as `h5i capture run` / `h5i recall object`,
which agents working in this repo already know.

---

## 7. Session persistence

The mechanism the whole premise depends on: `h5i-db nb exec` is a fresh process
every time an agent calls it, so *something* has to hold the kernel between
calls.

A supervisor process owns the kernel, subscribes to iopub continuously, journals
outputs to a session log, and serves a control API over a unix socket at
`$XDG_RUNTIME_DIR/h5i-db/nb/<hash>.sock`. It is auto-spawned on first use and
idles out after a configurable TTL.

The simpler alternative, reconnecting to the kernel's existing connection file
per invocation the way `jupyter console --existing` does, was rejected for one
reason: ZMQ PUB drops messages when no subscriber is attached, so any output
produced while no client was connected is gone forever. That breaks `--detach`,
breaks recovery from an interrupted CLI call, and loses exactly the long-running
cell output that matters most. Continuous iopub capture is worth a supervisor.

---

## 8. TUI

ratatui + crossterm, modal, with Jupyter's key bindings because that is the
muscle memory the audience has (`Esc`/`Enter` for command/edit, `a`/`b` insert,
`dd` delete, `Shift+Enter` run-and-advance).

- Cell editor: character-indexed (a notebook full of `日本語` must not put the
  cursor inside a codepoint), auto-indent, block-opening indent after `:`,
  goal-column memory on vertical movement.
- **Highlighting is a hand-written lexer, not tree-sitter.** The grammars are C,
  and adding a `cc` toolchain to the build of a project whose distribution story
  is a single static binary — and which already picked a pure-Rust ZeroMQ for
  that reason — buys accuracy nobody can see in a terminal. Keywords, builtins,
  strings, comments, and numbers are what colouring needs.
- **`Shift+Enter` cannot be sent by most terminals at all**: without the kitty
  keyboard protocol it arrives as a plain `Enter`. The protocol is requested
  when the terminal supports it, and `e` / `E` are the bindings that always
  work.
- Completion popup from `complete_request`, anchored under the cursor and
  flipped above it near the bottom of the screen; inspection overlay from
  `inspect_request`.
- Inline images through the kitty and iTerm2 protocols, which pass base64
  straight through. Sixel and half-blocks would need a PNG decoder and a
  resampler; unsupported terminals get a labelled placeholder and the output
  stays reachable through `nb output --save`.
- Event loop is a `tokio::select!` over crossterm events, runner events, and a
  tick. The session lives on its own task, so a twenty-minute cell never
  freezes the screen; `Ctrl-C` interrupts the cell rather than killing the UI,
  because the notebook is the state.

---

## 9. CLI surface

Matches the §8 contract in `DESIGN.md`: `--format table|json|jsonl`, machine
errors on stderr, stable exit codes, no prompts, no TTY assumptions.

Shipped as its own binary, `h5i-nb`, rather than as an `h5i-db nb`
subcommand: the notebook crate carries its own error enum, and folding it
into the main CLI's error plumbing is a mechanical follow-up that would
have destabilised that binary for no gain during the build.

```
h5i-nb new <file> [--kernel python3]
h5i-nb exec <file> --code -            # append a cell, run it, print result
h5i-nb run  <file> [--cells 3-7|--all] [--from-clean] [--keep-going]
h5i-nb output <file> <cell> [--index n] [--raw|--save <path>]
h5i-nb cells <file>                    # index, type, status, output shape
h5i-nb kernel list|start|status|interrupt|restart|stop
h5i-nb edit <file> set|insert|delete|move|clear-outputs
h5i-nb view <file>                     # TUI (N4, not built)
h5i-nb watch <file> [--split right]    # read-only live view (N6, designed)
h5i-nb ls                              # running sessions on this machine (N6)
h5i-nb export <file> --to md|py|html   # also -o <path>, --without-outputs
h5i-nb inspect|complete <file> <code>  # what the TUI uses, from a shell
```

---

## 10. Testing

What separates this from a demo:

- **Document round-trip**: property tests over generated notebook JSON, plus a
  corpus of real-world `.ipynb` files, asserting byte-identical rewrite and
  unknown-field preservation.
- **Dispatch layer**: `jupyter-zmq-client`'s `test-kernel` feature gives a fake
  kernel for fast deterministic tests of correlation, ordering, and orphan
  handling.
- **Real kernel integration**: a pinned ipykernel venv, covering start,
  stdout/stderr interleaving, `execute_result` vs `display_data`, errors,
  interrupt mid-loop, restart-clears-state, shutdown-reaps-process, and
  kernel-death detection. Marked `#[ignore]` so a dev box without ipykernel
  still passes `cargo test`, run explicitly in its own CI job.
- **TUI**: ratatui `TestBackend` snapshot tests for layout; input handling
  tested as pure state transitions with no terminal involved.
- Tests run serially (the dev environment OOMs on concurrent heavy suites).

---

## 11. Phases

Each phase ends with something an agent can actually use.

| Phase | Content | Status |
| --- | --- | --- |
| N0 | Document model, canonical nbformat writer, atomic saves | **Done.** Differential-tested against real `nbformat` |
| N1 | `JupyterKernel`, session, supervisor, CLI | **Done.** Persistent state across separate CLI processes, verified end to end |
| N2 | Agent digest renderer, elision, `\r` collapsing, `--raw` | **Done** for text and errors. Images are named, not yet written to `<notebook>_files/` |
| N3 | `SqlKernel`, `%%sql`, Arrow handoff | **Done.** 291ms cold SQL cell against 1.6s for a Python kernel |
| N4 | Editable TUI | **Done.** Verified by driving the real UI through a pty |
| N5 | Export, images to disk, `--detach`, `h5i-db nb` mount | **Done** |
| N6 | `watch` live view, `--split`, `ls`, skill reference | **Done.** Watch panes verified through a pty; `--split` verified against a real tmux server |

### Known gaps in what is built

- **Multi-line strings** highlight only on their first line: the lexer treats
  lines independently. Wrong for one keystroke inside a triple-quoted block,
  and not worth a stateful lexer to fix.
- **ipywidgets** do not render. Comm traffic is accepted so a widget-producing
  library does not crash the session, but nothing interactive draws.
- **Markdown in exported HTML** is shown as preformatted text rather than
  parsed. A markdown parser is a dependency and a class of injection bugs, and
  an exported notebook is read for its outputs.
- **`option-ext` (MPL-2.0)** rides in transitively through
  `jupyter-zmq-client -> dirs`, backing a directory lookup we do not call. It
  carries a narrow per-crate exception in `deny.toml` rather than widening the
  permissive-only allowlist.

### N5 notes

- **Images** are written to `<notebook stem>_files/cell-<id>-<n>.png`, which is
  nbconvert's convention, and named by path in the digest. Base64 still never
  reaches the terminal, and the payload stays in the `.ipynb` so JupyterLab
  renders it too. Names are deterministic, so re-running a cell replaces its
  figure instead of accumulating one per execution.
- **`exec --detach`** returns as soon as the cell exists. The supervisor owns
  the notebook, so outputs are recorded after the client has gone. While a
  detached cell runs, `status` answers from the file rather than waiting for
  the lock, `interrupt` goes through the `InterruptHandle` instead of the lock,
  and a second `exec` is refused with `session_busy` (exit 3, retryable) rather
  than being left to look like a hang.
- **Export** targets three different readers: markdown for a diff or a README,
  a `# %%` script for re-running the work without notebook machinery, and a
  single self-contained HTML file with images inlined as `data:` URIs. IPython
  magics are commented out of the Python export, because `%matplotlib inline`
  is a `SyntaxError` in a plain script.
- **`h5i-db nb`** mounts the same command tree inside the main binary. It keeps
  the notebook crate's own error codes rather than flattening them into core's,
  and the two binaries share one session, so `h5i-db nb exec` and
  `h5i-nb status` talk to the same kernel. The supervisor is started by
  re-executing the running binary, so each entry point declares how `serve` is
  reached rather than guessing from the executable's name.

---

## 12. N6: the pair-programming surface

Everything so far serves one reader at a time: the agent through the CLI, or a
human inside the TUI. N6 is about both at once. The agent drives cells through
the supervisor; the human watches them land, live, in a pane beside the
conversation, outputs and figures included. The shape is borrowed from
`terminal-browser`, whose headline workflow ("ask an agent to make HTML plans
and open them in a split pane next to your agent") is exactly this with a
browser where we have a notebook. Their hard part, rendering chromium pixels
into a terminal, is not our problem: our document already renders in a
terminal. What transfers is the *interaction grammar*: a read-only live
surface, a `--split` that puts it next to the human unprompted, an `ls` that
names the running instances, and a skill file that teaches agents the verbs.

Four pieces, in build order. Each is independently useful; together they
compose into the demo: the agent says "watch this" and the human sees every
cell arrive as it runs.

### 12.1 `watch`: a read-only live view

`h5i-nb watch <file>` opens the notebook in the TUI's rendering, updates it
whenever the file changes, and can neither edit nor execute. It is the
spectator to `view`'s player.

**Ownership.** `watch` is read-only in the strongest sense: it never writes
the file, never takes the supervisor lock, and never starts a kernel or a
supervisor. `view` keeps its existing exclusive semantics (it stops a running
supervisor before taking the notebook, because two writers to one file was the
data-loss bug of the last review). `watch` is the mode that *coexists*: with a
supervisor mid-cell, with a `view` in another terminal, with other watches.
Any number may run at once, which is also what makes `--split` safe to hand to
an agent: opening a pane can never steal a session.

**Change detection: poll the file, no notify crate.** The supervisor rewrites
the whole `.ipynb` after every cell (§7), and saves are atomic renames (§5),
so the file is the broadcast channel and a reader can never observe a torn
write. The watcher polls `(mtime, len)` every 250ms and re-reads on change; a
byte-compare against the previous content drops no-op wakeups, which the
canonical writer makes meaningful (identical state is identical bytes). The
`notify` crate would trade that loop for inotify/kqueue backends and a
dependency tree, buying latency nobody can see next to cells that take
seconds; the project has spent this argument before on pure-Rust ZeroMQ and
the hand-written lexer, and it comes out the same way here.

**Reuse: a watcher task where the runner was.** The TUI already receives a
replacement document as an event (`Event::Notebook`), with selection clamping
and redraw handled downstream. `watch` reuses the whole `App`/`render` stack
and swaps the runner task for a watcher task that reads the file and emits
that event. Keys are filtered at the top of `on_key`: navigation, scrolling,
overlays, and quit pass through; every mutating key is ignored and flashes
"read-only" in the status bar, which also gets a `WATCH` badge so a human is
never unsure which mode owns the keyboard.

**Kernel status without ownership.** The file cannot say whether the kernel is
busy. The supervisor's `Status` request answers without the session lock (that
is the point of it), so the watcher polls it at 1s and feeds the existing
status badge. No supervisor running renders as the badge's "not running"; the
file is still watched, so `watch` is also a passable way to eyeball a notebook
another machine is syncing.

**Follow mode.** On a file change, the watcher diffs old against new cells (by
id, then by outputs and execution count) and selects the last cell that
changed, so the human's viewport tracks the agent's activity without touching
the keyboard. Any manual navigation disables following; `f` re-enables it.
This mirrors pager follow (`less +F`), which is the muscle memory that exists
for "watch a thing grow".

**One deliberate exception to read-only: `ii` interrupts.** Interrupt goes
through the supervisor's lock-free `Interrupt` request and mutates no document
state, and the person staring at a runaway cell in a watch pane is precisely
the person who needs it. Everything else stays inert.

**The v2 that this explicitly is not.** A `Watch` request in the supervisor
protocol, holding the connection open and broadcasting `StreamEvent`s, would
push output live instead of at cell-save granularity. It became feasible when
connections moved to per-task handling, and it is the right upgrade if
polling granularity ever matters (streaming a ten-minute cell's stdout as it
prints). It is not v1 because the file-watching version needs no protocol
change, no broadcast list in the supervisor, and no reconnect logic, and it
delivers the workflow: cells land when they finish, which for the pairing use
case is when the human cares.

### 12.2 `--split`: put it next to the human

`--split right|left|down|up` on `watch` (and `view`) asks the surrounding
multiplexer for a new pane and runs the command there. The invoking process
spawns the pane and exits 0 immediately, which is what makes it agent-shaped:
an agent can call `h5i-db nb watch nb.ipynb --split right` mid-turn without
blocking on a UI a human will sit in for an hour.

Detection is by environment, first match wins, and the spawned command is the
current binary re-executed with the same arguments minus `--split` (the same
re-exec pattern, and the same `command_prefix` plumbing, that supervisor
spawning already uses):

| Environment | Spawn |
| --- | --- |
| `$TMUX` | `tmux split-window -h\|-v [-b] <shell-quoted cmd>` |
| `$KITTY_WINDOW_ID` | `kitten @ launch --location=vsplit\|hsplit --cwd=current <argv>` |
| `$WEZTERM_PANE` | `wezterm cli split-pane --right\|... -- <argv>` |
| `$ZELLIJ` | `zellij action new-pane -d right\|... -- <argv>` |

Only tmux takes a shell string and therefore needs quoting; the rest take
argv vectors and get them verbatim. No match is a plain error (exit 2) that
names what was looked for, because "silently open in the current pane
instead" would take over the very terminal the agent is talking in, which is
the one surprise this feature exists to avoid. Kitty's remote control is off
by default (`allow_remote_control`); the error for a refused kitten call says
so. Terminal-native splits beyond these four (iTerm2 AppleScript, Windows
Terminal) are out of scope: the four cover tmux users and the three
multiplexing terminals this crate's image protocols already target.

### 12.3 `ls`: what is running

`h5i-nb ls` answers "what sessions exist on this machine", which today cannot
be asked without already knowing each notebook's path. It reads the session
directory (`$XDG_RUNTIME_DIR/h5i-db/nb/`), sends the lock-free `Status`
request to every `*.sock` with a short per-socket timeout, and prints one row
per live session: notebook path, kernel name, idle/busy, cell count,
supervisor pid, idle seconds. `Status` answers mid-cell from the file, so a
busy session lists as readily as an idle one. `--format json` emits the same
rows as the rest of the CLI contract.

A socket that refuses connection is a leftover from a supervisor that died
without cleanup. `ls` confirms that by taking the paired ownership lock
non-blocking (acquirable means no owner; the lock is the liveness authority
precisely so pid recycling cannot lie), unlinks the socket and lock, and
reports what it cleaned. Listing doubles as the sweep, the same way kernel
start sweeps orphaned kernels; the janitor work rides on the command people
run when things look wrong.

Naming: `kernel list` stays what it is (installed kernelspecs, a property of
the machine); `ls` is running sessions (a property of the moment). The
symmetry with `terminal-browser ls` is intentional: it is the discovery verb
an agent tries unprompted.

### 12.4 The skill reference: teach the agent the verbs

The CLI was designed against an agent contract (stable exit codes, error
codes, `--format json`, detach-and-poll), but that contract lives in
`DESIGN.md` §8 and code comments, where no agent harness will find it. The
fix is the same one `terminal-browser` ships: a skill file whose frontmatter
description sells the capability and whose body is the minimal operating
manual.

It lives at `skills/h5i-db/references/notebook.md`, beside the other
references, rather than as a separate skill: h5i-db is one tool with several
surfaces, and an installer that copies `skills/` gets the notebook manual along
with everything else. `skills/h5i-db/SKILL.md` gains a short section that says
when to reach for a notebook at all and links here, because the entry point is
what decides whether this file is ever read. Content, in order of how often an
agent needs it:

1. **The loop**: `new`, `exec --code`, `cells`, `output [--index]`, and the
   one-sentence thesis (state persists between invocations, so probe rather
   than re-run).
2. **Long cells**: `exec --detach`, poll `cells`, fetch `output`; failures
   land in the cell's outputs, so polling is sufficient.
3. **SQL**: `%%sql` runs natively (no Python startup), `--into df` hands the
   frame to Python, `--write` is required before anything can mutate.
4. **Errors**: the code table (code, exit, retryable, meaning), because
   `session_busy`/exit 3/retryable is a different next action than
   `cell_raised`/exit 2/not.
5. **Digest contract**: outputs are token-budgeted, figures come back as
   paths, `--raw` rehydrates; the same summarize-then-rehydrate shape as
   `h5i capture run`.
6. **Showing the human**: `watch --split right` opens a live pane; when to do
   it (built something visual, starting a long run) and that it never blocks
   or steals the session.

Budget: about 150 lines. A skill is loaded into a context window, so it obeys
the same economy as the digest renderer: the untruncated story stays in this
file, the skill carries what changes an agent's next action.

### 12.5 Testing

- **`watch`** through the existing forkpty harness (`tests/tui.rs`): open a
  watch on a file, run `exec` against the same notebook from outside, assert
  the new output paints without a keypress; press mutating keys, assert the
  file's bytes did not change; kill the supervisor, assert the badge degrades
  instead of the pane dying.
- **`ls`** in `tests/cli.rs`: empty directory lists nothing; a started
  session lists with the right busy flag; a hand-planted dead socket is
  cleaned exactly when its lock is acquirable.
- **`--split`**: multiplexer detection and argv construction are pure
  functions over an injected environment, tested as such (including tmux
  quoting). Actually spawning panes is covered by one `#[ignore]` smoke test
  that runs inside a throwaway `tmux -L <tmp>` server when tmux exists, and
  otherwise stays manual: CI owes us the logic, not a terminal zoo.
