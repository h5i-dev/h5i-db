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
  in hand-rolled clients.

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
h5i-nb export <file> --to md|py|html   # (N5, not built)
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
| N5 | Export, comm plumbing, `h5i-db nb` mount | Not started |

### Known gaps in what is built

- **`exec --detach`** is refused with a clear error rather than silently
  running synchronously. The supervisor exists partly to enable it (it owns
  the notebook, so it can finish recording a cell after the client leaves),
  and the delivered benefit today is narrower: a client killed mid-cell
  still gets its outputs written, because the supervisor, not the client,
  owns the file.
- **Images** are reported as `[image/png]` rather than written to disk and
  referenced by path. The base64 never reaches the terminal either way, so
  the token guarantee holds; what is missing is the ability to then look at
  the picture.
- **Completion and inspection** are reachable from the TUI and over the
  supervisor protocol, but no CLI verb exposes them.
- **Multi-line strings** highlight only on their first line: the lexer treats
  lines independently. Wrong for one keystroke inside a triple-quoted block,
  and not worth a stateful lexer to fix.
- **Export** (`--to md|py|html`) and the `h5i-db nb` subcommand mount are N5.
