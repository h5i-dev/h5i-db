---
title: Notebooks
description: In-terminal Jupyter notebooks whose kernel outlives the command, with %%sql cells that skip the interpreter entirely.
order: 4.5
seo_title: "h5i-db notebooks: a kernel that outlives the command"
---

# Notebooks

`h5i-db nb` is a notebook *client* for the terminal: it owns a real
`.ipynb` file, drives a real Jupyter kernel, and renders results for two
audiences at once — a human in a TUI, and a program reading a
token-budgeted digest on stdout. The same commands are available on a
standalone binary, `h5i-nb`, which drives the same sessions.

The property everything else follows from: **the kernel persists between
invocations.** `exec` twice and the second cell sees what the first defined,
so a forty-second load is paid once instead of once per idea. If you would
run a script once and never again, run the script; this is for the case
where state accumulates.

```console
$ h5i-db nb new research.ipynb --kernel python3 --db market.db
$ h5i-db nb exec research.ipynb --code "import pandas as pd; df = pd.read_parquet('big.parquet')"
$ h5i-db nb exec research.ipynb --code "df.shape"          # df is still there
```

The file is nbformat v4.5 and stays readable in JupyterLab. Writes are
atomic (temp file, fsync, rename) and byte-compatible with `nbformat`'s own
writer, so opening a notebook in JupyterLab does not churn the diff.

## Running cells

`exec` appends a cell and runs it. `run` re-runs cells the notebook already
has.

```console
$ h5i-db nb exec research.ipynb --code "edge(book)"
$ h5i-db nb exec research.ipynb --code - <<'PY'
def edge(book):
    return book.bid - book.ask
PY
$ h5i-db nb run research.ipynb --cells 3-7
$ h5i-db nb run research.ipynb --from-clean
```

`--code -` (the default) reads the cell from stdin, which is how code
containing quotes and newlines gets in without fighting the shell.

| `exec` flag | Meaning |
|---|---|
| `--code <src>` | Cell source, or `-` for stdin (default `-`) |
| `--timeout <secs>` | Interrupt the cell after this many seconds; `0` disables (default 300) |
| `--stream` | Print output as it arrives rather than only at the end |
| `--detach` | Return as soon as the cell is queued |
| `--raw` | Do not elide long output |

| `run` flag | Meaning |
|---|---|
| `--cells <sel>` | `3`, `3-7`, `3-`, `1,4,9`, or `all` (default `all`) |
| `--from-clean` | Restart the kernel and clear outputs first |
| `--keep-going` | Keep going after a cell raises |
| `--timeout`, `--raw` | As in `exec` |

`run --from-clean` is the reproducibility check: if it passes, the notebook
tells the truth about what produced its outputs.

## Output is budgeted, and nothing is lost

Cell output is summarised on stdout, not dumped:

- **Text** is elided in the middle: head lines, tail lines, and a
  `… 4,912 lines elided …` marker between them.
- **Frames** are re-rendered as a compact aligned table — first and last
  rows plus a `[10,000 rows x 12 columns]` shape line. For the next
  decision, the shape is usually the whole information content.
- **Images** are never inlined as base64. They are written to
  `<notebook>_files/cell-<id>-<n>.png` (nbconvert's convention, so
  JupyterLab renders them too) and reported as a path.
- **Tracebacks** are printed in full, ANSI stripped. A tool that discards
  the traceback on error forces a re-run to find out what broke.

The untruncated output stays in the `.ipynb`. `--raw` prints it, and
`nb output --save <path>` writes a binary output to a file.

```console
$ h5i-db nb cells research.ipynb          # index, id, type, exec count, output shape
 #  id        type  exec  outputs                                           source
 0  a7822631  code  1     result(application/vnd.h5i.table+text+text/html)  %%sql
 1  5afe1a3f  code  2     -                                                 x = 41
 2  e00300f7  code  3     result                                            x + 1

$ h5i-db nb output research.ipynb 2            # index or cell id
$ h5i-db nb output research.ipynb 2 --index 1  # one output, when a cell made several
$ h5i-db nb output research.ipynb 2 --raw
```

Reach for `--raw` when you need the bytes. The digest is what makes a
notebook cost less context than a script, and `--raw` gives that back.

## `%%sql`: querying without an interpreter

A cell whose first line is `%%sql` runs against an h5i-db database
**in-process**. The magic is resolved by the client, not by the kernel, so
the statement never crosses into Python: no interpreter, no driver round
trip. Schema discovery and the first twenty exploratory queries need no
kernel at all.

```console
$ h5i-db nb exec research.ipynb --code - <<'SQL'
%%sql
SELECT symbol, count(*) AS n FROM trades GROUP BY symbol ORDER BY n DESC
SQL
[1] cell 0 · ok · 3ms
+--------+-----+
| symbol | n   |
+--------+-----+
| AAPL   | 120 |
| MSFT   | 120 |
+--------+-----+
[2 rows x 2 columns]
```

| Magic option | Meaning |
|---|---|
| `--db <path>` | Database for this cell; `--database` is accepted too |
| `--fork <name>` | Query inside a [fork](concepts.html#forks) |
| `--into <name>` | Bind the result in the Python kernel under this name |
| `--max-rows <N>` | Cap the *rendered* table |
| `--write` | Open the database read-write for this cell |

`h5i-db nb new … --db market.db` records a notebook-wide default in the
file's own metadata, so it travels with the notebook and the `%%sql` cells
need no `--db`.

Two options are worth reading twice:

- **`--into` binds the full result, not the rendered one.** The handoff is
  Arrow through a temp file, so `--max-rows 1 --into frame` prints one row
  and still binds all of them.

  ```console
  $ h5i-db nb exec research.ipynb --code - <<'SQL'
  %%sql --into frame --max-rows 1
  SELECT symbol, price FROM trades LIMIT 5
  SQL
  [5] cell 4 · ok · 1ms
  stderr: note: kept the first 1 rows of a larger result (--max-rows)
  …
  frame: pandas.DataFrame 5 rows x 2 columns
  ```

- **`--write` is required before a cell can modify anything.** Without it
  the database is opened read-only, which also means opening it never runs
  transaction recovery against a database another process may be using. Add
  it deliberately, not by habit.

The syntax deliberately matches IPython's `%%sql`, so the notebook still
reads correctly in JupyterLab even though JupyterLab would run the cell a
different way.

## Long-running cells

`--detach` returns as soon as the cell is queued. The session process owns
the notebook, so outputs are recorded whether or not anyone is still
attached — including the failure, if it fails.

```console
$ h5i-db nb exec research.ipynb --detach --code "backtest(years=10)"
$ h5i-db nb cells research.ipynb              # poll, about once a second
$ h5i-db nb output research.ipynb 7
```

There is never a state where "still running" and "died" look the same.

```console
$ h5i-db nb kernel interrupt research.ipynb   # stop the cell, keep the state
$ h5i-db nb kernel restart research.ipynb     # throw the state away
$ h5i-db nb kernel status research.ipynb      # answers even mid-cell
```

| `kernel` subcommand | Meaning |
|---|---|
| `kernel list` | Installed kernelspecs |
| `kernel start <nb>` | Start the session and its kernel without running anything |
| `kernel status <nb>` | Whether a session is running, and what the kernel is doing |
| `kernel interrupt <nb>` | Interrupt the running cell, keeping in-memory state |
| `kernel restart <nb>` | Restart the kernel; `--clear-outputs` also drops recorded outputs |
| `kernel stop <nb>` | Stop the session and its kernel |

### Which interpreter a kernel name resolves to

`--kernel python3` is a name, not a path, and the same name usually exists in
more than one place. The search order is Jupyter's own: `JUPYTER_PATH`, then
the active environment's `share/jupyter` and `~/.local/share/jupyter` (or
`~/Library/Jupyter`), then `/usr/local/share/jupyter` and `/usr/share/jupyter`.

Inside a virtualenv or a non-base conda env, the environment comes first, so a
project venv that has `ipykernel` installed runs cells with that venv's
packages even when a `python3` spec in your home directory points at another
interpreter. Set `JUPYTER_PREFER_ENV_PATH=0` to put the home directory back on
top, or `=1` to force the environment when there is none active.

`nb kernel list --format json` prints the `kernel.json` each name resolved to,
which is the quickest answer to "why does this import fail here but not in my
shell".

!!! note "Why a supervisor, not `--existing`"
    A supervisor process owns the kernel, subscribes to iopub continuously,
    and serves a control socket under `$XDG_RUNTIME_DIR`. Reconnecting to a
    kernel's connection file per invocation, the way `jupyter console
    --existing` does, loses any output produced while no client was
    attached — ZMQ PUB drops messages with no subscriber — which is exactly
    the long-running cell output that matters most.

## Editing without running

```console
$ h5i-db nb edit research.ipynb set 3 --code "fixed()"
$ h5i-db nb edit research.ipynb insert --at 2 --kind markdown --code "## Findings"
$ h5i-db nb edit research.ipynb delete 4
$ h5i-db nb edit research.ipynb move 4 1
$ h5i-db nb edit research.ipynb clear-outputs
```

`set` drops the cell's now-stale outputs. `insert` appends when `--at` is
omitted and takes `--kind code|markdown|raw` (default `code`). Edits are
routed through the running session, so they are safe while a kernel is
alive.

## Showing a human what you are doing

```console
$ h5i-db nb view research.ipynb                # editable TUI
$ h5i-db nb watch research.ipynb --split right # live, read-only
$ h5i-db nb ls                                 # sessions running on this machine
```

`view` is the full terminal UI: Jupyter's key bindings (`Esc`/`Enter` for
command and edit mode, `a`/`b` to insert, `dd` to delete), a completion
popup from `complete_request`, an inspection overlay from `inspect_request`,
and inline plots. `Ctrl-C` interrupts the running cell rather than killing
the UI, because the notebook is the state.

`watch` is a live read-only view. It writes nothing, holds no lock, and
starts no kernel, so any number of them can follow a session someone else is
driving; from it a human can interrupt the cell and nothing else. Both take
`--split right|left|down|up`, which opens the view in a new pane beside the
current one (tmux, zellij, WezTerm, kitty) and returns immediately.

`ls` reports every notebook session on the machine with its kernel, state,
cell count, pid and idle time, and clears up after sessions whose supervisor
died.

!!! note "`Shift+Enter` may not reach the TUI"
    Most terminals cannot send it: without the kitty keyboard protocol it
    arrives as a plain `Enter`. The protocol is requested where it is
    supported, and `e` / `E` are the run bindings that always work.

### Inline plots

A `image/png` output is drawn in the terminal, at the figure's own aspect
ratio. Which escape sequence is used depends on what the terminal answers
when asked: the kitty graphics protocol (kitty, ghostty, WezTerm), iTerm2's
inline images (iTerm2, WezTerm, Konsole), or sixel (Windows Terminal 1.22+,
foot, xterm, mlterm, recent VS Code). A terminal with none of them gets
unicode half-blocks, which are coarse but readable.

The terminal is asked rather than guessed at from `TERM_PROGRAM` and friends,
because those variables do not survive the trip into WSL: under Windows
Terminal, environment sniffing sees a bare `xterm-256color` and would draw
nothing.

Set `H5I_NB_IMAGES` to override:

```console
$ H5I_NB_IMAGES=sixel h5i-db nb view research.ipynb   # force a protocol
$ H5I_NB_IMAGES=off h5i-db nb view research.ipynb     # placeholders only
```

Accepted values are `kitty`, `iterm2`, `sixel`, `halfblocks`, and `off`.
With `off`, each plot becomes a `[image/png · N bytes]` label and the figure
is still reachable through `h5i-db nb output --save`.

## Exporting

```console
$ h5i-db nb export research.ipynb --to md      # a readable summary for a PR
$ h5i-db nb export research.ipynb --to py      # a runnable script, magics commented out
$ h5i-db nb export research.ipynb --to html    # one self-contained file, images inlined
$ h5i-db nb export research.ipynb --to md -o notes.md --without-outputs
```

`--without-outputs` leaves the outputs out, for a clean diff or a script
meant to be run rather than read.

## Introspection from a shell

The two requests the TUI uses are also commands, which is what lets an
editor or a script ask the kernel what it knows:

```console
$ h5i-db nb inspect research.ipynb "pd.read_parquet"    # what `?` does in IPython
$ h5i-db nb complete research.ipynb "df.gro"            # completions at the cursor
```

Both take `--cursor <offset>` when the cursor is not at the end of the code.

## Errors

Failures print the usual [structured envelope](cli.html#errors-and-exit-codes)
on stderr. Branch on `code`, not on the wording.

| `code` | Exit | Retryable | Meaning |
|---|---|---|---|
| `cell_raised` | 2 | no | The code raised; the traceback is on stdout and in the cell |
| `execute_timeout` | 4 | yes | Hit `--timeout`; the cell was interrupted and the kernel still works |
| `session_busy` | 3 | yes | Another cell is running; wait, or interrupt it |
| `kernel_died` | 5 | yes | The kernel is gone; restart, and the state is lost |
| `kernel_not_found` | 2 | no | No such kernelspec; `nb kernel list` shows what exists |
| `cell_index_out_of_range` | 2 | no | `nb cells` shows what exists |

`cell_raised` is the ordinary outcome of exploring, not a tool failure. The
traceback is the answer.

## What does not render

ipywidgets do not draw. Comm traffic is accepted so a widget-producing
library does not crash the session, but nothing interactive appears.
Markdown in exported HTML is shown as preformatted text rather than parsed.
Syntax highlighting is a per-line lexer, so a multi-line string highlights
only on its first line.
