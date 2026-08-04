# Notebooks — a kernel that outlives the command

`h5i-db nb <command>`, or the standalone `h5i-nb <command>`. Both drive the
same session, so it does not matter which you use.

The kernel persists between invocations. `exec` twice and the second cell sees
what the first defined, so a 40-second load is paid once rather than once per
idea. That is the whole reason to reach for this: if you would run a script
once and never again, run the script.

```bash
h5i-db nb new research.ipynb --kernel python3
h5i-db nb exec research.ipynb --code "import pandas as pd; df = pd.read_parquet('big.parquet')"
h5i-db nb exec research.ipynb --code "df.shape"          # df is still there
h5i-db nb exec research.ipynb --code "df.groupby('venue').size()"
```

`--code -` reads the cell from stdin, which is how you send code containing
quotes or newlines without fighting the shell:

```bash
h5i-db nb exec research.ipynb --code - <<'PY'
def edge(book):
    return book.bid - book.ask
PY
```

The file is a real `.ipynb`. A human opens it in JupyterLab, and it is the
durable record of what the exploration actually did.

## Reading what happened

```bash
h5i-db nb cells research.ipynb              # index, id, type, count, output shape
h5i-db nb output research.ipynb 7           # one cell's outputs (index or cell id)
h5i-db nb output research.ipynb 7 --index 1 # one output, when a cell produced several
```

Outputs are budgeted, not dumped: long text is elided in the middle, frames are
summarised by shape, and images are written to `<file>_files/` and reported as
paths rather than inlined as base64. Nothing is lost — the untruncated output
stays in the `.ipynb` and `--raw` prints it. Reach for `--raw` when you need
the bytes, not by default; the digest is what makes a notebook cost less
context than a script, and `--raw` gives that back.

## Long-running cells

`--detach` returns as soon as the cell exists. The session owns the notebook,
so outputs are recorded whether or not anyone is still watching.

```bash
h5i-db nb exec research.ipynb --detach --code "backtest(years=10)"   # → queued cell 7
h5i-db nb cells research.ipynb                                       # poll
h5i-db nb output research.ipynb 7
```

A detached cell that fails writes the failure into its own outputs, so polling
`cells` and `output` is enough to learn what happened: there is never a state
where "still running" and "died" look the same. Poll about once a second, not
in a tight loop.

```bash
h5i-db nb kernel interrupt research.ipynb   # stop the cell, keep the state
h5i-db nb kernel restart research.ipynb     # throw the state away
h5i-db nb kernel status research.ipynb      # answers even mid-cell
```

## SQL without Python in the way

A cell starting `%%sql` runs against an h5i-db database in-process: no
interpreter, no driver, roughly a tenth of the cost of the Python equivalent.
Schema discovery and the first twenty exploratory queries never pay for a
kernel at all.

```bash
h5i-db nb exec research.ipynb --code - <<'SQL'
%%sql --db market.db
SELECT venue, count(*) AS fills FROM trades GROUP BY venue ORDER BY fills DESC
SQL
```

Magic-line options: `--db <path>`, `--fork <name>`, `--max-rows N`,
`--into <name>`, `--write`.

- `--into df` binds the **full** result in the Python kernel as a DataFrame,
  handed over as Arrow rather than CSV. The rendered table is capped; the
  binding is not.
- `--write` is required before a cell can modify anything. Without it the
  database is opened read-only, which also means opening it never runs
  recovery against a database another process may be using. Add it
  deliberately, not by habit.
- `--fork` scopes the cell to a fork, the same way `--fork` does elsewhere
  (→ [forks.md](forks.md)).

Set a notebook-wide default with `h5i-db nb new … --db market.db`, and the
`%%sql` cells need no `--db` at all.

## Errors

Failures print the usual envelope on stderr with a stable `code`. Match on the
code, not the wording.

| code | exit | retryable | meaning |
| --- | --- | --- | --- |
| `cell_raised` | 2 | no | the code raised; the traceback is on stdout and in the cell |
| `execute_timeout` | 4 | yes | hit `--timeout`; the cell was interrupted and the kernel still works |
| `session_busy` | 3 | yes | another cell is running; wait, or interrupt it |
| `kernel_died` | 5 | yes | the kernel is gone; restart, and the state is lost |
| `kernel_not_found` | 2 | no | no such kernelspec; `nb kernel list` shows what exists |
| `cell_index_out_of_range` | 2 | no | `nb cells` shows what exists |

`cell_raised` is the ordinary outcome of exploring, not a tool failure. The
traceback is the answer.

## Editing and re-running

```bash
h5i-db nb edit research.ipynb set 3 --code "fixed()"     # replaces cell 3, drops its stale outputs
h5i-db nb edit research.ipynb insert --at 2 --code "…"   # also delete, move, clear-outputs
h5i-db nb run research.ipynb --cells 3-7                 # re-run a range in order
h5i-db nb run research.ipynb --from-clean                # restart, clear, run everything
```

`run --from-clean` is the reproducibility check: if it passes, the notebook
tells the truth about what produces its outputs. Edits are routed through the
running session automatically, so they are safe while a kernel is alive.

## Showing a human what you are doing

```bash
h5i-db nb watch research.ipynb --split right
```

`watch` is a live, read-only view. It writes nothing, holds no lock, and starts
no kernel, so it can never interfere with the session you are driving, and any
number of them can be open at once. `--split right|left|down|up` puts it in a
pane beside the human (tmux, zellij, WezTerm, kitty) and returns immediately,
so it costs you nothing to keep working.

Worth doing when you are starting a long run, or when the work has become
visual and showing it beats describing it. The pane updates itself as cells
finish and follows whatever changed last. From it a human can interrupt the
cell, and nothing else.

```bash
h5i-db nb ls          # every notebook session running on this machine
```

`ls` is how you find out what is already alive before starting something new.
It also clears up after sessions whose supervisor died.

## Exporting

```bash
h5i-db nb export research.ipynb --to md     # a readable summary for a PR or a message
h5i-db nb export research.ipynb --to py     # a runnable script, magics commented out
h5i-db nb export research.ipynb --to html   # one self-contained file, images inlined
```

## Two habits worth keeping

- **One session, many questions.** The value is state accumulating; a fresh
  notebook per idea is just a slower script.
- **Leave the notebook readable.** It is the artifact a human opens to check
  what you did, and the reason this exists rather than a REPL.
