#!/usr/bin/env python3
"""Regenerate the differential fixtures for crates/h5i-db-notebook.

Everything under `crates/h5i-db-notebook/tests/fixtures/` is produced by real
Python so the Rust round-trip tests compare against Jupyter's actual writer
rather than against our own idea of it.

    pip install nbformat
    python tools/gen_notebook_fixtures.py

Rerun after upgrading nbformat: if upstream changes how it splits lines or
formats JSON, these fixtures are what tells us.
"""

from __future__ import annotations

import pathlib
import random
import struct
import sys

import nbformat
from nbformat.v4 import (
    new_code_cell,
    new_markdown_cell,
    new_notebook,
    new_output,
    new_raw_cell,
)

ROOT = pathlib.Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "crates/h5i-db-notebook/tests/fixtures"


def notebooks() -> dict[str, object]:
    """Notebooks chosen to exercise each rule in nbformat's writer."""
    cases: dict[str, object] = {}

    cases["plain"] = new_notebook(
        cells=[
            new_markdown_cell("# Title\ntext"),
            new_code_cell(
                "print('hello')",
                execution_count=1,
                outputs=[new_output("stream", name="stdout", text="hello\n")],
            ),
        ]
    )

    # Every output type, plus unicode and a payload with no trailing newline.
    cases["outputs"] = new_notebook(
        cells=[
            new_code_cell(
                "df\n",
                execution_count=7,
                outputs=[
                    new_output("stream", name="stdout", text="a\nb\n"),
                    new_output("stream", name="stderr", text="no trailing newline"),
                    new_output(
                        "execute_result",
                        data={"text/plain": "   x\n0  1", "text/html": "<table>\n</table>"},
                        metadata={"h5i": {"rows": 1}},
                        execution_count=7,
                    ),
                    # image/png must stay on one line; svg and javascript split.
                    new_output(
                        "display_data",
                        data={
                            "image/png": "iVBORw0KGgo=",
                            "image/svg+xml": "<svg>\n</svg>",
                            "application/javascript": "a\nb",
                            "text/plain": "<Figure>",
                        },
                        metadata={"image/png": {"width": 400}},
                    ),
                    new_output(
                        "error",
                        ename="ValueError",
                        evalue="bad",
                        traceback=["Traceback:", "  line 1", "ValueError: bad"],
                    ),
                ],
            ),
            new_raw_cell("raw 日本語 content"),
        ]
    )

    # JSON mime payloads are structured values and must never be line-split.
    cases["jsonmime"] = new_notebook(
        cells=[
            new_code_cell(
                "v",
                execution_count=None,
                outputs=[
                    new_output(
                        "display_data",
                        data={
                            "application/json": {"a": [1, 2], "b": {"c": "x\ny"}},
                            "application/vnd.vega.v5+json": {"$schema": "https://vega", "data": []},
                            "text/plain": "one\ntwo\n",
                        },
                    )
                ],
            )
        ]
    )

    # Empty and boundary-shaped sources.
    cases["edges"] = new_notebook(
        cells=[
            new_code_cell(""),
            new_code_cell("\n"),
            new_code_cell("x"),
            new_markdown_cell(""),
            new_code_cell("a\r\nb"),
        ]
    )

    # Fields we do not model must survive a load/save cycle untouched.
    nb = new_notebook(cells=[new_code_cell("1", execution_count=2)])
    nb.metadata["colab"] = {"provenance": [], "collapsed_sections": ["a"]}
    nb.metadata["custom_thing"] = {"nested": {"deep": [1, 2.5, True, None]}}
    nb.cells[0].metadata["jupyter"] = {"source_hidden": True}
    nb.cells[0]["some_future_key"] = {"z": 1}
    cases["extensions"] = nb

    # Number formatting: Rust and Python disagree on presentation by default.
    cases["numbers"] = new_notebook(
        cells=[
            new_code_cell(
                "f",
                execution_count=1,
                outputs=[
                    new_output(
                        "display_data",
                        data={
                            "application/json": {
                                "small": 0.1,
                                "neg": -1.5e-8,
                                "big": 1e300,
                                "int": 9007199254740993,
                                "one": 1.0,
                                "zero": 0.0,
                                "third": 1 / 3,
                                "tie": 3236844372569.78125,
                            }
                        },
                    )
                ],
            )
        ]
    )

    # A markdown cell with attachments, which have their own mime bundles.
    nb = new_notebook(cells=[new_markdown_cell("![](attachment:a.png)")])
    nb.cells[0]["attachments"] = {"a.png": {"image/png": "AAAA", "text/plain": "x\ny"}}
    cases["attachments"] = nb

    return cases


def floats() -> list[float]:
    """A corpus wide enough that presentation bugs cannot hide in it."""
    random.seed(20260803)
    vals = [
        0.0, -0.0, 1.0, -1.0, 0.1, 1 / 3, 1e15, 1e16, 1e-4, 1e-5, 5e-324,
        1.7976931348623157e308, 2.2250738585072014e-308, 1e300, 1e-300,
        123456789012345678.0, 1e21, 0.0001, 1e-7, 2.5e-10, 9.87654321e22,
        3.141592653589793, 2.718281828459045, 6.02214076e23, 1.602176634e-19,
        3236844372569.78125,  # exact midpoint: half-to-even tie-break
    ]
    for e in range(-320, 309):
        try:
            vals.append(float(f"1e{e}"))
        except OverflowError:
            pass
    while len(vals) < 4000:
        v = struct.unpack("<d", struct.pack("<Q", random.getrandbits(64)))[0]
        if v == v and abs(v) != float("inf"):
            vals.append(v)
    for _ in range(2000):
        vals.append(round(random.uniform(-1e6, 1e6), random.randint(0, 12)))
    return vals


def main() -> int:
    canonical = FIXTURES / "canonical"
    canonical.mkdir(parents=True, exist_ok=True)
    for name, nb in notebooks().items():
        nbformat.write(nb, str(canonical / f"{name}.ipynb"), version=nbformat.NO_CONVERT)
    print(f"wrote {len(notebooks())} notebooks to {canonical}")

    corpus = FIXTURES / "float_repr.txt"
    vals = floats()
    with corpus.open("w") as f:
        for v in vals:
            bits = struct.unpack("<Q", struct.pack("<d", v))[0]
            f.write(f"{bits:016x}\t{v!r}\n")
    print(f"wrote {len(vals)} floats to {corpus}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
