"""Every Python symbol the agent skill names must exist.

`docs_are_executable.rs` extracts each `h5i-db …` invocation from `skills/` and
checks it against `--help`, so the CLI reference cannot go stale. The Python
reference had no such guard, and after the backtest, quant and venues surfaces
landed it became the largest thing in the skill with nothing holding it to the
code. A skill that names a function which no longer exists is worse than one that
omits it: an agent will call it and fail.

So this walks the skill's fenced Python blocks, pulls out every attribute access
on a module the skill imports, and resolves it for real. It is deliberately
conservative: it only checks what it can attribute unambiguously to a module or
to a documented class, and prints what it skipped so the coverage is visible
rather than assumed.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import h5i_db
from h5i_db import backtest, quant, venues

SKILLS = Path(__file__).resolve().parents[4] / "skills" / "h5i-db"

#: Aliases the skill uses in examples, mapped to what they actually are.
MODULES = {
    "h5i_db": h5i_db,
    "backtest": backtest,
    "quant": quant,
    "venues": venues,
}

#: Attribute access on these is checked against the class, not a module.
CLASSES = {
    "db": h5i_db.Database,
    "result": backtest.BacktestResult,
    "search": backtest.StudyResult,
    "signal_plan": backtest.SignalPlan,
    "inspection": backtest.PreflightInspection,
    "report": venues.IngestReport,
    "plan": h5i_db.MutationPlan,
}

#: Names bound inside an example rather than by an import, so an attribute on
#: them tells us nothing about the API. Recorded rather than silently ignored.
LOCALS = {
    "panel",
    "series",
    "config",
    "specs",
    "spec",
    "layout",
    "adv",
    "fit",
    "commands",
    "positions",
    "run",
    "study",
    "expr",
    "col",
    "frame",
    "self",
    "pd",
    "pa",
    "np",
    "cu",
    "plt",
    "json",
    "Path",
    "datetime",
    "dt",
}


def _python_blocks(text: str) -> list[str]:
    return re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)


def _skill_files() -> list[Path]:
    files = [SKILLS / "SKILL.md"] + sorted((SKILLS / "references").glob("*.md"))
    return [path for path in files if path.exists()]


def _attribute_root(node: ast.Attribute) -> tuple[str, list[str]] | None:
    """The base name of an attribute chain plus the attributes, outermost last."""
    parts: list[str] = []
    current: ast.expr = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    return current.id, list(reversed(parts))


def test_the_skill_directory_is_where_this_test_thinks_it_is():
    """Guard the path assumption, so a repo move fails loudly here."""
    assert SKILLS.is_dir(), f"expected the skill at {SKILLS}"
    assert (SKILLS / "SKILL.md").exists()
    assert _skill_files(), "no markdown found in the skill"


def test_every_module_symbol_the_skill_names_resolves():
    checked: list[str] = []
    missing: list[str] = []
    skipped: set[str] = set()

    for path in _skill_files():
        for block in _python_blocks(path.read_text(encoding="utf-8")):
            try:
                tree = ast.parse(block)
            except SyntaxError:
                # Fragments are legitimate in prose (a bare verb list, say).
                skipped.add(f"{path.name}: unparseable fragment")
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Attribute):
                    continue
                root = _attribute_root(node)
                if root is None:
                    continue
                base, attrs = root
                if base in MODULES:
                    owner: object = MODULES[base]
                    trail = base
                    for attr in attrs:
                        if not hasattr(owner, attr):
                            missing.append(f"{path.name}: {trail}.{attr}")
                            break
                        trail = f"{trail}.{attr}"
                        checked.append(trail)
                        owner = getattr(owner, attr)
                elif base in CLASSES:
                    owner = CLASSES[base]
                    attr = attrs[0]
                    # Dataclass fields and properties resolve on instances, so a
                    # class-level miss is only a miss when annotations agree.
                    annotations = getattr(owner, "__annotations__", {})
                    if not hasattr(owner, attr) and attr not in annotations:
                        missing.append(f"{path.name}: {base}.{attr} ({owner.__name__})")
                    else:
                        checked.append(f"{base}.{attr}")
                elif base not in LOCALS:
                    skipped.add(f"{base} (unknown base name)")

    print(f"\nresolved {len(set(checked))} distinct symbols from {len(_skill_files())} files")
    if skipped:
        print("not checked: " + ", ".join(sorted(skipped)))
    assert not missing, "the skill names symbols that do not exist:\n  " + "\n  ".join(
        sorted(set(missing))
    )
    # A guard against the guard: if the extractor stops finding anything, this
    # test would pass vacuously.
    assert len(set(checked)) > 40, f"only {len(set(checked))} symbols checked; extractor broken?"


def test_config_section_fields_named_in_the_skill_are_real():
    """The backtest reference tabulates every config field. Hold it to the code."""
    from dataclasses import fields

    text = (SKILLS / "references" / "backtest.md").read_text(encoding="utf-8")
    sections = {
        "DataConfig": backtest.DataConfig,
        "ExecutionConfig": backtest.ExecutionConfig,
        "PortfolioConfig": backtest.PortfolioConfig,
        "RiskConfig": backtest.RiskConfig,
        "OutputConfig": backtest.OutputConfig,
    }
    for name, cls in sections.items():
        row = next(
            (line for line in text.splitlines() if line.startswith(f"| `{name}`")),
            None,
        )
        assert row is not None, f"{name} is not tabulated in backtest.md"
        documented = set(re.findall(r"`([a-z_]+)`", row.split("|", 2)[2]))
        actual = {item.name for item in fields(cls)}
        assert documented == actual, (
            f"{name}: documented {sorted(documented)} != actual {sorted(actual)}"
        )


def test_cli_verbs_named_in_the_skill_exist():
    """`python -m h5i_db.…` invocations, which the Rust doc test does not see."""
    import argparse
    import contextlib
    import io as _io

    documented: dict[str, set[str]] = {}
    for path in _skill_files():
        for match in re.finditer(
            r"python -m (h5i_db\.[a-z_]+) ([a-z-]+)", path.read_text(encoding="utf-8")
        ):
            documented.setdefault(match.group(1), set()).add(match.group(2))
    assert documented, "no `python -m h5i_db.…` invocations found in the skill"

    for module_name, verbs in documented.items():
        module = __import__(module_name, fromlist=["main"])
        if not hasattr(module, "main"):
            # A package exposes its CLI through __main__, a module through itself.
            # Both are spelled `python -m <name>`, so accept either.
            module = __import__(f"{module_name}.__main__", fromlist=["main"])
        for verb in sorted(verbs):
            buffer = _io.StringIO()
            with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
                with pytest.raises(SystemExit) as exit_info:
                    module.main([verb, "--help"])
            assert exit_info.value.code == 0, (
                f"{module_name} {verb}: not a real subcommand ({buffer.getvalue()[:200]})"
            )


def test_skill_flags_for_the_venues_cli_exist():
    """Flags the on-ramp reference promises, checked against the parser."""
    import contextlib
    import io as _io

    from h5i_db.venues import __main__ as venues_cli

    text = (SKILLS / "references" / "data-onramp.md").read_text(encoding="utf-8")
    for verb, flag in re.findall(r"venues (\w+)[^\n]*?(--[a-z-]+)", text):
        buffer = _io.StringIO()
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            with pytest.raises(SystemExit):
                venues_cli.main([verb, "--help"])
        assert flag in buffer.getvalue(), f"venues {verb} has no {flag}"


def test_error_tree_named_in_python_md_exists_with_its_attributes():
    """python.md lists the exception subclasses and three attributes."""
    import builtins

    text = (SKILLS / "references" / "python.md").read_text(encoding="utf-8")
    # Digits are allowed: the base class is `H5iError`. Builtins are excluded,
    # because the prose legitimately names `AttributeError` when describing what
    # a bad import raises.
    named = {
        name
        for name in re.findall(r"\b([A-Z][A-Za-z0-9]*Error)\b", text)
        if not hasattr(builtins, name)
    }
    assert "H5iError" in named, "python.md should name the base error"
    for name in sorted(named):
        assert hasattr(h5i_db, name), f"python.md names {name}, which does not exist"
        assert issubclass(getattr(h5i_db, name), Exception)

    # `.code`, `.retryable` and `.hint` resolve on instances, not on the class,
    # so a class-level check would pass while the documented usage failed.
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/probe.db", create=True)
        try:
            with pytest.raises(h5i_db.H5iError) as caught:
                db.sql("SELECT * FROM table_that_does_not_exist")
            for attribute in ("code", "retryable", "hint"):
                assert hasattr(caught.value, attribute), (
                    f"python.md documents .{attribute} on H5iError instances"
                )
        finally:
            db.close()


def test_the_import_nuance_python_md_warns_about_is_still_true():
    """`backtest`/`venues` bind on import; `quant` needs naming explicitly.

    Documented because an agent writing `import h5i_db` then
    `h5i_db.quant.tearsheet(...)` gets an AttributeError. If the package ever
    starts importing `quant` eagerly, the warning becomes wrong and this fails.
    """
    import subprocess
    import sys

    package_root = str(Path(h5i_db.__file__).resolve().parents[1])
    probe = (
        "import h5i_db;"
        "print(hasattr(h5i_db,'backtest'), hasattr(h5i_db,'venues'),"
        " hasattr(h5i_db,'quant'))"
    )
    # A subprocess, because any earlier `from h5i_db import quant` in this
    # session would have bound the attribute and hidden the behaviour.
    out = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        env={"PYTHONPATH": package_root, "PATH": "/usr/bin:/bin"},
        check=True,
    ).stdout.split()
    assert out == ["True", "True", "False"], (
        f"python.md's import note no longer matches reality: {out}"
    )


def test_console_scripts_python_md_mentions_match_the_packaging():
    """The two entry points named in python.md must be declared in pyproject."""
    pyproject = Path(h5i_db.__file__).resolve().parents[2] / "pyproject.toml"
    if not pyproject.exists():  # installed wheel rather than a source checkout
        pytest.skip("not a source checkout")
    declared = pyproject.read_text(encoding="utf-8")
    text = (SKILLS / "references" / "python.md").read_text(encoding="utf-8")
    for script in re.findall(r"`(h5i-[a-z]+)`", text):
        assert f"{script} =" in declared, (
            f"python.md mentions the {script} console script; pyproject does not "
            "declare it"
        )
