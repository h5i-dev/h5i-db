---
title: Installation
description: Install the h5i-db CLI and Python library, or build both from source.
order: 1
---

# Installation

h5i-db ships as two installable artifacts backed by the same Rust core: the
`h5i-db` command-line tool and the `h5i_db` Python library. Install either or
both — they read and write the same database directories.

## CLI

```console
$ cargo install h5i-db-cli
$ h5i-db --version
```

The binary is self-contained; no runtime dependencies, no server to start.
Requires a Rust toolchain ([rustup.rs](https://rustup.rs)) to build during
install.

## Python

```console
$ pip install h5i-db
```

```python
import h5i_db
print(h5i_db.__version__)
```

The wheel bundles the native engine — no separate CLI install needed. The only
required dependency is `pyarrow >= 14`; `to_pandas()` and `to_polars()` work
when pandas/Polars are present.

## Build from source

```console
$ git clone https://github.com/h5i-dev/h5i-db
$ cd h5i-db
$ cargo build --release -p h5i-db-cli          # CLI -> target/release/h5i-db
$ pip install maturin
$ maturin develop -m crates/h5i-db-python/Cargo.toml --release   # Python module
```

!!! note "Filesystem requirements"
    Crash-safety relies on POSIX `fsync` and atomic rename. Keep databases on a
    local filesystem (ext4, xfs, apfs, NTFS). On WSL2 use the Linux side
    (`~/data/…`), not `/mnt/c`. Network filesystems are not recommended — see
    the [Operations guide](operations.html#filesystem-caveats).

## Verifying the install

```console
$ h5i-db init /tmp/smoke.db
$ h5i-db tables /tmp/smoke.db
$ python -c "import h5i_db; h5i_db.Database('/tmp/smoke.db')"
```

Next: the [Quickstart](quickstart.html).
