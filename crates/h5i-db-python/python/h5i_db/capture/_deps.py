"""Lazy imports for the recorder's third-party dependencies.

Sockets, compression and crypto are an extra rather than a dependency, because
the overwhelming majority of `h5i_db` users read files somebody else recorded
and should not pay for a TLS stack to do it. That means every one of these
imports happens inside a function, and it means a missing one must say what to
install: an unadorned `ModuleNotFoundError: No module named 'lz4'` sends a user
looking for the wrong package.
"""

from __future__ import annotations

import importlib
from typing import Any

#: Which distribution supplies each top-level package, with the version the
#: extra pins. Keyed on the top-level name so a failed
#: `cryptography.hazmat.primitives.serialization` still names `cryptography`
#: rather than repeating a dotted path nobody can pip install.
_PROVIDED_BY = {
    "lz4": "lz4>=4",
    "cryptography": "cryptography>=42",
    "websockets": "websockets>=12",
}


def require(module: str, *, why: str) -> Any:
    """Import `module`, or raise an error naming the extra that provides it.

    `why` completes the sentence "… is needed to …", so the message tells a
    reader which capability they lose rather than only which import failed.
    """
    try:
        return importlib.import_module(module)
    except ImportError as error:  # pragma: no cover - exercised by hand
        top_level = module.split(".")[0]
        package = _PROVIDED_BY.get(top_level, top_level)
        raise ImportError(
            f"{module} ({package}) is needed to {why}. Install the recorder's "
            "dependencies with: pip install 'h5i-db[capture]'"
        ) from error
