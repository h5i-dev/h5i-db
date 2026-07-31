"""A websocket recorder: connect to a venue, write every frame with the
nanosecond it arrived, and do nothing else.

```python
from h5i_db.capture import CaptureWriter, archive_line, now_nanos

with CaptureWriter("./capture", "kalshi", flush_after=5.0) as writer:
    received_at = now_nanos()
    writer.write_line(received_at, archive_line(received_at, frame))
```

Reading one back:

```python
from h5i_db.capture import read_hour

lines = read_hour("./capture/kalshi/2026-07-31", "14")
```

It is a separate package from `h5i_db.venues` on purpose. That package is
parse-only: every function there takes bytes the caller already downloaded,
which is what makes the mapping a pure function testable offline against
recorded payloads. Sockets, credentials and reconnect policy would end that, so
they live here. The two meet at a file format, not at a function call.

The recorder's dependencies (`websockets`, `cryptography`, `lz4`) are an extra:
`pip install 'h5i-db[capture]'`. Importing this package without them works;
every one of them is imported inside the function that needs it, so a user who
only reads archives never pays for a TLS stack.
"""

from __future__ import annotations

from ._archive import (
    ARCHIVE_VERSION,
    MARKER_CHANNEL,
    archive_line,
    format_archive_time,
    marker_line,
    now_nanos,
)
from ._venue import (
    KALSHI_DEMO_URL,
    KALSHI_KEY_ID_ENV,
    KALSHI_PRIVATE_KEY_ENV,
    KALSHI_TOKEN_ENV,
    BearerToken,
    Credential,
    KalshiKeyPair,
    Keepalive,
    MissingCredential,
    NoCredential,
    Venue,
    kalshi_headers,
    sign_kalshi,
)
from ._writer import CaptureWriter, read_capture, read_capture_text, read_hour

__all__ = [
    "ARCHIVE_VERSION",
    "MARKER_CHANNEL",
    "KALSHI_DEMO_URL",
    "KALSHI_KEY_ID_ENV",
    "KALSHI_PRIVATE_KEY_ENV",
    "KALSHI_TOKEN_ENV",
    "BearerToken",
    "CaptureWriter",
    "Credential",
    "KalshiKeyPair",
    "Keepalive",
    "MissingCredential",
    "NoCredential",
    "Venue",
    "archive_line",
    "format_archive_time",
    "kalshi_headers",
    "marker_line",
    "now_nanos",
    "read_capture",
    "read_capture_text",
    "read_hour",
    "sign_kalshi",
]
