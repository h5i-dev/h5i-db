"""The on-disk envelope: one JSON object per line, arrival stamp first.

The shape is Hyperliquid's, on purpose. `h5i_db.venues` already reads that
envelope, and a format with one reader is worth more than a format tuned to one
venue.

```json
{"time":"2026-07-31T14:03:11.482913770","ver_num":1,"raw":{ … }}
```

`time` is when *this process* saw the frame, not when the venue says it
happened; `raw` is the payload verbatim.
"""

from __future__ import annotations

import datetime as _datetime
import json
import time
from typing import Any, Mapping

__all__ = [
    "ARCHIVE_VERSION",
    "MARKER_CHANNEL",
    "archive_line",
    "format_archive_time",
    "marker_line",
    "now_nanos",
]

#: Envelope version. Bump only if `time`/`raw` change meaning, since a reader
#: keys its interpretation off this and old files never get rewritten.
ARCHIVE_VERSION = 1

#: The channel name reserved for recorder markers. Not a venue channel, so a
#: reader classifies these lines as an unmodelled channel rather than as
#: corruption.
MARKER_CHANNEL = "h5iCapture"

_EPOCH = _datetime.datetime(1970, 1, 1, tzinfo=_datetime.timezone.utc)

# Compact, and non-ASCII left as UTF-8 rather than escaped. Both choices are
# what serde_json emits, and byte equality with the Rust writer is the only
# thing that keeps one reader able to open a recording and a vendor download.
_COMPACT = {"separators": (",", ":"), "ensure_ascii": False}


def now_nanos() -> int:
    """Nanoseconds since the Unix epoch, right now.

    Wall clock rather than a monotonic counter: a backtest needs an absolute
    epoch to join against venue data, and no monotonic clock provides one. The
    cost is that an NTP step can make two lines non-monotonic, which is why
    readers must sort rather than assume file order.
    """
    return time.time_ns()


def format_archive_time(received_at: int) -> str:
    """Format an arrival stamp the way the archives do: naive UTC, nine digits.

    Nine digits always, including trailing zeros. `datetime` only carries
    microseconds, so the nanosecond remainder is appended as text rather than
    formatted: routing the stamp through a microsecond type would round away
    the three digits that distinguish two frames in the same millisecond, which
    is exactly the resolution a queue-position study needs.
    """
    seconds, nanos = divmod(int(received_at), 1_000_000_000)
    try:
        stamp = _EPOCH + _datetime.timedelta(seconds=seconds)
    except OverflowError:
        # A stamp outside the representable range means the clock is wrong, not
        # that the frame is worthless. Falling back to the epoch keeps the line
        # writable and leaves the anomaly visible in the file.
        stamp = _EPOCH
    return f"{stamp.strftime('%Y-%m-%dT%H:%M:%S.')}{nanos:09d}"


def archive_line(received_at: int, payload: str) -> str:
    """Wrap one websocket text frame for the archive.

    Infallible by design. A frame that is not JSON is stored as a JSON *string*
    rather than rejected: the recorder's job is to lose nothing, and a venue
    that starts emitting an unparseable heartbeat must not be able to end the
    capture. Readers see a string where they expected an object and skip it,
    which is a diagnosable outcome; a dropped line is not.
    """
    return _envelope(received_at, _raw_body(payload))


def marker_line(received_at: int, event: str, data: Mapping[str, Any] | None = None) -> str:
    """A recorder-generated line: connection lifecycle, not market data.

    Shaped as a Hyperliquid-style `{channel, data}` body so existing readers
    classify it as a channel they do not model (skipped, counted) rather than
    as a corrupt line.
    """
    body: dict[str, Any] = {"event": event}
    if data:
        body.update(data)
    raw = json.dumps({"channel": MARKER_CHANNEL, "data": body}, **_COMPACT)
    return _envelope(received_at, raw)


def _raw_body(payload: str) -> str:
    """The `raw` field's bytes for one frame.

    Valid JSON is spliced in as the venue sent it rather than parsed and
    re-emitted. Re-serialising would silently rewrite `1.50` to `1.5` and
    reorder nothing but still cost a full parse on the write path, and the
    whole point of this recorder is that a parser bug costs an afternoon rather
    than the data. The parse here decides only *whether* the payload is JSON;
    its result is thrown away.

    Two payloads cannot be spliced. One that is not JSON becomes a JSON string,
    so nothing is dropped. One that is JSON but contains a literal newline
    (a venue pretty-printing its frames) is re-emitted compactly, because
    splicing it would end the line early and turn one message into two
    unparseable ones.
    """
    text = payload.strip()
    if text:
        try:
            value = json.loads(text)
        except ValueError:
            pass
        else:
            if "\n" not in text and "\r" not in text:
                return text
            return json.dumps(value, **_COMPACT)
    return json.dumps(payload, **_COMPACT)


def _envelope(received_at: int, raw: str) -> str:
    # Assembled as text rather than dumped from a dict so `raw` keeps the bytes
    # `_raw_body` decided on. Key order is fixed here for the same reason the
    # Rust writer fixes it: `time`, `ver_num`, `raw` is what the Hyperliquid
    # archives emit, and equality is only meaningful byte for byte.
    return (
        f'{{"time":"{format_archive_time(received_at)}",'
        f'"ver_num":{ARCHIVE_VERSION},'
        f'"raw":{raw}}}'
    )
