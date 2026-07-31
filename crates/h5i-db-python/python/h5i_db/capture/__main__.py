"""`h5i-capture`: point it at a venue websocket, get hourly lz4 NDJSON.

```text
export KALSHI_API_KEY_ID=…
export KALSHI_PRIVATE_KEY_PATH=~/.kalshi/key.pem
h5i-capture --venue kalshi --out ./capture \
    --market KXPRESPARTY-28-D --market KXBTCD-25DEC31
```

The loop is deliberately dull: connect, subscribe, write every frame with the
nanosecond it arrived, reconnect with backoff when the socket dies, and leave a
marker at every seam so a reader can see where data might be missing.
Everything clever belongs downstream, where it can be re-run.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from ._archive import archive_line, format_archive_time, marker_line, now_nanos
from ._deps import require
from ._venue import Credential, MissingCredential, Venue
from ._writer import CaptureWriter

log = logging.getLogger("h5i_db.capture")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="h5i-capture",
        description=(
            "Record a venue websocket to lz4-compressed newline-delimited JSON. "
            "Credentials are read from the environment, never from a flag: a "
            "flag is visible in `ps` and in supervisor logs for the whole life "
            "of the process."
        ),
    )
    parser.add_argument(
        "--venue",
        required=True,
        choices=[venue.value for venue in Venue],
        help="Venue to record. Kalshi needs credentials; Polymarket is public.",
    )
    parser.add_argument(
        "--out",
        "-o",
        required=True,
        type=Path,
        help="Directory to write under. Files land in <out>/<venue>/<date>/<hour>.",
    )
    parser.add_argument(
        "--market",
        dest="markets",
        action="append",
        default=[],
        required=True,
        help="Market to subscribe to. Repeat, or pass a comma-separated list.",
    )
    parser.add_argument(
        "--channel",
        dest="channels",
        action="append",
        default=[],
        help=(
            "Override the venue's default channels. Ignored by Polymarket, "
            "whose market socket is a single stream selected by token id."
        ),
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Override the websocket endpoint, for a demo or staging environment.",
    )
    parser.add_argument(
        "--max-backoff-secs",
        type=float,
        default=60.0,
        help="Longest wait between reconnect attempts, in seconds.",
    )
    parser.add_argument(
        "--flush-secs",
        type=float,
        default=5.0,
        help=(
            "How often to flush completed lz4 blocks to the file, in seconds. "
            "Bounds what a `kill -9` can destroy, since anything still "
            "buffered when the process dies is gone."
        ),
    )
    parser.add_argument(
        "--keepalive-secs",
        type=float,
        default=10.0,
        help="How often to send a keepalive, in seconds.",
    )
    return parser


def _split(values: Sequence[str]) -> list[str]:
    """Flatten repeated flags that may each hold a comma-separated list.

    Both spellings exist in the wild (a shell loop emits one flag per market, a
    config file emits one line), and accepting only one of them turns a working
    command into an empty subscription.
    """
    out: list[str] = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def _install_shutdown() -> asyncio.Event:
    """A latch that flips once on SIGINT or SIGTERM and stays flipped.

    SIGTERM as well as SIGINT: a recorder lives under a supervisor, and systemd
    stops a unit with SIGTERM. Ignoring it would mean every planned restart
    truncates the current lz4 frame.
    """
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        number = getattr(signal, name, None)
        if number is None:
            continue
        try:
            loop.add_signal_handler(number, stop.set)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - non-unix
            signal.signal(number, lambda *_: loop.call_soon_threadsafe(stop.set))
    return stop


async def _connect(url: str, credential: Credential) -> Any:
    """Open the websocket, signing the handshake if the venue wants one.

    `max_size=None` because a size cap turns a deep orderbook snapshot into a
    closed connection, and the one thing this program must not do is decide a
    message was too big to keep. `ping_interval=None` because the keepalive is
    driven from the read loop, on the interval the operator asked for.
    """
    websockets = require("websockets.asyncio.client", why="connect to a venue")
    return await websockets.connect(
        url,
        additional_headers=credential.headers(url),
        ping_interval=None,
        max_size=None,
    )


async def _pump(
    connection: Any,
    venue: Venue,
    subscribe: Sequence[str],
    writer: CaptureWriter,
    stop: asyncio.Event,
    keepalive_every: float,
) -> Optional[str]:
    """Subscribe, then write every frame until the socket or the operator stops.

    Returns `None` when the operator stopped us and a reason string when the
    socket went away, because those two lead to different next steps: one exits,
    the other reconnects and leaves a marker.
    """
    for frame in subscribe:
        try:
            await connection.send(frame)
        except Exception as error:
            return f"subscribe failed: {error}"

    loop = asyncio.get_running_loop()
    keepalive = venue.keepalive()
    stop_task = asyncio.ensure_future(stop.wait())
    recv_task: Optional[asyncio.Future] = None
    next_keepalive = loop.time() + keepalive_every
    try:
        while True:
            if recv_task is None:
                recv_task = asyncio.ensure_future(connection.recv())
            done, _pending = await asyncio.wait(
                {recv_task, stop_task},
                timeout=max(0.0, next_keepalive - loop.time()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            # Shutdown first: on a busy market the read branch would otherwise
            # win the race indefinitely.
            if stop_task in done:
                return None
            if recv_task not in done:
                next_keepalive = loop.time() + keepalive_every
                if keepalive is not None:
                    try:
                        if keepalive.kind == "ping":
                            await connection.ping()
                        else:
                            await connection.send(keepalive.text)
                    except Exception as error:
                        return f"keepalive failed: {error}"
                # A quiet market should still get its bytes on disk.
                writer.flush()
                continue

            # First statement after the wait on purpose: every line of work
            # between the frame arriving and this call is error in the only
            # timestamp a replay can trust.
            received_at = now_nanos()
            try:
                message = recv_task.result()
            except asyncio.CancelledError:  # pragma: no cover - shutdown race
                raise
            except Exception as error:
                return str(error) or type(error).__name__
            finally:
                recv_task = None

            if isinstance(message, str):
                writer.write_line(received_at, archive_line(received_at, message))
            else:
                # Neither venue sends binary today. Base64 rather than a lossy
                # decode, and a marker rather than a guess at what channel it
                # belongs to: an unexpected frame is still evidence.
                encoded = base64.b64encode(bytes(message)).decode("ascii")
                writer.write_line(
                    received_at,
                    marker_line(received_at, "binary", {"base64": encoded}),
                )
    finally:
        for task in (recv_task, stop_task):
            if task is not None and not task.done():
                task.cancel()


async def run(args: argparse.Namespace) -> int:
    venue = Venue(args.venue)
    url = args.url or venue.default_url()
    markets = _split(args.markets)
    channels = _split(args.channels) or list(venue.default_channels())
    if not markets:
        raise SystemExit("--market listed no markets")
    # Resolved before the first connect so a missing credential fails in the
    # first second rather than after a night of retrying a 401.
    credential = venue.credential()
    subscribe = venue.subscribe_frames(markets, channels)

    writer = CaptureWriter(args.out, venue.value, args.flush_secs)
    stop = _install_shutdown()
    log.info(
        "capture starting: venue=%s url=%s markets=%d out=%s (%s)",
        venue.value,
        url,
        len(markets),
        args.out,
        venue.market_kind(),
    )

    max_backoff = max(1.0, float(args.max_backoff_secs))
    keepalive_every = max(1.0, float(args.keepalive_secs))
    backoff = 1.0
    attempt = 0
    # Set the moment the stream is known to be down, cleared once the marker
    # recording that outage has been written.
    lost_at: Optional[int] = None

    try:
        started = now_nanos()
        writer.write_line(
            started,
            marker_line(
                started,
                "start",
                {
                    "venue": venue.value,
                    "url": url,
                    "markets": markets,
                    "channels": channels,
                },
            ),
        )
        while True:
            attempt += 1
            connection = None
            try:
                connection = await _connect(url, credential)
            except Exception as error:
                if lost_at is None:
                    lost_at = now_nanos()
                log.warning("connect attempt %d failed: %s", attempt, error)
            else:
                backoff, attempt = 1.0, 0
                # Written on reconnect, not on disconnect, because only now is
                # the length of the hole known. Silently resuming is how a hole
                # becomes invisible.
                if lost_at is not None:
                    now = now_nanos()
                    writer.write_line(
                        now,
                        marker_line(
                            now,
                            "reconnect",
                            {
                                "gap_nanos": max(0, now - lost_at),
                                "lost_at": format_archive_time(lost_at),
                            },
                        ),
                    )
                    lost_at = None
                log.info("connected")
                try:
                    why = await _pump(
                        connection, venue, subscribe, writer, stop, keepalive_every
                    )
                finally:
                    await connection.close()
                if why is None:
                    break
                lost_at = now_nanos()
                log.warning("connection lost: %s", why)

            # Interruptible: an operator who hits Ctrl-C during a 60 second
            # backoff should not wait out the backoff.
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff)
                break
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, max_backoff)

        stopped = now_nanos()
        # Counts this marker, so the number is the run total a reader can check
        # against the files this run produced rather than being one short of
        # every one of them.
        writer.write_line(
            stopped, marker_line(stopped, "stop", {"lines": writer.lines + 1})
        )
        path = writer.current_path
    finally:
        writer.close()
    log.info("capture stopped cleanly: lines=%d file=%s", writer.lines, path)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=os.environ.get("H5I_CAPTURE_LOG", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        return asyncio.run(run(args))
    except MissingCredential as error:
        log.error("%s", error)
        return 1
    except KeyboardInterrupt:  # pragma: no cover - the signal handler is first
        return 130


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
