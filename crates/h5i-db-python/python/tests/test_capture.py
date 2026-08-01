"""Tests for the websocket recorder.

Every claim here was a bug first. The line format, the single lz4 frame, the
part number, the arrival-hour rolling and the Kalshi salt length are each a
rule that costs data when it is broken, and none of them fails loudly on its
own: a second frame in a file reads back as silence, a rounded stamp reads back
as a plausible number, a wrong salt length reads back as "unauthorized".
"""

from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import os
import signal
import time

import pytest

from h5i_db.capture import (
    ARCHIVE_VERSION,
    KALSHI_KEY_ID_ENV,
    KALSHI_PRIVATE_KEY_ENV,
    KALSHI_TOKEN_ENV,
    BearerToken,
    CaptureWriter,
    KalshiKeyPair,
    MissingCredential,
    NoCredential,
    Venue,
    archive_line,
    format_archive_time,
    kalshi_headers,
    marker_line,
    now_nanos,
    read_capture,
    read_capture_text,
    read_hour,
    sign_kalshi,
)

# The writer needs the [capture] extra. Skipping without it keeps a plain
# checkout green, and CI installs the extra so these still run there: the
# lz4 framing rule they pin fails silently, so it must not go unchecked.
requires_lz4 = pytest.mark.skipif(
    importlib.util.find_spec("lz4") is None,
    reason="lz4 comes with the [capture] extra: pip install 'h5i-db[capture]'",
)

# 2025-01-01T00:00:02.238437296Z, the stamp the Rust tests use.
STAMP = 1_735_689_602_238_437_296
# 2025-01-01T00:00:02Z and 2025-01-01T01:00:02Z.
HOUR_00 = 1_735_689_602_000_000_000
HOUR_01 = 1_735_693_202_000_000_000


# --------------------------------------------------------------------------
# The line format
# --------------------------------------------------------------------------


def test_a_line_is_byte_identical_to_the_rust_writer():
    """The envelope is copied from the Hyperliquid archives, not invented.

    The expected string is the one `h5i_db_venues::hyperliquid::archive_line`
    produced for this input, copied from the Rust crate's own test rather than
    regenerated here. If the two ever diverge, files this recorder writes stop
    being readable by the archive reader and nobody finds out until replay.
    """
    payload = '{"channel":"trades","data":[{"coin":"BTC","px":"1.5"}]}'
    assert archive_line(STAMP, payload) == (
        '{"time":"2025-01-01T00:00:02.238437296","ver_num":1,'
        '"raw":{"channel":"trades","data":[{"coin":"BTC","px":"1.5"}]}}'
    )


def test_stamps_keep_nanosecond_digits_that_a_microsecond_format_would_round_away():
    # datetime carries microseconds. Routing the stamp through it and calling
    # isoformat() would silently drop "296", which is the resolution that
    # distinguishes two frames inside one microsecond.
    assert format_archive_time(STAMP) == "2025-01-01T00:00:02.238437296"
    assert format_archive_time(1_000_000_000) == "1970-01-01T00:00:01.000000000"


def test_a_non_json_frame_is_stored_rather_than_dropped():
    line = archive_line(1_000_000_000, "PONG")
    parsed = json.loads(line)
    assert parsed["raw"] == "PONG"
    assert parsed["ver_num"] == ARCHIVE_VERSION


def test_the_payload_goes_to_disk_verbatim():
    # Number formatting a re-serialiser would normalise away: 1.50 is not 1.5
    # to anyone comparing this capture against the venue's own record of it.
    payload = '{"px":1.50,"sz":0.000100,"seq":9007199254740993}'
    assert archive_line(STAMP, payload).endswith(f'"raw":{payload}}}')


def test_a_pretty_printed_frame_is_compacted_rather_than_splitting_the_line():
    # Splicing this verbatim would end the NDJSON line at the first newline and
    # turn one message into several unparseable ones.
    payload = '{\n  "type": "hello"\n}'
    line = archive_line(STAMP, payload)
    assert "\n" not in line
    assert json.loads(line)["raw"] == {"type": "hello"}


def test_markers_carry_their_fields_on_a_channel_no_venue_uses():
    line = marker_line(0, "reconnect", {"gap_nanos": 8412330991, "lost_at": "x"})
    parsed = json.loads(line)
    assert parsed["raw"]["channel"] == "h5iCapture"
    assert parsed["raw"]["data"]["event"] == "reconnect"
    assert parsed["raw"]["data"]["gap_nanos"] == 8412330991
    assert parsed["raw"]["data"]["lost_at"] == "x"
    # Key order is part of the format, not an accident of the dict.
    assert line.startswith('{"time":"1970-01-01T00:00:00.000000000","ver_num":1,"raw":')
    assert '"channel":"h5iCapture","data":{"event":"reconnect"' in line


def test_now_nanos_is_nanoseconds_not_scaled_milliseconds():
    before = time.time_ns()
    value = now_nanos()
    assert before <= value <= time.time_ns()
    # A stamp built from time.time() * 1e9 would end in zeros far more often
    # than one in a thousand; this only checks the magnitude is right.
    assert value > 1_700_000_000_000_000_000


# --------------------------------------------------------------------------
# The writer
# --------------------------------------------------------------------------


@requires_lz4
def test_a_written_file_round_trips(tmp_path):
    payloads = [
        '{"type":"orderbook_delta","seq":48213}',
        '{"type":"trade","px":1.50}',
        "PONG",
    ]
    stamps = [STAMP, STAMP + 1, STAMP + 2]
    with CaptureWriter(tmp_path, "kalshi", 3600.0) as writer:
        for stamp, payload in zip(stamps, payloads):
            writer.write_line(stamp, archive_line(stamp, payload))
        path = writer.current_path

    lines = read_capture(path, tolerant=False)
    assert [line["ver_num"] for line in lines] == [ARCHIVE_VERSION] * 3
    assert [line["time"] for line in lines] == [
        "2025-01-01T00:00:02.238437296",
        "2025-01-01T00:00:02.238437297",
        "2025-01-01T00:00:02.238437298",
    ]
    assert lines[0]["raw"]["seq"] == 48213
    assert lines[2]["raw"] == "PONG"
    # Verbatim, all the way through the file: 1.50 survives the round trip as
    # text even though json.loads renders it back as 1.5.
    assert '"px":1.50' in read_capture_text(path)


@requires_lz4
def test_lines_land_in_the_hour_they_arrived_in(tmp_path):
    writer = CaptureWriter(tmp_path, "kalshi", 3600.0)
    writer.write_line(HOUR_00, "a")
    writer.write_line(HOUR_01, "b")
    writer.close()

    day = tmp_path / "kalshi" / "2025-01-01"
    assert read_capture_text(day / "00.ndjson.lz4", tolerant=False) == "a\n"
    assert read_capture_text(day / "01.ndjson.lz4", tolerant=False) == "b\n"
    assert writer.lines == 2


@requires_lz4
def test_flushing_mid_file_leaves_one_frame_that_still_reads_whole(tmp_path):
    # A zero flush interval flushes after every line. If a flush ever ended the
    # frame instead of the block, the decoder would stop at the first line and
    # this test would say so: it reads back as truncation with no error.
    writer = CaptureWriter(tmp_path, "polymarket", 0.0)
    for index in range(4):
        writer.write_line(HOUR_00, f"line{index}")
    writer.close()
    path = tmp_path / "polymarket" / "2025-01-01" / "00.ndjson.lz4"
    assert read_capture_text(path, tolerant=False) == "line0\nline1\nline2\nline3\n"


def test_a_second_frame_in_a_file_would_read_back_as_silence(tmp_path):
    """The failure the part number exists to prevent, demonstrated.

    Two frames concatenated into one file decode as only the first, with no
    error anywhere. This is why a restart opens a new part instead of appending.
    """
    lz4 = pytest.importorskip("lz4.frame")
    path = tmp_path / "two-frames.lz4"
    path.write_bytes(lz4.compress(b"first\n") + lz4.compress(b"second\n"))
    assert read_capture_text(path) == "first\n"


@requires_lz4
def test_a_restart_inside_the_same_hour_opens_the_next_part(tmp_path):
    for line in ["first", "second", "third"]:
        writer = CaptureWriter(tmp_path, "kalshi", 3600.0)
        writer.write_line(HOUR_00, line)
        writer.close()

    day = tmp_path / "kalshi" / "2025-01-01"
    assert read_capture_text(day / "00.ndjson.lz4", tolerant=False) == "first\n"
    assert read_capture_text(day / "00.001.ndjson.lz4", tolerant=False) == "second\n"
    assert read_capture_text(day / "00.002.ndjson.lz4", tolerant=False) == "third\n"


@requires_lz4
def test_read_hour_sees_every_part(tmp_path):
    for index in range(3):
        writer = CaptureWriter(tmp_path, "kalshi", 3600.0)
        stamp = HOUR_00 + index
        writer.write_line(stamp, archive_line(stamp, json.dumps({"n": index})))
        writer.close()
    lines = read_hour(tmp_path / "kalshi" / "2025-01-01", "00")
    assert [line["raw"]["n"] for line in lines] == [0, 1, 2]


@requires_lz4
def test_a_killed_capture_still_yields_its_flushed_blocks(tmp_path):
    """Simulates kill -9: flush, then never close, so the frame has no end mark.

    The claim being tested is that the loss is bounded by the flush interval
    rather than being the whole file.
    """
    writer = CaptureWriter(tmp_path, "kalshi", 0.0)
    writer.write_line(HOUR_00, "kept")
    writer.flush()
    path = writer.current_path
    # Drop the writer's state on the floor without closing the frame, the way a
    # SIGKILL does. Closing the file descriptor only is what the kernel does.
    os.close(writer._open.handle.fileno())
    writer._open = None

    assert read_capture_text(path) == "kept\n"
    with pytest.raises(ValueError, match="no lz4 end mark"):
        read_capture_text(path, tolerant=False)


@requires_lz4
def test_writing_after_a_close_opens_a_fresh_part(tmp_path):
    # `close` is idempotent and leaves the writer usable, which is what makes
    # the shutdown path safe to call from both the signal handler and __del__.
    writer = CaptureWriter(tmp_path, "kalshi", 3600.0)
    writer.write_line(HOUR_00, "a")
    writer.close()
    writer.close()
    writer.write_line(HOUR_00, "b")
    writer.close()
    day = tmp_path / "kalshi" / "2025-01-01"
    assert read_capture_text(day / "00.ndjson.lz4", tolerant=False) == "a\n"
    assert read_capture_text(day / "00.001.ndjson.lz4", tolerant=False) == "b\n"


# --------------------------------------------------------------------------
# Venues
# --------------------------------------------------------------------------


def test_kalshi_subscribes_by_ticker_and_channel():
    frames = Venue.KALSHI.subscribe_frames(["KXTEST"], ["orderbook_delta"])
    parsed = json.loads(frames[0])
    assert parsed["cmd"] == "subscribe"
    assert parsed["params"]["market_tickers"] == ["KXTEST"]
    assert parsed["params"]["channels"] == ["orderbook_delta"]


def test_polymarket_subscribes_by_token_and_needs_no_credential():
    frames = Venue.POLYMARKET.subscribe_frames(["12345"], [])
    parsed = json.loads(frames[0])
    assert parsed["type"] == "market"
    assert parsed["assets_ids"] == ["12345"]
    credential = Venue.POLYMARKET.credential({})
    assert isinstance(credential, NoCredential)
    assert credential.headers(Venue.POLYMARKET.default_url()) == {}


def test_polymarket_wants_an_application_keepalive_and_kalshi_a_protocol_ping():
    # Not interchangeable: Polymarket's gateway ignores a protocol ping and
    # closes the socket within seconds.
    assert Venue.POLYMARKET.keepalive().kind == "text"
    assert Venue.POLYMARKET.keepalive().text == "PING"
    assert Venue.KALSHI.keepalive().kind == "ping"


def test_a_missing_kalshi_credential_fails_immediately():
    with pytest.raises(MissingCredential, match=KALSHI_KEY_ID_ENV):
        Venue.KALSHI.credential({})


def test_half_a_key_pair_is_not_a_credential():
    with pytest.raises(MissingCredential, match=KALSHI_PRIVATE_KEY_ENV):
        Venue.KALSHI.credential({KALSHI_KEY_ID_ENV: "abc"})


def test_a_bearer_token_is_still_accepted():
    credential = Venue.KALSHI.credential({KALSHI_TOKEN_ENV: "tok"})
    assert isinstance(credential, BearerToken)
    assert credential.headers("wss://x/y") == {"Authorization": "Bearer tok"}


# --------------------------------------------------------------------------
# Kalshi request signing
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_key():
    """A throwaway key pair. No real credential is needed to test the scheme."""
    rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def test_kalshi_signing_produces_the_documented_headers(rsa_key):
    url = "wss://api.elections.kalshi.com/trade-api/ws/v2?ignored=1"
    headers = kalshi_headers("key-id", rsa_key, url, timestamp_ms=1735689602238)
    assert set(headers) == {
        "KALSHI-ACCESS-KEY",
        "KALSHI-ACCESS-SIGNATURE",
        "KALSHI-ACCESS-TIMESTAMP",
    }
    assert headers["KALSHI-ACCESS-KEY"] == "key-id"
    assert headers["KALSHI-ACCESS-TIMESTAMP"] == "1735689602238"
    # Base64, not hex and not raw bytes.
    base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"], validate=True)


def test_the_signature_verifies_against_the_public_key(rsa_key):
    """The whole scheme, checked the way the venue checks it.

    Salt length is the part worth pinning: `cryptography`'s default is the
    maximum the key allows, and Kalshi verifies with the digest length. The
    difference is invisible locally and shows up as a bare 401.
    """
    padding = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.padding")
    hashes = pytest.importorskip("cryptography.hazmat.primitives.hashes")

    timestamp, method, path = 1735689602238, "GET", "/trade-api/ws/v2"
    signature = base64.b64decode(sign_kalshi(rsa_key, timestamp, method, path))
    rsa_key.public_key().verify(
        signature,
        f"{timestamp}{method}{path}".encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )


def test_the_signed_message_is_timestamp_then_method_then_path(rsa_key):
    padding = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.padding")
    hashes = pytest.importorskip("cryptography.hazmat.primitives.hashes")
    exceptions = pytest.importorskip("cryptography.exceptions")

    signature = base64.b64decode(sign_kalshi(rsa_key, 1, "GET", "/trade-api/ws/v2"))
    # Any other concatenation of the same three pieces must fail, or the test
    # above would pass for a signature over the wrong string.
    for wrong in (b"GET1/trade-api/ws/v2", b"1/trade-api/ws/v2GET"):
        with pytest.raises(exceptions.InvalidSignature):
            rsa_key.public_key().verify(
                signature,
                wrong,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )


def test_the_library_default_salt_length_would_not_verify(rsa_key):
    """Guards against someone "simplifying" the padding to the library default.

    The check has to run this way round. Verifying with `MAX_LENGTH` accepts a
    digest-length signature too, because OpenSSL recovers the salt length from
    the signature; verifying with `DIGEST_LENGTH` does not. So the test signs
    with the default this code deliberately does *not* use and shows that a
    verifier configured the way Kalshi's is rejects it.
    """
    padding = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.padding")
    hashes = pytest.importorskip("cryptography.hazmat.primitives.hashes")
    exceptions = pytest.importorskip("cryptography.exceptions")

    message = b"1GET/x"
    default_salt = rsa_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    digest_verifier = padding.PSS(
        mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH
    )
    with pytest.raises(exceptions.InvalidSignature):
        rsa_key.public_key().verify(default_salt, message, digest_verifier, hashes.SHA256())
    # Ours does verify under exactly that verifier.
    rsa_key.public_key().verify(
        base64.b64decode(sign_kalshi(rsa_key, 1, "GET", "/x")),
        message,
        digest_verifier,
        hashes.SHA256(),
    )


def test_a_key_pair_credential_signs_the_url_path(tmp_path, rsa_key):
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    pem = rsa_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_file = tmp_path / "kalshi.pem"
    key_file.write_bytes(pem)

    credential = Venue.KALSHI.credential(
        {KALSHI_KEY_ID_ENV: "key-id", KALSHI_PRIVATE_KEY_ENV: str(key_file)}
    )
    assert isinstance(credential, KalshiKeyPair)
    headers = credential.headers("wss://external-api-ws.kalshi.com/trade-api/ws/v2")
    assert headers["KALSHI-ACCESS-KEY"] == "key-id"
    assert "KALSHI-ACCESS-SIGNATURE" in credential.sensitive

    padding = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.padding")
    hashes = pytest.importorskip("cryptography.hazmat.primitives.hashes")
    rsa_key.public_key().verify(
        base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
        f"{headers['KALSHI-ACCESS-TIMESTAMP']}GET/trade-api/ws/v2".encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )


def test_an_unreadable_key_path_fails_in_the_first_second(tmp_path):
    with pytest.raises(MissingCredential, match="cannot be read"):
        Venue.KALSHI.credential(
            {
                KALSHI_KEY_ID_ENV: "key-id",
                KALSHI_PRIVATE_KEY_ENV: str(tmp_path / "nope.pem"),
            }
        )


# --------------------------------------------------------------------------
# The CLI surface
# --------------------------------------------------------------------------


def test_market_flags_repeat_and_split_on_commas():
    from h5i_db.capture.__main__ import _parser, _split

    args = _parser().parse_args(
        ["--venue", "kalshi", "-o", "/tmp/x", "--market", "A,B", "--market", "C"]
    )
    assert _split(args.markets) == ["A", "B", "C"]
    assert args.max_backoff_secs == 60.0
    assert args.flush_secs == 5.0
    assert args.keepalive_secs == 10.0


@requires_lz4
def test_the_recorder_records_markers_frames_and_a_reconnect(tmp_path):
    """The whole loop, against a socket that drops once.

    The reconnect marker is the reason this test exists. It is written when the
    socket comes back, not when it goes away, because only then is the length
    of the hole known, and a recorder that resumes silently turns a hole into
    something nobody can see afterwards.
    """
    pytest.importorskip("websockets.asyncio.server")
    asyncio.run(_record_against_a_flaky_server(tmp_path))

    files = sorted(tmp_path.rglob("*.ndjson.lz4"))
    assert files, "the recorder wrote nothing"
    lines = read_capture(files[0])

    markers = [
        line["raw"]["data"] for line in lines if line["raw"].get("channel") == "h5iCapture"
    ]
    events = [marker["event"] for marker in markers]
    assert events[0] == "start"
    assert events[-1] == "stop"
    assert "reconnect" in events

    start = markers[0]
    assert start["venue"] == "polymarket"
    assert start["markets"] == ["12345"]

    reconnect = markers[events.index("reconnect")]
    # The gap is a duration, not a moment: it is what a reader needs to decide
    # whether the missing window matters.
    assert reconnect["gap_nanos"] > 0
    assert reconnect["lost_at"].startswith("20")

    payloads = [line["raw"] for line in lines if line["raw"].get("channel") != "h5iCapture"]
    assert [payload["n"] for payload in payloads] == [1, 2]
    # `stop` counts itself, so the number is the run total rather than one short.
    assert markers[-1]["lines"] == len(lines)
    # The path is a true statement about the file: it names the date and hour
    # its own first line arrived in, taken from that line rather than from a
    # second reading of the clock.
    arrived = lines[0]["time"]
    assert files[0].parent.name == arrived[:10]
    assert files[0].name.startswith(f"{arrived[11:13]}.")


async def _record_against_a_flaky_server(tmp_path):
    from websockets.asyncio.server import serve

    from h5i_db.capture.__main__ import _parser, run

    connections = 0

    async def handler(connection):
        nonlocal connections
        connections += 1
        # Read the subscribe frame first: sending before the client has
        # subscribed would only test the server's timing, not the recorder's.
        await connection.recv()
        await connection.send(json.dumps({"n": connections}))
        if connections == 1:
            # Drop the first client, so the recorder has an outage to describe.
            await asyncio.sleep(0.1)
            await connection.close()
            return
        await asyncio.sleep(5)

    server = await serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    args = _parser().parse_args(
        [
            "--venue",
            "polymarket",
            "-o",
            str(tmp_path),
            "--market",
            "12345",
            "--url",
            f"ws://127.0.0.1:{port}/ws",
            "--flush-secs",
            "0",
            "--keepalive-secs",
            "1",
        ]
    )
    recorder = asyncio.ensure_future(run(args))
    try:
        while connections < 2:
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.2)
        # The real shutdown path: the signal a supervisor sends, handled by the
        # latch the recorder installs rather than by KeyboardInterrupt.
        os.kill(os.getpid(), signal.SIGTERM)
        assert await asyncio.wait_for(recorder, timeout=10) == 0
    finally:
        if not recorder.done():
            recorder.cancel()
        server.close()
        await server.wait_closed()


def test_the_cli_exposes_every_flag_the_rust_recorder_had():
    from h5i_db.capture.__main__ import _parser

    options = {
        option for action in _parser()._actions for option in action.option_strings
    }
    assert {
        "--venue",
        "--out",
        "-o",
        "--market",
        "--channel",
        "--url",
        "--max-backoff-secs",
        "--flush-secs",
        "--keepalive-secs",
    } <= options
