# h5i_db.capture

A websocket recorder. It connects to a venue, writes every frame it receives to
disk with the nanosecond it arrived, and does nothing else.

## Why it exists

Kalshi publishes no historical order-book deltas. Third-party archives exist,
but the best free one lags by weeks and its snapshot rows carry no venue
timestamp at all, so queue position cannot be reconstructed from them. If you
want queue-accurate Kalshi data, you have to record it while it happens. There
is no way to obtain it afterwards.

It is a separate package from `h5i_db.venues` on purpose. That package is
parse-only: every function there takes bytes the caller already downloaded,
which is what makes the mapping a pure function testable offline against
recorded payloads. Sockets, credentials and reconnect policy would end that, so
they live here. The two meet at a file format, not at a function call.

## Installing

```sh
pip install 'h5i-db[capture]'
```

The extra pulls in `websockets`, `cryptography` and `lz4`. It is an extra
because most users read archives somebody else recorded and none of them should
have to build a TLS stack to do it. `import h5i_db.capture` works without the
extra: every one of those imports happens inside the function that needs it, so
a missing one names the package to install instead of failing at import time.

## Two rules of the write path

**Stamp arrival, in nanoseconds.** The instant this process saw a frame is the
only clock a backtest can honestly order on. Venue timestamps are the venue's
opinion about a moment you could not have acted on, and they arrive out of
order. An arrival stamp cannot be reconstructed later, so a capture that omits
it is permanently broken. `datetime` only carries microseconds, which is why
the stamp is formatted from an integer rather than routed through one.

**Never normalise.** The payload goes to disk exactly as it came off the
socket. Parsing on the write path means a parser bug costs you the data rather
than an afternoon, because you cannot re-run the recorder over yesterday. A
frame that parses as JSON is spliced into the line as the venue's own bytes, so
`1.50` stays `1.50`; the parse only decides whether it was JSON at all.

## Running it

```sh
h5i-capture \
    --venue polymarket \
    --out ./capture \
    --market 71321045679252212594626385532706912750332728571942532289631379312455583992563
```

```sh
export KALSHI_API_KEY_ID=…                     # never a flag: see below
export KALSHI_PRIVATE_KEY_PATH=~/.kalshi/key.pem
h5i-capture \
    --venue kalshi \
    --out ./capture \
    --market KXPRESPARTY-28-D,KXBTCD-25DEC31
```

Stop it with Ctrl-C (or SIGTERM, which is what a supervisor sends). Either one
flushes the buffer, writes the lz4 end mark, fsyncs and exits.

### Options

| Flag | Default | Meaning |
| --- | --- | --- |
| `--venue` | required | `kalshi` or `polymarket` |
| `--out`, `-o` | required | Root directory to write under |
| `--market` | required | Repeatable, or comma-separated. Kalshi: market ticker. Polymarket: CLOB token id |
| `--channel` | per venue | Kalshi: `orderbook_delta,trade`. Polymarket ignores it, its market socket is one stream |
| `--url` | per venue | Override the endpoint, for a demo environment |
| `--max-backoff-secs` | 60 | Ceiling on reconnect backoff, which doubles from 1s |
| `--flush-secs` | 5 | How often buffered blocks are pushed to the file |
| `--keepalive-secs` | 10 | Kalshi gets a websocket ping, Polymarket the literal `PING` it requires |

`H5I_CAPTURE_LOG` controls log verbosity, defaulting to `INFO`.

### Credentials

Kalshi's websocket is authenticated; Polymarket's market channel is public.

Kalshi signs each handshake with an RSA key pair. Export the key id and the
path to the PEM:

```sh
export KALSHI_API_KEY_ID=8f2a…
export KALSHI_PRIVATE_KEY_PATH=~/.kalshi/key.pem
```

Neither is accepted as a flag: a flag lands in shell history, in `ps` output for
every user on the box, and in whatever supervisor log wraps the command. A
recorder runs for weeks under exactly such a supervisor. The key travels as a
path rather than as a value for the same reason, and because a PEM does not
survive being pasted into an env file intact.

The signature is RSA-PSS over `timestamp_ms + "GET" + path`, SHA-256, MGF1 with
SHA-256, and a **salt length equal to the digest length**. That last one is the
detail worth stating: `cryptography`'s own default salt length is the maximum
the key allows, and a signature made that way fails Kalshi's check with a bare
authentication error that names nothing. The path is the URL's path without its
query string, so `--url` can point at the demo host
(`wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2`) or the newer
`external-api-ws.kalshi.com` host without touching the credential. It goes out
as `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE` and `KALSHI-ACCESS-TIMESTAMP`,
recomputed on every connect attempt because the timestamp is inside the signed
message and a reconnect three hours later cannot reuse it.

`KALSHI_API_TOKEN` still works for an account that was issued a bearer token out
of band.

If a credential is missing, the process exits in its first second rather than
spending a night retrying a 401.

## What lands on disk

```
<out>/<venue>/<YYYY-MM-DD>/<HH>.ndjson.lz4
<out>/<venue>/<YYYY-MM-DD>/<HH>.001.ndjson.lz4    # a restart inside that hour
```

Each file is a single lz4 frame containing newline-delimited JSON, one message
per line. The date and hour are UTC and refer to **arrival**, not to anything
in the payload, so the name is a true statement about the file's contents.

There is a part number because the lz4 frame decoder stops at the first frame's
end mark: a file holding two concatenated frames reads back as only its first
frame, silently. A recorder restarted inside an hour it already wrote therefore
cannot append and must not truncate, so it opens the next part.

Read an hour with `read_hour`, not with a bare glob. Part zero has no suffix, so
`sorted(glob("14*.ndjson.lz4"))` puts `14.001` before `14` and hands you the
hour with its two runs swapped.

### The line format

```json
{"time":"2026-07-31T14:03:11.482913770","ver_num":1,"raw":{"type":"orderbook_delta","seq":48213,"msg":{…}}}
```

* `time` is when this process saw the frame: naive UTC, nine digits, always.
* `raw` is the payload, verbatim. A frame that is not JSON is stored as a JSON
  *string* rather than dropped, because losing nothing outranks tidiness.

This is exactly the envelope Hyperliquid's own archives use, and a unit test
asserts our lines are byte-identical to the ones the Rust archive writer
produces. That is the point: one reader handles both a recording and a vendor
download.

### Markers

The recorder writes its own lifecycle into the stream, on a channel no venue
uses, so a reader classifies them as an unmodelled channel rather than as
corruption:

```json
{"time":"…","ver_num":1,"raw":{"channel":"h5iCapture","data":{"event":"reconnect","gap_nanos":8412330991,"lost_at":"…"}}}
```

| `event` | When | Carries |
| --- | --- | --- |
| `start` | Capture begins | venue, url, markets, channels |
| `reconnect` | After a socket is re-established | `gap_nanos`, `lost_at` |
| `stop` | Clean shutdown | `lines` written |
| `binary` | A binary frame arrived (neither venue sends one today) | base64 of the payload |

The `reconnect` marker is written **on reconnect rather than on disconnect**,
because only then is the length of the hole known. This matters more than it
looks: silently resuming is how a hole becomes invisible. Every marker is a
place where messages may be missing, and for Kalshi the sequence numbers in the
payloads tell you whether any actually were.

## Reading it back

```python
from h5i_db.capture import read_capture, read_hour

lines = read_capture("capture/kalshi/2026-07-31/14.ndjson.lz4")
hour = read_hour("capture/kalshi/2026-07-31", "14")   # every part, in order
```

Both tolerate a file with no end mark by default, which is what a killed
recorder leaves; pass `tolerant=False` to have that refused instead of
recovered.

From a shell, the files are ordinary lz4 frames:

```sh
lz4 -dc capture/kalshi/2026-07-31/14.ndjson.lz4 | jq -c '.raw.type' | sort | uniq -c
lz4 -dc capture/kalshi/2026-07-31/*.ndjson.lz4 | jq -c 'select(.raw.channel=="h5iCapture")'
```

## What survives a crash

A clean stop writes the frame's end mark and fsyncs, so the file is complete.

A `kill -9` does not get that chance. Completed lz4 blocks are flushed to the
file every `--flush-secs`, so a killed recorder leaves a frame that is missing
only its end mark: `read_capture` still recovers every flushed block, and a
half-written trailing line is dropped rather than handed back. Strict readers
refuse such a file, so re-frame it first if you need one. The bound on what is
lost is the flush interval, not the hour.

## Limits

* Arrival stamps come from the wall clock, because a backtest needs an absolute
  epoch and no monotonic clock provides one. An NTP step can therefore make two
  lines non-monotonic; readers should sort rather than trust file order.
* The recorder subscribes to what you pass and does not discover markets. A
  market list that goes stale is your problem, and the `start` marker records
  exactly what was asked for so you can tell later.
* The Kalshi signing scheme is tested against a throwaway key pair, not against
  Kalshi. Every documented property holds (message, algorithm, salt length,
  headers), but nobody here has watched the venue accept one.
