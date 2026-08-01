"""Per-venue connection details: endpoint, subscribe frames, credentials.

Everything venue-specific about *fetching* lives here, which is the line this
package exists to draw. Nothing here interprets a payload.
"""

from __future__ import annotations

import base64
import enum
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import urlsplit

from ._deps import require

__all__ = [
    "BearerToken",
    "Credential",
    "KALSHI_DEMO_URL",
    "KALSHI_KEY_ID_ENV",
    "KALSHI_PRIVATE_KEY_ENV",
    "KALSHI_TOKEN_ENV",
    "KalshiKeyPair",
    "Keepalive",
    "MissingCredential",
    "NoCredential",
    "Venue",
    "kalshi_headers",
    "sign_kalshi",
]

#: Kalshi credential environment variables.
#:
#: Environment variables and not flags: a flag lands in the shell history, in
#: `ps` output for every user on the box, and in whatever process supervisor log
#: wraps the command. A recorder tends to run for weeks under exactly such a
#: supervisor. A key *path* rather than the key itself for the same reason, and
#: because a PEM does not survive being pasted into an env file intact.
KALSHI_KEY_ID_ENV = "KALSHI_API_KEY_ID"
KALSHI_PRIVATE_KEY_ENV = "KALSHI_PRIVATE_KEY_PATH"
KALSHI_TOKEN_ENV = "KALSHI_API_TOKEN"

#: Kalshi's demo environment, for a dry run against a market that costs
#: nothing. Pass it with `--url`; the signed path is the same either way.
KALSHI_DEMO_URL = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"

#: The path Kalshi's websocket handshake is signed over. Taken from the URL at
#: connect time rather than hardcoded, so `--url` pointing at the demo host or
#: at the newer `external-api-ws` host still signs the path that host serves.
_KALSHI_METHOD = "GET"


class MissingCredential(RuntimeError):
    """A required credential was not in the environment."""


@dataclass(frozen=True)
class Keepalive:
    """How to keep a connection from being dropped for idleness.

    `kind` is `"ping"` for a websocket-protocol ping frame or `"text"` for a
    literal the venue expects at the application layer. The two are not
    interchangeable: a venue that wants the literal ignores the protocol ping
    and closes the socket anyway.
    """

    kind: str
    text: Optional[str] = None


class Credential:
    """Headers to add to the websocket handshake, if the venue needs any."""

    def headers(self, url: str) -> dict[str, str]:
        raise NotImplementedError

    @property
    def sensitive(self) -> tuple[str, ...]:
        """Header names that must never be logged, even on a failed handshake."""
        return ()


class NoCredential(Credential):
    """A public channel. Kept as a type so callers need no `None` branch."""

    def headers(self, url: str) -> dict[str, str]:
        return {}


@dataclass(frozen=True)
class BearerToken(Credential):
    """A token obtained out of band.

    Kept because some Kalshi setups still hand out a bearer token, and because
    it costs nothing: the key-pair path below is what a current account gets.
    """

    token: str

    def headers(self, url: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    @property
    def sensitive(self) -> tuple[str, ...]:
        return ("Authorization",)


@dataclass(frozen=True)
class KalshiKeyPair(Credential):
    """Kalshi's RSA key-pair request signing.

    The account holder generates a key pair, Kalshi keeps the public half, and
    every request carries a fresh signature over `timestamp + method + path`.
    Fresh per request matters here in a way it does not for a bearer token: the
    timestamp is inside the signed message, so the headers cannot be computed
    once at startup and reused across a reconnect three hours later. They are
    built per connect attempt.
    """

    key_id: str
    private_key: Any

    @classmethod
    def from_pem(cls, key_id: str, pem: bytes) -> "KalshiKeyPair":
        serialization = require(
            "cryptography.hazmat.primitives.serialization",
            why="load the Kalshi RSA private key",
        )
        return cls(key_id=key_id, private_key=serialization.load_pem_private_key(pem, password=None))

    @classmethod
    def from_path(cls, key_id: str, path: str | os.PathLike[str]) -> "KalshiKeyPair":
        location = Path(path).expanduser()
        try:
            pem = location.read_bytes()
        except OSError as error:
            raise MissingCredential(
                f"{KALSHI_PRIVATE_KEY_ENV} points at {location}, which cannot be "
                f"read: {error}"
            ) from error
        try:
            return cls.from_pem(key_id, pem)
        except ValueError as error:
            raise MissingCredential(
                f"{location} is not a PEM private key Kalshi can sign with: {error}"
            ) from error

    def headers(self, url: str) -> dict[str, str]:
        return kalshi_headers(self.key_id, self.private_key, url)

    @property
    def sensitive(self) -> tuple[str, ...]:
        return ("KALSHI-ACCESS-SIGNATURE",)


def sign_kalshi(private_key: Any, timestamp_ms: int, method: str, path: str) -> str:
    """Sign one Kalshi request, base64 of an RSA-PSS/SHA-256 signature.

    The parameters are Kalshi's, not defaults: PSS with MGF1(SHA-256) and a
    salt length equal to the digest length. `cryptography`'s own default salt
    length is the maximum the key allows, which produces a signature Kalshi
    rejects with an authentication error that says nothing about salt.

    The path is taken without its query string, which is what the venue signs.
    """
    padding = require(
        "cryptography.hazmat.primitives.asymmetric.padding",
        why="sign the Kalshi handshake",
    )
    hashes = require(
        "cryptography.hazmat.primitives.hashes", why="sign the Kalshi handshake"
    )
    message = f"{timestamp_ms}{method}{path}".encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("ascii")


def kalshi_headers(
    key_id: str,
    private_key: Any,
    url: str,
    *,
    method: str = _KALSHI_METHOD,
    timestamp_ms: Optional[int] = None,
) -> dict[str, str]:
    """The three headers Kalshi's handshake wants, signed for `url`.

    `timestamp_ms` exists so a test can pin the message; live callers leave it
    alone, because the server checks it against its own clock.
    """
    stamp = int(time.time() * 1000) if timestamp_ms is None else int(timestamp_ms)
    path = urlsplit(url).path or "/"
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-SIGNATURE": sign_kalshi(private_key, stamp, method, path),
        "KALSHI-ACCESS-TIMESTAMP": str(stamp),
    }


class Venue(enum.Enum):
    """A venue this recorder can connect to."""

    KALSHI = "kalshi"
    POLYMARKET = "polymarket"

    def __str__(self) -> str:
        return self.value

    def default_url(self) -> str:
        if self is Venue.KALSHI:
            # The legacy elections host, which Kalshi still serves and which
            # the Rust recorder ran against for weeks. `wss://external-api-ws.
            # kalshi.com/trade-api/ws/v2` is the current name for the same
            # service; both sign the same path, so `--url` switches hosts
            # without touching the credential.
            return "wss://api.elections.kalshi.com/trade-api/ws/v2"
        return "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def market_kind(self) -> str:
        """What `--market` means here, for help text and error messages."""
        if self is Venue.KALSHI:
            return "market ticker (for example KXPRESPARTY-28-D)"
        return "CLOB token id (the numeric asset id of one outcome)"

    def default_channels(self) -> tuple[str, ...]:
        if self is Venue.KALSHI:
            # The delta channel opens with a snapshot, so subscribing to it
            # alone is enough to rebuild the book; `trade` is the other half a
            # backtest needs and costs almost nothing in bandwidth.
            return ("orderbook_delta", "trade")
        # Polymarket's market socket is one stream, not selectable channels:
        # the subscription is a set of token ids.
        return ()

    def keepalive(self) -> Optional[Keepalive]:
        if self is Venue.KALSHI:
            # The websockets library answers server pings automatically but is
            # told not to initiate them here, so that the recorder controls the
            # interval. Kalshi drops a silent client.
            return Keepalive("ping")
        # Polymarket's gateway wants an application-level literal, not a
        # protocol ping, and closes the socket within seconds without it.
        return Keepalive("text", "PING")

    def subscribe_frames(
        self, markets: Sequence[str], channels: Sequence[str]
    ) -> list[str]:
        """The frames to send immediately after the handshake."""
        if self is Venue.KALSHI:
            return [
                json.dumps(
                    {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": list(channels),
                            "market_tickers": list(markets),
                        },
                    },
                    separators=(",", ":"),
                )
            ]
        return [
            json.dumps(
                {"type": "market", "assets_ids": list(markets)}, separators=(",", ":")
            )
        ]

    def credential(self, environ: Optional[Mapping[str, str]] = None) -> Credential:
        """Resolve the venue's credential, or explain what is missing.

        Called before the first connect so a missing credential fails in the
        first second rather than after a night of retrying a 401.

        Kalshi's websocket is authenticated; Polymarket's market channel is
        public, which is the practical reason this tool exists mainly for
        Kalshi.
        """
        env = os.environ if environ is None else environ
        if self is Venue.POLYMARKET:
            return NoCredential()

        key_id = (env.get(KALSHI_KEY_ID_ENV) or "").strip()
        key_path = (env.get(KALSHI_PRIVATE_KEY_ENV) or "").strip()
        if key_id and key_path:
            return KalshiKeyPair.from_path(key_id, key_path)
        if key_id or key_path:
            missing = KALSHI_PRIVATE_KEY_ENV if key_id else KALSHI_KEY_ID_ENV
            raise MissingCredential(
                f"{missing} is not set. Key-pair signing needs both "
                f"{KALSHI_KEY_ID_ENV} and {KALSHI_PRIVATE_KEY_ENV}; half a "
                "credential is not a credential."
            )

        token = (env.get(KALSHI_TOKEN_ENV) or "").strip()
        if token:
            return BearerToken(token)
        raise MissingCredential(
            f"No Kalshi credential in the environment. Export "
            f"{KALSHI_KEY_ID_ENV} and {KALSHI_PRIVATE_KEY_ENV} (the path to "
            f"your PEM private key), or {KALSHI_TOKEN_ENV} for an out-of-band "
            "bearer token. These are deliberately not accepted as flags: a "
            "flag is visible in `ps` for the whole life of the process."
        )
