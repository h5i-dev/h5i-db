//! Per-venue connection details: endpoint, subscribe frames, credentials.
//!
//! Everything venue-specific about *fetching* lives here, which is the line
//! this crate exists to draw. Nothing here interprets a payload.

use std::fmt;

/// How to keep a connection from being dropped for idleness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keepalive {
    /// A websocket-protocol ping frame.
    Ping,
    /// A text frame the venue expects at the application layer.
    Text(&'static str),
}

/// A venue this recorder can connect to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Venue {
    Kalshi,
    Polymarket,
}

/// The environment variable Kalshi credentials are read from.
///
/// An environment variable and not a flag: a flag lands in the shell history,
/// in `ps` output for every user on the box, and in whatever process
/// supervisor log wraps the command. A recorder tends to run for weeks under
/// exactly such a supervisor.
pub const KALSHI_TOKEN_ENV: &str = "KALSHI_API_TOKEN";

impl Venue {
    pub const fn as_str(self) -> &'static str {
        match self {
            Venue::Kalshi => "kalshi",
            Venue::Polymarket => "polymarket",
        }
    }

    pub const fn default_url(self) -> &'static str {
        match self {
            Venue::Kalshi => "wss://api.elections.kalshi.com/trade-api/ws/v2",
            Venue::Polymarket => "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        }
    }

    /// What `--market` means for this venue, for help text and error messages.
    pub const fn market_kind(self) -> &'static str {
        match self {
            Venue::Kalshi => "market ticker (for example KXPRESPARTY-28-D)",
            Venue::Polymarket => "CLOB token id (the numeric asset id of one outcome)",
        }
    }

    pub const fn default_channels(self) -> &'static [&'static str] {
        match self {
            // The delta channel opens with a snapshot, so subscribing to it
            // alone is enough to rebuild the book; `trade` is the other half
            // a backtest needs and costs almost nothing in bandwidth.
            Venue::Kalshi => &["orderbook_delta", "trade"],
            // Polymarket's market socket is one stream, not selectable
            // channels: the subscription is a set of token ids.
            Venue::Polymarket => &[],
        }
    }

    pub const fn keepalive(self) -> Option<Keepalive> {
        match self {
            // tungstenite answers server pings automatically but never
            // initiates. Kalshi drops a silent client, so we initiate.
            Venue::Kalshi => Some(Keepalive::Ping),
            // Polymarket's gateway wants an application-level literal, not a
            // protocol ping, and closes the socket within seconds without it.
            Venue::Polymarket => Some(Keepalive::Text("PING")),
        }
    }

    /// The frames to send immediately after the handshake.
    pub fn subscribe_frames(self, markets: &[String], channels: &[String]) -> Vec<String> {
        match self {
            Venue::Kalshi => vec![
                serde_json::json!({
                    "id": 1,
                    "cmd": "subscribe",
                    "params": {
                        "channels": channels,
                        "market_tickers": markets,
                    },
                })
                .to_string(),
            ],
            Venue::Polymarket => vec![
                serde_json::json!({
                    "type": "market",
                    "assets_ids": markets,
                })
                .to_string(),
            ],
        }
    }

    /// The credential header to add to the handshake, if the venue needs one.
    ///
    /// Kalshi's websocket is authenticated; Polymarket's market channel is
    /// public, which is the practical reason this tool exists mainly for
    /// Kalshi.
    pub fn auth_header(self) -> Result<Option<(&'static str, String)>, MissingCredential> {
        match self {
            Venue::Kalshi => {
                let token = std::env::var(KALSHI_TOKEN_ENV)
                    .ok()
                    .filter(|token| !token.trim().is_empty())
                    .ok_or(MissingCredential {
                        var: KALSHI_TOKEN_ENV,
                    })?;
                Ok(Some(("Authorization", format!("Bearer {token}"))))
            }
            Venue::Polymarket => Ok(None),
        }
    }
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A required credential was not in the environment.
#[derive(Clone, Copy, Debug)]
pub struct MissingCredential {
    pub var: &'static str,
}

impl fmt::Display for MissingCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} is not set. Export the websocket bearer token in the environment; \
             it is deliberately not accepted as a command-line flag.",
            self.var
        )
    }
}

impl std::error::Error for MissingCredential {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kalshi_subscribes_by_ticker_and_channel() {
        let frames = Venue::Kalshi
            .subscribe_frames(&["KXTEST".to_string()], &["orderbook_delta".to_string()]);
        let parsed: serde_json::Value = serde_json::from_str(&frames[0]).unwrap();
        assert_eq!(parsed["cmd"], "subscribe");
        assert_eq!(parsed["params"]["market_tickers"][0], "KXTEST");
        assert_eq!(parsed["params"]["channels"][0], "orderbook_delta");
    }

    #[test]
    fn polymarket_subscribes_by_token_and_needs_no_credential() {
        let frames = Venue::Polymarket.subscribe_frames(&["12345".to_string()], &[]);
        let parsed: serde_json::Value = serde_json::from_str(&frames[0]).unwrap();
        assert_eq!(parsed["type"], "market");
        assert_eq!(parsed["assets_ids"][0], "12345");
        assert!(Venue::Polymarket.auth_header().unwrap().is_none());
    }
}
