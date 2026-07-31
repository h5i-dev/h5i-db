//! `h5i-db-capture`: point it at a venue websocket, get hourly lz4 NDJSON.
//!
//! ```text
//! KALSHI_API_TOKEN=… h5i-db-capture --venue kalshi --out ./capture \
//!     --market KXPRESPARTY-28-D --market KXBTCD-25DEC31
//! ```
//!
//! The loop is deliberately dull: connect, subscribe, write every frame with
//! the nanosecond it arrived, reconnect with backoff when the socket dies,
//! and leave a marker at every seam so a reader can see where data might be
//! missing. Everything clever belongs downstream, where it can be re-run.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use base64::Engine as _;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{error, info, warn};

use h5i_db_capture::{
    CaptureWriter, Keepalive, Venue, archive_line, format_archive_time, marker_line, now_nanos,
};

/// Record a venue websocket to lz4-compressed newline-delimited JSON.
///
/// Credentials are read from the environment, never from a flag: a flag is
/// visible in `ps` and in supervisor logs for the whole life of the process.
#[derive(Debug, Parser)]
#[command(name = "h5i-db-capture", version, about, long_about = None)]
struct Args {
    /// Venue to record. Kalshi needs KALSHI_API_TOKEN; Polymarket is public.
    #[arg(long, value_enum)]
    venue: Venue,

    /// Directory to write under. Files land in <out>/<venue>/<date>/<hour>.
    #[arg(long, short = 'o')]
    out: PathBuf,

    /// Market to subscribe to. Repeat, or pass a comma-separated list.
    #[arg(long = "market", required = true, value_delimiter = ',')]
    markets: Vec<String>,

    /// Override the venue's default channels. Ignored by Polymarket, whose
    /// market socket is a single stream selected by token id.
    #[arg(long = "channel", value_delimiter = ',')]
    channels: Vec<String>,

    /// Override the websocket endpoint, for a demo or staging environment.
    #[arg(long)]
    url: Option<String>,

    /// Longest wait between reconnect attempts, in seconds.
    #[arg(long, default_value_t = 60)]
    max_backoff_secs: u64,

    /// How often to flush completed lz4 blocks to the file, in seconds.
    /// Bounds what a `kill -9` can destroy, since anything still buffered
    /// when the process dies is gone.
    #[arg(long, default_value_t = 5)]
    flush_secs: u64,

    /// How often to send a keepalive, in seconds.
    #[arg(long, default_value_t = 10)]
    keepalive_secs: u64,
}

/// A single reader thread is plenty: the work per frame is a JSON parse and a
/// buffered write, and a multi-threaded runtime would only add scheduling
/// jitter to the arrival timestamps that are the point of this program.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    // Explicit, so TLS cannot fail at the first connect over which crypto
    // provider rustls should have picked. `install_default` errs only if one
    // is already installed, which is equally fine.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    if let Err(error) = run(Args::parse()).await {
        error!("{error:#}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    let venue = args.venue;
    let url = args
        .url
        .clone()
        .unwrap_or_else(|| venue.default_url().to_string());
    let channels: Vec<String> = if args.channels.is_empty() {
        venue
            .default_channels()
            .iter()
            .map(|channel| (*channel).to_string())
            .collect()
    } else {
        args.channels.clone()
    };
    // Resolved before the first connect so a missing credential fails in the
    // first second rather than after a night of retrying a 401.
    let auth = venue.auth_header()?;
    let subscribe = venue.subscribe_frames(&args.markets, &channels);

    let mut writer = CaptureWriter::new(
        &args.out,
        venue.as_str(),
        Duration::from_secs(args.flush_secs),
    );
    let mut shutdown = Shutdown::install();

    info!(
        venue = venue.as_str(),
        url = url.as_str(),
        markets = args.markets.len(),
        out = %args.out.display(),
        "capture starting ({})",
        venue.market_kind()
    );
    let started = now_nanos();
    writer.write_line(
        started,
        &marker_line(
            started,
            "start",
            json!({
                "venue": venue.as_str(),
                "url": url,
                "markets": args.markets,
                "channels": channels,
            }),
        ),
    )?;

    let max_backoff = Duration::from_secs(args.max_backoff_secs.max(1));
    let keepalive_every = Duration::from_secs(args.keepalive_secs.max(1));
    let mut backoff = Duration::from_secs(1);
    let mut attempt: u32 = 0;
    // Set the moment the stream is known to be down, cleared once the marker
    // recording that outage has been written.
    let mut lost_at: Option<u64> = None;

    loop {
        attempt += 1;
        match connect(&url, auth.as_ref()).await {
            Ok(stream) => {
                backoff = Duration::from_secs(1);
                attempt = 0;
                // Written on reconnect, not on disconnect, because only now
                // is the length of the hole known. Silently resuming is how a
                // hole becomes invisible.
                if let Some(down_since) = lost_at.take() {
                    let now = now_nanos();
                    writer.write_line(
                        now,
                        &marker_line(
                            now,
                            "reconnect",
                            json!({
                                "gap_nanos": now.saturating_sub(down_since),
                                "lost_at": format_archive_time(down_since),
                            }),
                        ),
                    )?;
                }
                info!("connected");
                match pump(
                    stream,
                    venue,
                    &subscribe,
                    &mut writer,
                    &mut shutdown,
                    keepalive_every,
                )
                .await?
                {
                    Ended::Stopped => break,
                    Ended::Lost(why) => {
                        lost_at = Some(now_nanos());
                        warn!("connection lost: {why}");
                    }
                }
            }
            Err(error) => {
                lost_at.get_or_insert_with(now_nanos);
                warn!("connect attempt {attempt} failed: {error:#}");
            }
        }

        // Interruptible: an operator who hits Ctrl-C during a 60 second
        // backoff should not wait out the backoff.
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(max_backoff);
    }

    let stopped = now_nanos();
    writer.write_line(
        stopped,
        // Counts this marker, so the number is the run total a reader can
        // check against the files this run produced rather than being one
        // short of every one of them.
        &marker_line(stopped, "stop", json!({ "lines": writer.lines() + 1 })),
    )?;
    let path = writer.current_path().map(|path| path.display().to_string());
    writer.close().context("closing the capture file")?;
    info!(
        lines = writer.lines(),
        file = path,
        "capture stopped cleanly"
    );
    Ok(())
}

/// Why the read loop returned.
enum Ended {
    /// A shutdown signal arrived. Do not reconnect.
    Stopped,
    /// The socket went away. Reconnect after a backoff.
    Lost(String),
}

async fn connect(
    url: &str,
    auth: Option<&(&'static str, String)>,
) -> anyhow::Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let mut request = url
        .into_client_request()
        .with_context(|| format!("{url} is not a usable websocket url"))?;
    if let Some((name, value)) = auth {
        let mut header =
            HeaderValue::from_str(value).context("credential is not a valid header")?;
        // Keeps the token out of any `Debug` rendering of the request, which
        // is exactly what ends up in a log line when a handshake fails.
        header.set_sensitive(true);
        request
            .headers_mut()
            .insert(HeaderName::from_bytes(name.as_bytes())?, header);
    }
    let (stream, _response) = connect_async(request).await?;
    Ok(stream)
}

/// Subscribe, then write every frame until the socket or the operator stops us.
async fn pump(
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    venue: Venue,
    subscribe: &[String],
    writer: &mut CaptureWriter,
    shutdown: &mut Shutdown,
    keepalive_every: Duration,
) -> anyhow::Result<Ended> {
    // Split so a keepalive can be sent while a read is pending; the unsplit
    // stream would need one mutable borrow for both select branches.
    let (mut sink, mut source) = stream.split();
    for frame in subscribe {
        if let Err(error) = sink.send(Message::text(frame.clone())).await {
            return Ok(Ended::Lost(format!("subscribe failed: {error}")));
        }
    }

    let mut ticker = tokio::time::interval(keepalive_every);
    // The first tick fires immediately and we have just sent the subscribe.
    ticker.tick().await;

    loop {
        tokio::select! {
            // Shutdown first: on a busy market the read branch would
            // otherwise win the race indefinitely.
            biased;

            _ = shutdown.recv() => return Ok(Ended::Stopped),

            _ = ticker.tick() => {
                let frame = match venue.keepalive() {
                    Some(Keepalive::Ping) => Some(Message::Ping(Default::default())),
                    Some(Keepalive::Text(text)) => Some(Message::text(text)),
                    None => None,
                };
                if let Some(frame) = frame
                    && let Err(error) = sink.send(frame).await {
                    return Ok(Ended::Lost(format!("keepalive failed: {error}")));
                }
                // A quiet market should still get its bytes on disk.
                writer.flush()?;
            }

            message = source.next() => {
                // First statement in the branch on purpose: every line of
                // work between the frame arriving and this call is error in
                // the only timestamp a replay can trust.
                let received_at = now_nanos();
                match message {
                    None => return Ok(Ended::Lost("stream ended".to_string())),
                    Some(Err(error)) => return Ok(Ended::Lost(error.to_string())),
                    Some(Ok(Message::Text(text))) => {
                        writer.write_line(received_at, &archive_line(received_at, text.as_str()))?;
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        // Neither venue sends binary today. Base64 rather
                        // than a lossy decode, and a marker rather than a
                        // guess at what channel it belongs to: an unexpected
                        // frame is still evidence.
                        let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
                        writer.write_line(
                            received_at,
                            &marker_line(received_at, "binary", json!({ "base64": encoded })),
                        )?;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        return Ok(Ended::Lost(match frame {
                            Some(frame) => format!("server closed: {} {}", frame.code, frame.reason),
                            None => "server closed".to_string(),
                        }));
                    }
                    // Pings are answered by the library and carry no data.
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}

/// A latch that flips once on SIGINT or SIGTERM and stays flipped.
///
/// A `watch` rather than awaiting `ctrl_c` inline because the signal has to be
/// selectable from several places (the read loop and the backoff sleep) and a
/// one-shot future cannot be polled again after it resolves.
struct Shutdown(tokio::sync::watch::Receiver<bool>);

impl Shutdown {
    fn install() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            wait_for_signal().await;
            info!("shutdown signal received, flushing");
            let _ = tx.send(true);
        });
        Self(rx)
    }

    /// Cancel-safe, and resolves immediately once the latch is set.
    async fn recv(&mut self) {
        loop {
            if *self.0.borrow_and_update() {
                return;
            }
            if self.0.changed().await.is_err() {
                // The sender is gone, so no signal can ever arrive. Never
                // resolving is right: resolving would fake a shutdown.
                std::future::pending::<()>().await;
            }
        }
    }
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(term) => term,
        Err(error) => {
            warn!("cannot listen for SIGTERM: {error}");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    // SIGTERM as well as SIGINT: a recorder lives under a supervisor, and
    // systemd stops a unit with SIGTERM. Ignoring it would mean every planned
    // restart truncates the current lz4 frame.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
