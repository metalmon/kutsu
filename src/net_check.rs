//! Fail-closed network preflight: probe the Gemini endpoint (WSS ping RTT) and
//! decide whether the network is good enough to place a call.
//!
//! The preflight needs a *raw* WebSocket to the Gemini endpoint so it can
//! measure ping/pong RTT directly (the `gemini-live` crate's `Session` sends
//! `setup` and swallows ping/pong, so it can't be used for probing). The small
//! proxy-aware connect helper + endpoint-URL builder therefore live here, the
//! only remaining consumer, rather than in the crate.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::config::{Model, NetCheckConfig, Proxy, ServerConfig};
use crate::error::{Error, Result};

/// The unified WebSocket stream type, identical for the direct and proxied paths.
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    Unusable,
}

#[derive(Clone, Copy, Debug)]
pub struct NetworkHealth {
    pub rtt_p50_ms: u32,
    pub rtt_p95_ms: u32,
    pub jitter_ms: u32,
    pub loss_pct: f32,
}

impl NetworkHealth {
    pub fn summary(&self) -> String {
        format!(
            "rtt_p50={}ms rtt_p95={}ms jitter={}ms loss={}%",
            self.rtt_p50_ms, self.rtt_p95_ms, self.jitter_ms, self.loss_pct
        )
    }
}

pub fn verdict(h: &NetworkHealth, cfg: &NetCheckConfig) -> Verdict {
    if h.rtt_p95_ms > cfg.max_rtt_ms || h.jitter_ms > cfg.max_jitter_ms || h.loss_pct > cfg.max_loss_pct {
        Verdict::Unusable
    } else {
        Verdict::Ok
    }
}

/// The `GenerativeService` API version each model is served under (native-audio
/// is only served under `v1alpha`). Mirrors the crate's `Model::api_version`;
/// kept here so the preflight can build the probe URL without the crate's
/// internal transport types.
fn api_version(m: Model) -> &'static str {
    match m {
        Model::HalfCascade => "v1beta",
        Model::NativeAudio => "v1alpha",
    }
}

/// Build the `BidiGenerateContent` WebSocket endpoint URL for the configured
/// model, carrying `api_key` as the `key` query param.
fn endpoint_url(server: &ServerConfig) -> String {
    format!(
        "wss://generativelanguage.googleapis.com/ws/\
         google.ai.generativelanguage.{ver}.GenerativeService.BidiGenerateContent?key={key}",
        ver = api_version(server.model),
        key = server.api_key,
    )
}

/// Open a real WSS connection to the Gemini endpoint and measure ping/pong RTT.
/// Uses the configured proxy (if any) so the probe traverses the same path the
/// call will.
pub async fn preflight(server: &ServerConfig) -> Result<NetworkHealth> {
    let url = endpoint_url(server);
    let mut ws = connect_ws(server.proxy.as_ref(), &url)
        .await
        .map_err(|e| Error::Connect(format!("preflight connect: {e}")))?;

    let mut rtts: Vec<u32> = Vec::new();
    let mut lost = 0u32;
    let n = server.net_check.samples.max(1);
    for i in 0..n {
        let payload = vec![i as u8];
        let sent = Instant::now();
        ws.send(Message::Ping(payload.clone().into()))
            .await
            .map_err(|e| Error::Connect(format!("preflight ping: {e}")))?;
        // Wait up to max_rtt*4 for the matching pong.
        let budget = Duration::from_millis((server.net_check.max_rtt_ms as u64) * 4);
        match tokio::time::timeout(budget, wait_for_pong(&mut ws)).await {
            Ok(Ok(())) => rtts.push(sent.elapsed().as_millis() as u32),
            _ => lost += 1,
        }
    }
    let _ = ws.close(None).await;

    Ok(summarize(&mut rtts, lost, n))
}

async fn wait_for_pong<S>(ws: &mut S) -> Result<()>
where
    S: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(msg) = ws.next().await {
        match msg {
            Ok(Message::Pong(_)) => return Ok(()),
            Ok(_) => continue,
            Err(e) => return Err(Error::Connect(format!("preflight recv: {e}"))),
        }
    }
    Err(Error::Connect("preflight stream ended".into()))
}

fn summarize(rtts: &mut Vec<u32>, lost: u32, total: u32) -> NetworkHealth {
    let loss_pct = (lost as f32 / total as f32) * 100.0;
    if rtts.is_empty() {
        return NetworkHealth { rtt_p50_ms: u32::MAX, rtt_p95_ms: u32::MAX, jitter_ms: u32::MAX, loss_pct };
    }
    rtts.sort_unstable();
    let p = |q: f32| rtts[((rtts.len() as f32 - 1.0) * q).round() as usize];
    let mean = rtts.iter().sum::<u32>() as f32 / rtts.len() as f32;
    let jitter = (rtts.iter().map(|&r| (r as f32 - mean).abs()).sum::<f32>() / rtts.len() as f32) as u32;
    NetworkHealth { rtt_p50_ms: p(0.50), rtt_p95_ms: p(0.95), jitter_ms: jitter, loss_pct }
}

// --- Proxy-aware WS connect (ported from the deleted `proxy.rs`; the preflight
// is now its only consumer — the crate owns the call-path transport). ---------

/// rustls 0.23 does not pick a crypto provider automatically. Install `ring`
/// exactly once, process-wide, before the first TLS handshake. Idempotent and
/// safe to call from every connection attempt.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static TLS_INIT: Once = Once::new();
    TLS_INIT.call_once(|| {
        // Ignore the error if another provider was already installed.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Open a WebSocket connection to `url`, tunneling through `proxy` when present.
async fn connect_ws(proxy: Option<&Proxy>, url: &str) -> Result<Ws> {
    ensure_crypto_provider();
    match proxy {
        None => {
            let (ws, _) = tokio_tungstenite::connect_async(url)
                .await
                .map_err(|e| Error::Connect(format!("connect: {e}")))?;
            Ok(ws)
        }
        Some(p) => connect_via_proxy(p, url).await,
    }
}

async fn connect_via_proxy(proxy: &Proxy, url: &str) -> Result<Ws> {
    let (target_host, target_port) = target_host_port(url)?;
    let (proxy_host, proxy_port) = parse_host_port(&proxy.url, 80)?;

    let mut stream = TcpStream::connect((proxy_host.as_str(), proxy_port))
        .await
        .map_err(|e| Error::Connect(format!("proxy TCP connect to {proxy_host}:{proxy_port}: {e}")))?;

    match (proxy.user.as_deref(), proxy.password.as_deref()) {
        (Some(user), Some(password)) => {
            async_http_proxy::http_connect_tokio_with_basic_auth(
                &mut stream,
                &target_host,
                target_port,
                user,
                password,
            )
            .await
            .map_err(|e| Error::Connect(format!("proxy CONNECT: {e}")))?;
        }
        _ => {
            async_http_proxy::http_connect_tokio(&mut stream, &target_host, target_port)
                .await
                .map_err(|e| Error::Connect(format!("proxy CONNECT: {e}")))?;
        }
    }

    let request = url
        .into_client_request()
        .map_err(|e| Error::Connect(format!("bad ws url: {e}")))?;
    let (ws, _) = tokio_tungstenite::client_async_tls(request, stream)
        .await
        .map_err(|e| Error::Connect(format!("ws handshake over proxy: {e}")))?;
    Ok(ws)
}

/// Extract `(host, port)` from a `ws://`/`wss://` URL. Defaults to 443 for
/// `wss` and 80 for `ws` when no explicit port is present.
fn target_host_port(url: &str) -> Result<(String, u16)> {
    let (rest, default_port) = if let Some(r) = url.strip_prefix("wss://") {
        (r, 443u16)
    } else if let Some(r) = url.strip_prefix("ws://") {
        (r, 80u16)
    } else {
        return Err(Error::Connect(format!("not a ws/wss url: {url}")));
    };
    // Authority is everything up to the first '/', '?' or '#'.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    parse_host_port(authority, default_port)
}

/// Parse `host` or `host:port`, applying `default_port` when the port is absent.
/// Accepts an optional `http://` prefix (proxy URLs carry one).
fn parse_host_port(s: &str, default_port: u16) -> Result<(String, u16)> {
    let s = s.strip_prefix("http://").unwrap_or(s);
    let s = s.strip_suffix('/').unwrap_or(s);
    match s.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|_| Error::Connect(format!("bad port in '{s}'")))?;
            if host.is_empty() {
                return Err(Error::Connect(format!("empty host in '{s}'")));
            }
            Ok((host.to_string(), port))
        }
        None => {
            if s.is_empty() {
                return Err(Error::Connect("empty host".into()));
            }
            Ok((s.to_string(), default_port))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NetCheckConfig;

    #[test]
    fn verdict_respects_thresholds() {
        let cfg = NetCheckConfig::default(); // 300/50/2.0
        let good = NetworkHealth { rtt_p50_ms: 40, rtt_p95_ms: 120, jitter_ms: 15, loss_pct: 0.0 };
        assert!(matches!(verdict(&good, &cfg), Verdict::Ok));

        let high_rtt = NetworkHealth { rtt_p95_ms: 800, ..good };
        assert!(matches!(verdict(&high_rtt, &cfg), Verdict::Unusable));

        let lossy = NetworkHealth { loss_pct: 10.0, ..good };
        assert!(matches!(verdict(&lossy, &cfg), Verdict::Unusable));

        let jittery = NetworkHealth { jitter_ms: 200, ..good };
        assert!(matches!(verdict(&jittery, &cfg), Verdict::Unusable));
    }

    #[test]
    fn summary_is_readable() {
        let h = NetworkHealth { rtt_p50_ms: 40, rtt_p95_ms: 120, jitter_ms: 15, loss_pct: 1.5 };
        let s = h.summary();
        assert!(s.contains("rtt_p95=120ms"));
        assert!(s.contains("loss=1.5%"));
    }

    #[test]
    fn endpoint_version_depends_on_model() {
        let mut srv = ServerConfig {
            api_key: "KEY".into(), proxy: None, model: Model::HalfCascade, voice: "Autonoe".into(),
            voice_gender: crate::config::Gender::Female, language: "en-US".into(),
            net_check: NetCheckConfig::default(), max_concurrent_channels: 3,
            greet_after_silence_ms: 0, transcript_dir: None, dump_uplink_dir: None,
            dump_downlink_dir: None, max_call_secs: 600, quality: Default::default(),
            retry: Default::default(), vad: Default::default(), resume_cue: String::new(),
        };
        assert!(endpoint_url(&srv).contains(".v1beta."));
        assert!(endpoint_url(&srv).contains("key=KEY"));
        srv.model = Model::NativeAudio;
        assert!(endpoint_url(&srv).contains(".v1alpha."));
    }

    // --- WS connect URL/host parsing (ported from the deleted proxy.rs). ---

    #[test]
    fn target_defaults_to_443_for_wss() {
        let (h, p) = target_host_port(
            "wss://generativelanguage.googleapis.com/ws/foo.BidiGenerateContent?key=K",
        )
        .unwrap();
        assert_eq!(h, "generativelanguage.googleapis.com");
        assert_eq!(p, 443);
    }

    #[test]
    fn proxy_url_parses_host_and_port() {
        let (h, p) = parse_host_port("http://46.202.204.37:46250", 80).unwrap();
        assert_eq!(h, "46.202.204.37");
        assert_eq!(p, 46250);
    }

    #[test]
    fn host_without_port_uses_default() {
        let (h, p) = parse_host_port("proxy.example.com", 8080).unwrap();
        assert_eq!(h, "proxy.example.com");
        assert_eq!(p, 8080);
    }

    #[test]
    fn non_ws_url_is_rejected() {
        assert!(target_host_port("https://example.com").is_err());
    }
}
