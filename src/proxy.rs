//! WebSocket connection helper with optional HTTP CONNECT proxy support.
//!
//! `tokio_tungstenite::connect_async` cannot tunnel through an HTTP proxy, but
//! reaching Gemini Live from a geo-restricted network requires one. When a
//! [`Proxy`] is configured, this establishes a TCP connection to the proxy,
//! issues an `HTTP CONNECT` to the target host (with optional Basic auth), then
//! runs the TLS + WebSocket handshake over that tunnel. Without a proxy it falls
//! back to a plain `connect_async`. Both paths yield the same stream type.

use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::config::Proxy;
use crate::error::{Error, Result};

/// The unified WebSocket stream type, identical for the direct and proxied paths.
pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
pub async fn connect_ws(proxy: Option<&Proxy>, url: &str) -> Result<Ws> {
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
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(rest);
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
