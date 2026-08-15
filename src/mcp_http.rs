//! streamable-http transport wiring + ops endpoints (`/health`, `/ready`,
//! `/metrics`) with an optional bearer-token guard on `/mcp`.
//!
//! `/health`, `/ready`, and `/metrics` are intentionally NOT gated by the
//! bearer token: they carry no PII, only counts, and ops tooling (load
//! balancer health checks, Prometheus scrapers) typically can't attach an
//! `Authorization` header. Only `/mcp` — the actual tool-call surface — is
//! gated when `auth_token` is configured.

use std::sync::Arc;

use subtle::ConstantTimeEq;

use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

use crate::engine::{Engine, MetricsSnapshot};
use crate::mcp::KutsuServer;

/// Render an engine [`MetricsSnapshot`] as Prometheus text exposition format
/// (version 0.0.4). Pure and side-effect free so it can be unit tested
/// without spinning up the HTTP server.
pub fn render_prometheus(m: &MetricsSnapshot) -> String {
    let mut s = String::new();
    let g = |s: &mut String, name: &str, help: &str, kind: &str, v: String| {
        s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {v}\n"));
    };
    g(&mut s, "kutsu_calls_active", "Calls ringing or in progress.", "gauge", m.active.to_string());
    g(&mut s, "kutsu_calls_queued", "Calls waiting for a channel.", "gauge", m.queued.to_string());
    g(
        &mut s,
        "kutsu_calls_placed_total",
        "Calls placed since start.",
        "counter",
        m.placed_total.to_string(),
    );
    g(
        &mut s,
        "kutsu_calls_completed_total",
        "Calls completed.",
        "counter",
        m.completed_total.to_string(),
    );
    g(&mut s, "kutsu_calls_failed_total", "Calls failed.", "counter", m.failed_total.to_string());
    g(
        &mut s,
        "kutsu_calls_cancelled_total",
        "Calls cancelled.",
        "counter",
        m.cancelled_total.to_string(),
    );
    g(&mut s, "kutsu_channels_cap", "Max concurrent channels.", "gauge", m.channels_cap.to_string());
    s
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn ready() -> impl IntoResponse {
    (StatusCode::OK, "ready")
}

async fn metrics(State(engine): State<Arc<Engine>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        render_prometheus(&engine.metrics_snapshot()),
    )
}

/// Constant-time comparison of a presented bearer token against the
/// configured one, so response timing can't be used to recover the token
/// byte-by-byte. A length difference short-circuits — acceptable, since a
/// token's length is not the secret.
fn token_matches(presented: &str, expected: &str) -> bool {
    presented.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// Bearer-token guard for `/mcp`. Compares the `Authorization: Bearer
/// <token>` header against the configured token in constant time; responds
/// `401` on a missing or mismatched header. The presented token is never
/// logged, only whether the check passed.
async fn bearer_guard(token: Arc<str>, req: Request, next: Next) -> Response {
    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(p) if token_matches(p, token.as_ref()) => next.run(req).await,
        _ => (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
    }
}

/// Wait for a shutdown signal: Ctrl-C on all platforms, plus SIGTERM on
/// Unix (Windows has no SIGTERM; Ctrl-C is the portable baseline there).
pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Best-effort graceful teardown after a transport loop returns: hang up every
/// live call (so callees get a BYE via `run_call` teardown), give that a
/// moment, then shut the SIP transport down if no other `Arc<Engine>` clone
/// lingers (streamable-http session handlers may hold clones until the app
/// drops; if so we skip — process exit reclaims the socket).
pub async fn graceful_teardown(engine: Arc<Engine>) {
    for rec in engine.store().list() {
        engine.end_call(&rec.call_id);
    }
    // end_call only signals teardown (it returns immediately); give the
    // per-call run_call task a brief moment to process the hangup before we
    // try to reclaim the engine below.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    // Engine::shutdown consumes `self`, so it only runs here if this is the
    // last surviving Arc<Engine> clone. The streamable-http path hands
    // clones into StreamableHttpService's per-session closures, which are
    // owned by the axum app/listener; those are dropped when `serve` above
    // returns, but a lingering clone (e.g. an in-flight request handler that
    // hasn't unwound yet) can make try_unwrap fail. That's fine — this is
    // best-effort cleanup, not correctness-critical: process exit reclaims
    // the SIP transport (sockets, tasks) regardless.
    if let Ok(e) = Arc::try_unwrap(engine) {
        e.shutdown().await;
    }
}

/// Serve the `/mcp` streamable-http endpoint plus the ungated ops endpoints
/// (`/health`, `/ready`, `/metrics`) on `bind`. When `auth_token` is
/// `Some`, `/mcp` requires a matching `Authorization: Bearer <token>`
/// header; the ops endpoints are never gated. Runs until a shutdown signal
/// arrives (see [`shutdown_signal`]).
pub async fn serve(engine: Arc<Engine>, bind: &str, auth_token: Option<String>) -> anyhow::Result<()> {
    let mcp_engine = engine.clone();
    let svc = StreamableHttpService::new(
        move || Ok(KutsuServer::new(mcp_engine.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );
    let mut mcp_router = Router::new().route_service("/mcp", svc);
    if let Some(token) = auth_token {
        let token: Arc<str> = Arc::from(token);
        mcp_router = mcp_router
            .layer(axum::middleware::from_fn(move |req, next| bearer_guard(token.clone(), req, next)));
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(engine)
        .merge(mcp_router);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "kutsu MCP streamable-http listening");
    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MetricsSnapshot;

    /// Build a valid engine for tests, bound to loopback so `SipTransport::new`
    /// binds offline (no real trunk) — mirrors `mcp::tests::test_engine`.
    async fn test_engine(cap: usize) -> Engine {
        use crate::config::{Model, NetCheckConfig, ServerConfig, SipConfig};
        let server = ServerConfig {
            api_key: "k".into(),
            proxy: None,
            model: Model::HalfCascade,
            voice: "Autonoe".into(),
            voice_gender: crate::config::Gender::Female,
            language: "en-US".into(),
            net_check: NetCheckConfig::default(),
            max_concurrent_channels: cap,
            greet_after_silence_ms: 4000,
            transcript_dir: None,
            max_call_secs: 600,
        };
        let sip = SipConfig {
            server: "127.0.0.1:5060".into(),
            username: "t".into(),
            password: "t".into(),
            from_user: None,
            local_ip: Some("127.0.0.1".parse().unwrap()),
            register: false,
            transport: Default::default(),
        };
        Engine::new(Arc::new(server), &sip).await.unwrap()
    }

    /// Proves `graceful_teardown` (a) fires `end_call` for a still-live call,
    /// (b) that call settles to `Cancelled`, and (c) `try_unwrap` + `shutdown`
    /// run to completion with no panic/hang — the path never exercised by the
    /// force-killed manual smoke, since a real Ctrl-C isn't deliverable here.
    #[tokio::test]
    async fn graceful_teardown_cancels_live_calls() {
        use crate::config::ScenarioConfig;
        use crate::state::CallState;

        // cap 0 -> the call parks in Queued (no permit), so it's still "live"
        // (non-terminal) at the moment teardown runs.
        let engine = Arc::new(test_engine(0).await);
        let id = engine
            .place_call(
                "600".into(),
                ScenarioConfig {
                    system_prompt: "hi".into(),
                    goal_schema: serde_json::json!({ "type": "object" }),
                    context: None,
                },
            )
            .await;
        let store = engine.store().clone(); // CallStore is Arc-backed; survives teardown.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(store.get(&id).unwrap().state, CallState::Queued);

        // Consumes the only Arc<Engine> -> try_unwrap succeeds -> shutdown runs.
        graceful_teardown(engine).await;

        // end_call fired for the queued call -> it finalizes Cancelled;
        // teardown returned without panic/hang.
        assert_eq!(store.get(&id).unwrap().state, CallState::Cancelled);
    }

    #[test]
    fn prometheus_has_all_series() {
        let m = MetricsSnapshot {
            active: 1,
            queued: 2,
            placed_total: 3,
            completed_total: 4,
            failed_total: 5,
            cancelled_total: 6,
            channels_cap: 3,
        };
        let s = render_prometheus(&m);
        for line in [
            "kutsu_calls_active 1",
            "kutsu_calls_queued 2",
            "kutsu_calls_placed_total 3",
            "kutsu_calls_completed_total 4",
            "kutsu_calls_failed_total 5",
            "kutsu_calls_cancelled_total 6",
            "kutsu_channels_cap 3",
        ] {
            assert!(s.contains(line), "missing: {line}");
        }
        assert!(s.contains("# TYPE kutsu_calls_placed_total counter"));
    }

    #[test]
    fn token_matches_accepts_only_the_exact_token() {
        assert!(token_matches("s3cret-token", "s3cret-token"));
        assert!(token_matches("", ""));
        assert!(!token_matches("s3cret-token", "s3cret-tokeN")); // last byte differs
        assert!(!token_matches("s3cret-token", "s3cret-toke")); // shorter
        assert!(!token_matches("s3cret-token", "s3cret-tokenX")); // longer
        assert!(!token_matches("", "x"));
    }
}
