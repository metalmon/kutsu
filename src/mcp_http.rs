//! streamable-http transport wiring + ops endpoints (`/health`, `/ready`,
//! `/metrics`) with an optional bearer-token guard on `/mcp`.
//!
//! `/health`, `/ready`, and `/metrics` are intentionally NOT gated by the
//! bearer token: they carry no PII, only counts, and ops tooling (load
//! balancer health checks, Prometheus scrapers) typically can't attach an
//! `Authorization` header. Only `/mcp` — the actual tool-call surface — is
//! gated when `auth_token` is configured.

use std::sync::Arc;

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

/// Bearer-token guard for `/mcp`. Compares the `Authorization: Bearer
/// <token>` header against the configured token; responds `401` on a
/// missing or mismatched header. The presented token is never logged, only
/// whether the check passed.
async fn bearer_guard(token: Arc<str>, req: Request, next: Next) -> Response {
    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(p) if p == token.as_ref() => next.run(req).await,
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
}
