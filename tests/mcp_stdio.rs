//! Live MCP round-trip against the WSL Asterisk stend, driven through an
//! **in-process** rmcp client <-> server duplex (no subprocess, no real
//! stdio pipes — `tokio::io::duplex` stands in for the transport). Exercises
//! the same wiring a real MCP client would: `place_call` -> poll
//! `get_call_status` -> `get_call_transcript`. Requires the WSL Asterisk
//! stand (echo ext 600) AND a reachable Gemini (api key + proxy per
//! ServerConfig) — mirrors `tests/engine_call.rs`, which this test's Engine
//! construction reuses so the two agree. Run with:
//!   cargo test --features vendor-openssl --test mcp_stdio -- --ignored --nocapture
//! Env: KUTSU_SIP_SERVER/_USER/_PASS/_EXT + the Gemini env the live harness uses.

use std::sync::Arc;
use std::time::Duration;

use rmcp::model::{CallToolRequestParams, CallToolResult, ContentBlock};
use rmcp::service::ServiceExt;

use kutsu::engine::Engine;
use kutsu::mcp::KutsuServer;

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

/// The MCP tools always return a single `ContentBlock::Text` JSON body (see
/// `src/mcp.rs`'s own `first_text` test helper, which this mirrors).
fn first_text(r: &CallToolResult) -> String {
    match r.content.first().expect("tool result has content") {
        ContentBlock::Text(t) => t.text.clone(),
        other => panic!("expected text content, got {other:?}"),
    }
}

fn call_args(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[tokio::test]
#[ignore = "requires the WSL Asterisk stend + a reachable trunk"]
async fn place_call_via_mcp_completes() {
    // Build server + sip config from env, the same construction the `kutsu
    // call` CLI and tests/engine_call.rs use, so this test and those agree.
    let (server_cfg, sip_cfg) = kutsu::main_support::configs_from_env().expect("configs from env");
    let scenario = kutsu::main_support::default_scenario();
    let engine = Arc::new(
        Engine::new(Arc::new(server_cfg), &sip_cfg)
            .await
            .expect("engine up"),
    );

    let handler = KutsuServer::new(engine.clone());

    // In-process transport: one `tokio::io::duplex` pair, each end handed to
    // one side. `Service::serve` (rmcp-3.1.2 src/service/server.rs and
    // src/service/client.rs, both via the `ServiceExt::serve` trait method
    // at src/service.rs:330) performs the MCP `initialize` handshake before
    // resolving, and the *server* side blocks reading the client's
    // `initialize` request first (src/service/server.rs:499
    // `serve_server_with_ct_inner`) — so the two `.serve()` calls must run
    // concurrently (`tokio::join!`), not sequentially, or they deadlock.
    // `()` is rmcp's default `ClientHandler` (src/handler/client.rs:296
    // `impl ClientHandler for ()`), sufficient for a client that only calls
    // tools and doesn't need to answer server-initiated requests.
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (server_res, client_res) = tokio::join!(handler.serve(server_io), ().serve(client_io));
    let server = server_res.expect("server-side MCP handshake");
    let client = client_res.expect("client-side MCP handshake");

    // place_call: resolves to `RunningService<RoleClient, _>`'s *inherent*
    // `call_tool` (the MRTR-round-aware wrapper) — Rust method resolution
    // prefers an inherent method on the concrete receiver over the
    // `Peer<RoleClient>::call_tool` reachable via `Deref<Target = Peer<R>>`.
    // Both return `Result<CallToolResult, ServiceError>`; behavior here is
    // identical. Arguments are a `serde_json::Map<String, Value>`
    // (`JsonObject`).
    let place_args = call_args(&[
        (
            "to_number",
            serde_json::Value::String(env_or("KUTSU_SIP_EXT", "600")),
        ),
        ("goal_schema", scenario.goal_schema.clone()),
    ]);
    let place_result = client
        .call_tool(CallToolRequestParams::new("place_call").with_arguments(place_args))
        .await
        .expect("place_call call_tool succeeds");
    let place_body: serde_json::Value =
        serde_json::from_str(&first_text(&place_result)).expect("place_call result is JSON");
    let call_id = place_body["call_id"]
        .as_str()
        .expect("place_call result has call_id")
        .to_string();
    assert!(!call_id.is_empty());
    eprintln!("[mcp_stdio] placed call_id={call_id}");

    // Poll get_call_status until a terminal CallState (snake_case on the
    // wire per `#[serde(rename_all = "snake_case")]` on CallState,
    // src/state.rs:14), bounded so a stuck call can't hang the test forever.
    const POLL_INTERVAL: Duration = Duration::from_millis(500);
    const MAX_POLLS: u32 = 240; // ~120 s bound.
    let mut terminal_state: Option<String> = None;
    for _ in 0..MAX_POLLS {
        let status_result = client
            .call_tool(
                CallToolRequestParams::new("get_call_status").with_arguments(call_args(&[(
                    "call_id",
                    serde_json::Value::String(call_id.clone()),
                )])),
            )
            .await
            .expect("get_call_status call_tool succeeds");
        let status_body: serde_json::Value =
            serde_json::from_str(&first_text(&status_result)).expect("status result is JSON");
        let state = status_body["state"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if matches!(
            state.as_str(),
            "completed" | "failed" | "hung_up" | "cancelled"
        ) {
            terminal_state = Some(state);
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    let terminal_state =
        terminal_state.expect("call reached a terminal state within the poll bound");
    eprintln!("[mcp_stdio] terminal state={terminal_state}");

    // get_call_transcript: assert the transcript field is present (dialogue
    // quality is out of scope, mirroring engine_call.rs).
    let tr_result = client
        .call_tool(
            CallToolRequestParams::new("get_call_transcript").with_arguments(call_args(&[(
                "call_id",
                serde_json::Value::String(call_id.clone()),
            )])),
        )
        .await
        .expect("get_call_transcript call_tool succeeds");
    let tr_body: serde_json::Value =
        serde_json::from_str(&first_text(&tr_result)).expect("transcript result is JSON");
    assert_eq!(tr_body["call_id"], call_id);
    let transcript = tr_body.get("transcript").expect("transcript field present");
    eprintln!(
        "[mcp_stdio] transcript entries={}",
        transcript.as_array().map(|a| a.len()).unwrap_or(0)
    );

    // Teardown: close both ends of the in-process connection, then shut the
    // engine down (it hangs up any still-live call and releases the SIP
    // transport). `Engine::shutdown` takes `self` by value
    // (src/engine.rs:151), so the `Arc` must be unwrapped first — the same
    // pattern `src/mcp.rs`'s own test module uses after dropping every other
    // holder of a clone. `handler` was already moved into `.serve()` above
    // and `server.cancel()` drops the last clone the background task held.
    let _ = client.cancel().await;
    let _ = server.cancel().await;
    Arc::into_inner(engine)
        .expect("only strong ref left")
        .shutdown()
        .await;
}
