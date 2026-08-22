//! Live integration test for the call engine. Requires the WSL Asterisk stand
//! (echo ext 600) AND a reachable Gemini (api key + proxy per ServerConfig).
//! Validates WIRING & CLEANUP, not dialogue quality. #[ignore]d; run:
//!   cargo test --features vendor-openssl --test engine_call -- --ignored --nocapture
//! Env: KUTSU_SIP_SERVER/_USER/_PASS/_EXT + the Gemini env the live harness uses.

use std::sync::Arc;
use std::time::Duration;

use kutsu::config::ScenarioConfig;
use kutsu::engine::Engine;
use kutsu::state::CallState;

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

#[tokio::test]
#[ignore = "requires the live WSL Asterisk stand + reachable Gemini; run with --ignored"]
async fn engine_places_and_finalizes_a_call() {
    // Build server + sip config from env. Reuse whatever construction the
    // `kutsu call` CLI uses (Task 5 Step 3) so this test and the CLI agree.
    let (server, sip_cfg) = kutsu::main_support::configs_from_env().expect("configs from env");
    let scenario: ScenarioConfig = kutsu::main_support::default_scenario();

    let engine = Engine::new(Arc::new(server), &sip_cfg)
        .await
        .expect("engine up");
    let id = engine
        .place_call(env_or("KUTSU_SIP_EXT", "600"), scenario)
        .await;

    // Poll until InProgress (proves answer + bridge wiring), then until terminal.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut saw_in_progress = false;
    loop {
        if tokio::time::Instant::now() > deadline {
            break;
        }
        if let Some(rec) = engine.store().get(&id) {
            if rec.state == CallState::InProgress {
                saw_in_progress = true;
            }
            if matches!(rec.state, CallState::Ended) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let rec = engine.store().get(&id).expect("record exists");
    eprintln!(
        "[engine_call] final state={:?} transcript_len={}",
        rec.state,
        rec.transcript.len()
    );
    assert!(
        saw_in_progress,
        "call never reached InProgress — wiring/answer failed"
    );
    assert!(
        rec.ended_ms.is_some() || rec.state == CallState::InProgress,
        "call did not progress"
    );

    engine.shutdown().await;
}
