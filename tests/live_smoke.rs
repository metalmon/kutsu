//! Opt-in live smoke test against the real Gemini Live API.
//! Run with: `GEMINI_API_KEY=... cargo test --features live-tests --test live_smoke -- --ignored`

#![cfg(feature = "live-tests")]

#[tokio::test]
#[ignore = "requires GEMINI_API_KEY and network"]
async fn short_live_session_returns_audio_and_ends() {
    let api_key = std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY");
    let server = kutsu::config::ServerConfig {
        api_key, proxy: None, model: kutsu::config::Model::HalfCascade,
        voice: "Autonoe".into(), language: "ru-RU".into(),
        net_check: kutsu::config::NetCheckConfig::default(), max_concurrent_channels: 3,
    };
    let scenario = kutsu::config::ScenarioConfig {
        system_prompt: "You are a friendly assistant. Greet briefly, then call end_call.".into(),
        goal_schema: serde_json::json!({"type":"object","required":["disposition"],
            "properties":{"disposition":{"type":"string"}}}),
        context: None,
    };

    let health = kutsu::net_check::preflight(&server).await.expect("preflight");
    assert!(matches!(kutsu::net_check::verdict(&health, &server.net_check),
                     kutsu::net_check::Verdict::Ok), "network: {}", health.summary());

    let mut session = kutsu::gemini_live::start(&server, &scenario).await.expect("start");
    // Send ~1s of silence to trigger a turn.
    let silence = vec![0i16; 512];
    for _ in 0..30 { let _ = session.audio_in.send(silence.clone()).await; tokio::time::sleep(std::time::Duration::from_millis(32)).await; }

    let mut got_audio = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while let Ok(Some(ev)) = tokio::time::timeout_at(deadline, session.events.recv()).await {
        if let kutsu::gemini_live::Event::OutputAudio(_) = ev { got_audio = true; }
        if let kutsu::gemini_live::Event::EndCall { .. } = ev { break; }
    }
    session.hangup().await;
    assert!(got_audio, "expected some model audio");
}
