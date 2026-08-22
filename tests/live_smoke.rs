//! Opt-in live smoke test against the real Gemini Live API.
//! Run with: `GEMINI_API_KEY=... cargo test --features live-tests --test live_smoke -- --ignored`

#![cfg(feature = "live-tests")]

#[tokio::test]
#[ignore = "requires GEMINI_API_KEY and network"]
async fn short_live_session_returns_audio_and_ends() {
    let api_key = std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY");
    // Optional proxy from env (needed where Gemini is geo-restricted):
    // PROXY_URL [+ PROXY_USER / PROXY_PASSWORD].
    let non_empty = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    let proxy = non_empty("PROXY_URL").map(|url| kutsu::config::Proxy {
        url,
        user: non_empty("PROXY_USER"),
        password: non_empty("PROXY_PASSWORD"),
    });
    // Relaxed thresholds: this smoke test routes through a remote proxy (to reach
    // geo-restricted Gemini), which adds latency/jitter well above the default
    // direct-path limits. The point here is to reach the conversation, not to
    // grade the proxy link; production keeps the stricter defaults.
    let net_check = kutsu::config::NetCheckConfig {
        max_rtt_ms: 2000,
        max_jitter_ms: 500,
        max_loss_pct: 5.0,
        ..kutsu::config::NetCheckConfig::default()
    };
    let server = kutsu::config::ServerConfig {
        api_key,
        proxy,
        model: kutsu::config::Model::HalfCascade,
        voice: "Autonoe".into(),
        voice_gender: kutsu::config::Gender::Female,
        language: "ru-RU".into(),
        net_check,
        max_concurrent_channels: 3,
        greet_after_silence_ms: kutsu::config::DEFAULT_GREET_AFTER_SILENCE_MS,
        transcript_dir: None,
        dump_uplink_dir: None,
        dump_downlink_dir: None,
        max_call_secs: 600,
        quality: kutsu::config::QualityConfig::default(),
        retry: kutsu::config::RetryConfig::default(),
        vad: kutsu::config::VadConfig::default(),
        agc: kutsu::config::AgcConfig::default(),
        resume_cue: kutsu::config::RESUME_CUE.into(),
    };
    let scenario = kutsu::config::ScenarioConfig {
        system_prompt: "You are a friendly assistant. Greet briefly, then call end_call.".into(),
        goal_schema: serde_json::json!({"type":"object","required":["disposition"],
            "properties":{"disposition":{"type":"string"}}}),
        context: None,
    };

    let health = kutsu::net_check::preflight(&server)
        .await
        .expect("preflight");
    assert!(
        matches!(
            kutsu::net_check::verdict(&health, &server.net_check),
            kutsu::net_check::Verdict::Ok
        ),
        "network: {}",
        health.summary()
    );

    // Pre-answered stub: this smoke test has no ring/answer signal of its own.
    let mut session = kutsu::gemini_live::start(&server, &scenario, {
        let (_tx, rx) = tokio::sync::watch::channel(true);
        rx
    })
    .await
    .expect("start");
    // Send ~1s of silence to trigger a turn.
    let silence = vec![0i16; 512];
    for _ in 0..30 {
        let _ = session.audio_in.send(silence.clone()).await;
        tokio::time::sleep(std::time::Duration::from_millis(32)).await;
    }

    let mut got_audio = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while let Ok(Some(ev)) = tokio::time::timeout_at(deadline, session.events.recv()).await {
        if let kutsu::gemini_live::Event::OutputAudio(_) = ev {
            got_audio = true;
        }
        if let kutsu::gemini_live::Event::EndCall { .. } = ev {
            break;
        }
    }
    session.hangup().await;
    assert!(got_audio, "expected some model audio");
}
