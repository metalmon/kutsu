//! Live integration test for the outbound SIP transport. Requires the WSL
//! Asterisk stand (see dev/sip-test/README.md). #[ignore]d; run explicitly:
//!   cargo test --features vendor-openssl --test sip_outbound -- --ignored --nocapture
//! Env overrides: KUTSU_SIP_SERVER / _USER / _PASS / _EXT.

use std::time::Duration;

use bytes::Bytes;
use kutsu::config::SipConfig;
use kutsu::sip::{G711Kind, SipEvent, SipTransport};

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

#[tokio::test]
#[ignore = "requires the live WSL Asterisk stand; run with --ignored"]
async fn outbound_echo_600_bidirectional_rtp() {
    let cfg = SipConfig {
        server: env_or("KUTSU_SIP_SERVER", "192.168.88.243:5060"),
        username: env_or("KUTSU_SIP_USER", "kutsu"),
        password: env_or("KUTSU_SIP_PASS", "kutsupw"),
        from_user: None,
        local_ip: None,
        local_port: None,
        sip_domain: None,
        register: false,
        register_expiry_secs: None,
        transport: Default::default(),
    };
    let transport = SipTransport::new(&cfg).await.expect("transport up");
    let mut call = transport
        .place_call(&env_or("KUTSU_SIP_EXT", "600"))
        .await
        .expect("place_call");

    // Wait for answer, bounded so a dead stand fails fast instead of hanging.
    let codec = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        match call.events().recv().await.expect("event stream open") {
            SipEvent::Answered { codec } => codec,
            SipEvent::Terminated(r) => panic!("terminated before answer: {r:?}"),
        }
    })
    .await
    .expect("no answer within 15s");
    assert!(matches!(codec.kind, G711Kind::Ulaw | G711Kind::Alaw));

    // Send ~150 frames of G.711 silence while counting echoed frames.
    let out = call.audio_out();
    let sender = tokio::spawn(async move {
        let payload = Bytes::from(vec![0xFFu8; 160]);
        let mut ticker = tokio::time::interval(Duration::from_millis(20));
        for _ in 0..150 {
            ticker.tick().await;
            if out.send(payload.clone()).await.is_err() {
                break;
            }
        }
    });

    let mut received = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        tokio::select! {
            frame = call.audio_in().recv() => {
                if frame.is_some() { received += 1; if received >= 50 { break; } }
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    sender.abort();

    eprintln!("[sip_outbound] codec={:?} received={received}", codec.kind);
    assert!(
        received > 0,
        "no echoed RTP frames — bidirectional media failed"
    );

    call.hangup().await;
    transport.shutdown().await;
}
