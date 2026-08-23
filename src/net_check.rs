//! Fail-closed network preflight: probe the Gemini endpoint (WSS ping RTT) and
//! decide whether the network is good enough to place a call.
//!
//! The endpoint URL + proxy-aware WS connect are the crate's (single source of
//! truth — the preflight probes the exact URL, over the exact path, the call
//! will use). This module keeps only the kutsu-specific RTT measurement: the
//! crate's `Session` sends `setup` and swallows ping/pong, so the raw
//! `connect_ws` stream is used here for direct ping/pong timing.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use ::gemini_live::transport::{ProxyConfig, connect_ws, endpoint_url};

use crate::config::{NetCheckConfig, ScenarioConfig, ServerConfig};
use crate::error::{Error, Result};

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
    pub connect_ms: u32,
    pub setup_ms: u32,
}

impl NetworkHealth {
    pub fn summary(&self) -> String {
        format!(
            "rtt_p50={}ms rtt_p95={}ms jitter={}ms loss={}% connect={}ms setup={}ms",
            self.rtt_p50_ms, self.rtt_p95_ms, self.jitter_ms, self.loss_pct,
            self.connect_ms, self.setup_ms
        )
    }
}

pub fn verdict(h: &NetworkHealth, cfg: &NetCheckConfig) -> Verdict {
    let setup_bad = cfg.max_setup_ms > 0
        && h.connect_ms.saturating_add(h.setup_ms) > cfg.max_setup_ms;
    if h.rtt_p95_ms > cfg.max_rtt_ms
        || h.jitter_ms > cfg.max_jitter_ms
        || h.loss_pct > cfg.max_loss_pct
        || setup_bad
    {
        Verdict::Unusable
    } else {
        Verdict::Ok
    }
}

/// Exponential backoff (ms) to wait after a failed preflight `attempt` (1-based):
/// `base * 2^(attempt-1)`. Shift is capped to avoid overflow.
pub fn retry_backoff_ms(attempt: u32, base_ms: u64) -> u64 {
    let shift = attempt.saturating_sub(1).min(16);
    base_ms.saturating_mul(1u64 << shift)
}

/// Open a real WSS connection to the Gemini endpoint and measure ping/pong RTT.
/// Uses the configured proxy (if any) so the probe traverses the same path the
/// call will — and the crate's `endpoint_url`/`connect_ws`, so it probes the
/// exact URL the session opens.
pub async fn preflight(server: &ServerConfig, scenario: &ScenarioConfig) -> Result<NetworkHealth> {
    let url = endpoint_url(crate::gemini_live::map_model(server.model), &server.api_key);
    let proxy = server.proxy.as_ref().map(|p| ProxyConfig {
        url: p.url.clone(),
        user: p.user.clone(),
        password: p.password.clone(),
    });
    let connect_started = Instant::now();
    let mut ws = connect_ws(proxy.as_ref(), &url)
        .await
        .map_err(|e| Error::Connect(format!("preflight connect: {e}")))?;
    let connect_ms = connect_started.elapsed().as_millis() as u32;

    // Warm-up pings: exclude the cold first RTT(s) from the steady-state stats.
    for i in 0..server.net_check.warmup_pings {
        let _ = ws.send(Message::Ping(vec![0xff, i as u8])).await;
        let budget = Duration::from_millis((server.net_check.max_rtt_ms as u64) * 4);
        let _ = tokio::time::timeout(budget, wait_for_pong(&mut ws)).await;
    }

    let mut rtts: Vec<u32> = Vec::new();
    let mut lost = 0u32;
    let n = server.net_check.samples.max(1);
    for i in 0..n {
        let payload = vec![i as u8];
        let sent = Instant::now();
        ws.send(Message::Ping(payload.clone()))
            .await
            .map_err(|e| Error::Connect(format!("preflight ping: {e}")))?;
        // Wait up to max_rtt*4 for the matching pong.
        let budget = Duration::from_millis((server.net_check.max_rtt_ms as u64) * 4);
        match tokio::time::timeout(budget, wait_for_pong(&mut ws)).await {
            Ok(Ok(())) => rtts.push(sent.elapsed().as_millis() as u32),
            _ => lost += 1,
        }
    }
    // Validate the actual session setup, not just transport reachability. The
    // ping/pong above proves the socket is up but never sends the real `setup`
    // (tools/goal_schema), so a setup rejection — e.g. WS close 1007 on a
    // malformed schema — would pass preflight and then storm reconnects on the
    // live call. Send the exact setup this call will use and require a
    // `setupComplete` before declaring the network usable.
    let setup_started = Instant::now();
    let setup = crate::gemini_live::build_setup_config(server, scenario);
    ws.send(Message::Text(
        ::gemini_live::wire::build_setup(&setup).to_string(),
    ))
    .await
    .map_err(|e| Error::Connect(format!("preflight setup send: {e}")))?;
    let ack_budget = Duration::from_millis((server.net_check.max_rtt_ms as u64 * 4).max(2000));
    let probe = await_setup_ack(&mut ws, ack_budget).await;
    let setup_ms = setup_started.elapsed().as_millis() as u32;
    let _ = ws.close(None).await;
    match probe {
        SetupProbe::Ok => {
            let mut health = summarize(&mut rtts, lost, n);
            health.connect_ms = connect_ms;
            health.setup_ms = setup_ms;
            Ok(health)
        }
        SetupProbe::Rejected(code, reason) => Err(Error::Connect(format!(
            "gemini setup rejected (ws close {code}): {reason}"
        ))),
        SetupProbe::Timeout => Err(Error::Connect(
            "gemini setup not acknowledged (no setupComplete within budget)".into(),
        )),
    }
}

/// Outcome of the setup handshake probe.
#[derive(Debug)]
enum SetupProbe {
    /// `setupComplete` received — the session is genuinely usable.
    Ok,
    /// The server closed the WS before acknowledging setup. Carries the close
    /// code + reason (e.g. 1007 "…only allowed for OBJECT type").
    Rejected(u16, String),
    /// No `setupComplete` (nor a close) within the budget.
    Timeout,
}

/// Read server frames until the session acknowledges `setup` (`setupComplete`),
/// the server closes (rejection), or `budget` elapses. The caller must have
/// already sent the setup frame. Generic over the stream so it is unit-testable
/// with a scripted frame sequence.
async fn await_setup_ack<S>(ws: &mut S, budget: Duration) -> SetupProbe
where
    S: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    let deadline = tokio::time::sleep(budget);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => return SetupProbe::Timeout,
            msg = ws.next() => match msg {
                None => {
                    return SetupProbe::Rejected(0, "stream ended before setupComplete".into());
                }
                Some(Ok(Message::Close(frame))) => {
                    let (code, reason) = frame
                        .map(|f| (u16::from(f.code), f.reason.to_string()))
                        .unwrap_or((0, "no close frame".into()));
                    return SetupProbe::Rejected(code, reason);
                }
                Some(Ok(Message::Text(t))) if is_setup_complete(t.as_bytes()) => {
                    return SetupProbe::Ok;
                }
                Some(Ok(Message::Binary(b))) if is_setup_complete(&b) => return SetupProbe::Ok,
                Some(Ok(_)) => continue, // pong / other data frame — keep waiting
                Some(Err(e)) => return SetupProbe::Rejected(0, e.to_string()),
            }
        }
    }
}

/// True if a server frame's bytes carry a `setupComplete` acknowledgement.
fn is_setup_complete(bytes: &[u8]) -> bool {
    ::gemini_live::wire::parse_server_message(bytes)
        .map(|evs| {
            evs.iter()
                .any(|e| matches!(e, ::gemini_live::types::ServerEvent::SetupComplete))
        })
        .unwrap_or(false)
}

async fn wait_for_pong<S>(ws: &mut S) -> Result<()>
where
    S: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
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

fn summarize(rtts: &mut [u32], lost: u32, total: u32) -> NetworkHealth {
    let loss_pct = (lost as f32 / total as f32) * 100.0;
    if rtts.is_empty() {
        return NetworkHealth {
            rtt_p50_ms: u32::MAX,
            rtt_p95_ms: u32::MAX,
            jitter_ms: u32::MAX,
            loss_pct,
            connect_ms: 0,
            setup_ms: 0,
        };
    }
    rtts.sort_unstable();
    let p = |q: f32| rtts[((rtts.len() as f32 - 1.0) * q).round() as usize];
    // Jitter as the p95−p50 RTT spread — percentile-based, robust to outliers
    // (unlike mean absolute deviation, which a single spike skews).
    let jitter = p(0.95).saturating_sub(p(0.50));
    NetworkHealth {
        rtt_p50_ms: p(0.50),
        rtt_p95_ms: p(0.95),
        jitter_ms: jitter,
        loss_pct,
        connect_ms: 0,
        setup_ms: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NetCheckConfig;

    #[test]
    fn verdict_respects_thresholds() {
        let cfg = NetCheckConfig::default(); // 300/50/2.0
        let good = NetworkHealth {
            rtt_p50_ms: 40,
            rtt_p95_ms: 120,
            jitter_ms: 15,
            loss_pct: 0.0,
            connect_ms: 0,
            setup_ms: 0,
        };
        assert!(matches!(verdict(&good, &cfg), Verdict::Ok));

        let high_rtt = NetworkHealth {
            rtt_p95_ms: 800,
            ..good
        };
        assert!(matches!(verdict(&high_rtt, &cfg), Verdict::Unusable));

        let lossy = NetworkHealth {
            loss_pct: 10.0,
            ..good
        };
        assert!(matches!(verdict(&lossy, &cfg), Verdict::Unusable));

        let jittery = NetworkHealth {
            jitter_ms: 200,
            ..good
        };
        assert!(matches!(verdict(&jittery, &cfg), Verdict::Unusable));
    }

    #[test]
    fn verdict_fails_on_high_setup_cost() {
        let cfg = NetCheckConfig::default(); // max_setup_ms = 2500
        let base = NetworkHealth {
            rtt_p50_ms: 40,
            rtt_p95_ms: 120,
            jitter_ms: 15,
            loss_pct: 0.0,
            connect_ms: 1500,
            setup_ms: 1500, // sum 3000 > 2500
        };
        assert!(matches!(verdict(&base, &cfg), Verdict::Unusable));
    }

    #[test]
    fn verdict_setup_gate_disabled_when_zero() {
        let cfg = NetCheckConfig {
            max_setup_ms: 0,
            ..NetCheckConfig::default()
        };
        let h = NetworkHealth {
            rtt_p50_ms: 40,
            rtt_p95_ms: 120,
            jitter_ms: 15,
            loss_pct: 0.0,
            connect_ms: 9000,
            setup_ms: 9000,
        };
        assert!(matches!(verdict(&h, &cfg), Verdict::Ok));
    }

    #[test]
    fn backoff_is_exponential() {
        assert_eq!(retry_backoff_ms(1, 5000), 5000);
        assert_eq!(retry_backoff_ms(2, 5000), 10000);
        assert_eq!(retry_backoff_ms(3, 5000), 20000);
    }

    #[test]
    fn jitter_is_the_p95_minus_p50_spread() {
        let mut rtts = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        let h = summarize(&mut rtts, 0, 10);
        assert!(h.rtt_p95_ms >= h.rtt_p50_ms);
        assert_eq!(h.jitter_ms, h.rtt_p95_ms - h.rtt_p50_ms);
    }

    #[tokio::test]
    async fn setup_ack_ok_on_setup_complete() {
        let frames: Vec<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> = vec![
            Ok(Message::Pong(vec![])),
            Ok(Message::Binary(br#"{"setupComplete":{}}"#.to_vec())),
        ];
        let mut s = futures_util::stream::iter(frames);
        assert!(matches!(
            await_setup_ack(&mut s, Duration::from_secs(1)).await,
            SetupProbe::Ok
        ));
    }

    #[tokio::test]
    async fn setup_ack_rejected_on_close_carries_code() {
        use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        let frames: Vec<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> =
            vec![Ok(Message::Close(Some(CloseFrame {
                code: CloseCode::from(1007u16),
                reason: "properties: only allowed for OBJECT type".into(),
            })))];
        let mut s = futures_util::stream::iter(frames);
        match await_setup_ack(&mut s, Duration::from_secs(1)).await {
            SetupProbe::Rejected(code, _) => assert_eq!(code, 1007),
            other => panic!("expected Rejected(1007), got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn setup_ack_times_out_when_silent() {
        // A stream that yields nothing (pends) must hit the budget → Timeout.
        let mut s = futures_util::stream::pending::<
            std::result::Result<Message, tokio_tungstenite::tungstenite::Error>,
        >();
        assert!(matches!(
            await_setup_ack(&mut s, Duration::from_millis(50)).await,
            SetupProbe::Timeout
        ));
    }

    #[test]
    fn summary_is_readable() {
        let h = NetworkHealth {
            rtt_p50_ms: 40,
            rtt_p95_ms: 120,
            jitter_ms: 15,
            loss_pct: 1.5,
            connect_ms: 200,
            setup_ms: 300,
        };
        let s = h.summary();
        assert!(s.contains("rtt_p95=120ms"));
        assert!(s.contains("loss=1.5%"));
        assert!(s.contains("connect=200ms"));
        assert!(s.contains("setup=300ms"));
    }
}
