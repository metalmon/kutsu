//! Call engine — drives one call's full lifecycle (dial → bridge → end → finalize).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use crate::bridge::{self, BridgePorts};
use crate::config::{ScenarioConfig, ServerConfig, SipConfig};
use crate::gemini_live::{self, Event, TranscriptEntry};
use crate::sip::{SipCallParts, SipEvent, SipTransport};
use crate::state::{CallRecord, CallState, CallStore};

/// Errors from placing a call.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("concurrency cap reached")]
    CapReached,
    #[error(transparent)]
    Sip(#[from] crate::sip::SipError),
}

/// Milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The call engine: owns the SIP transport, the call store, and config.
pub struct Engine {
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    active: Arc<AtomicUsize>,
    seq: AtomicUsize,
}

impl Engine {
    /// Build the engine (binds the SIP transport).
    pub async fn new(server: Arc<ServerConfig>, sip_cfg: &SipConfig) -> Result<Self, EngineError> {
        let sip = SipTransport::new(sip_cfg).await?;
        Ok(Self {
            sip,
            store: CallStore::new(),
            server,
            active: Arc::new(AtomicUsize::new(0)),
            seq: AtomicUsize::new(0),
        })
    }

    pub fn store(&self) -> &CallStore {
        &self.store
    }

    /// Place an outbound call. Cap-checks, spawns the call task, returns its id.
    pub async fn place_call(
        &self,
        number: String,
        scenario: ScenarioConfig,
    ) -> Result<String, EngineError> {
        // Reserve a slot atomically against the cap.
        let cap = self.server.max_concurrent_channels;
        let mut cur = self.active.load(Ordering::Acquire);
        loop {
            if cur >= cap {
                return Err(EngineError::CapReached);
            }
            match self.active.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }

        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let call_id = format!("call-{n}");
        self.store.insert(CallRecord {
            call_id: call_id.clone(),
            number: number.clone(),
            state: CallState::Ringing,
            transcript: vec![],
            goal: None,
            error: None,
            started_ms: now_ms(),
            ended_ms: None,
        });

        let sip = self.sip.clone();
        let store = self.store.clone();
        let server = self.server.clone();
        let active = self.active.clone();
        let id = call_id.clone();
        tokio::spawn(async move {
            run_call(sip, store, server, scenario, number, id).await;
            active.fetch_sub(1, Ordering::AcqRel);
        });

        Ok(call_id)
    }

    pub async fn shutdown(self) {
        self.sip.shutdown().await;
    }
}

/// Drive one call to completion. Safe ordering: SIP first, answer, THEN Gemini.
async fn run_call(
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    scenario: ScenarioConfig,
    number: String,
    call_id: String,
) {
    // 1. INVITE.
    let call = match sip.place_call(&number).await {
        Ok(c) => c,
        Err(e) => {
            store.finalize(&call_id, CallState::Failed, None, Some(e.to_string()), now_ms());
            return;
        }
    };
    // 2. Decompose into owned channel ends.
    let SipCallParts { events: mut sip_events, audio_in, audio_out, hangup: sip_hangup, .. } = call.split();

    // 3. Await answer (no Gemini yet — nothing to tear down on failure here).
    let codec = loop {
        match sip_events.recv().await {
            Some(SipEvent::Answered { codec }) => break codec,
            Some(SipEvent::Terminated(reason)) => {
                store.finalize(&call_id, CallState::Failed, None, Some(format!("no answer: {reason:?}")), now_ms());
                return;
            }
            None => {
                store.finalize(&call_id, CallState::Failed, None, Some("sip closed before answer".into()), now_ms());
                return;
            }
        }
    };
    let answered_at = now_ms();
    store.set_state(&call_id, CallState::InProgress);

    // 4. Connect Gemini AFTER answer (its events now have a drain waiting).
    let session = match gemini_live::start(&server, &scenario).await {
        Ok(s) => s,
        Err(e) => {
            let _ = sip_hangup.send(());
            store.finalize(&call_id, CallState::Failed, None, Some(format!("gemini connect: {e}")), now_ms());
            return;
        }
    };
    let gemini_connected_at = now_ms();
    tracing::info!(%call_id, dead_air_ms = gemini_connected_at - answered_at, "gemini connected after answer");

    // 5. Split the session; 6. start the bridge.
    let (gemini_handle, gemini_in, gemini_events) = session.split();
    let (events_out_tx, mut events_out_rx) = mpsc::channel::<Event>(256);
    let ports = BridgePorts {
        codec: codec.kind,
        phone_in: audio_in,
        phone_out: audio_out,
        gemini_in,
        gemini_events,
        events_out: events_out_tx,
    };
    let mut bridge_task = tokio::spawn(bridge::run(ports));

    // 7. Orchestration loop.
    let mut goal = None;
    let mut first_transcript = false;
    let deadline = tokio::time::sleep(Duration::from_secs(server.max_call_secs));
    tokio::pin!(deadline);
    let end_state = loop {
        tokio::select! {
            ev = events_out_rx.recv() => match ev {
                // NOTE: the bridge CONSUMES OutputAudio and Interrupted (they never
                // reach events_out); it forwards only Transcript/TurnComplete/EndCall/
                // Warning. So the earliest agent-activity signal the engine can observe
                // is the first Transcript (a proxy for greeting time — true audio-onset
                // timing would need a bridge-level stamp, deferred).
                Some(Event::Transcript { role, text, final_ }) => {
                    if !first_transcript {
                        first_transcript = true;
                        tracing::info!(%call_id, first_transcript_after_answer_ms = now_ms() - answered_at, "first transcript after answer");
                    }
                    if final_ {
                        store.append_transcript(&call_id, TranscriptEntry { role, text, ts_ms: now_ms() });
                    }
                }
                Some(Event::EndCall { goal: g }) => { goal = Some(g); break CallState::Completed; }
                Some(Event::TurnComplete) => {}
                Some(Event::Warning(w)) => tracing::warn!(%call_id, "gemini warning: {w}"),
                Some(_) => {} // OutputAudio/Interrupted: consumed by the bridge, not forwarded
                None => {} // bridge dropped events_out; the bridge_task arm will fire
            },
            r = sip_events.recv() => match r {
                Some(SipEvent::Terminated(_)) | None => break CallState::HungUp,
                Some(_) => {}
            },
            _ = &mut bridge_task => break CallState::HungUp,
            _ = &mut deadline => break CallState::Completed,
        }
    };

    // 8. Teardown (BYE both sides, stop the bridge, get the authoritative outcome).
    let _ = sip_hangup.send(());
    gemini_handle.hangup().await;
    bridge_task.abort();
    let outcome = gemini_handle.join().await;

    // 9. Finalize: authoritative transcript + goal (model's EndCall goal wins).
    store.set_transcript(&call_id, outcome.transcript);
    let final_goal = goal.or(outcome.goal);
    store.finalize(&call_id, end_state, final_goal, None, now_ms());

    // 10. Persist.
    if let Some(dir) = &server.transcript_dir {
        if let Some(rec) = store.get(&call_id) {
            let path = dir.join(format!("{call_id}.json"));
            if let Ok(json) = serde_json::to_string_pretty(&rec) {
                if let Err(e) = std::fs::write(&path, json) {
                    tracing::warn!(%call_id, "failed to write transcript: {e}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Model, NetCheckConfig};

    /// Build a valid `(ServerConfig, SipConfig)` pair for tests. The SIP config
    /// is bound to loopback so `SipTransport::new` binds offline (no real trunk).
    fn test_configs(cap: usize) -> (ServerConfig, SipConfig) {
        let server = ServerConfig {
            api_key: "test-key".into(),
            proxy: None,
            model: Model::HalfCascade,
            voice: "Autonoe".into(),
            language: "en-US".into(),
            net_check: NetCheckConfig::default(),
            max_concurrent_channels: cap,
            greet_after_silence_ms: 4000,
            transcript_dir: None,
            max_call_secs: 600,
        };
        let sip_cfg = SipConfig {
            server: "127.0.0.1:5060".into(),
            username: "test".into(),
            password: "test".into(),
            from_user: None,
            local_ip: Some("127.0.0.1".parse().unwrap()),
            register: false,
            transport: Default::default(),
        };
        (server, sip_cfg)
    }

    fn test_scenario() -> ScenarioConfig {
        ScenarioConfig {
            system_prompt: "You are a test assistant.".into(),
            goal_schema: serde_json::json!({"type": "object", "required": ["disposition"]}),
            context: None,
        }
    }

    #[test]
    fn now_ms_is_nonzero() {
        assert!(now_ms() > 0);
    }

    #[tokio::test]
    async fn place_call_rejects_when_at_cap() {
        // Build a config with max_concurrent_channels = 0 so any call is over cap.
        let (server, sip_cfg) = test_configs(0);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
        let err = engine
            .place_call("600".into(), test_scenario())
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::CapReached));
        engine.shutdown().await;
    }
}
