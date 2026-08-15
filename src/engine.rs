//! Call engine — drives one call's full lifecycle (dial → bridge → end → finalize).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot};

use crate::bridge::{self, BridgePorts};
use crate::config::{QualityConfig, ScenarioConfig, ServerConfig, SipConfig};
use crate::gemini_live::{self, EndedBy, Event, TranscriptEntry};
use crate::sip::{CallOutcome, SipCallParts, SipEvent, SipTransport};
use crate::state::{CallQuality, CallRecord, CallState, CallStore};

/// Quality-abort gate: should a call in progress be aborted for degraded
/// audio? `abort_underruns == 0` disables the gate (never abort). Pure
/// decision, extracted from the `qtick` arm of `run_call`'s select loop so
/// it's unit-testable without a live call.
fn should_abort(cfg: &QualityConfig, underruns: u64) -> bool {
    cfg.abort_underruns > 0 && underruns >= cfg.abort_underruns as u64
}

/// Merge a downlink `CallQuality` snapshot with the uplink (phone→bridge)
/// snapshot, so both directions are reported through the one `CallQuality`
/// record. Kept in one place, used by both the `qtick` arm and finalize.
fn merge_quality(mut q: CallQuality, u: crate::sip::UplinkQuality) -> CallQuality {
    q.uplink_received = u.received;
    q.uplink_lost = u.lost;
    q.uplink_reordered = u.reordered;
    q
}

/// Reconcile the select loop's `end_state` with the authoritative outcome
/// from the Gemini session: a model-initiated end is a success even if its
/// `EndCall` event lost the select race against e.g. `GeminiClosed` (which
/// would otherwise read `Failed`).
fn reconcile_final_state(end_state: CallState, ended_by: EndedBy) -> CallState {
    match ended_by {
        EndedBy::ModelEndCall => CallState::Completed,
        _ => end_state,
    }
}

/// Only attach the abort error if the reconciled outcome is still `Failed` —
/// a late `ModelEndCall` can override `end_state` to `Completed` even after
/// the quality-abort arm fired; don't leave a stale error on a completed call.
fn finalize_error(final_state: CallState, abort_reason: Option<String>) -> Option<String> {
    if final_state == CallState::Failed { abort_reason } else { None }
}

/// Map the reconciled `final_state` of an in-call terminal finalize to a
/// `CallOutcome`. `HungUp` is a successful connection (the caller or model
/// ended a connected call) — same success bucket as `Completed` per
/// `bump_counter`. `Cancelled` never got a dial outcome (call was cancelled
/// before/without a SIP result reaching this point in-call), so `None`.
fn finalize_outcome(final_state: CallState) -> Option<CallOutcome> {
    match final_state {
        CallState::Completed | CallState::HungUp => Some(CallOutcome::Completed),
        CallState::Failed => Some(CallOutcome::Failed),
        CallState::Cancelled => None,
        CallState::Queued | CallState::Ringing | CallState::InProgress => None,
    }
}

/// Errors from placing a call.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
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

/// Cumulative call counters, bumped for the life of the process (unlike the
/// live `CallStore` counts, these never decrease). Bundled in one `Arc` so
/// `run_call` takes a single param instead of four.
#[derive(Default)]
struct Counters {
    placed: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    /// Cumulative audio-quality totals, added once per call at finalize (not
    /// per tick) so a call's underruns/starved-ms are counted exactly once.
    underruns: AtomicU64,
    starved_ms: AtomicU64,
    quality_aborted: AtomicU64,
    /// Cumulative uplink (phone→bridge) RTP loss totals, added once per call
    /// at finalize alongside the downlink totals above.
    uplink_received: AtomicU64,
    uplink_lost: AtomicU64,
}

/// A point-in-time view of engine load and lifetime totals, for the
/// `/metrics` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub active: usize,
    pub queued: usize,
    pub placed_total: u64,
    pub completed_total: u64,
    pub failed_total: u64,
    pub cancelled_total: u64,
    pub channels_cap: usize,
    pub underruns_total: u64,
    pub starved_ms_total: u64,
    pub quality_aborted_total: u64,
    pub uplink_received_total: u64,
    pub uplink_lost_total: u64,
}

/// The call engine: owns the SIP transport, the call store, and config.
pub struct Engine {
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    permits: Arc<tokio::sync::Semaphore>,
    cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    seq: AtomicUsize,
    counters: Arc<Counters>,
}

impl Engine {
    /// Build the engine (binds the SIP transport).
    pub async fn new(server: Arc<ServerConfig>, sip_cfg: &SipConfig) -> Result<Self, EngineError> {
        let sip = SipTransport::new(sip_cfg).await?;
        let permits = Arc::new(tokio::sync::Semaphore::new(server.max_concurrent_channels));
        Ok(Self {
            sip,
            store: CallStore::new(),
            server,
            permits,
            cancels: Arc::new(Mutex::new(HashMap::new())),
            seq: AtomicUsize::new(0),
            counters: Arc::new(Counters::default()),
        })
    }

    pub fn store(&self) -> &CallStore {
        &self.store
    }

    /// Live load (from the `CallStore`) plus lifetime cumulative totals.
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        let counts = self.store.counts();
        MetricsSnapshot {
            active: counts.active,
            queued: counts.queued,
            placed_total: self.counters.placed.load(Ordering::Relaxed),
            completed_total: self.counters.completed.load(Ordering::Relaxed),
            failed_total: self.counters.failed.load(Ordering::Relaxed),
            cancelled_total: self.counters.cancelled.load(Ordering::Relaxed),
            channels_cap: self.server.max_concurrent_channels,
            underruns_total: self.counters.underruns.load(Ordering::Relaxed),
            starved_ms_total: self.counters.starved_ms.load(Ordering::Relaxed),
            quality_aborted_total: self.counters.quality_aborted.load(Ordering::Relaxed),
            uplink_received_total: self.counters.uplink_received.load(Ordering::Relaxed),
            uplink_lost_total: self.counters.uplink_lost.load(Ordering::Relaxed),
        }
    }

    /// Maximum wall-clock duration of a single call, in seconds (the engine's
    /// hard deadline). Used by the MCP task branch to size the task TTL so a
    /// long-but-valid call is never TTL-swept mid-flight.
    pub fn max_call_secs(&self) -> u64 {
        self.server.max_call_secs
    }

    /// Place an outbound call: records it as `Queued` and spawns the call
    /// task, which waits for a concurrency permit before dialing. Always
    /// succeeds; SIP/other failures surface later via `CallState::Failed`.
    pub async fn place_call(&self, number: String, scenario: ScenarioConfig) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let call_id = format!("call-{n}");
        self.store.insert(CallRecord {
            call_id: call_id.clone(),
            number: number.clone(),
            state: CallState::Queued,
            transcript: vec![],
            goal: None,
            error: None,
            started_ms: now_ms(),
            ended_ms: None,
            quality: CallQuality::default(),
            outcome: None,
            attempt: 1,
            retry_of: None,
        });
        self.counters.placed.fetch_add(1, Ordering::Relaxed);

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        self.cancels.lock().unwrap().insert(call_id.clone(), cancel_tx);

        let sip = self.sip.clone();
        let store = self.store.clone();
        let server = self.server.clone();
        let permits = self.permits.clone();
        let cancels = self.cancels.clone();
        let counters = self.counters.clone();
        let id = call_id.clone();
        tokio::spawn(async move {
            run_call(sip, store, server, permits, cancels, counters, cancel_rx, scenario, number, id).await;
        });

        call_id
    }

    /// Signal a running/queued call to end. Returns true if a live signal was sent.
    pub fn end_call(&self, call_id: &str) -> bool {
        if let Some(tx) = self.cancels.lock().unwrap().remove(call_id) {
            tx.send(()).is_ok()
        } else {
            false
        }
    }

    pub async fn shutdown(self) {
        self.sip.shutdown().await;
    }
}

/// Removes a call's `cancels` map entry when dropped, on every exit path —
/// early returns, the normal end-of-function fallthrough, and panic unwind
/// alike. Mirrors the owned-permit drop-safety pattern used for `_permit`
/// above. `end_call` may have already removed the entry (to send on it); a
/// second `remove` on an absent key is a harmless no-op.
struct CancelGuard {
    cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    call_id: String,
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.cancels.lock().unwrap().remove(&self.call_id);
    }
}

/// Bump the cumulative counter matching a terminal `CallState`. `Ringing`,
/// `InProgress`, and `Queued` never reach `finalize` and are unreachable here.
fn bump_counter(counters: &Counters, state: CallState) {
    match state {
        CallState::Completed | CallState::HungUp => {
            counters.completed.fetch_add(1, Ordering::Relaxed);
        }
        CallState::Failed => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
        }
        CallState::Cancelled => {
            counters.cancelled.fetch_add(1, Ordering::Relaxed);
        }
        CallState::Queued | CallState::Ringing | CallState::InProgress => {}
    }
}

/// Drive one call to completion. Safe ordering: SIP first, answer, THEN Gemini.
async fn run_call(
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    permits: Arc<tokio::sync::Semaphore>,
    cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    counters: Arc<Counters>,
    mut cancel_rx: oneshot::Receiver<()>,
    scenario: ScenarioConfig,
    number: String,
    call_id: String,
) {
    let _cancel_guard = CancelGuard { cancels: cancels.clone(), call_id: call_id.clone() };

    // Wait for a concurrency slot. Held (as an owned permit) for the whole
    // call; released on drop, including on panic unwind — replaces SlotGuard.
    // Raced against the cancel signal so a call parked in Queued (no permit
    // available yet) can still be cancelled instead of blocking forever.
    let _permit = tokio::select! {
        p = permits.acquire_owned() => p.expect("semaphore not closed"),
        _ = &mut cancel_rx => {
            store.finalize(&call_id, CallState::Cancelled, None, None, None, now_ms());
            bump_counter(&counters, CallState::Cancelled);
            return;
        }
    };
    store.set_state(&call_id, CallState::Ringing);
    // 1. INVITE.
    let call = match sip.place_call(&number).await {
        Ok(c) => c,
        Err(e) => {
            store.finalize(&call_id, CallState::Failed, None, Some(e.to_string()), Some(CallOutcome::Failed), now_ms());
            bump_counter(&counters, CallState::Failed);
            return;
        }
    };
    // 2. Decompose into owned channel ends.
    let SipCallParts { events: mut sip_events, audio_in, audio_out, hangup: sip_hangup, uplink_quality, .. } = call.split();

    // 3. Await answer (no Gemini yet — nothing to tear down on failure here).
    let codec = loop {
        match sip_events.recv().await {
            Some(SipEvent::Answered { codec }) => break codec,
            Some(SipEvent::Terminated(reason)) => {
                let (outcome, detail) = match reason {
                    crate::sip::TermReason::Failed { outcome, detail } => (outcome, detail),
                    // Remote/local hangup before answer is effectively no-answer.
                    _ => (CallOutcome::NoAnswer, format!("{reason:?}")),
                };
                store.finalize(&call_id, CallState::Failed, None, Some(detail), Some(outcome), now_ms());
                bump_counter(&counters, CallState::Failed);
                return;
            }
            None => {
                store.finalize(&call_id, CallState::Failed, None, Some("sip closed before answer".into()), Some(CallOutcome::Failed), now_ms());
                bump_counter(&counters, CallState::Failed);
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
            store.finalize(&call_id, CallState::Failed, None, Some(format!("gemini connect: {e}")), Some(CallOutcome::Failed), now_ms());
            bump_counter(&counters, CallState::Failed);
            return;
        }
    };
    let gemini_connected_at = now_ms();
    tracing::info!(%call_id, dead_air_ms = gemini_connected_at - answered_at, "gemini connected after answer");

    // 5. Split the session; 6. start the bridge.
    let (gemini_handle, gemini_in, gemini_events) = session.split();
    let (events_out_tx, mut events_out_rx) = mpsc::channel::<Event>(256);
    let quality = bridge::QualityShared::new();
    let ports = BridgePorts {
        codec: codec.kind,
        phone_in: audio_in,
        phone_out: audio_out,
        gemini_in,
        gemini_events,
        events_out: events_out_tx,
        prebuffer_ms: server.quality.prebuffer_ms,
        resume_ms: server.quality.resume_ms,
        quality: quality.clone(),
        call_id: call_id.clone(),
        uplink_dump: server.dump_uplink_dir.clone(),
    };
    let mut bridge_task = tokio::spawn(bridge::run(ports));

    // 7. Orchestration loop.
    let mut goal = None;
    let mut first_transcript = false;
    let mut events_open = true;
    let mut abort_reason: Option<String> = None;
    let deadline = tokio::time::sleep(Duration::from_secs(server.max_call_secs));
    tokio::pin!(deadline);
    let mut qtick = tokio::time::interval(Duration::from_secs(1));
    let end_state = loop {
        tokio::select! {
            ev = events_out_rx.recv(), if events_open => match ev {
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
                None => events_open = false, // bridge closed events_out; stop polling (the bridge_task arm ends the call)
            },
            r = sip_events.recv() => match r {
                Some(SipEvent::Terminated(_)) | None => break CallState::HungUp,
                Some(_) => {}
            },
            r = &mut bridge_task => break match r {
                Ok(crate::bridge::BridgeEnd::PhoneClosed) => CallState::HungUp,
                Ok(crate::bridge::BridgeEnd::GeminiClosed) => CallState::Failed,
                Err(_) => CallState::Failed, // bridge task panicked
            },
            _ = &mut deadline => break CallState::Completed,
            _ = &mut cancel_rx => break CallState::Cancelled,
            _ = qtick.tick() => {
                let q = merge_quality(quality.snapshot(), uplink_quality.snapshot());
                store.set_quality(&call_id, q);
                if should_abort(&server.quality, q.underruns) {
                    // NOTE: don't bump `quality_aborted` here — the abort is not
                    // authoritative yet. A model `EndCall` racing this tick can
                    // reconcile `final_state` back to Completed (see finalize),
                    // in which case this was not a quality abort. Count it at
                    // finalize, gated on the surviving abort error.
                    abort_reason = Some(format!(
                        "aborted: audio quality degraded ({} underruns, {} ms silence)",
                        q.underruns, q.starved_ms
                    ));
                    break CallState::Failed;
                }
            }
        }
    };

    // 8. Teardown (BYE both sides, stop the bridge, get the authoritative outcome).
    let _ = sip_hangup.send(());
    gemini_handle.hangup().await;
    bridge_task.abort();
    let outcome = gemini_handle.join().await;

    // 9. Finalize. Reconcile the loop's end_state with the authoritative outcome
    // (see `reconcile_final_state`).
    let final_state = reconcile_final_state(end_state, outcome.ended_by);
    // Final quality snapshot + cumulative totals, added exactly once here
    // (not per tick, which only updates the live record + checks the abort
    // threshold).
    let q = merge_quality(quality.snapshot(), uplink_quality.snapshot());
    store.set_quality(&call_id, q);
    counters.underruns.fetch_add(q.underruns, Ordering::Relaxed);
    counters.starved_ms.fetch_add(q.starved_ms, Ordering::Relaxed);
    counters.uplink_received.fetch_add(q.uplink_received, Ordering::Relaxed);
    counters.uplink_lost.fetch_add(q.uplink_lost, Ordering::Relaxed);
    tracing::info!(
        %call_id, codec = ?codec.kind,
        uplink_received = q.uplink_received, uplink_lost = q.uplink_lost,
        uplink_reordered = q.uplink_reordered,
        underruns = q.underruns, starved_ms = q.starved_ms, max_gap_ms = q.max_gap_ms,
        "call audio quality"
    );

    store.set_transcript(&call_id, outcome.transcript);
    let final_goal = goal.or(outcome.goal);
    let error = finalize_error(final_state, abort_reason);
    // Count the quality abort exactly once here, only if it survived reconcile
    // (a racing model EndCall can flip Failed -> Completed and clear the error).
    // `error` is Some iff this was an abort that stuck as Failed.
    if error.is_some() {
        counters.quality_aborted.fetch_add(1, Ordering::Relaxed);
    }
    store.finalize(&call_id, final_state, final_goal, error, finalize_outcome(final_state), now_ms());
    bump_counter(&counters, final_state);

    // 10. Persist. Owner-only permissions: the transcript may contain PII.
    if let Some(dir) = &server.transcript_dir {
        if let Some(rec) = store.get(&call_id) {
            let path = dir.join(format!("{call_id}.json"));
            if let Ok(json) = serde_json::to_string_pretty(&rec) {
                if let Err(e) = write_owner_only(&path, json.as_bytes()) {
                    tracing::warn!(%call_id, "failed to write transcript: {e}");
                }
            }
        }
    }
}

/// Write `data` to `path`, restricted to the owner (the transcript may
/// contain call PII). On non-Unix (this project also builds/tests on
/// Windows) we fall back to a plain write; the directory ACL must be
/// configured to be private (documented in the deployment guide).
#[cfg(unix)]
fn write_owner_only(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
    std::io::Write::write_all(&mut f, data)
}
#[cfg(not(unix))]
fn write_owner_only(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, data) // Windows: rely on directory ACL (documented)
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
            voice_gender: crate::config::Gender::Female,
            language: "en-US".into(),
            net_check: NetCheckConfig::default(),
            max_concurrent_channels: cap,
            greet_after_silence_ms: 4000,
            transcript_dir: None,
            dump_uplink_dir: None,
            max_call_secs: 600,
            quality: crate::config::QualityConfig::default(),
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
    async fn place_call_queues_when_at_cap() {
        // cap = 0: every call must sit in Queued forever (no permit available).
        let (server, sip_cfg) = test_configs(0);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
        let id = engine.place_call("600".into(), test_scenario()).await;
        // No permit → the spawned run_call is parked before INVITE; state stays Queued.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let rec = engine.store().get(&id).unwrap();
        assert_eq!(rec.state, CallState::Queued);
        assert_eq!(engine.store().queued_position(&id), Some(1));
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn end_call_cancels_a_queued_call() {
        let (server, sip_cfg) = test_configs(0); // cap 0 → parked in Queued
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
        let id = engine.place_call("600".into(), test_scenario()).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(engine.store().get(&id).unwrap().state, CallState::Queued);
        assert!(engine.end_call(&id));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(engine.store().get(&id).unwrap().state, CallState::Cancelled);
        // Cancel map entry cleaned up: a second end_call finds nothing live.
        assert!(!engine.end_call(&id));
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn end_call_unknown_id_is_false() {
        let (server, sip_cfg) = test_configs(1);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
        assert!(!engine.end_call("nope"));
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_snapshot_has_quality_fields() {
        let (server, sip_cfg) = test_configs(1);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
        let m = engine.metrics_snapshot();
        assert_eq!(m.underruns_total, 0);
        assert_eq!(m.starved_ms_total, 0);
        assert_eq!(m.quality_aborted_total, 0);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_snapshot_exposes_uplink_totals() {
        let (server, sip_cfg) = test_configs(1);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
        let m = engine.metrics_snapshot();
        assert_eq!(m.uplink_received_total, 0);
        assert_eq!(m.uplink_lost_total, 0);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_snapshot_counts_placed_and_queued() {
        let (server, sip_cfg) = test_configs(0); // cap 0 → calls park in Queued
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg).await.unwrap();
        let _a = engine.place_call("600".into(), test_scenario()).await;
        let _b = engine.place_call("601".into(), test_scenario()).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let m = engine.metrics_snapshot();
        assert_eq!(m.placed_total, 2);
        assert_eq!(m.queued, 2);
        assert_eq!(m.active, 0);
        assert_eq!(m.channels_cap, 0);
        engine.shutdown().await;
    }

    #[test]
    fn should_abort_disabled_never_aborts() {
        let cfg = crate::config::QualityConfig { abort_underruns: 0, ..crate::config::QualityConfig::default() };
        assert!(!should_abort(&cfg, 0));
        assert!(!should_abort(&cfg, 1_000_000));
    }

    #[test]
    fn should_abort_trips_at_threshold_not_before() {
        let cfg = crate::config::QualityConfig { abort_underruns: 40, ..crate::config::QualityConfig::default() };
        assert!(!should_abort(&cfg, 0));
        assert!(!should_abort(&cfg, 39));
        assert!(should_abort(&cfg, 40));
        assert!(should_abort(&cfg, 41));
    }

    #[test]
    fn reconcile_final_state_table() {
        // A model-initiated end always wins as Completed, regardless of what
        // the select loop landed on (it may have raced GeminiClosed -> Failed).
        for end_state in [CallState::Failed, CallState::HungUp, CallState::Completed, CallState::Cancelled] {
            assert_eq!(reconcile_final_state(end_state, EndedBy::ModelEndCall), CallState::Completed);
        }
        // Any other ended_by leaves end_state untouched.
        for ended_by in [EndedBy::CallerHangup, EndedBy::RemoteClose, EndedBy::Error] {
            for end_state in [CallState::Failed, CallState::HungUp, CallState::Completed, CallState::Cancelled] {
                assert_eq!(reconcile_final_state(end_state, ended_by), end_state);
            }
        }
    }

    #[test]
    fn finalize_error_only_attached_when_failed() {
        assert_eq!(finalize_error(CallState::Failed, Some("boom".into())), Some("boom".into()));
        assert_eq!(finalize_error(CallState::Failed, None), None);
        // A reconciled Completed (e.g. late ModelEndCall overriding an aborted
        // end_state) must not carry a stale abort error.
        assert_eq!(finalize_error(CallState::Completed, Some("boom".into())), None);
        assert_eq!(finalize_error(CallState::HungUp, Some("boom".into())), None);
        assert_eq!(finalize_error(CallState::Cancelled, Some("boom".into())), None);
    }

    #[test]
    fn finalize_outcome_maps_hungup_to_completed() {
        // HungUp is a successful connection (caller/model ended a connected
        // call) — same success bucket as Completed per `bump_counter`, not a
        // technical failure.
        assert_eq!(finalize_outcome(CallState::Completed), Some(CallOutcome::Completed));
        assert_eq!(finalize_outcome(CallState::HungUp), Some(CallOutcome::Completed));
        assert_eq!(finalize_outcome(CallState::Failed), Some(CallOutcome::Failed));
        assert_eq!(finalize_outcome(CallState::Cancelled), None);
    }
}
