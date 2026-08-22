//! Call engine — drives one call's full lifecycle (dial → bridge → end → finalize).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Notify, mpsc, oneshot};

use crate::bridge::{self, BridgePorts};
use crate::config::{QualityConfig, ScenarioConfig, ServerConfig, SipConfig};
use crate::gemini_live::{self, EndedBy, Event, TranscriptEntry};
use crate::queue::{MemQueue, PendingEntry, QueueStore};
use crate::sip::{CallOutcome, SipCallParts, SipEvent, SipTransport};
use crate::state::{CallQuality, CallRecord, CallState, CallStore};

/// Quality-abort gate: should a call in progress be aborted for degraded
/// audio? `abort_underruns == 0` disables the gate (never abort). Pure
/// decision, extracted from the `qtick` arm of `run_call`'s select loop so
/// it's unit-testable without a live call.
fn should_abort(cfg: &QualityConfig, underruns: u64) -> bool {
    cfg.abort_underruns > 0 && underruns >= cfg.abort_underruns as u64
}

/// Number of 1 s quality ticks in the rolling uplink-loss window (~8 s).
const UPLINK_WINDOW_TICKS: usize = 8;

/// Rolling-window uplink RTP loss %, from the cumulative `(received, lost)` at
/// the window start vs now. `None` until the window holds enough packets to
/// judge (so a tiny early sample can't trigger an abort). Rolling (not
/// cumulative) so a late burst on a long call isn't diluted away.
fn window_loss_pct(oldest: (u64, u64), current: (u64, u64)) -> Option<f32> {
    let recv = current.0.saturating_sub(oldest.0);
    let lost = current.1.saturating_sub(oldest.1);
    let total = recv + lost;
    if total < 200 {
        None
    } else {
        Some(lost as f32 / total as f32 * 100.0)
    }
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

/// Should a busy dial attempt be retried? `attempt` is the 1-based attempt
/// number of the call that just finalized Busy; retry is allowed as long as
/// it hasn't yet reached the configured cap. Pure, unit-tested independent of
/// any live call.
fn should_retry_busy(attempt: u32, cfg: &crate::config::RetryConfig) -> bool {
    attempt < cfg.busy_max_attempts
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
    if final_state == CallState::Failed {
        abort_reason
    } else {
        None
    }
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
    /// Per-outcome cumulative totals for the dial-failure outcomes.
    /// `bump_counter_outcome` maps `CallOutcome::Completed`/`Failed` onto
    /// `completed`/`failed` above and the rest onto these.
    busy: AtomicU64,
    no_answer: AtomicU64,
    rejected: AtomicU64,
    not_found: AtomicU64,
    unavailable: AtomicU64,
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
    pub busy_total: u64,
    pub no_answer_total: u64,
    pub rejected_total: u64,
    pub not_found_total: u64,
    pub unavailable_total: u64,
}

/// Everything needed to enqueue a new call (`place_call_at`'s body), bundled
/// so it can be handed to `run_call` — which has no `&Engine` — for Task 8's
/// busy-retry requeue. Cloning is cheap (all fields are `Arc`/already-`Clone`
/// handles), matching the clones the dispatcher already threads into
/// `run_call`.
#[derive(Clone)]
struct Enqueuer {
    seq: Arc<AtomicUsize>,
    store: CallStore,
    queue: Arc<Mutex<Box<dyn QueueStore>>>,
    wake: Arc<Notify>,
    counters: Arc<Counters>,
}

impl Enqueuer {
    /// Enqueue an outbound call eligible for dispatch at `eligible_at_ms`.
    /// Records the call `Queued`, pushes a `PendingEntry`, and notifies the
    /// dispatcher. Returns the new call_id. Shared body for
    /// `Engine::place_call_at` and `run_call`'s busy-retry requeue.
    async fn place_call_at(
        &self,
        number: String,
        scenario: ScenarioConfig,
        eligible_at_ms: u64,
        attempt: u32,
        retry_of: Option<String>,
    ) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let call_id = format!("call-{n}");
        self.store.insert(CallRecord {
            call_id: call_id.clone(),
            number: number.clone(),
            state: CallState::Queued,
            transcript: vec![],
            affect: vec![],
            goal: None,
            error: None,
            started_ms: now_ms(),
            ended_ms: None,
            quality: CallQuality::default(),
            outcome: None,
            attempt,
            retry_of: retry_of.clone(),
        });
        self.counters.placed.fetch_add(1, Ordering::Relaxed);
        self.queue.lock().unwrap().push(PendingEntry {
            call_id: call_id.clone(),
            number,
            scenario,
            eligible_at_ms,
            attempt,
            retry_of,
        });
        // Wake the dispatcher so an immediate call dispatches without waiting
        // for its eligibility timer, and a scheduled call re-arms the timer.
        self.wake.notify_one();
        call_id
    }
}

/// The call engine: owns the SIP transport, the call store, config, and the
/// pending-call queue driven by a central dispatcher task (replaces the old
/// spawn-and-park-on-a-semaphore model).
pub struct Engine {
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    /// Pending (not-yet-dispatched) calls, ordered by (eligible_at_ms, call_id).
    queue: Arc<Mutex<Box<dyn QueueStore>>>,
    /// Count of dispatched-but-not-yet-finalized calls; the concurrency gate
    /// (`running < max_concurrent_channels`) that the semaphore used to enforce.
    running: Arc<AtomicUsize>,
    cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    counters: Arc<Counters>,
    /// Bundles the id sequence + store/queue/wake/counters handles needed to
    /// enqueue a call; also cloned into every `run_call` task so Task 8's
    /// busy-retry can requeue without needing `&Engine`.
    enqueuer: Enqueuer,
    /// The central dispatcher task; aborted on `shutdown`.
    dispatcher: tokio::task::JoinHandle<()>,
}

impl Engine {
    /// Build the engine (binds the SIP transport) and spawn the dispatcher.
    pub async fn new(server: Arc<ServerConfig>, sip_cfg: &SipConfig) -> Result<Self, EngineError> {
        let sip = SipTransport::new(sip_cfg).await?;
        let store = CallStore::new();
        let queue: Arc<Mutex<Box<dyn QueueStore>>> =
            Arc::new(Mutex::new(Box::new(MemQueue::new())));
        let running = Arc::new(AtomicUsize::new(0));
        let wake = Arc::new(Notify::new());
        let cancels = Arc::new(Mutex::new(HashMap::new()));
        let counters = Arc::new(Counters::default());
        let seq = Arc::new(AtomicUsize::new(0));
        let enqueuer = Enqueuer {
            seq,
            store: store.clone(),
            queue: queue.clone(),
            wake: wake.clone(),
            counters: counters.clone(),
        };
        let dispatcher = tokio::spawn(dispatcher(
            sip.clone(),
            store.clone(),
            server.clone(),
            queue.clone(),
            running.clone(),
            wake.clone(),
            cancels.clone(),
            counters.clone(),
            enqueuer.clone(),
        ));
        Ok(Self {
            sip,
            store,
            server,
            queue,
            running,
            cancels,
            counters,
            enqueuer,
            dispatcher,
        })
    }

    pub fn store(&self) -> &CallStore {
        &self.store
    }

    /// Live load (dispatched count + pending-queue depth) plus lifetime
    /// cumulative totals.
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            active: self.running.load(Ordering::Relaxed),
            queued: self.queue.lock().unwrap().len(),
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
            busy_total: self.counters.busy.load(Ordering::Relaxed),
            no_answer_total: self.counters.no_answer.load(Ordering::Relaxed),
            rejected_total: self.counters.rejected.load(Ordering::Relaxed),
            not_found_total: self.counters.not_found.load(Ordering::Relaxed),
            unavailable_total: self.counters.unavailable.load(Ordering::Relaxed),
        }
    }

    /// Maximum wall-clock duration of a single call, in seconds (the engine's
    /// hard deadline). Used by the MCP task branch to size the task TTL so a
    /// long-but-valid call is never TTL-swept mid-flight.
    pub fn max_call_secs(&self) -> u64 {
        self.server.max_call_secs
    }

    /// Place an outbound call now: enqueues it as `Queued` (attempt 1, no
    /// `retry_of`) and wakes the dispatcher. Always succeeds; SIP/other
    /// failures surface later via `CallState::Failed`.
    pub async fn place_call(&self, number: String, scenario: ScenarioConfig) -> String {
        self.place_call_at(number, scenario, now_ms(), 1, None)
            .await
    }

    /// Enqueue an outbound call eligible for dispatch at `eligible_at_ms`
    /// (immediate callers pass `now_ms()`; scheduled/retry callers pass a
    /// future time). `attempt` is 1 for a first dial; Task 8's busy-retry
    /// passes the incremented attempt with the prior call's id as `retry_of`.
    /// Records the call `Queued`, pushes a `PendingEntry`, and notifies the
    /// dispatcher. Returns the new call_id.
    pub async fn place_call_at(
        &self,
        number: String,
        scenario: ScenarioConfig,
        eligible_at_ms: u64,
        attempt: u32,
        retry_of: Option<String>,
    ) -> String {
        self.enqueuer
            .place_call_at(number, scenario, eligible_at_ms, attempt, retry_of)
            .await
    }

    /// 1-based position of a pending call in real dispatch order
    /// (`queue.position`). `None` once it has been dispatched or was never
    /// queued. Reflects `eligible_at_ms` ordering, so a future-scheduled call
    /// ranks behind sooner-eligible ones.
    pub fn queued_position(&self, call_id: &str) -> Option<usize> {
        self.queue.lock().unwrap().position(call_id)
    }

    /// Cancel a call. A still-`Queued` call is removed from the queue and
    /// finalized `Cancelled` immediately (it was never dispatched); a
    /// dispatched call is signalled through its cancel channel as before.
    /// Returns true if a queued call was cancelled or a live signal was sent.
    ///
    /// The queue lock is held across the `cancels` lookup so a call racing
    /// from queued to dispatched is caught in exactly one path: the dispatcher
    /// pops it and registers its cancel channel under the same queue lock.
    /// Lock order is always queue → cancels (never reversed), so no deadlock.
    pub fn end_call(&self, call_id: &str) -> bool {
        let mut q = self.queue.lock().unwrap();
        if q.remove(call_id).is_some() {
            drop(q);
            self.store
                .finalize(call_id, CallState::Cancelled, None, None, None, now_ms());
            bump_counter(&self.counters, CallState::Cancelled);
            return true;
        }
        let tx = self.cancels.lock().unwrap().remove(call_id);
        drop(q);
        match tx {
            Some(tx) => tx.send(()).is_ok(),
            None => false,
        }
    }

    pub async fn shutdown(self) {
        self.dispatcher.abort();
        self.sip.shutdown().await;
    }
}

/// Central dispatcher: the single task that turns eligible pending entries
/// into running calls, enforcing the `max_concurrent_channels` cap via the
/// `running` counter. Spawned once in `Engine::new`; holds `Arc` clones of the
/// engine's shared state. Never busy-spins: when nothing is dispatchable it
/// parks on `wake` (enqueue / freed slot) or a timer to the next eligibility.
#[allow(clippy::too_many_arguments)]
async fn dispatcher(
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    queue: Arc<Mutex<Box<dyn QueueStore>>>,
    running: Arc<AtomicUsize>,
    wake: Arc<Notify>,
    cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    counters: Arc<Counters>,
    enqueuer: Enqueuer,
) {
    let cap = server.max_concurrent_channels;
    loop {
        // Drain every currently-eligible entry until the cap is hit or nothing
        // is eligible. Each iteration takes the queue lock only briefly.
        loop {
            let mut q = queue.lock().unwrap();
            if running.load(Ordering::Relaxed) >= cap {
                break;
            }
            let entry = match q.pop_eligible(now_ms()) {
                Some(e) => e,
                None => break,
            };
            // Register the cancel channel and claim the slot while still
            // holding the queue lock, so `end_call` observes this call in
            // exactly one place (queue OR cancels), never neither.
            let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
            cancels
                .lock()
                .unwrap()
                .insert(entry.call_id.clone(), cancel_tx);
            running.fetch_add(1, Ordering::Relaxed);
            // Panic-safe slot release: the guard's Drop does the paired
            // `running -= 1` + `wake.notify_one()` on both normal completion and
            // panic unwind, so a `run_call` panic can never leak capacity.
            let slot = RunningGuard {
                running: running.clone(),
                wake: wake.clone(),
            };
            drop(q);

            store.set_state(&entry.call_id, CallState::Ringing);

            let sip = sip.clone();
            let store = store.clone();
            let server = server.clone();
            let cancels = cancels.clone();
            let counters = counters.clone();
            let enqueuer = enqueuer.clone();
            let PendingEntry {
                call_id,
                number,
                scenario,
                attempt,
                retry_of,
                ..
            } = entry;
            tokio::spawn(async move {
                let _slot = slot; // released (decrement + wake) on task exit / panic
                run_call(
                    sip, store, server, cancels, counters, enqueuer, cancel_rx, scenario, number,
                    call_id, attempt, retry_of,
                )
                .await;
            });
        }

        // Nothing more to dispatch right now. Park until woken (a new enqueue
        // or a freed slot) or — if we have spare capacity and a future-eligible
        // entry exists — until that entry becomes eligible.
        let sleep_until = if running.load(Ordering::Relaxed) < cap {
            queue.lock().unwrap().peek_next_eligible_at()
        } else {
            None
        };
        match sleep_until {
            Some(t) => {
                let dur = Duration::from_millis(t.saturating_sub(now_ms()));
                tokio::select! {
                    _ = wake.notified() => {}
                    _ = tokio::time::sleep(dur) => {}
                }
            }
            None => wake.notified().await,
        }
    }
}

/// Removes a call's `cancels` map entry when dropped, on every exit path —
/// early returns, the normal end-of-function fallthrough, and panic unwind
/// alike. `end_call` may have already removed the entry (to send on it); a
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

/// Releases a dispatched call's concurrency slot (`running -= 1`) and wakes the
/// dispatcher to backfill it, on EVERY exit path of the spawned call task —
/// normal completion AND panic unwind. Constructed right after the dispatcher
/// claims the slot (`running += 1`) and moved into the task, so the decrement
/// happens exactly once. This restores the panic-safety the old owned-permit
/// model got from `Semaphore` permit-Drop: without it, a panic inside
/// `run_call` would leak the slot and never wake the dispatcher, permanently
/// lowering capacity.
struct RunningGuard {
    running: Arc<AtomicUsize>,
    wake: Arc<Notify>,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.running.fetch_sub(1, Ordering::Relaxed);
        self.wake.notify_one();
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

/// Bump the cumulative counter matching a `CallOutcome`, for finalize sites
/// that carry one (i.e. everywhere `bump_counter` would otherwise be told
/// `CallState::Failed`, which loses the dial-outcome detail).
fn bump_counter_outcome(counters: &Counters, outcome: CallOutcome) {
    match outcome {
        CallOutcome::Completed => counters.completed.fetch_add(1, Ordering::Relaxed),
        CallOutcome::Busy => counters.busy.fetch_add(1, Ordering::Relaxed),
        CallOutcome::NoAnswer => counters.no_answer.fetch_add(1, Ordering::Relaxed),
        CallOutcome::Rejected => counters.rejected.fetch_add(1, Ordering::Relaxed),
        CallOutcome::NotFound => counters.not_found.fetch_add(1, Ordering::Relaxed),
        CallOutcome::Unavailable => counters.unavailable.fetch_add(1, Ordering::Relaxed),
        CallOutcome::Failed => counters.failed.fetch_add(1, Ordering::Relaxed),
    };
}

/// Outcome of the ring-wait: what the SIP side did while we waited (feeding a
/// warm Gemini session silence keepalive). Extracted so the ring loop — the
/// only part that can't be unit-tested against a live SIP answer — is testable
/// with a mock `SipEvent` source and a test audio sink.
enum RingOutcome {
    /// 200 OK: the negotiated codec to bridge with.
    Answered(crate::sip::NegotiatedCodec),
    /// Call ended before answer (busy/rejected/hangup); carries the reason.
    Terminated(crate::sip::TermReason),
    /// SIP event channel closed before any answer.
    Closed,
}

/// Ring-wait loop: await the SIP answer while pushing 20 ms of 16 kHz silence
/// (`vec![0i16; 320]`) into the warm Gemini session every 20 ms as keepalive,
/// so the connection stays live through the ring window. `warm_audio` is
/// `Some` only when a warm session exists; when `None` the silence branch is
/// disabled and we simply await the next event (no busy-spin). The silence
/// `interval` lives entirely inside this function, so it is dropped the moment
/// we return — guaranteeing `audio_in` has a single writer (the bridge) once
/// the call is up. `recv` is cancel-safe, so racing it against the tick never
/// drops an event.
async fn ring_wait(
    sip_events: &mut mpsc::Receiver<SipEvent>,
    warm_audio: Option<&mpsc::Sender<Vec<i16>>>,
) -> RingOutcome {
    let mut silence = tokio::time::interval(Duration::from_millis(20));
    loop {
        tokio::select! {
            ev = sip_events.recv() => return match ev {
                Some(SipEvent::Answered { codec }) => RingOutcome::Answered(codec),
                Some(SipEvent::Terminated(reason)) => RingOutcome::Terminated(reason),
                None => RingOutcome::Closed,
            },
            _ = silence.tick(), if warm_audio.is_some() => {
                // Keepalive: drop on a full/closed channel — the warm session
                // hasn't started draining audio yet, and losing a silence frame
                // is harmless.
                if let Some(s) = warm_audio {
                    let _ = s.try_send(vec![0i16; 320]);
                }
            }
        }
    }
}

/// Drive one call to completion. Safe ordering: SIP first, answer, THEN Gemini.
///
/// The concurrency slot is now claimed by the dispatcher (which set this call
/// `Ringing` and incremented `running` before spawning us), so there is no
/// permit to acquire and no cancel-before-permit race — a still-`Queued` call
/// is cancelled by `end_call` removing it from the queue before it ever
/// reaches here. `attempt`/`retry_of` are carried for Task 8's busy-retry.
#[allow(clippy::too_many_arguments)]
async fn run_call(
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    cancels: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    counters: Arc<Counters>,
    enqueuer: Enqueuer,
    mut cancel_rx: oneshot::Receiver<()>,
    scenario: ScenarioConfig,
    number: String,
    call_id: String,
    attempt: u32,
    retry_of: Option<String>,
) {
    let _cancel_guard = CancelGuard {
        cancels: cancels.clone(),
        call_id: call_id.clone(),
    };
    tracing::info!(%call_id, attempt, retry_of = ?retry_of, "dispatching call");

    // 0. Network preflight (fail-closed): probe the Gemini leg BEFORE dialing so
    // a bad network refuses the call instead of burning a PSTN call on it. Gated
    // by net_check.enabled (KUTSU_NETCHECK_ENABLED). Note: this covers only the
    // Gemini leg, not the callee's RTP/cellular leg — see the mid-call RTP abort
    // below and docs/backlog.md.
    if server.net_check.enabled {
        match crate::net_check::preflight(&server).await {
            Ok(health) => {
                let v = crate::net_check::verdict(&health, &server.net_check);
                tracing::info!(%call_id, health = %health.summary(), verdict = ?v, "network preflight");
                if v == crate::net_check::Verdict::Unusable {
                    store.finalize(
                        &call_id,
                        CallState::Failed,
                        None,
                        Some(format!("network preflight unusable: {}", health.summary())),
                        Some(CallOutcome::Failed),
                        now_ms(),
                    );
                    bump_counter_outcome(&counters, CallOutcome::Failed);
                    return;
                }
            }
            Err(e) => {
                store.finalize(
                    &call_id,
                    CallState::Failed,
                    None,
                    Some(format!("network preflight failed: {e}")),
                    Some(CallOutcome::Failed),
                    now_ms(),
                );
                bump_counter_outcome(&counters, CallOutcome::Failed);
                return;
            }
        }
    }

    // 1. INVITE.
    let call = match sip.place_call(&number).await {
        Ok(c) => c,
        Err(e) => {
            store.finalize(
                &call_id,
                CallState::Failed,
                None,
                Some(e.to_string()),
                Some(CallOutcome::Failed),
                now_ms(),
            );
            bump_counter_outcome(&counters, CallOutcome::Failed);
            return;
        }
    };
    // 2. Decompose into owned channel ends.
    let SipCallParts {
        events: mut sip_events,
        audio_in,
        audio_out,
        hangup: sip_hangup,
        uplink_quality,
        ..
    } = call.split();

    // 3. Warm-start Gemini DURING the ring window. `answered` starts `false`,
    // so Task 1's gate keeps the greeting armed only once we fire it on the
    // real 200 OK — never during ring.
    let (answered_tx, answered_rx) = tokio::sync::watch::channel(false);
    let warm = gemini_live::start(&server, &scenario, answered_rx.clone()).await;

    // 4. Ring-wait: await answer while feeding the warm session silence
    // keepalive (see `ring_wait`). The silence `interval` lives inside
    // `ring_wait`, so it is gone before the bridge starts — single writer on
    // `audio_in`.
    let codec = match ring_wait(&mut sip_events, warm.as_ref().ok().map(|s| &s.audio_in)).await {
        RingOutcome::Answered(codec) => codec,
        RingOutcome::Terminated(reason) => {
            // No answer: tear down the warm session (hang up + drop) so it does
            // not leak, then finalize exactly as before.
            if let Ok(s) = &warm {
                s.hangup().await;
            }
            drop(warm);
            let (outcome, detail) = match reason {
                crate::sip::TermReason::Failed { outcome, detail } => (outcome, detail),
                // Remote/local hangup before answer is effectively no-answer.
                _ => (CallOutcome::NoAnswer, format!("{reason:?}")),
            };
            store.finalize(
                &call_id,
                CallState::Failed,
                None,
                Some(detail),
                Some(outcome),
                now_ms(),
            );
            bump_counter_outcome(&counters, outcome);
            // Busy is the only auto-retried outcome (NoAnswer and all others
            // finalize terminally here). The finalized record above is linked
            // forward by the follow-up's `retry_of`; the follow-up gets a fresh
            // call_id from `place_call_at`, so this record is never
            // re-finalized (no double-finalize).
            if outcome == CallOutcome::Busy && should_retry_busy(attempt, &server.retry) {
                enqueuer
                    .place_call_at(
                        number.clone(),
                        scenario.clone(),
                        now_ms() + server.retry.busy_retry_interval_ms,
                        attempt + 1,
                        Some(call_id.clone()),
                    )
                    .await;
            }
            return;
        }
        RingOutcome::Closed => {
            if let Ok(s) = &warm {
                s.hangup().await;
            }
            drop(warm);
            store.finalize(
                &call_id,
                CallState::Failed,
                None,
                Some("sip closed before answer".into()),
                Some(CallOutcome::Failed),
                now_ms(),
            );
            bump_counter_outcome(&counters, CallOutcome::Failed);
            return;
        }
    };

    // 5. Answered — arm the greeting NOW (the ONLY place `answered_tx` is set
    // true), stamp the answer time, mark in-progress.
    let _ = answered_tx.send(true);
    let answered_at = now_ms();
    store.set_state(&call_id, CallState::InProgress);

    // 6. Obtain the live session from the warm start.
    // gemini_live::start is infallible before its internal spawn today;
    // ring-failure resilience is the session's internal reconnect loop. This
    // Err arm is a defensive guard, not the primary mechanism.
    let session = match warm {
        Ok(s) => s,
        Err(e) => {
            let _ = sip_hangup.send(());
            store.finalize(
                &call_id,
                CallState::Failed,
                None,
                Some(format!("gemini connect: {e}")),
                Some(CallOutcome::Failed),
                now_ms(),
            );
            bump_counter_outcome(&counters, CallOutcome::Failed);
            return;
        }
    };
    let gemini_connected_at = now_ms();
    // `dead_air_ms` is ~0 since the session was already connected through the
    // ring (warm-start).
    tracing::info!(%call_id, dead_air_ms = gemini_connected_at - answered_at, "gemini connected");

    // 5. Split the session; 6. start the bridge.
    let (gemini_handle, gemini_in, gemini_events) = session.split();
    let (events_out_tx, mut events_out_rx) = mpsc::channel::<Event>(256);
    let quality = bridge::QualityShared::new();
    let (bridge_cancel_tx, bridge_cancel_rx) = tokio::sync::oneshot::channel::<()>();
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
        downlink_dump: server.dump_downlink_dir.clone(),
        agc_cfg: server.agc,
        cancel: bridge_cancel_rx,
    };
    // Run the bridge (downlink pacer + uplink) on a dedicated OS thread with its
    // own current-thread runtime, promoted to real-time priority — so the 20 ms
    // downlink pacing is not delayed by shared-runtime contention. The engine
    // stops it via `bridge_cancel_tx` (not task abort) and reads the outcome from
    // `bridge_done_rx`.
    let (bridge_done_tx, mut bridge_done_rx) =
        tokio::sync::oneshot::channel::<crate::bridge::BridgeEnd>();
    let bridge_thread = std::thread::Builder::new()
        .name("kutsu-bridge".into())
        .spawn(move || {
            let _rt_priority = crate::realtime::promote_current_thread(
                "kutsu-bridge",
                crate::realtime::rt_enabled(std::env::var("KUTSU_RT_PRIORITY").ok()),
            );
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build bridge runtime");
            let end = rt.block_on(bridge::run(ports));
            let _ = bridge_done_tx.send(end);
        })
        .expect("spawn bridge thread");

    // 7. Orchestration loop.
    let mut goal = None;
    let mut first_transcript = false;
    let mut events_open = true;
    let mut abort_reason: Option<String> = None;
    let deadline = tokio::time::sleep(Duration::from_secs(server.max_call_secs));
    tokio::pin!(deadline);
    let mut qtick = tokio::time::interval(Duration::from_secs(1));
    // Rolling window of cumulative (uplink_received, uplink_lost) snapshots, one
    // per quality tick, for the mid-call uplink-RTP-loss gate.
    let mut uplink_hist: std::collections::VecDeque<(u64, u64)> = std::collections::VecDeque::new();
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
                Some(Event::Affect { role, label }) => {
                    store.append_affect(&call_id, crate::gemini_live::AffectEntry { role, label, ts_ms: now_ms() });
                }
                Some(_) => {} // OutputAudio/Interrupted: consumed by the bridge, not forwarded
                None => events_open = false, // bridge closed events_out; stop polling (the bridge_task arm ends the call)
            },
            r = sip_events.recv() => match r {
                Some(SipEvent::Terminated(_)) | None => break CallState::HungUp,
                Some(_) => {}
            },
            r = &mut bridge_done_rx => break match r {
                Ok(crate::bridge::BridgeEnd::PhoneClosed) => CallState::HungUp,
                Ok(crate::bridge::BridgeEnd::GeminiClosed) => CallState::Failed,
                // Engine-initiated cancel is sent only during teardown (after the
                // loop), so this arm normally sees Phone/GeminiClosed; treat a
                // stray Cancelled as a hang-up.
                Ok(crate::bridge::BridgeEnd::Cancelled) => CallState::HungUp,
                Err(_) => CallState::Failed, // bridge thread died before sending
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
                // Mid-call RTP gate: abort on sustained uplink packet loss over a
                // rolling ~8 s window (a failing callee/cellular leg — the
                // preflight covered only the Gemini leg). Rolling, not cumulative,
                // so a late burst isn't diluted away by a long clean call.
                uplink_hist.push_back((q.uplink_received, q.uplink_lost));
                while uplink_hist.len() > UPLINK_WINDOW_TICKS {
                    uplink_hist.pop_front();
                }
                if uplink_hist.len() >= UPLINK_WINDOW_TICKS
                    && let (Some(&oldest), Some(&current)) = (uplink_hist.front(), uplink_hist.back())
                        && let Some(loss_pct) = window_loss_pct(oldest, current)
                            && loss_pct > server.net_check.uplink_loss_abort_pct {
                                abort_reason = Some(format!(
                                    "aborted: uplink RTP loss {loss_pct:.1}% over ~{UPLINK_WINDOW_TICKS}s window"
                                ));
                                break CallState::Failed;
                            }
            }
        }
    };

    // 8. Teardown (BYE both sides, stop the bridge, get the authoritative outcome).
    // On a model-initiated end (goodbye + `end_call`, so `goal` is set), let the
    // downlink pacer play the buffered goodbye out to the phone before we hang
    // up — otherwise BYE + stopping the bridge truncate the model's last words.
    // The bridge keeps pacing while we wait. Bounded so a buffer that never
    // drains can't hang teardown; every other exit path tears down immediately.
    if goal.is_some() {
        // Cap covers a full closing turn played at real time (~5 s) plus slack;
        // it only bites if the downlink never goes idle. Normally the wait ends
        // the moment the goodbye finishes and the buffer drains.
        let drain_cap = tokio::time::Instant::now() + Duration::from_secs(8);
        while quality.downlink_active() && tokio::time::Instant::now() < drain_cap {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    let _ = sip_hangup.send(());
    gemini_handle.hangup().await;
    // Stop the bridge (dedicated thread) and join it — replaces task abort.
    // Gemini hangup above usually ends the bridge on its own (GeminiClosed);
    // the cancel covers the paths where it is still running.
    let _ = bridge_cancel_tx.send(());
    let _ = tokio::task::spawn_blocking(move || bridge_thread.join()).await;
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
    counters
        .starved_ms
        .fetch_add(q.starved_ms, Ordering::Relaxed);
    counters
        .uplink_received
        .fetch_add(q.uplink_received, Ordering::Relaxed);
    counters
        .uplink_lost
        .fetch_add(q.uplink_lost, Ordering::Relaxed);
    // Uplink level in dBFS (0 dBFS = full-scale PCM16); -inf when silent.
    let uplink_dbfs = if q.uplink_rms > 0 {
        20.0 * (q.uplink_rms as f64 / 32768.0).log10()
    } else {
        f64::NEG_INFINITY
    };
    tracing::info!(
        %call_id, codec = ?codec.kind,
        uplink_received = q.uplink_received, uplink_lost = q.uplink_lost,
        uplink_reordered = q.uplink_reordered,
        uplink_rms = q.uplink_rms, uplink_dbfs = format!("{uplink_dbfs:.1}"),
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
    let outcome = finalize_outcome(final_state);
    store.finalize(&call_id, final_state, final_goal, error, outcome, now_ms());
    match outcome {
        Some(o) => bump_counter_outcome(&counters, o),
        None => bump_counter(&counters, CallState::Cancelled),
    }

    // 10. Persist. Owner-only permissions: the transcript may contain PII.
    if let Some(dir) = &server.transcript_dir
        && let Some(rec) = store.get(&call_id)
    {
        let path = dir.join(format!("{call_id}.json"));
        if let Ok(json) = serde_json::to_string_pretty(&rec)
            && let Err(e) = write_owner_only(&path, json.as_bytes())
        {
            tracing::warn!(%call_id, "failed to write transcript: {e}");
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
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    std::io::Write::write_all(&mut f, data)
}
#[cfg(not(unix))]
fn write_owner_only(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, data) // Windows: rely on directory ACL (documented)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Model, NetCheckConfig, VadConfig};

    #[test]
    fn window_loss_pct_needs_enough_samples_then_reports_windowed_loss() {
        // Too few packets in the window -> no verdict yet.
        assert_eq!(window_loss_pct((0, 0), (100, 0)), None);
        // Clean window -> ~0%.
        assert_eq!(window_loss_pct((0, 0), (500, 0)), Some(0.0));
        // Lossy window -> the loss over the window (50 lost of 500).
        assert_eq!(window_loss_pct((0, 0), (450, 50)), Some(10.0));
        // Subtracts the window-start baseline (rolling, not cumulative):
        // 400 recv + 40 lost since baseline -> 40/440 ≈ 9.09%.
        let p = window_loss_pct((1000, 10), (1400, 50)).unwrap();
        assert!((p - 9.09).abs() < 0.1, "got {p}");
    }

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
            dump_downlink_dir: None,
            max_call_secs: 600,
            quality: crate::config::QualityConfig::default(),
            retry: crate::config::RetryConfig::default(),
            vad: VadConfig::default(),
            agc: crate::config::AgcConfig::default(),
            prompts: crate::config::PromptsConfig::default(),
        };
        let sip_cfg = SipConfig {
            server: "127.0.0.1:5060".into(),
            username: "test".into(),
            password: "test".into(),
            from_user: None,
            local_ip: Some("127.0.0.1".parse().unwrap()),
            local_port: None,
            sip_domain: None,
            register: false,
            register_expiry_secs: None,
            transport: Default::default(),
        };
        (server, sip_cfg)
    }

    fn test_scenario() -> ScenarioConfig {
        ScenarioConfig {
            goal_schema: serde_json::json!({"type": "object", "required": ["disposition"]}),
            context: None,
            prompt_override: None,
        }
    }

    #[test]
    fn now_ms_is_nonzero() {
        assert!(now_ms() > 0);
    }

    #[tokio::test]
    async fn scheduler_respects_cap_and_eligibility() {
        // Part A — cap: with cap = 1, the dispatcher must NEVER run more than
        // `max_concurrent_channels` calls at once, and must account for every
        // placed call (nothing silently lost). We assert those invariants
        // rather than a specific transient (Ringing==1 / Queued==1), which
        // would be OS-timing-fragile: on a platform where the loopback INVITE
        // fast-fails (ICMP port-unreachable → quick error) the dispatched call
        // finalizes Failed and `b` dispatches, flipping a transient snapshot.
        // The cap invariant holds regardless of how fast a call fails.
        let cap = 1usize;
        let (server, sip_cfg) = test_configs(cap);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg)
            .await
            .unwrap();
        let a = engine.place_call("600".into(), test_scenario()).await;
        let b = engine.place_call("601".into(), test_scenario()).await;
        // Sample the invariants repeatedly across the settling window, using
        // ONLY observable store state for the cap check (the `running` counter
        // and the store are not updated atomically — a call is finalized in the
        // store just before its slot guard drops — so mixing them would race).
        //   1. `running` never exceeds the cap (gate held under the queue lock).
        //   2. The count of dispatched-not-finalized calls (store Ringing|
        //      InProgress) never exceeds the cap.
        //   3. Both placed calls always exist in the store (nothing lost).
        for _ in 0..20 {
            let m = engine.metrics_snapshot();
            assert!(
                m.active <= cap,
                "running {} must never exceed cap {cap}",
                m.active
            );
            let store_active = [&a, &b]
                .iter()
                .filter(|id| {
                    matches!(
                        engine.store().get(id).unwrap().state,
                        CallState::Ringing | CallState::InProgress
                    )
                })
                .count();
            assert!(
                store_active <= cap,
                "dispatched calls {store_active} must never exceed cap {cap}"
            );
            assert!(
                engine.store().get(&a).is_some() && engine.store().get(&b).is_some(),
                "both placed calls remain accounted for in the store"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // Both calls were counted as placed regardless of their live disposition.
        assert_eq!(engine.metrics_snapshot().placed_total, 2);
        engine.shutdown().await;

        // Part B — eligibility: a future-eligible enqueue stays Queued until its
        // time, even with free capacity. Uses real (wall-clock) durations, not
        // tokio::time::pause(): eligibility is driven by `now_ms()` (SystemTime,
        // per Task 6's queue), which pause() does not advance — pausing would
        // decouple the dispatcher's timer from `now_ms()` and never fire.
        let (server2, sip_cfg2) = test_configs(3);
        let engine2 = Engine::new(std::sync::Arc::new(server2), &sip_cfg2)
            .await
            .unwrap();
        let future_ms = now_ms() + 200;
        let c = engine2
            .place_call_at("602".into(), test_scenario(), future_ms, 1, None)
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert_eq!(
            engine2.store().get(&c).unwrap().state,
            CallState::Queued,
            "not-yet-eligible call must stay Queued despite free capacity"
        );
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_ne!(
            engine2.store().get(&c).unwrap().state,
            CallState::Queued,
            "call must be dispatched once its eligibility time passes"
        );
        engine2.shutdown().await;
    }

    #[tokio::test]
    async fn place_call_queues_when_at_cap() {
        // cap = 0: every call must sit in Queued forever (no permit available).
        let (server, sip_cfg) = test_configs(0);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg)
            .await
            .unwrap();
        let id = engine.place_call("600".into(), test_scenario()).await;
        // No permit → the spawned run_call is parked before INVITE; state stays Queued.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let rec = engine.store().get(&id).unwrap();
        assert_eq!(rec.state, CallState::Queued);
        assert_eq!(engine.queued_position(&id), Some(1));
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn end_call_cancels_a_queued_call() {
        let (server, sip_cfg) = test_configs(0); // cap 0 → parked in Queued
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg)
            .await
            .unwrap();
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
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg)
            .await
            .unwrap();
        assert!(!engine.end_call("nope"));
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_snapshot_has_quality_fields() {
        let (server, sip_cfg) = test_configs(1);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg)
            .await
            .unwrap();
        let m = engine.metrics_snapshot();
        assert_eq!(m.underruns_total, 0);
        assert_eq!(m.starved_ms_total, 0);
        assert_eq!(m.quality_aborted_total, 0);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_snapshot_exposes_uplink_totals() {
        let (server, sip_cfg) = test_configs(1);
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg)
            .await
            .unwrap();
        let m = engine.metrics_snapshot();
        assert_eq!(m.uplink_received_total, 0);
        assert_eq!(m.uplink_lost_total, 0);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn metrics_snapshot_counts_placed_and_queued() {
        let (server, sip_cfg) = test_configs(0); // cap 0 → calls park in Queued
        let engine = Engine::new(std::sync::Arc::new(server), &sip_cfg)
            .await
            .unwrap();
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
        let cfg = crate::config::QualityConfig {
            abort_underruns: 0,
            ..crate::config::QualityConfig::default()
        };
        assert!(!should_abort(&cfg, 0));
        assert!(!should_abort(&cfg, 1_000_000));
    }

    #[test]
    fn should_abort_trips_at_threshold_not_before() {
        let cfg = crate::config::QualityConfig {
            abort_underruns: 40,
            ..crate::config::QualityConfig::default()
        };
        assert!(!should_abort(&cfg, 0));
        assert!(!should_abort(&cfg, 39));
        assert!(should_abort(&cfg, 40));
        assert!(should_abort(&cfg, 41));
    }

    #[test]
    fn reconcile_final_state_table() {
        // A model-initiated end always wins as Completed, regardless of what
        // the select loop landed on (it may have raced GeminiClosed -> Failed).
        for end_state in [
            CallState::Failed,
            CallState::HungUp,
            CallState::Completed,
            CallState::Cancelled,
        ] {
            assert_eq!(
                reconcile_final_state(end_state, EndedBy::ModelEndCall),
                CallState::Completed
            );
        }
        // Any other ended_by leaves end_state untouched.
        for ended_by in [EndedBy::CallerHangup, EndedBy::RemoteClose, EndedBy::Error] {
            for end_state in [
                CallState::Failed,
                CallState::HungUp,
                CallState::Completed,
                CallState::Cancelled,
            ] {
                assert_eq!(reconcile_final_state(end_state, ended_by), end_state);
            }
        }
    }

    #[test]
    fn finalize_error_only_attached_when_failed() {
        assert_eq!(
            finalize_error(CallState::Failed, Some("boom".into())),
            Some("boom".into())
        );
        assert_eq!(finalize_error(CallState::Failed, None), None);
        // A reconciled Completed (e.g. late ModelEndCall overriding an aborted
        // end_state) must not carry a stale abort error.
        assert_eq!(
            finalize_error(CallState::Completed, Some("boom".into())),
            None
        );
        assert_eq!(finalize_error(CallState::HungUp, Some("boom".into())), None);
        assert_eq!(
            finalize_error(CallState::Cancelled, Some("boom".into())),
            None
        );
    }

    #[test]
    fn should_retry_busy_stops_at_max() {
        let cfg = crate::config::RetryConfig {
            busy_max_attempts: 3,
            busy_retry_interval_ms: 1000,
        };
        assert!(should_retry_busy(1, &cfg));
        assert!(should_retry_busy(2, &cfg));
        assert!(!should_retry_busy(3, &cfg)); // 3rd attempt is the last; no 4th
    }

    #[tokio::test(start_paused = true)]
    async fn ring_wait_feeds_silence_then_returns_codec() {
        // A warm session exists: the ring-wait must push silence keepalive on
        // each 20 ms tick until the SIP side answers, then return the codec.
        use crate::sip::{G711Kind, NegotiatedCodec};
        let (ev_tx, mut ev_rx) = mpsc::channel::<SipEvent>(8);
        let (aud_tx, mut aud_rx) = mpsc::channel::<Vec<i16>>(64);
        let codec = NegotiatedCodec {
            pt: 0,
            kind: G711Kind::Ulaw,
            ptime_ms: 20,
        };
        // Driver: let several silence ticks elapse under paused time, then answer.
        let driver = async {
            for _ in 0..5 {
                tokio::time::advance(Duration::from_millis(20)).await;
            }
            ev_tx.send(SipEvent::Answered { codec }).await.unwrap();
        };
        let (outcome, ()) = tokio::join!(ring_wait(&mut ev_rx, Some(&aud_tx)), driver);
        match outcome {
            RingOutcome::Answered(c) => {
                assert_eq!(c.pt, 0);
                assert_eq!(c.kind, crate::sip::G711Kind::Ulaw);
            }
            _ => panic!("expected Answered"),
        }
        // At least one silence frame per elapsed tick, each exactly 20 ms of
        // 16 kHz mono (320 samples).
        let mut frames = 0;
        while let Ok(f) = aud_rx.try_recv() {
            assert_eq!(f.len(), 320);
            frames += 1;
        }
        assert!(
            frames >= 5,
            "expected >=5 silence frames during ring, got {frames}"
        );
    }

    #[tokio::test]
    async fn ring_wait_terminated_returns_terminated_and_stops_feeding() {
        // A Terminated before answer must return the teardown path; no live
        // answer, so run_call hangs up + drops the warm session (asserted here
        // only that the helper reports Terminated with the reason preserved).
        let (ev_tx, mut ev_rx) = mpsc::channel::<SipEvent>(8);
        let (aud_tx, _aud_rx) = mpsc::channel::<Vec<i16>>(64);
        ev_tx
            .send(SipEvent::Terminated(crate::sip::TermReason::Failed {
                outcome: CallOutcome::Busy,
                detail: "486 Busy Here".into(),
            }))
            .await
            .unwrap();
        match ring_wait(&mut ev_rx, Some(&aud_tx)).await {
            RingOutcome::Terminated(crate::sip::TermReason::Failed { outcome, .. }) => {
                assert_eq!(outcome, CallOutcome::Busy);
            }
            _ => panic!("expected Terminated(Failed{{Busy}})"),
        }
    }

    #[tokio::test]
    async fn ring_wait_closed_channel_returns_closed() {
        // SIP event channel dropped before any answer -> Closed. No warm
        // session (None) means the silence branch is disabled: no busy-spin.
        let (ev_tx, mut ev_rx) = mpsc::channel::<SipEvent>(8);
        drop(ev_tx);
        assert!(matches!(
            ring_wait(&mut ev_rx, None).await,
            RingOutcome::Closed
        ));
    }

    #[test]
    fn finalize_outcome_maps_hungup_to_completed() {
        // HungUp is a successful connection (caller/model ended a connected
        // call) — same success bucket as Completed per `bump_counter`, not a
        // technical failure.
        assert_eq!(
            finalize_outcome(CallState::Completed),
            Some(CallOutcome::Completed)
        );
        assert_eq!(
            finalize_outcome(CallState::HungUp),
            Some(CallOutcome::Completed)
        );
        assert_eq!(
            finalize_outcome(CallState::Failed),
            Some(CallOutcome::Failed)
        );
        assert_eq!(finalize_outcome(CallState::Cancelled), None);
    }
}
