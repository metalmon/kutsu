# Call Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `src/state` (call store) and `src/engine` (one call's full lifecycle: dial → answer → bridge audio+events → end → finalize + persist), plus the `SipCall`/`Session` seam splits and a `kutsu call <number>` CLI.

**Architecture:** `Engine` holds a `SipTransport` + `CallStore` + config; `place_call` cap-checks and spawns `run_call`. `run_call` runs the deadlock-safe sequence **SIP first → answer → then Gemini → bridge**, drives a `select!` orchestration loop over the bridge's forwarded events + SIP lifecycle, then tears down and finalizes. All-`Send`, on the normal tokio runtime.

**Tech Stack:** Rust 2024, tokio, existing `sip`/`bridge`/`gemini_live` modules. No new dependencies (`std::time` for `now_ms`; `serde_json` already present for persistence).

**Spec:** `docs/superpowers/specs/2026-08-14-call-engine-design.md`

## Global Constraints

- **Every `cargo` build/test command MUST include `--features vendor-openssl`.** Toolchain configured.
- English only in all code/comments.
- **Deadlock-safe ordering is mandatory** (spec §2/§5): connect Gemini only AFTER the SIP call answers, so Gemini never produces events before the bridge drains them.
- **Do not assert post-answer latency is acceptable** (spec §6): only instrument it (`tracing` stamps). It's an open question for a real trunk.
- `engine` is orchestration only — DSP is `bridge`, SIP is `sip`, Gemini protocol is `gemini_live`.
- No real SIP trunk exists → the live integration test validates wiring/cleanup against echo-600, NOT dialogue quality.

## File Structure

- `src/state.rs` — (currently a stub) `CallState`, `CallRecord`, `CallStore`.
- `src/gemini_live.rs` — add `Session::split` + `SessionHandle`; ensure `TranscriptEntry`/`Role` derive `Clone + Serialize`.
- `src/sip/mod.rs` — add `SipCall::split` + `SipCallParts`.
- `src/config.rs` — add `transcript_dir` + `max_call_secs` to `ServerConfig`.
- `src/engine.rs` — (currently a stub) `Engine`, `place_call`, `run_call`, `now_ms`.
- `src/main.rs` — add `Command::Call` + its handler.
- `tests/engine_call.rs` — `#[ignore]` live integration test.

Task order: 1 state → 2 config → 3 splits → 4 engine → 5 CLI + integration.

---

## Task 1: `state.rs` — call store

**Files:**
- Modify: `src/state.rs` (replace the stub), `src/gemini_live.rs` (derives)
- Test: `src/state.rs` (inline)

**Interfaces:**
- Consumes: `crate::gemini_live::{TranscriptEntry, Role}`.
- Produces: `state::{CallState, CallRecord, CallStore}`.

- [ ] **Step 1: Make `TranscriptEntry`/`Role` serializable.** In `src/gemini_live.rs`, add `serde::Serialize` (and `Clone` if missing) to the `TranscriptEntry` struct and the `Role` enum derives. Example: `#[derive(Clone, Debug, serde::Serialize)]` on `TranscriptEntry`; ensure `Role` derives `Clone, Copy, Debug, serde::Serialize` (add `#[serde(rename_all = "snake_case")]` on `Role` if it isn't already serialized elsewhere). Read the current derives first and only add what's missing.

- [ ] **Step 2: Write the failing tests.** Replace the doc-only body of `src/state.rs` with the doc comment plus:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::gemini_live::{Role, TranscriptEntry};

    fn rec(id: &str) -> CallRecord {
        CallRecord {
            call_id: id.into(),
            number: "600".into(),
            state: CallState::Ringing,
            transcript: vec![],
            goal: None,
            error: None,
            started_ms: 1000,
            ended_ms: None,
        }
    }

    #[test]
    fn insert_and_get_roundtrip() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        let got = store.get("c1").unwrap();
        assert_eq!(got.call_id, "c1");
        assert_eq!(got.state, CallState::Ringing);
        assert!(store.get("missing").is_none());
    }

    #[test]
    fn state_and_transcript_mutations() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.set_state("c1", CallState::InProgress);
        store.append_transcript("c1", TranscriptEntry { role: Role::Agent, text: "hi".into(), ts_ms: 5 });
        let got = store.get("c1").unwrap();
        assert_eq!(got.state, CallState::InProgress);
        assert_eq!(got.transcript.len(), 1);
        assert_eq!(got.transcript[0].text, "hi");
    }

    #[test]
    fn finalize_sets_terminal_fields() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.set_transcript("c1", vec![TranscriptEntry { role: Role::User, text: "bye".into(), ts_ms: 9 }]);
        store.finalize("c1", CallState::Completed, Some(serde_json::json!({"ok": true})), None, 2000);
        let got = store.get("c1").unwrap();
        assert_eq!(got.state, CallState::Completed);
        assert_eq!(got.ended_ms, Some(2000));
        assert!(got.goal.is_some());
        assert_eq!(got.transcript.len(), 1);
    }

    #[test]
    fn list_returns_all_and_serializes() {
        let store = CallStore::new();
        store.insert(rec("c1"));
        store.insert(rec("c2"));
        assert_eq!(store.list().len(), 2);
        let json = serde_json::to_string(&store.get("c1").unwrap()).unwrap();
        assert!(json.contains("\"state\":\"ringing\""));
    }
}
```

Note: the `Role::Agent`/`Role::User` variant names must match the real enum in `gemini_live` — read it and adjust the test if the variants are named differently.

- [ ] **Step 3: Run tests to verify they fail.**

Run: `cargo test --features vendor-openssl --lib state::tests`
Expected: FAIL — `CallStore`/`CallRecord`/`CallState` not defined.

- [ ] **Step 4: Implement the store.** Prepend to `src/state.rs` (imports at top):

```rust
//! Call state store: records one outbound call's lifecycle, keyed by call_id.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::gemini_live::TranscriptEntry;

/// Lifecycle state of a call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallState {
    Ringing,
    InProgress,
    Completed,
    Failed,
    HungUp,
}

/// One call's record.
#[derive(Clone, Debug, serde::Serialize)]
pub struct CallRecord {
    pub call_id: String,
    pub number: String,
    pub state: CallState,
    pub transcript: Vec<TranscriptEntry>,
    pub goal: Option<Value>,
    pub error: Option<String>,
    pub started_ms: u64,
    pub ended_ms: Option<u64>,
}

/// In-memory store of call records, keyed by call_id. Cheap to clone (Arc).
#[derive(Clone, Default)]
pub struct CallStore {
    inner: Arc<Mutex<HashMap<String, CallRecord>>>,
}

impl CallStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, rec: CallRecord) {
        self.inner.lock().unwrap().insert(rec.call_id.clone(), rec);
    }

    pub fn set_state(&self, call_id: &str, state: CallState) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.state = state;
        }
    }

    pub fn append_transcript(&self, call_id: &str, entry: TranscriptEntry) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.transcript.push(entry);
        }
    }

    /// Replace the running transcript with the authoritative one from the session.
    pub fn set_transcript(&self, call_id: &str, transcript: Vec<TranscriptEntry>) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.transcript = transcript;
        }
    }

    pub fn finalize(
        &self,
        call_id: &str,
        state: CallState,
        goal: Option<Value>,
        error: Option<String>,
        ended_ms: u64,
    ) {
        if let Some(r) = self.inner.lock().unwrap().get_mut(call_id) {
            r.state = state;
            r.goal = goal;
            r.error = error;
            r.ended_ms = Some(ended_ms);
        }
    }

    pub fn get(&self, call_id: &str) -> Option<CallRecord> {
        self.inner.lock().unwrap().get(call_id).cloned()
    }

    pub fn list(&self) -> Vec<CallRecord> {
        self.inner.lock().unwrap().values().cloned().collect()
    }
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `cargo test --features vendor-openssl --lib state::tests`
Expected: PASS (4 tests). Then `cargo test --features vendor-openssl --lib` — no regressions.

- [ ] **Step 6: Commit.**

```bash
git add src/state.rs src/gemini_live.rs
git commit -m "feat(state): call store (CallStore/CallRecord/CallState)"
```

---

## Task 2: config additions

**Files:**
- Modify: `src/config.rs`, `src/main.rs` (existing `ServerConfig` literal in `run_live`)
- Test: `src/config.rs` (inline)

**Interfaces:**
- Produces: `ServerConfig::transcript_dir: Option<PathBuf>`, `ServerConfig::max_call_secs: u64`.

**Note:** `ServerConfig` is `#[derive(Clone, Debug)]` only — **NOT `Deserialize`**; it is constructed field-by-field in code (see `run_live` in `main.rs`). So these are plain fields (no `#[serde(...)]`), and there is no serde-default test to write — this is a mechanical field addition verified by compilation, with the real exercise landing in Tasks 4/5.

- [ ] **Step 1: Add the fields.** In `src/config.rs`, add to `ServerConfig` (add `use std::path::PathBuf;` to the imports if absent):

```rust
    /// Directory to write finalized CallRecord JSON to; None = skip persistence.
    pub transcript_dir: Option<PathBuf>,
    /// Safety cap on a single call's duration (seconds).
    pub max_call_secs: u64,
```

- [ ] **Step 2: Update the existing `ServerConfig` literal.** In `src/main.rs`, `run_live` constructs `ServerConfig { ... }` (around line 143, ending with `greet_after_silence_ms: ...`). Add the two new fields to that literal so it still compiles:

```rust
        transcript_dir: None,
        max_call_secs: 600,
```

- [ ] **Step 3: Add a small construction test** (proves the fields exist + are wired, since there's no serde behavior to test). Append to `src/config.rs` tests:

```rust
#[test]
fn server_config_has_engine_fields() {
    let c = ServerConfig {
        api_key: "k".into(),
        proxy: None,
        model: Model::HalfCascade,
        voice: "Autonoe".into(),
        language: "ru-RU".into(),
        net_check: NetCheckConfig::default(),
        max_concurrent_channels: 3,
        greet_after_silence_ms: DEFAULT_GREET_AFTER_SILENCE_MS,
        transcript_dir: Some(std::path::PathBuf::from("/tmp/x")),
        max_call_secs: 600,
    };
    assert_eq!(c.max_call_secs, 600);
    assert!(c.transcript_dir.is_some());
}
```

- [ ] **Step 4: Run test + build.**

Run: `cargo test --features vendor-openssl --lib config::tests::server_config_has_engine_fields` then `cargo build --features vendor-openssl`
Expected: PASS + compiles (the `run_live` literal now sets both fields).

- [ ] **Step 5: Commit.**

```bash
git add src/config.rs src/main.rs
git commit -m "feat(config): ServerConfig transcript_dir + max_call_secs"
```

---

## Task 3: seam splits (`SipCall::split`, `Session::split`)

**Files:**
- Modify: `src/sip/mod.rs`, `src/gemini_live.rs`
- Test: `src/sip/mod.rs` + `src/gemini_live.rs` (inline)

**Interfaces:**
- Produces: `sip::SipCallParts` + `SipCall::split(self) -> SipCallParts`; `gemini_live::SessionHandle` + `Session::split(self) -> (SessionHandle, mpsc::Sender<Vec<i16>>, mpsc::Receiver<Event>)`.

- [ ] **Step 1: Write the failing tests.**

In `src/sip/mod.rs` tests:

```rust
#[tokio::test]
async fn sipcall_split_yields_working_channel_ends() {
    let (ev_tx, ev_rx) = mpsc::channel(4);
    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, mut out_rx) = mpsc::channel::<bytes::Bytes>(4);
    let (hup_tx, mut hup_rx) = oneshot::channel();
    let call = SipCall::from_parts("c1".into(), ev_rx, in_rx, out_tx, hup_tx);

    let parts = call.split();
    assert_eq!(parts.call_id, "c1");
    // events end works
    ev_tx.send(SipEvent::Answered { codec: NegotiatedCodec { pt: 0, kind: G711Kind::Ulaw, ptime_ms: 20 } }).await.unwrap();
    // (parts.events is the receiver — drop check only; construction is the assertion)
    // audio_out end works
    parts.audio_out.send(bytes::Bytes::from_static(b"x")).await.unwrap();
    assert!(out_rx.recv().await.is_some());
    // hangup end works
    parts.hangup.send(()).unwrap();
    assert!(hup_rx.try_recv().is_ok());
    let _ = (parts.events, parts.audio_in, ev_tx, in_tx);
}
```

In `src/gemini_live.rs` tests (there is an existing `#[cfg(test)] mod tests`):

```rust
#[tokio::test]
async fn session_split_yields_control_and_channels() {
    let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<i16>>(4);
    let (event_tx, event_rx) = mpsc::channel::<Event>(4);
    let (hangup_tx, mut hangup_rx) = mpsc::channel::<()>(1);
    let join = tokio::spawn(async {
        CallOutcome { ended_by: EndedBy::RemoteClose, goal: None, transcript: vec![] }
    });
    let session = Session { audio_in: audio_tx, events: event_rx, join, hangup_tx };

    let (handle, gemini_in, mut gemini_events) = session.split();
    // gemini_in is the audio sender
    gemini_in.send(vec![0i16; 4]).await.unwrap();
    assert!(audio_rx.recv().await.is_some());
    // events receiver moved out
    event_tx.send(Event::TurnComplete).await.unwrap();
    assert!(matches!(gemini_events.recv().await, Some(Event::TurnComplete)));
    // handle can hang up and join
    handle.hangup().await;
    assert!(hangup_rx.recv().await.is_some());
    let outcome = handle.join().await;
    assert!(matches!(outcome.ended_by, EndedBy::RemoteClose));
}
```

- [ ] **Step 2: Run to verify they fail.**

Run: `cargo test --features vendor-openssl --lib sip::tests::sipcall_split gemini_live::tests::session_split`
Expected: FAIL — `split`/`SipCallParts`/`SessionHandle` not defined.

- [ ] **Step 3: Implement `SipCall::split`.** In `src/sip/mod.rs`, add (near `SipCall`):

```rust
/// Owned channel ends of a `SipCall`, for handing to the bridge.
pub struct SipCallParts {
    pub call_id: String,
    pub events: mpsc::Receiver<SipEvent>,
    pub audio_in: mpsc::Receiver<Bytes>,
    pub audio_out: mpsc::Sender<Bytes>,
    pub hangup: oneshot::Sender<()>,
}

impl SipCall {
    /// Decompose into owned channel ends. Consumes the call.
    pub fn split(mut self) -> SipCallParts {
        SipCallParts {
            call_id: self.call_id,
            events: self.events,
            audio_in: self.rtp_in,
            audio_out: self.rtp_out,
            hangup: self.hangup.take().expect("SipCall always has a hangup sender until split"),
        }
    }
}
```

Field names must match the real `SipCall` struct (`events`, `rtp_in`, `rtp_out`, `hangup: Option<oneshot::Sender<()>>`) — read it and adjust if needed. Because `split` moves fields out of `self` while also `.take()`ing `hangup`, bind `self` as `mut` and move the plain fields directly (they are not behind `Option`).

> If the borrow checker rejects moving some fields while `.take()`ing another, first `let hangup = self.hangup.take().expect(...);` then construct `SipCallParts` moving the remaining fields.

- [ ] **Step 4: Implement `Session::split`.** In `src/gemini_live.rs`, add:

```rust
/// Control handle for a split-off `Session`: hang up + await the outcome.
pub struct SessionHandle {
    join: tokio::task::JoinHandle<CallOutcome>,
    hangup_tx: mpsc::Sender<()>,
}

impl SessionHandle {
    pub async fn hangup(&self) {
        let _ = self.hangup_tx.send(()).await;
    }
    pub async fn join(self) -> CallOutcome {
        self.join.await.unwrap_or(CallOutcome {
            ended_by: EndedBy::Error,
            goal: None,
            transcript: Vec::new(),
        })
    }
}

impl Session {
    /// Decompose into a control handle + the audio sink + the event stream.
    pub fn split(self) -> (SessionHandle, mpsc::Sender<Vec<i16>>, mpsc::Receiver<Event>) {
        (
            SessionHandle { join: self.join, hangup_tx: self.hangup_tx },
            self.audio_in,
            self.events,
        )
    }
}
```

- [ ] **Step 5: Run to verify they pass.**

Run: `cargo test --features vendor-openssl --lib sip::tests::sipcall_split gemini_live::tests::session_split`
Expected: PASS. Then `cargo test --features vendor-openssl --lib` — no regressions.

- [ ] **Step 6: Commit.**

```bash
git add src/sip/mod.rs src/gemini_live.rs
git commit -m "feat(sip,gemini): SipCall::split + Session::split for the engine seam"
```

---

## Task 4: `engine.rs` — Engine + run_call

**Files:**
- Modify: `src/engine.rs` (replace the stub)
- Test: `src/engine.rs` (inline — cap logic + now_ms)

**Interfaces:**
- Consumes: `sip::{SipTransport, SipCall, SipEvent, SipCallParts}`, `bridge::BridgePorts`, `gemini_live::{start, Event, SessionHandle}`, `state::{CallStore, CallRecord, CallState}`, `config::{ServerConfig, SipConfig, ScenarioConfig}`, `gemini_live::TranscriptEntry`.
- Produces: `engine::{Engine, EngineError}`; `Engine::{new, place_call, store, shutdown}`.

- [ ] **Step 1: Write the failing tests.** Replace the doc-only body of `src/engine.rs` with the doc + tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

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
```

Provide the `test_configs(cap)` and `test_scenario()` helpers in the same test module: `test_configs` builds a `ServerConfig` (any valid values; `max_concurrent_channels = cap`, `transcript_dir: None`, `max_call_secs: 600`) and a `SipConfig` bound to loopback (`server: "127.0.0.1:5060"`, `local_ip: Some("127.0.0.1")`, dummy creds) so `SipTransport::new` binds offline. Read `ServerConfig`/`SipConfig`/`ScenarioConfig` field lists and construct valid literals.

- [ ] **Step 2: Run to verify it fails.**

Run: `cargo test --features vendor-openssl --lib engine::tests`
Expected: FAIL — `Engine`/`now_ms`/`EngineError` not defined.

- [ ] **Step 3: Implement the engine.** Prepend to `src/engine.rs`:

```rust
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
        codec,
        phone_in: audio_in,
        phone_out: audio_out,
        gemini_in,
        gemini_events,
        events_out: events_out_tx,
    };
    let mut bridge_task = tokio::spawn(bridge::run(ports));

    // 7. Orchestration loop.
    let mut goal = None;
    let mut greeted = false;
    let deadline = tokio::time::sleep(Duration::from_secs(server.max_call_secs));
    tokio::pin!(deadline);
    let end_state = loop {
        tokio::select! {
            ev = events_out_rx.recv() => match ev {
                Some(Event::Transcript { role, text, final_ }) => {
                    if final_ {
                        store.append_transcript(&call_id, TranscriptEntry { role, text, ts_ms: now_ms() });
                    }
                }
                Some(Event::OutputAudio(_)) => {
                    if !greeted {
                        greeted = true;
                        tracing::info!(%call_id, greeting_after_answer_ms = now_ms() - answered_at, "first agent audio");
                    }
                }
                Some(Event::EndCall { goal: g }) => { goal = Some(g); break CallState::Completed; }
                Some(Event::TurnComplete) => {}
                Some(Event::Warning(w)) => tracing::warn!(%call_id, "gemini warning: {w}"),
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
```

- [ ] **Step 4: Run tests + full suite.**

Run: `cargo test --features vendor-openssl --lib engine::tests` then `cargo test --features vendor-openssl --lib`
Expected: PASS. Note: `run_call`'s end-to-end behavior is exercised by the live integration test in Task 5, not here.

- [ ] **Step 5: Commit.**

```bash
git add src/engine.rs
git commit -m "feat(engine): Engine + run_call orchestration (SIP->answer->Gemini->bridge)"
```

---

## Task 5: `kutsu call` CLI + live integration test

**Files:**
- Modify: `src/main.rs` (add `Command::Call` + handler)
- Create: `tests/engine_call.rs` (`#[ignore]`)

**Interfaces:**
- Consumes: `engine::Engine`, `state::{CallState, CallStore}`, `config::{ServerConfig, SipConfig, ScenarioConfig}`.

- [ ] **Step 1: Write the failing integration test.** Create `tests/engine_call.rs`:

```rust
//! Live integration test for the call engine. Requires the WSL Asterisk stand
//! (echo ext 600) AND a reachable Gemini (api key + proxy per ServerConfig).
//! Validates WIRING & CLEANUP, not dialogue quality. #[ignore]d; run:
//!   cargo test --features vendor-openssl --test engine_call -- --ignored --nocapture
//! Env: KUTSU_SIP_SERVER/_USER/_PASS/_EXT + the Gemini env the live harness uses.

use std::sync::Arc;
use std::time::Duration;

use kutsu::config::{ScenarioConfig, ServerConfig, SipConfig};
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

    let engine = Engine::new(Arc::new(server), &sip_cfg).await.expect("engine up");
    let id = engine
        .place_call(env_or("KUTSU_SIP_EXT", "600"), scenario)
        .await
        .expect("place_call");

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
            if matches!(rec.state, CallState::Completed | CallState::Failed | CallState::HungUp) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let rec = engine.store().get(&id).expect("record exists");
    eprintln!("[engine_call] final state={:?} transcript_len={}", rec.state, rec.transcript.len());
    assert!(saw_in_progress, "call never reached InProgress — wiring/answer failed");
    assert!(rec.ended_ms.is_some() || rec.state == CallState::InProgress, "call did not progress");

    engine.shutdown().await;
}
```

> This test references a small `kutsu::main_support` helper module so the CLI and the test build configs identically. Create it as `pub mod main_support` in `src/lib.rs` (a thin module with `configs_from_env() -> (ServerConfig, SipConfig)` and `default_scenario() -> ScenarioConfig`), and have the CLI use it too. If you prefer not to add a lib module, inline the env-based config construction in both the test and the CLI (duplicated) — but the shared helper is cleaner. Read how `run_live` builds `ServerConfig` and mirror it.

- [ ] **Step 2: Run to verify it fails to compile.**

Run: `cargo test --features vendor-openssl --test engine_call --no-run`
Expected: FAIL — `Command::Call` / `main_support` / config construction not present.

- [ ] **Step 3: Add the `main_support` helper + `Command::Call`.**

In `src/lib.rs`, add `pub mod main_support;` and create `src/main_support.rs`:

```rust
//! Shared config construction for the `kutsu call` CLI and its integration test.

use crate::config::{
    Model, NetCheckConfig, Proxy, ScenarioConfig, ServerConfig, SipConfig, SipTransportKind,
    DEFAULT_GREET_AFTER_SILENCE_MS,
};

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

fn non_empty(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

/// Build (ServerConfig, SipConfig) from environment (mirrors `run_live`'s
/// ServerConfig construction). Errors if GEMINI_API_KEY is unset. SIP fields
/// come from KUTSU_SIP_*.
pub fn configs_from_env() -> anyhow::Result<(ServerConfig, SipConfig)> {
    let api_key =
        std::env::var("GEMINI_API_KEY").map_err(|_| anyhow::anyhow!("GEMINI_API_KEY not set"))?;
    let proxy = non_empty("PROXY_URL").map(|url| Proxy {
        url,
        user: non_empty("PROXY_USER"),
        password: non_empty("PROXY_PASSWORD"),
    });
    let server = ServerConfig {
        api_key,
        proxy,
        model: Model::HalfCascade,
        voice: env_or("KUTSU_VOICE", "Autonoe"),
        language: "ru-RU".into(),
        net_check: NetCheckConfig::default(),
        max_concurrent_channels: 3,
        greet_after_silence_ms: DEFAULT_GREET_AFTER_SILENCE_MS,
        transcript_dir: non_empty("KUTSU_TRANSCRIPT_DIR").map(std::path::PathBuf::from),
        max_call_secs: 600,
    };
    let sip = SipConfig {
        server: env_or("KUTSU_SIP_SERVER", "192.168.88.243:5060"),
        username: env_or("KUTSU_SIP_USER", "kutsu"),
        password: env_or("KUTSU_SIP_PASS", "kutsupw"),
        from_user: None,
        local_ip: None,
        register: false,
        transport: SipTransportKind::Udp,
    };
    Ok((server, sip))
}

/// A minimal valid scenario (system prompt from KUTSU_SYSTEM_PROMPT or a default).
pub fn default_scenario() -> ScenarioConfig {
    ScenarioConfig {
        system_prompt: env_or(
            "KUTSU_SYSTEM_PROMPT",
            "You are a friendly assistant making a phone call. Greet the person warmly and have a short, natural conversation.",
        ),
        goal_schema: serde_json::json!({}),
        context: None,
    }
}
```

`configs_from_env` returns `anyhow::Result` because `GEMINI_API_KEY` may be unset — callers handle the error (the test `.expect`s it; the CLI prints + exits 1). This mirrors `run_live`'s existing env handling; the field lists above are the exact current `ServerConfig`/`SipConfig`/`ScenarioConfig` structs.

Then in `src/main.rs`, add a `Call` variant to the `Command` enum:

```rust
    /// Place one outbound call: dial <number>, bridge to Gemini, print the transcript.
    Call {
        /// Number/extension to dial.
        number: String,
        /// Optional scenario JSON file (system prompt, goal schema). Uses a default if absent.
        #[arg(long)]
        scenario: Option<std::path::PathBuf>,
    },
```

And handle it in `main` (mirroring the `Live` arm's runtime setup):

```rust
        Some(Command::Call { number, scenario }) => {
            let rt = /* same tokio runtime builder as Live */;
            let code = rt.block_on(run_call_cli(number, scenario));
            std::process::exit(code);
        }
```

- [ ] **Step 4: Implement `run_call_cli`.** Add to `src/main.rs`:

```rust
async fn run_call_cli(number: String, scenario_path: Option<std::path::PathBuf>) -> i32 {
    let (server, sip_cfg) = match kutsu::main_support::configs_from_env() {
        Ok(x) => x,
        Err(e) => { eprintln!("config error: {e}"); return 1; }
    };
    let scenario = match scenario_path {
        Some(p) => match std::fs::read_to_string(&p).ok().and_then(|s| serde_json::from_str(&s).ok()) {
            Some(s) => s,
            None => { eprintln!("failed to read scenario {p:?}"); return 1; }
        },
        None => kutsu::main_support::default_scenario(),
    };

    let engine = match kutsu::engine::Engine::new(std::sync::Arc::new(server), &sip_cfg).await {
        Ok(e) => e,
        Err(e) => { eprintln!("engine init failed: {e}"); return 1; }
    };
    let id = match engine.place_call(number, scenario).await {
        Ok(id) => id,
        Err(e) => { eprintln!("place_call failed: {e}"); return 1; }
    };

    // Poll the store until terminal; print transcript lines as they grow.
    let mut printed = 0usize;
    let final_state = loop {
        if let Some(rec) = engine.store().get(&id) {
            for entry in rec.transcript.iter().skip(printed) {
                println!("[{:?}] {}", entry.role, entry.text);
            }
            printed = rec.transcript.len();
            if matches!(rec.state, kutsu::state::CallState::Completed | kutsu::state::CallState::Failed | kutsu::state::CallState::HungUp) {
                break rec.state;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };

    println!("call ended: {final_state:?}");
    engine.shutdown().await;
    match final_state {
        kutsu::state::CallState::Failed => 1,
        _ => 0,
    }
}
```

- [ ] **Step 5: Compile + unit suite.**

Run: `cargo test --features vendor-openssl --test engine_call --no-run` then `cargo test --features vendor-openssl --lib`
Expected: compiles; unit suite green.

- [ ] **Step 6: (Optional, if the stand + Gemini are reachable) run the live test.**

Prereq: WSL Asterisk up (`wsl.exe -- pgrep -a asterisk`) + Gemini env set.
Run: `cargo test --features vendor-openssl --test engine_call -- --ignored --nocapture`
Expected: reaches `InProgress`, ends in a terminal state, prints final state. If Gemini env isn't available in this environment, report that the test compiles and is ready to run where Gemini is reachable — do NOT treat an unreachable-Gemini skip as a failure.

- [ ] **Step 7: Commit.**

```bash
git add src/main.rs src/lib.rs src/main_support.rs tests/engine_call.rs
git commit -m "feat(engine): kutsu call CLI + live integration test"
```

---

## Self-Review

**Spec coverage:**
- §3 state store (`CallStore`/`CallRecord`/`CallState`, reuse `TranscriptEntry`) → Task 1. ✔
- §4 seam splits (`SipCall::split`, `Session::split`/`SessionHandle`) → Task 3. ✔
- §5 orchestration (cap-check + spawn; SIP→answer→Gemini→bridge; select loop; teardown; finalize; persist) → Task 4 `place_call`/`run_call`. ✔
- §6 instrumentation (`dead_air_ms`, `greeting_after_answer_ms`) + no-latency-assertion → Task 4 `tracing` stamps; the constraint is honored (only logged). ✔
- §7 config (`transcript_dir`, `max_call_secs`) → Task 2. ✔
- §8 CLI (`kutsu call`, poll store, print, exit code) → Task 5. ✔
- §9 tests (state unit; split channel round-trips; cap unit; live `#[ignore]` engine test) → Tasks 1/3/4/5. ✔

**Placeholder scan:** Task 5 Step 3 intentionally contains `unimplemented_*`/`TODO-for-implementer` markers *inside guidance the implementer must complete by reading `run_live`* — this is because the exact `ServerConfig`/`ScenarioConfig` construction lives in existing code the plan should not blindly duplicate. Every other step has complete code. The implementer MUST replace those markers with real construction mirroring `run_live`; a reviewer should reject leftover `unimplemented!()`.

**Type consistency:** `CallStore::{new,insert,set_state,append_transcript,set_transcript,finalize,get,list}` are defined in Task 1 and used in Task 4 (`set_state`/`append_transcript`/`set_transcript`/`finalize`/`get`) and Task 5 (`get`). `SipCallParts{call_id,events,audio_in,audio_out,hangup}` (Task 3) is destructured in Task 4. `SessionHandle::{hangup,join}` + `Session::split` (Task 3) used in Task 4. `Engine::{new,place_call,store,shutdown}` (Task 4) used in Task 5. `BridgePorts` field names (`codec,phone_in,phone_out,gemini_in,gemini_events,events_out`) match the bridge module. `Event` variants (`Transcript{role,text,final_}`, `OutputAudio`, `EndCall{goal}`, `TurnComplete`, `Warning`) match `gemini_live::Event`. `EngineError::{CapReached,Sip}` consistent.
