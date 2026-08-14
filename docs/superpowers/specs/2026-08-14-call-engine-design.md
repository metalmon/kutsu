# Design: `src/engine` + `src/state` — call engine & state store

Status: approved for implementation planning
Date: 2026-08-14
Depends on: `src/sip` (`SipTransport`/`SipCall`), `src/bridge` (`run`/`BridgePorts`),
`src/gemini_live` (`start`/`Session`/`CallOutcome`) — all merged.

## 1. Purpose & boundaries

`engine` drives one outbound call's full lifecycle: dial via `sip` → wait for
answer → bridge audio via `bridge` between the phone and a `gemini_live` session
→ end (SIP BYE, remote hangup, or the model's `end_call`) → finalize the call's
`state::CallRecord` and persist its transcript. `state` is the in-memory store of
call records, keyed by `call_id`, that the engine writes and (phase 5) MCP reads.

Boundaries:
- `engine` owns orchestration + the concurrency cap; it does NOT do DSP (`bridge`),
  SIP/RTP (`sip`), or the Gemini protocol (`gemini_live`).
- `state` is a passive store — no orchestration, no I/O beyond what the engine asks.
- MCP tool surface (`place_call`/`get_call_status`/…) is **phase 5**; this phase
  exposes the library API + a CLI entry point that phase 5 will wrap.

## 2. Scope (this iteration)

In scope:
- `state`: `CallStore` (`Arc<Mutex<HashMap<String, CallRecord>>>`), `CallRecord`,
  `CallState`, reusing `gemini_live::TranscriptEntry`. Write + query methods.
- `engine`: `Engine` (holds `SipTransport` + `CallStore` + config), async
  `place_call(number, scenario) -> call_id` (cap-checks, spawns the call task,
  returns immediately — the MCP async model), internal `run_call` orchestration,
  transcript persistence to disk (JSON).
- Small seam additions so the channel-based `bridge` can own its channel ends:
  `SipCall::split` (in `sip`) and `Session::split` (in `gemini_live`).
- CLI: `kutsu call <number> [--scenario <file>]` — one real end-to-end call,
  mirroring the existing `kutsu live` harness; polls the store, prints transcript
  + outcome, exit code by final state.
- **Timing instrumentation** (§6) so post-answer latency can be measured live.

Out of scope (documented, not built):
- MCP tools (phase 5) — but `Engine`/`CallStore` are shaped so phase 5 just wraps them.
- Inbound calls; multi-trunk; call recording to audio files.
- The early-connect (connect Gemini during ringing) latency optimization — see the
  open question in §6; needs live measurement first.

## 3. `state` — the call store

```rust
use crate::gemini_live::TranscriptEntry;   // reuse: { role: Role, text: String, ts_ms: u64 }
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallState { Ringing, InProgress, Completed, Failed, HungUp }

#[derive(Clone, Debug, serde::Serialize)]
pub struct CallRecord {
    pub call_id: String,
    pub number: String,
    pub state: CallState,
    pub transcript: Vec<TranscriptEntry>,
    pub goal: Option<Value>,        // set when the model calls end_call
    pub error: Option<String>,      // set on Failed
    pub started_ms: u64,
    pub ended_ms: Option<u64>,
}

#[derive(Clone, Default)]
pub struct CallStore { inner: Arc<Mutex<HashMap<String, CallRecord>>> }

impl CallStore {
    pub fn new() -> Self;
    pub fn insert(&self, rec: CallRecord);
    pub fn set_state(&self, call_id: &str, state: CallState);
    pub fn append_transcript(&self, call_id: &str, entry: TranscriptEntry);
    pub fn finalize(&self, call_id: &str, state: CallState, goal: Option<Value>, error: Option<String>, ended_ms: u64);
    pub fn get(&self, call_id: &str) -> Option<CallRecord>;     // clones out
    pub fn list(&self) -> Vec<CallRecord>;
}
```

`TranscriptEntry`/`Role` must be `Clone + Serialize` — if `gemini_live` doesn't
already derive them, add the derives there (small, in-scope change).

Timestamps: milliseconds since epoch. Because `Instant`/`SystemTime::now()` are
used, keep a single `fn now_ms()` helper (engine owns it; state takes ms as args
so it stays pure/testable).

## 4. Seam additions (owned channel ends for the bridge)

`bridge::run(BridgePorts)` needs **owned** channel ends (it moves them into spawned
tasks). `SipCall` and `Session` currently only lend theirs, so each gets a
consuming `split`:

```rust
// sip
pub struct SipCallParts {
    pub call_id: String,
    pub events: mpsc::Receiver<SipEvent>,
    pub audio_in: mpsc::Receiver<Bytes>,
    pub audio_out: mpsc::Sender<Bytes>,
    pub hangup: oneshot::Sender<()>,   // fire (or drop) to BYE
}
impl SipCall { pub fn split(self) -> SipCallParts; }   // consumes; replaces field access for the engine

// gemini_live
pub struct SessionHandle { /* hangup_tx + join handle */ }
impl SessionHandle {
    pub async fn hangup(&self);
    pub async fn join(self) -> CallOutcome;
}
impl Session {
    pub fn split(self) -> (SessionHandle, mpsc::Sender<Vec<i16>>, mpsc::Receiver<Event>);
}
```

`SipCall::hangup(self)` (the existing convenience) stays; `split` is the
decompose path the engine uses. `Session::split` returns the control handle plus
the `audio_in` sender and `events` receiver the bridge needs.

## 5. `engine` — orchestration

```rust
pub struct Engine {
    sip: SipTransport,
    store: CallStore,
    server: Arc<ServerConfig>,
    active: Arc<AtomicUsize>,   // engine-owned in-flight count (see place_call)
}

impl Engine {
    pub async fn new(server: Arc<ServerConfig>) -> Result<Engine, Error>;   // builds SipTransport
    /// Place an outbound call. Cap-checks, spawns the call task, returns its id.
    pub async fn place_call(&self, number: String, scenario: ScenarioConfig) -> Result<String, Error>;
    pub fn store(&self) -> &CallStore;
    pub async fn shutdown(self);
}
```

`place_call` (uses an engine-owned `active` counter, not `sip.active_calls()`,
to avoid a race: `sip`'s count only increments once `sip.place_call` runs inside
the spawned task, which lags the cap check):
1. Atomically: if `active >= server.max_concurrent_channels` → `Err(CapReached)`;
   else increment `active`.
2. Generate `call_id`; `store.insert(CallRecord{ state: Ringing, started_ms, .. })`.
3. `tokio::spawn(async { run_call(...).await; active.fetch_sub(1) })`; return
   `call_id` immediately. `active` is decremented when `run_call` returns on
   every path.

`run_call(sip, store, server, scenario, number, call_id)` — **safe sequential
default: SIP first, answer, THEN Gemini + bridge.** This ordering is what makes it
deadlock-free: Gemini only starts producing events after the bridge exists to
drain `Session.events` (connecting Gemini *before* answer would let its greeting
block `gemini_live`'s event send — the risk §6 is about). The cost is that the
Gemini connect handshake lands after answer as dead air — see §6, do not assume
it's fine.
1. `call = sip.place_call(&number).await` — INVITE. On error → `store.finalize
   (Failed, err)`, return. (No Gemini session exists yet, nothing else to clean up.)
2. `SipCallParts { events, audio_in, audio_out, hangup: sip_hangup, .. } = call.split()`.
3. Await `SipEvent::Answered { codec }` on `events` (ignore any other pre-answer
   event; a `Terminated` here → finalize `Failed`/no-answer, return — still no
   Gemini to tear down). **Stamp `answered_at`.** `store.set_state(InProgress)`.
4. `session = gemini_live::start(&server, &scenario).await` — connect Gemini now,
   after answer, so its events have a drain waiting. On error → BYE the phone via
   `sip_hangup`, finalize `Failed`, return. **Stamp `gemini_connected_at`.**
5. `(gemini_handle, gemini_in, gemini_events) = session.split()`.
6. `(events_out_tx, mut events_out_rx) = mpsc::channel(256)`.
   `ports = BridgePorts { codec, phone_in: audio_in, phone_out: audio_out,
   gemini_in, gemini_events, events_out: events_out_tx }`.
   `bridge_task = tokio::spawn(bridge::run(ports))`. **Stamp `bridged_at`.**
7. Orchestration `select!` loop until an end condition:
   - `events_out_rx.recv()` (forwarded non-audio Gemini events):
     - `Transcript{role,text,final_}` → on `final_`, `store.append_transcript`.
       **First OutputAudio/Transcript stamps `first_audio_at` once** (for latency).
     - `EndCall{goal}` → model ended the call: keep `goal`, `end = Completed`, break.
     - `TurnComplete` / `Warning(w)` → log; continue.
     - channel closed → bridge gone; continue (bridge_task arm will fire).
   - `events.recv()` (SIP lifecycle): `SipEvent::Terminated(reason)` → remote/BYE:
     `end = HungUp`, break. (`Answered` won't recur.)
   - `bridge_task` join → `BridgeEnd` → a side closed: `end = HungUp` (phone) or
     `Completed`-ish (gemini); break.
   - Optional `tokio::time::sleep(max_call_secs)` guard → `end = Completed`, break
     (a safety cap; value from config, generous default).
8. Teardown (order matters): fire `sip_hangup` (BYE to phone); `gemini_handle
   .hangup().await`; `bridge_task.abort()`; `outcome = gemini_handle.join().await`
   (authoritative `ended_by`/`goal`/`transcript` + clean gemini shutdown).
9. Finalize: reconcile — prefer the loop's `end` state, fold in `outcome.goal` if
   the model ended it, replace the running transcript with `outcome.transcript`
   (authoritative), `store.finalize(state, goal, error=None, ended_ms)`.
10. Persist: if `server.transcript_dir` is set, write the finalized `CallRecord`
    as `<dir>/<call_id>.json`. Log the timing marks (§6).

All handles crossing into `tokio::spawn` are `Send` (sip parts, gemini parts,
bridge) → the engine runs on the normal multi-thread runtime.

## 6. Timing instrumentation & the open latency question

Stamp and log (structured `tracing`, ms deltas): `answered_at`,
`gemini_connected_at`, `bridged_at`, `first_audio_at`/`greeting_at` (first
`OutputAudio` after answer). The key figure is `greeting_at − answered_at` — the
real post-answer dead air the callee experiences. Measurable once a real SIP
trunk exists.

**OPEN QUESTION (validate on a real trunk — do NOT assume it's acceptable):**
the sequential default connects Gemini only after answer, so the phone hears
Gemini's ~connect-handshake latency as dead air before the greeting timer even
starts. The likely optimization is to connect Gemini *during ringing* (concurrent
with awaiting `Answered`), but that risks a deadlock: if Gemini produces its
greeting before the bridge drains `Session.events`, `gemini_live`'s event send
blocks and stalls the session — especially if ringing outlasts
`greet_after_silence_ms`. Applying it safely needs either gating Gemini's
greeting until an "answered" signal or starting the bridge earlier. Deferred
until the instrumented numbers justify it. No real trunk yet ⇒ currently
unmeasurable (see [[validate-latency-empirically]], [[kutsu-current-state]]).

## 7. Config additions (`ServerConfig`)

- `transcript_dir: Option<PathBuf>` (`#[serde(default)]`) — where finalized
  `CallRecord` JSON is written; `None` → skip persistence.
- `max_call_secs: u64` (`#[serde(default = ...)]`, generous default e.g. 600) —
  the §5.7 safety cap.

`max_concurrent_channels` already exists.

## 8. CLI — `kutsu call <number>`

Mirrors `kutsu live` (clap `Command::Call { number, scenario: Option<PathBuf>, config }`):
1. Load `ServerConfig` (same path/env as `live`) + `ScenarioConfig` (from
   `--scenario` file, or a default).
2. `engine = Engine::new(server).await?`; `id = engine.place_call(number, scenario).await?`.
3. Poll `engine.store().get(&id)` every ~500 ms until `state` is terminal
   (`Completed`/`Failed`/`HungUp`); print transcript lines as they grow.
4. Print final outcome (state, goal); `engine.shutdown().await`; exit code:
   `Completed` → 0, `HungUp` → 0, `Failed` → 1.

## 9. Testing

Given **no real SIP trunk**, a real conversational call is not testable; the
plumbing is.

Unit (pure, no I/O):
- `state`: insert → get round-trips; `set_state`/`append_transcript`/`finalize`
  mutate correctly; `list` returns all; `get` of a missing id → `None`;
  `CallRecord` serializes to the expected JSON shape.
- `engine`: `place_call` returns `Err(CapReached)` when
  `active_calls >= max_concurrent_channels` (drive via a store/sip stand-in or a
  cap of 0). `call_id` generation is unique. `now_ms` monotonic-ish.
- `SipCall::split` / `Session::split`: parts carry the same channel ends (send on
  one, receive on the other) — a small channel round-trip test each.

Integration (`#[ignore]`, live — validates **wiring & cleanup, not dialogue
quality**): `tests/engine_call.rs` against the WSL Asterisk **echo 600** + real
Gemini. Asserts: `place_call` → record reaches `InProgress`; audio flows both
ways for a few seconds without panic/deadlock; hang up → record reaches a
terminal state with a non-empty timing log; `transcript_dir` file written. (Echo
600 loops audio, so the "conversation" is degenerate — this proves the engine
mechanics, per §1.) Same env knobs as the sip/bridge live tests.

Manual: `kutsu call <ext>` against the stand for end-to-end smoke.

## 10. Out of scope / deferred

MCP tools (phase 5, wraps `Engine`/`CallStore`); inbound calls; the early-connect
latency optimization (§6, needs live measurement); audio-file call recording;
per-call scenario overrides beyond `--scenario`; retry/redial policy.
