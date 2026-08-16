# Gemini Warm-Start Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect Gemini during the ring window (silence keepalive) so the session is warm when the callee answers, and gate the greeting so the model never speaks during ring.

**Architecture:** A `watch::channel<bool>` "answered" signal is created by the engine, threaded into `gemini_live::start` → the reconnect loop → `run_session`, where the greeting timer arms only after answer. `run_call` starts Gemini before the answer-wait, feeds 20 ms silence into `session.audio_in` while ringing, fires `answered` on `Answered`, and builds the bridge with the already-warm session.

**Tech Stack:** Rust 2024, tokio (`select!`, `watch`, `time`), existing FakeTransport test harness in `gemini_live.rs`.

**Spec:** `docs/superpowers/specs/2026-08-16-gemini-warm-start-design.md`

## Global Constraints

- Build/test ONLY with `cargo test --lib --features vendor-openssl`. Plain `cargo test` fails to link OpenSSL on this Windows host.
- All in-repo text (code, comments) is English.
- **No greeting audio during ring** is the hard invariant: the greet timer must not arm until the `answered` signal is true.
- `session.audio_in` must have a single writer during the call: the engine's ring-time silence feed lives ONLY inside the ring-wait `select!` and is gone before the bridge (the other writer) starts.
- The `watch` "answered" signal is level-triggered (a late reader still sees `true`) — do not replace it with an edge-triggered `Notify` without handling the missed-edge case.
- Warm-connect failure during ring must NOT drop the call — fall back to connecting after answer.

---

### Task 1: `answered` gate for the greeting (`gemini_live.rs`)

**Files:**
- Modify: `src/gemini_live.rs` (`run_session` greeting logic + signature; `start` signature threads `answered`; the join-loop passes it to `run_session`; existing tests updated + a new gate test)

**Interfaces:**
- Consumes: nothing new.
- Produces: `pub async fn start(server: &ServerConfig, scenario: &ScenarioConfig, answered: tokio::sync::watch::Receiver<bool>) -> Result<Session>`.

- [ ] **Step 1: Write the failing test(s)**

In the `gemini_live.rs` test module (uses `FakeTransport` + `run_session` directly — mirror the existing session tests), add tests that drive `run_session` with an `answered` `watch::Receiver`:

```rust
#[tokio::test(start_paused = true)]
async fn no_greeting_before_answered() {
    // greet_after_silence_ms small; answered stays false the whole time.
    // Advance time well past the greet delay; assert FakeTransport received
    // NO GREET_CUE (build_client_content(GREET_CUE)) message.
}

#[tokio::test(start_paused = true)]
async fn greets_after_answered_when_callee_silent() {
    // answered flips true at t0; no model output arrives; after
    // greet_after_silence_ms, assert FakeTransport received exactly one
    // GREET_CUE message.
}
```
(Follow the exact `run_session(...)` call shape the existing tests use — pass the extra `answered` receiver. Use `tokio::time` pause/advance since `start_paused = true`. Assert against whatever the existing tests use to inspect `FakeTransport`'s sent messages; if there is no such hook, add a minimal capture to `FakeTransport` in the test module only.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib --features vendor-openssl gemini_live::tests::no_greeting_before_answered gemini_live::tests::greets_after_answered_when_callee_silent`
Expected: FAIL to compile (run_session has no `answered` param) then, once wired, RED on behaviour before the gate is implemented.

- [ ] **Step 3: Implement the gate**

`run_session` signature — add the answered receiver (by value; it is `Clone`):
```rust
pub(crate) async fn run_session<T: Transport>(
    mut transport: T,
    server: &ServerConfig,
    scenario: &ScenarioConfig,
    resume_handle: Option<String>,
    audio_in: &mut mpsc::Receiver<Vec<i16>>,
    events: &mpsc::Sender<Event>,
    latest_handle: &mut Option<String>,
    setup_ok: &mut bool,
    mut answered: tokio::sync::watch::Receiver<bool>,
) -> SessionEnd {
```

Replace the fixed `greet_at` with an armed-on-answer deadline. After the `greet_enabled`/`greeted` setup:
```rust
    // Greeting arms only after `answered` — never during ring. `greet_armed`
    // gates the timer; `greet_deadline` is meaningless until armed.
    let mut greet_armed = *answered.borrow(); // already answered (e.g. reconnect path) -> arm now
    let mut greet_deadline = tokio::time::Instant::now()
        + std::time::Duration::from_millis(server.greet_after_silence_ms);
    if !greet_armed {
        // far future placeholder so the timer never fires until armed
        greet_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(86_400);
    }
```

In the `select!` loop, replace the greet arm and add an arming arm:
```rust
            // Arm the greeting the moment the call is answered (once).
            _ = answered.changed(), if !greet_armed && !greeted => {
                if *answered.borrow() {
                    greet_armed = true;
                    greet_deadline = tokio::time::Instant::now()
                        + std::time::Duration::from_millis(server.greet_after_silence_ms);
                }
            }
            _ = tokio::time::sleep_until(greet_deadline), if greet_armed && !greeted && !had_activity => {
                greeted = true;
                tracing::info!("gemini: callee silent — sending greeting kickoff");
                if transport.send_text(build_client_content(GREET_CUE)).await.is_err() {
                    return SessionEnd::Resumable("send greeting failed".into());
                }
            }
```
(Keep the `audio_in.recv()` and `transport.recv()` arms exactly as they are. `answered.changed()` returns `Err` if the sender dropped — treat that as "no more changes"; guard so a dropped sender doesn't busy-loop: once `greet_armed` is set or the sender is gone, the arm's precondition `!greet_armed` or a `changed()` error naturally disables it. If `changed()` erroring would busy-spin the arm, bind it to a fused/again-checked form — the implementer verifies no busy-loop.)

`start` — add the parameter and thread it into the spawned join loop and each `run_session` call:
```rust
pub async fn start(
    server: &ServerConfig,
    scenario: &ScenarioConfig,
    answered: tokio::sync::watch::Receiver<bool>,
) -> Result<Session> {
```
Move `answered` into the `tokio::spawn(async move { ... })` (it is `Send`), and pass `answered.clone()` into each `run_session(...)` call inside the reconnect loop.

- [ ] **Step 4: Fix existing callers/tests (keep the crate compiling)**

Every existing `run_session(...)` / `start(...)` call must pass an `answered` receiver:
- `gemini_live.rs` tests: for tests expecting the current greet-after-delay behaviour, create `let (_tx, rx) = tokio::sync::watch::channel(true);` (pre-answered) so their timing is unchanged.
- **`src/engine.rs:588`** (the real caller) currently calls `gemini_live::start(&server, &scenario)`. Change it MINIMALLY to `gemini_live::start(&server, &scenario, { let (_tx, rx) = tokio::sync::watch::channel(true); rx })` — a temporary pre-answered receiver so the crate compiles and behaviour is unchanged (greeting still arms right after this post-answer connect). Task 2 replaces this whole section with the real warm-start wiring. This keeps the crate green between Task 1 and Task 2.

Build to find all sites: `cargo build --lib --features vendor-openssl`.

- [ ] **Step 5: Run tests**

Run: `cargo test --lib --features vendor-openssl gemini_live::`
Expected: PASS (new gate tests + existing session tests green).

- [ ] **Step 6: Commit**

```bash
git add src/gemini_live.rs
git commit -m "feat(gemini): gate greeting on an answered signal (no greeting during ring)"
```

---

### Task 2: `run_call` warm-start sequencing (`engine.rs`)

**Files:**
- Modify: `src/engine.rs` (`run_call`: create the `answered` channel, start Gemini during ring, silence-feed while awaiting `Answered`, fire `answered`, build bridge with the warm session; fallback + teardown)

**Interfaces:**
- Consumes: `gemini_live::start(.., answered_rx)` (Task 1), `Session` (`audio_in`, `split`, `hangup`, `join`).
- Produces: no new public interface.

**Design (integrate against the real code at engine.rs:545-620):**

Current: `split` → answer-wait loop (`Answered` → codec / `Terminated` → finalize / `None` → finalize) → `answered_at` → `InProgress` → `start().await` → `session.split()` → `BridgePorts` → spawn bridge.

New:
1. After `split`, create the signal: `let (answered_tx, answered_rx) = tokio::sync::watch::channel(false);`
2. Warm-start (tolerate failure): `let warm = gemini_live::start(&server, &scenario, answered_rx.clone()).await;` — keep `Ok(session)` as the warm session; on `Err`, log and set a `warm_failed` flag (the call still proceeds via the post-answer fallback).
3. Ring wait — replace the plain answer-wait loop with a `select!` that also feeds silence when a warm session exists:
```rust
   let mut silence = tokio::time::interval(std::time::Duration::from_millis(20));
   let codec = loop {
       tokio::select! {
           ev = sip_events.recv() => match ev {
               Some(SipEvent::Answered { codec }) => break codec,
               Some(SipEvent::Terminated(reason)) => { /* finalize per existing outcome logic; hang up warm session if any; return */ }
               None => { /* existing "sip closed before answer" finalize; hang up warm session if any; return */ }
           },
           _ = silence.tick(), if warm.is_ok() => {
               // Keepalive: 20 ms of 16 kHz silence into the warm session.
               if let Ok(s) = warm.as_ref() { let _ = s.audio_in.try_send(vec![0i16; 320]); }
           }
       }
   };
```
   (Adapt the `Terminated`/`None` arms to the EXACT current finalize code, adding: if `warm` is `Ok`, `warm.hangup().await` + drop so the warm session is torn down before returning.)
4. On `Answered`: `let _ = answered_tx.send(true);` `let answered_at = now_ms();` `store.set_state(&call_id, CallState::InProgress);`
5. Obtain the session:
   - If `warm` is `Ok(session)`: use it directly (already connected).
   - If `warm` is `Err`: fall back to the current post-answer path — `gemini_live::start(&server, &scenario, answered_rx.clone()).await` (with `answered_rx` already `true`, so the greeting arms immediately), handling its error exactly as the current code does (finalize `Failed`/`CallOutcome::Failed`).
6. `let (gemini_handle, gemini_in, gemini_events) = session.split();` then build `BridgePorts` and spawn the bridge EXACTLY as today (Component unchanged from `engine.rs:600-617`). The engine's silence `interval` is dropped when the ring loop exits, so `session.audio_in` (now `gemini_in`, fed by the bridge) has a single writer.
7. Adjust the `dead_air_ms` log: on the warm path it is ≈0 (session pre-connected); log e.g. `warm_start=true`.

- [ ] **Step 1: Write the failing test**

Add an engine test that a warm session is fed silence during ring and the greeting is gated. A full live test is impractical (no SIP/Gemini), so assert the observable ordering with the existing engine test scaffolding: the neighbouring scheduler/cap tests show how to construct an `Engine` and drive `place_call` without a live trunk. If run_call's ring path can't be unit-tested without a live SIP answer, factor the ring-wait (silence-feed + answer detection) into a small helper `async fn ring_wait(...)` and unit-test THAT: given a mock event source that yields `Answered` after N ticks, assert ≥N silence frames were sent to a test `mpsc::Sender` and the loop returns the codec. Name the helper and test in your report.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib --features vendor-openssl engine::` (the new test fails/does not compile).

- [ ] **Step 3: Implement** the warm-start sequencing per the design.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl engine:: gemini_live::` then the full suite.
Expected: PASS, including the existing engine tests (adapt any that assumed the post-answer `start`).

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): warm-start Gemini during ring with silence keepalive"
```

---

### Task 3: default greet delay 1500 → 400 (`config.rs`)

**Files:**
- Modify: `src/config.rs` (`DEFAULT_GREET_AFTER_SILENCE_MS`)

**Interfaces:** none.

- [ ] **Step 1: Write the failing test**

In `src/config.rs` tests (or wherever config consts are tested), add:
```rust
#[test]
fn default_greet_delay_is_400ms() {
    assert_eq!(DEFAULT_GREET_AFTER_SILENCE_MS, 400);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib --features vendor-openssl config::tests::default_greet_delay_is_400ms`
Expected: FAIL (value is 1500).

- [ ] **Step 3: Change the const**

`pub const DEFAULT_GREET_AFTER_SILENCE_MS: u64 = 400;`

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl config::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/config.rs
git commit -m "feat(config): default greet-after-silence 1500ms -> 400ms"
```

---

### Task 4: Whole-feature verification

**Files:** none.

- [ ] **Step 1:** `cargo test --lib --features vendor-openssl` — full suite green.
- [ ] **Step 2:** `cargo build --lib --features vendor-openssl` — no warnings.
- [ ] **Step 3:** `cargo test --no-run --features "vendor-openssl live-tests" --test live_smoke` — compiles.
- [ ] **Step 4:** Manual note (no commit): on a real `kutsu call 6001`, the `gemini connected` log should show warm-start (≈0 connect after answer), and the callee should not hear any greeting audio during ring; the agent greets ~400 ms after answer if the callee is silent.
- [ ] **Step 5:** Commit any verification fixups if needed.
