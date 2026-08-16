# Callee VAD Greeting Gate + Reconnect-Safe Greeting — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Suppress the proactive greeting the instant the callee speaks (energy VAD), never re-greet across Gemini reconnects, send a configurable RESUME_CUE on lost-context reconnect, and drop stale audio on reconnect so the session doesn't catch up.

**Architecture:** A pure adaptive-noise-floor `Vad` detects callee speech from incoming PCM16. Per-call `Arc<AtomicBool>` flags (`callee_active`, `greeted`, `resume_needed`), created in `gemini_live::start` and shared into every `run_session` attempt, gate the greeting and drive reconnect behaviour. On a reconnect (`is_reconnect`), `run_session` drains the buffered uplink, flushes the pacer (an `Interrupted` event), and — only if context was lost and an exchange was mid-flight — sends `RESUME_CUE`.

**Tech Stack:** Rust 2024, tokio (`select!`, `watch`, `AtomicBool`), the existing FakeTransport harness in `gemini_live.rs`.

**Spec:** `docs/superpowers/specs/2026-08-16-callee-vad-greet-design.md`

## Global Constraints

- Build/test ONLY with `cargo test --lib --features vendor-openssl`. Plain `cargo test` fails to link OpenSSL on this Windows host. Verify integration compiles with `cargo build --tests --features "vendor-openssl live-tests"`.
- All in-repo text (code, comments, cue strings) is English. `RESUME_CUE` is an English instruction to the model, configurable via `KUTSU_RESUME_CUE`; do not hardcode a non-English phrase.
- HARD: no proactive greeting once the callee has spoken (`callee_active`) or once a greeting has already been sent (`greeted`) — both persist across reconnects.
- The VAD only reads the incoming frame for RMS; it must not alter or delay the audio forwarded to Gemini.
- Reconnect actions (drain / Interrupted / RESUME_CUE) are `is_reconnect == true` only; the first-open path is unchanged.
- `callee_active`/`greeted`/`resume_needed` are shared `Arc<AtomicBool>`; `noise_floor`/onset counter are per-`run_session` (reset on reconnect).

---

### Task 1: `Vad` — pure adaptive-noise-floor energy VAD

**Files:**
- Create: `src/gemini_live/vad.rs` (or `src/vad.rs` if `gemini_live` is a flat file — check and register `mod vad;` accordingly)
- Modify: `src/config.rs` (`VadConfig`)
- Modify: crate root / `gemini_live` module to register the module.

**Interfaces:**
- Produces: `struct VadConfig { min_rms: u32, ratio: f32, onset_frames: u32 }` (+ `Default`); `struct Vad { .. }` with `fn new(cfg: VadConfig) -> Self` and `fn observe(&mut self, frame: &[i16]) -> bool` (returns `true` exactly once, on the frame that confirms speech onset; `false` afterwards/otherwise).

- [ ] **Step 1: Write the failing tests + skeleton**

`src/config.rs` — add near `QualityConfig`/`RetryConfig`:
```rust
/// Energy-VAD tuning for detecting that the callee has started speaking.
#[derive(Debug, Clone, Copy)]
pub struct VadConfig {
    /// Absolute RMS floor: a frame below this is never speech, even if the
    /// adaptive noise floor has decayed toward zero. Telephone-calibrated.
    pub min_rms: u32,
    /// Speech = frame RMS >= max(min_rms, noise_floor * ratio).
    pub ratio: f32,
    /// Consecutive speech frames required to confirm onset (rejects clicks).
    pub onset_frames: u32,
}
impl Default for VadConfig {
    fn default() -> Self { Self { min_rms: 200, ratio: 3.0, onset_frames: 3 } }
}
```

Create the vad module with the struct + this test block:
```rust
//! Pure adaptive-noise-floor energy VAD, ported from the voice-cloud prototype
//! (audio_io.py). Detects the onset of callee speech from incoming PCM16.
use crate::config::VadConfig;

pub struct Vad {
    cfg: VadConfig,
    noise_floor: f32,
    consec: u32,
    fired: bool,
}

impl Vad {
    pub fn new(cfg: VadConfig) -> Self {
        Self { cfg, noise_floor: 0.0, consec: 0, fired: false }
    }
    pub fn observe(&mut self, _frame: &[i16]) -> bool { unimplemented!("Step 3") }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(amp: i16, n: usize) -> Vec<i16> { vec![amp; n] }
    fn cfg() -> VadConfig { VadConfig { min_rms: 200, ratio: 3.0, onset_frames: 3 } }

    #[test]
    fn silence_never_fires() {
        let mut v = Vad::new(cfg());
        for _ in 0..50 { assert!(!v.observe(&frame(0, 320))); }
    }
    #[test]
    fn sustained_speech_fires_once_after_onset_frames() {
        let mut v = Vad::new(cfg());
        // Two loud frames: not yet (onset_frames = 3).
        assert!(!v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320)));
        // Third confirms onset -> fires exactly once.
        assert!(v.observe(&frame(4000, 320)));
        assert!(!v.observe(&frame(4000, 320))); // already fired
    }
    #[test]
    fn single_click_does_not_fire() {
        let mut v = Vad::new(cfg());
        assert!(!v.observe(&frame(8000, 320))); // one loud frame
        for _ in 0..10 { assert!(!v.observe(&frame(0, 320))); } // back to silence, consec resets
    }
    #[test]
    fn adapts_to_steady_background_no_false_fire() {
        let mut v = Vad::new(cfg());
        // A steady moderate background (well above min_rms but constant) must
        // be tracked by the noise floor and NOT read as speech.
        for _ in 0..200 { assert!(!v.observe(&frame(500, 320))); }
    }
    #[test]
    fn speech_above_raised_floor_still_fires() {
        let mut v = Vad::new(cfg());
        for _ in 0..100 { v.observe(&frame(500, 320)); }   // floor adapts up toward ~500
        // Real speech well above floor*ratio still fires.
        let mut fired = false;
        for _ in 0..5 { if v.observe(&frame(4000, 320)) { fired = true; } }
        assert!(fired);
    }
}
```
Register the module (`mod vad;` in `gemini_live.rs` or crate root; check how `mod pace;`/`mod g711;` are declared).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib --features vendor-openssl vad::` — FAIL (unimplemented + the deliberately-wrong `new`).

- [ ] **Step 3: Implement**

```rust
impl Vad {
    pub fn new(cfg: VadConfig) -> Self {
        Self { cfg, noise_floor: 0.0, consec: 0, fired: false }
    }

    /// Feed one incoming callee frame. Returns true exactly once — on the frame
    /// that confirms speech onset. Once fired, always returns false.
    pub fn observe(&mut self, frame: &[i16]) -> bool {
        if self.fired || frame.is_empty() { return false; }
        let sumsq: f64 = frame.iter().map(|&s| (s as f64) * (s as f64)).sum();
        let rms = (sumsq / frame.len() as f64).sqrt() as f32;
        let threshold = (self.cfg.min_rms as f32).max(self.noise_floor * self.cfg.ratio);
        if rms >= threshold {
            self.consec += 1;
            if self.consec >= self.cfg.onset_frames {
                self.fired = true;
                return true;
            }
        } else {
            self.consec = 0;
            // Track background only on non-speech frames (EMA, a = 0.1).
            self.noise_floor = self.noise_floor * 0.9 + rms * 0.1;
        }
        false
    }
}
```
(The `adapts_to_steady_background` test pins that a constant 500-RMS background raises the floor so `floor*ratio` climbs above it and it never reads as speech; verify the EMA + threshold make that hold. Adjust the `a`/seed only if a listed test needs it — do not change the test expectations.)

- [ ] **Step 4: Run tests**

Run: `cargo test --lib --features vendor-openssl vad:: config::` — PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gemini_live/vad.rs src/config.rs src/gemini_live.rs   # module registration
git commit -m "feat(gemini): pure adaptive-noise-floor energy VAD + VadConfig"
```

---

### Task 2: Config plumbing — VadConfig on ServerConfig, RESUME_CUE, greet 1000

**Files:**
- Modify: `src/config.rs` (`ServerConfig.vad`, `RESUME_CUE` const, `DEFAULT_GREET_AFTER_SILENCE_MS` 1500→1000)
- Modify: `src/main_support.rs` (env for vad + resume cue)
- Modify: every `ServerConfig { .. }` literal the compiler flags (build-driven)

**Interfaces:**
- Produces: `ServerConfig.vad: VadConfig`; `ServerConfig.resume_cue: String`; `pub const RESUME_CUE: &str`.

- [ ] **Step 1: tests first**

In `config.rs` tests:
```rust
#[test]
fn default_greet_delay_is_1000ms() {
    assert_eq!(DEFAULT_GREET_AFTER_SILENCE_MS, 1000);
}
```

- [ ] **Step 2: run to fail** — `cargo test --lib --features vendor-openssl config::tests::default_greet_delay_is_1000ms` (value is 1500).

- [ ] **Step 3: implement**

- `DEFAULT_GREET_AFTER_SILENCE_MS`: `1500` → `1000`.
- Add the const (English instruction, language-pinning clause):
```rust
/// Default instruction sent to the model when a session reconnects with lost
/// context mid-exchange. An instruction, not a spoken phrase; the model speaks
/// it in the conversation's language. Override with `KUTSU_RESUME_CUE`.
pub const RESUME_CUE: &str = "The connection dropped briefly and the last thing \
    the other party said may be lost. Ask them to repeat what they just said, \
    replying in the same language you have been speaking.";
```
- `ServerConfig`: add `pub vad: VadConfig,` and `pub resume_cue: String,`.
- `main_support.rs` `configs_from_env`: build `VadConfig` from `KUTSU_VAD_MIN_RMS` (env_u32, default 200), `KUTSU_VAD_RATIO` (parse f32, default 3.0), `KUTSU_VAD_ONSET_FRAMES` (env_u32, default 3); `resume_cue: env_or("KUTSU_RESUME_CUE", RESUME_CUE)`.
- Fix every flagged `ServerConfig { .. }` literal: add `vad: VadConfig::default(), resume_cue: RESUME_CUE.into(),` (or the built ones in main_support). Build to find all sites (`src/main.rs`, `src/engine.rs`, `src/mcp.rs`, `src/mcp_http.rs`, `src/gemini_live.rs`, `src/proto.rs`, `tests/live_smoke.rs`, config tests).

- [ ] **Step 4: verify** — `cargo test --lib --features vendor-openssl config::` PASS; `cargo build --tests --features "vendor-openssl live-tests"` compiles.

- [ ] **Step 5: commit**

```bash
git add -A
git commit -m "feat(config): ServerConfig.vad + resume_cue (env) + greet default 1000ms"
```

---

### Task 3: `run_session` — persistent flags, VAD gate, greeting suppression

**Files:**
- Modify: `src/gemini_live.rs` (`run_session` signature + greeting gate + `audio_in` arm VAD + `resume_needed` clear; `start` creates + threads the shared flags + `is_reconnect`)

**Interfaces:**
- Consumes: `Vad`/`VadConfig` (Task 1), `ServerConfig.vad` (Task 2).
- Produces: `run_session(.., callee_active: Arc<AtomicBool>, greeted: Arc<AtomicBool>, resume_needed: Arc<AtomicBool>, is_reconnect: bool)` (added after `answered`).

**Design (integrate against the real loop at gemini_live.rs:150-270):**
- `start` creates once per call: `let callee_active = Arc::new(AtomicBool::new(false)); let greeted = Arc::new(AtomicBool::new(false)); let resume_needed = Arc::new(AtomicBool::new(false));`. Move clones into the reconnect-loop `async move`; pass a clone of each into every `run_session` call, plus `is_reconnect` (`false` on the first loop iteration, `true` thereafter — track with a `bool` set after the first attempt).
- In `run_session`:
  - Replace the local greeting suppressor: the greet arm precondition becomes `greet_armed && !greeted.load(Relaxed) && !had_activity && !callee_active.load(Relaxed) && !is_reconnect`. On firing: `greeted.store(true, Relaxed)` (persistent) in addition to the local `greeted = true`. (Keep the local `greeted`/`had_activity`/`answered` machinery.) The initial local `let mut greeted = resume_handle.is_some() || !greet_enabled` also ORs in `greeted.load()` / `is_reconnect` so a reconnect never arms.
  - **VAD** in the `audio_in.recv()` arm: build `let mut vad = Vad::new(server.vad);` before the loop; on each received `pcm`, `if vad.observe(&pcm) { callee_active.store(true, Relaxed); resume_needed.store(true, Relaxed); tracing::info!("gemini: callee speech onset — greeting suppressed"); }` — do this BEFORE the existing forward-to-Gemini, and keep the forward unchanged.
  - **resume_needed clear**: on `ServerEvent::TurnComplete`, `resume_needed.store(false, Relaxed)` (the model finished its reply).
  - Belt-and-suspenders: on `ServerEvent::Transcript { role: Role::User, .. }`, also `callee_active.store(true)` + `resume_needed.store(true)`.

- [ ] **Step 1: failing test**

Extend the warm-start greeting tests (FakeTransport, `start_paused`): `greeting_suppressed_when_callee_speaks` — answered fires, feed the session `audio_in` several loud frames (enough to trip the VAD onset) BEFORE the greet delay, advance past the delay, assert NO `GREET_CUE` was sent. Keep `greets_after_answered_when_callee_silent` passing (no audio → greeting fires).

- [ ] **Step 2: run to fail** — the new param/gate doesn't exist yet.

- [ ] **Step 3: implement** per the design (signature, gate, VAD arm, resume_needed, start threading + is_reconnect).

- [ ] **Step 4: run tests** — `cargo test --lib --features vendor-openssl gemini_live::` PASS; adapt existing greeting tests to pass the new args (a silent callee: never feed audio).

- [ ] **Step 5: commit**

```bash
git add src/gemini_live.rs
git commit -m "feat(gemini): VAD-gated greeting + persistent callee_active/greeted/resume_needed"
```

---

### Task 4: Reconnect handling — drain, flush, RESUME_CUE

**Files:**
- Modify: `src/gemini_live.rs` (`run_session` reconnect branch: drain `audio_in`, emit `Interrupted`, send `RESUME_CUE`)

**Interfaces:**
- Consumes: `is_reconnect`, `resume_needed`, `resume_handle`, `server.resume_cue` (Tasks 2-3).

**Design:** at the START of `run_session`, after `setup` is sent, when `is_reconnect == true`:
1. **Drain the uplink backlog:** `while audio_in.try_recv().is_ok() { drained += 1; }` then `tracing::info!(drained, "gemini reconnect: dropped stale uplink frames");`.
2. **Flush the pacer:** `let _ = events.send(Event::Interrupted).await;` (the bridge's barge-in path clears the downlink buffer).
3. **RESUME_CUE (conditional):** if `resume_handle.is_none() && resume_needed.load(Relaxed)` → `transport.send_text(build_client_content(&server.resume_cue)).await` (handle send-error like the greeting: `Resumable`), and `tracing::info!("gemini reconnect: context lost mid-exchange — sent RESUME_CUE")`. If `resume_handle.is_some()` → send nothing (model resumes from context). If `!resume_needed` → send nothing.

(`build_client_content` currently takes a `&str`/`&'static str` for `GREET_CUE`; confirm the signature and adapt — `server.resume_cue` is a `String`, pass `&server.resume_cue`.)

- [ ] **Step 1: failing tests**

`run_session` reconnect tests (FakeTransport): construct with `is_reconnect=true`:
- `resume_handle=None` + `resume_needed=true` → assert `RESUME_CUE` (built from `server.resume_cue`) was sent, and no `GREET_CUE`.
- `resume_handle=Some(..)` → assert NO cue sent.
- `resume_needed=false` → assert NO cue sent.
- Pre-queue frames on `audio_in`, run one reconnect step, assert they were drained (not forwarded as realtime input) and one `Interrupted` event was emitted to `events`.

- [ ] **Step 2: run to fail**

- [ ] **Step 3: implement** the reconnect branch.

- [ ] **Step 4: run tests** — `cargo test --lib --features vendor-openssl gemini_live::` PASS.

- [ ] **Step 5: commit**

```bash
git add src/gemini_live.rs
git commit -m "feat(gemini): reconnect drops stale uplink + flushes pacer + RESUME_CUE on lost context"
```

---

### Task 5: Whole-feature verification

**Files:** none.

- [ ] **Step 1:** `cargo test --lib --features vendor-openssl` — full suite green.
- [ ] **Step 2:** `cargo build --lib --features vendor-openssl` — no warnings.
- [ ] **Step 3:** `cargo build --tests --features "vendor-openssl live-tests"` — integration compiles.
- [ ] **Step 4:** Manual note (no commit): on a real `kutsu call 6001`, saying "Алло" immediately should suppress the proactive greeting (single greeting, via the `greeting suppressed` log); a silent callee should get one greeting at ~1 s. Tune the VAD via `KUTSU_VAD_MIN_RMS`/`KUTSU_VAD_RATIO`/`KUTSU_VAD_ONSET_FRAMES` against the `uplink_rms` log if onset is missed or false-triggered.
- [ ] **Step 5:** Commit any verification fixups if needed.
