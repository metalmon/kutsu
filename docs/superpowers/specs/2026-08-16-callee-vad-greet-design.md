# Callee VAD greeting gate + reconnect-safe greeting — design

**Status:** approved design, pre-implementation
**Date:** 2026-08-16
**Scope:** `src/gemini_live.rs` (persistent call-state flags across reconnects; adaptive-noise-floor VAD on incoming callee audio; greeting gated on callee activity + a persistent "greeted" flag; RESUME_CUE on lost-context reconnect), `src/config.rs` (`DEFAULT_GREET_AFTER_SILENCE_MS` 1500→1000; VAD params; RESUME_CUE), `src/main_support.rs` (env for the new params).

## Problem (observed + from the prototype)

Two greeting defects, both about the model speaking a greeting when it shouldn't:

1. **Double greeting within a session.** With warm-start the agent greets ~`greet_after_silence_ms` after answer. When the callee says "Алло" during that wait, two greetings result: the proactive `GREET_CUE` fires on the timer AND the model separately responds to the transcribed "Алло". Confirmed on a live call. Increasing the pause is not a fix — it depends on Gemini's transcription latency (measured 1.2–6.4 s, unpredictable) and leaves the agent silent after "Алло" (the pause is held unconditionally, since the greet timer only checks Gemini *output* activity, never the callee's *incoming* audio).

2. **Re-greeting on reconnect.** The Gemini WSS reconnects on server `GoAway` (normal session-lifetime signal) or a network drop — kutsu never manipulates the channel itself. On a reconnect that lands *before* a session-resumption handle arrived (`handle == None`), `run_session` treats the fresh session as a first open and re-greets. The prototype (`e:/voice-cloud`, `gemini_live.py:241-262`) solved this with a persistent `opened` flag: on reconnect it **never re-greets** — with a handle it sends nothing (the model continues from context; any cue would duplicate), and only if context was lost (`handle == None`) *and* an exchange was mid-flight does it send a `RESUME_CUE` ("Алло? Вы пропали… повторите"), not a greeting.

## Decisions

- **Port the prototype's debugged energy VAD, don't reinvent it.** The prototype (`e:/voice-cloud`, `audio_io.py`) uses an adaptive-noise-floor energy VAD (`noise_floor` EMA + `barge_threshold` + onset/hang). We port that mechanism to Rust as-is in spirit. It detects the callee speaking from incoming audio energy — independent of Gemini's variable transcription latency — and sets `callee_active` the instant the callee speaks, suppressing the proactive greeting.
- **The adaptive floor self-calibrates, so telephone levels are not a problem.** The prototype's `noise_floor = noise_floor*0.9 + r*0.1` EMA tracks whatever background a given line has, and speech is detected relative to it (`r >= noise_floor * ratio`). The only value that must change from the prototype is the *absolute* floor: the prototype's `BARGE_RMS=1400` minimum is for a loud *local mic*; our callee audio is telephone-level (measured `uplink_rms` 124–491 whole-call average, i.e. voiced frames a few hundred RMS). So the absolute floor becomes an env param with a telephone-appropriate default; the adaptive ratio and onset logic are ported unchanged. (Silero, the prototype's optional ML backend, is a later swap — see seams.)
- **Persistent call-state across reconnects** (the prototype's `opened`/`resume_needed`): `greeted`, `callee_active`, `resume_needed` are created once per call and shared into every `run_session` attempt, so a reconnected session never re-greets and can resume gracefully.
- **RESUME_CUE on lost-context reconnect** (prototype polish, user-approved): a no-handle reconnect mid-exchange sends a "you cut out, please repeat" cue instead of a greeting. Following `GREET_CUE`, it is an **English instruction to the model, not a phrase to speak**, and it is **fully configurable** (`KUTSU_RESUME_CUE`) so a deployment sets it in its own language. Empirically the English `GREET_CUE` does not make the model switch to English (the system prompt pins the language), but the default RESUME_CUE additionally spells out "reply in the same language you have been speaking" as a defensive clause against a language switch. (The same clause can be added to `GREET_CUE`; noted, not required by this change.)
- **Handle reconnects unconditionally** (they are stochastic and may not reproduce; correctness must not depend on repro).
- **Shorten the default pause** to 1000 ms: it now only governs the silent-callee case, since the VAD handles the callee-speaks case.

## Component 1 — persistent call-state (`gemini_live.rs`, `start`)

`start` creates, once per call, three `Arc<AtomicBool>` shared into the reconnect loop and each `run_session`:
- `callee_active` — the callee has produced speech (set by the VAD; belt-and-suspenders: also set on a `Transcript { role: User }`).
- `greeted` — the proactive `GREET_CUE` has been sent once.
- `resume_needed` — the model owes a reply (an exchange is mid-flight): set when the callee speaks, cleared on `TurnComplete`.

The existing `answered` `watch<bool>` (warm-start) is unchanged. `run_session`'s signature gains these handles (by value; `Arc`/`watch` are `Clone`). The reconnect loop also passes an explicit `is_reconnect: bool` (false on the first attempt, true thereafter) — `handle == None` alone cannot distinguish a first open from an early reconnect.

## Component 2 — adaptive-noise-floor VAD (`run_session`, `audio_in` arm)

Incoming callee frames arrive on `audio_in.recv()` as PCM16 @ 16 kHz (~320 samples / 20 ms per frame from the bridge uplink). Per frame:
- `r = rms(frame)`.
- Maintain a per-session `noise_floor` (starts at a small seed): when the frame is NOT speech, decay it toward the frame — `noise_floor = noise_floor * (1 - a) + r * a` (a ≈ 0.1). This tracks the callee line's background.
- **Speech test:** `r >= max(min_rms, noise_floor * ratio)`. `min_rms` is an absolute floor so a silent line (noise_floor → ~0) still needs real energy; `ratio` is the margin above background.
- **Onset confirmation:** require `onset_frames` consecutive speech frames before declaring speech (rejects clicks/pops). On confirmation: `callee_active.store(true)`, `resume_needed.store(true)`.

`noise_floor`/onset counter are per-`run_session` (reset on reconnect); `callee_active`/`resume_needed` persist. The VAD only *reads* the frame for RMS — it does not alter the audio forwarded to Gemini.

Params (env, defaults calibrated to telephone levels from our `uplink_rms` data; the per-call `uplink_rms` log lets operators re-tune):
- `KUTSU_VAD_MIN_RMS` (default ~200) — absolute speech floor.
- `KUTSU_VAD_RATIO` (default ~3.0) — margin over noise floor.
- `KUTSU_VAD_ONSET_FRAMES` (default 3) — ~60 ms of sustained energy.

## Component 3 — greeting gate + reconnect handling (`run_session`)

The proactive greeting (`GREET_CUE`) fires only when ALL hold: `!is_reconnect` (first open), `greet_enabled`, `answered` observed, the greet-delay elapsed, no Gemini output yet (`!had_activity`), `!callee_active`, and `!greeted`. On firing: `greeted.store(true)` and send `GREET_CUE`. Because `greeted`/`callee_active` persist, no later session (reconnect) can re-greet.

On a reconnect (`is_reconnect == true`), never greet. After setup:
- if `resume_handle.is_none() && resume_needed` → send `RESUME_CUE` (context lost mid-exchange; ask the callee to repeat);
- else → send nothing (a handle means the model resumes from context; a cue would duplicate).

`resume_needed` is cleared on `ServerEvent::TurnComplete` (the model finished its reply). The existing `resume_handle.is_some()` local `greeted` shortcut is superseded by the persistent `greeted` + `is_reconnect` logic.

## Component 4 — drop stale audio on reconnect (no catch-up)

The prototype flushes both directions on reconnect (`gemini_live.py:251-253`: `audio_out.flush()` + drain `mic_q`) so the resumed session starts from live audio instead of replaying a backlog. We port both:

- **Uplink drain (the main lag source).** During the WSS gap, `run_session` is between attempts and not reading `audio_in`, so the bridge's callee frames accumulate in that channel (up to its 64-frame / ~1.3 s bound). On reconnect, before entering the session loop, non-blockingly drain the backlog — `while audio_rx.try_recv().is_ok() {}` — so the new Gemini session receives *live* callee audio, not 1.3 s of stale frames it would otherwise have to catch up on. This pairs naturally with RESUME_CUE: the mid-exchange callee audio is dropped, so asking them to repeat is the correct recovery.
- **Downlink flush.** Stale model audio still buffered in the pacer from before the drop should not prepend the resumed turn. The bridge does not observe the internal Gemini reconnect, so on reconnect `gemini_live` emits the existing `Event::Interrupted` to the bridge (the barge-in path already does `downlink.clear()` + `set_expecting(false)`), flushing the pacer. Effect is smaller than the uplink drain (the pacer largely self-drains during the ≥300 ms backoff), but it avoids a stale fragment playing ahead of the resumed audio.

Both are reconnect-only (`is_reconnect == true`); the first-open path is unchanged.

## Component 5 — config (`config.rs`, `main_support.rs`)

- `DEFAULT_GREET_AFTER_SILENCE_MS`: 1500 → 1000. (`KUTSU_GREET_AFTER_SILENCE_MS` override already exists.)
- `RESUME_CUE`: a const **English instruction to the model** (mirroring `GREET_CUE`'s style, not the prototype's spoken Russian phrase), with an explicit language-pinning clause, e.g. "The connection dropped briefly and the last exchange may be lost. Ask the other party to repeat what they just said, replying in the same language you have been speaking." Overridable via `KUTSU_RESUME_CUE`, so a deployment can set the exact wording/language.
- VAD params read in `main_support.rs` via the existing `env_u*` helpers, threaded onto `ServerConfig` (a small `VadConfig`, mirroring `QualityConfig`/`RetryConfig`).

## Error handling & edge cases

- Silent callee: never sets `callee_active`; the greeting fires once at ~1 s; `greeted` prevents any reconnect re-greet.
- Callee speaks immediately: `callee_active` set within `onset_frames` (~60 ms) → the proactive greeting is suppressed; the model responds reactively (its response is Gemini-latency-bound — out of our control, not a defect of this change).
- Reconnect with handle after the callee spoke: no cue (model resumes); `resume_needed` may still be set but the handle path sends nothing.
- Reconnect with no handle and no pending exchange (`!resume_needed`): send nothing (no spurious RESUME_CUE).
- VAD false-positive from line noise: mitigated by `min_rms` + `ratio` over the adaptive floor + `onset_frames`; worst case the greeting is suppressed for a genuinely silent callee — recoverable (the model still engages once the callee speaks, and the operator can raise the thresholds via env using the `uplink_rms` log).
- The VAD must not add latency to the audio forwarded to Gemini — RMS is computed on the frame already in hand, before/after the existing `send`.

## Testing

- VAD (pure, unit-tested): a helper `fn vad_step(state, frame, cfg) -> onset_bool` (or a small `Vad` struct) — silence keeps `callee_active` false; a burst of `onset_frames` above `max(min_rms, floor*ratio)` flips it; a single loud frame does not (onset gating); the noise floor adapts (a rising steady background does not eventually read as speech).
- Greeting gate (FakeTransport, extend the warm-start tests): greeting suppressed when `callee_active` is set before the timer; greeting fires when the callee stays silent; `greeted` set once — a second `run_session` (simulated reconnect, `is_reconnect=true`) sends no `GREET_CUE`.
- Reconnect cue: `is_reconnect=true` + `resume_handle=None` + `resume_needed=true` → `RESUME_CUE` sent; with `resume_handle=Some` → nothing sent; with `resume_needed=false` → nothing sent.
- Reconnect drain: with frames pre-queued on `audio_in`, a reconnect attempt drains them (they are not forwarded to the new session) while a first open forwards normally; the reconnect path emits one `Interrupted` to the bridge.
- Config: `DEFAULT_GREET_AFTER_SILENCE_MS == 1000`; VAD env parse; `KUTSU_RESUME_CUE` override.

## Improvements over the prototype

This is a port, but deliberately better than the prototype where it was rough:
- **Pure, unit-tested core** — the VAD is a pure `Vad::observe(frame) -> bool` (adaptive floor + onset), the greeting gate and reconnect-cue decision are pure predicates. The prototype was untested async glue; here each rule is asserted independently.
- **Adaptive + tunable, not a fixed mic-tuned constant** — the prototype's `BARGE_RMS=1400` was silently wrong for any non-local-mic source; ours self-calibrates via the noise floor and exposes the absolute floor as env, calibratable against the per-call `uplink_rms` log.
- **i18n-safe cue** — the prototype hardcoded a Russian spoken phrase; ours is an English model-*instruction*, configurable, with an explicit language-pinning clause. Repo text stays English.
- **Observability** — structured `tracing` on each decision (not the prototype's ad-hoc prints), so the behaviour is tunable in production: `INFO` when the proactive greeting is suppressed because the callee spoke (with the onset RMS + floor), when `RESUME_CUE` is sent (with the no-handle/resume_needed reason), and when the reconnect drain drops N stale uplink frames. These make threshold tuning and reconnect diagnosis a log read, not a guess.
- **Explicit reconnect signal** — `is_reconnect` is passed explicitly rather than inferred from `handle == None` (which conflates a first open with an early reconnect), removing a latent ambiguity the prototype's `opened` flag papered over implicitly.

## Extension seams (out of scope now)

- **Silero/ML VAD backend** (the prototype's optional `VAD_BACKEND=silero`) — energy VAD suffices for gating a greeting; a learned VAD is a later swap behind the same `callee_active` signal.
- **Reducing Gemini's first-response latency** — that is the model/proxy path, not addressable here.
