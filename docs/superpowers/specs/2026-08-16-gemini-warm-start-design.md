# Gemini warm-start: connect during ring, greet only after answer — design

**Status:** approved design, pre-implementation
**Date:** 2026-08-16
**Scope:** `src/engine.rs` (`run_call`: start Gemini during the ring window, feed silence, fire an `answered` signal on `Answered`), `src/gemini_live.rs` (`start`/`run_session` gain an `answered` gate so the greeting timer arms only after answer), `src/config.rs` (`DEFAULT_GREET_AFTER_SILENCE_MS` 1500 → 400).

## Problem (measured on a live call)

The agent leaves dead air at the start of an outbound call. `run_call` connects Gemini **after** `Answered` (`engine.rs:588`, log `gemini connected after answer`), so the WSS handshake + `setup` roundtrip + the first model response all happen while the callee is already holding a silent line. A live call logged `first transcript after answer = 6755 ms`; the greeting timer (`greet_after_silence_ms`, default 1500 ms) plus Gemini connect/latency stack up into a multi-second silence the callee fills with "алло, алло".

The connect + setup latency is **not** inherent: it can be hidden behind the ring. When the callee picks up, Gemini should already be connected and ready. The one hard constraint: **Gemini must not emit any greeting audio during ring** — only after the callee has answered.

## Decisions

- **Warm-start (option A), not pre-generate (option B).** Connect + `setup` during ring so the socket is warm on answer. The greeting is still generated after answer (its ~2 s generation latency is left for a later change if needed). The natural turn-taking ("who speaks first") is left to the model + the callee.
- **Silence keepalive is required.** Gemini closes an idle post-`setup` WSS; the engine must feed silence into the session during ring to keep it alive (confirmed behaviour, not speculative).
- **Greeting gated on answer.** The auto-greet timer inside `run_session` must not fire during ring. `start`/`run_session` take an `answered` signal; the greet timer arms only after it fires. During ring the model stays silent (no `GREET_CUE` sent, timer not armed).
- **Waste on no-answer is accepted.** If the callee never answers, the warm Gemini session is torn down (`hangup` + `join`). The user accepted this cost for option A.
- **Warm-connect resilience is internal to the session.** `start` is infallible before its internal spawn — all connect fallibility lives in the session's spawned reconnect loop, not in a `run_call`-level fallback. A connect failure during ring is retried inside the session, not by re-calling `start` after answer.
- **Default greet delay 1500 → 400 ms.** After answer, if the callee is silent, nudge Gemini to greet after a short 400 ms beat (user-chosen), not 1.5 s.

## Component 1 — `run_call` ring-window sequencing (`engine.rs`)

Today (`engine.rs:545-617`): `split` → await `Answered` (silent loop) → `gemini_live::start` → build `BridgePorts` → spawn bridge.

New order:
- After `split`, immediately `gemini_live::start(&server, &scenario, answered)` (see Component 2 for `answered`). Hold the returned `Session` (warm; WSS + setup in flight).
- Await `Answered` in a `select!` that ALSO ticks every 20 ms, pushing a 20 ms silence frame (`vec![0i16; 320]`, 16 kHz) into `session.audio_in` to keep the WSS alive. The existing `Terminated`/`None` arms are unchanged (finalize per the current outcome logic).
  - `start` is infallible before its internal spawn, so this arm is a defensive guard, not the primary mechanism; connect resilience lives in the session's internal reconnect loop.
- On `Answered`: stop the silence ticks, fire the `answered` signal, record `answered_at`, set `InProgress`, and build `BridgePorts` with the warm session's `audio_in`/`events` (exactly as today, but the session already exists — no `start().await` on the answer path). The bridge takes over feeding `audio_in` from the phone uplink; the engine's silence feed has stopped, so there is a single writer again.
- Teardown: on `Terminated`/no-answer during the ring wait, `session.hangup().await` + drop/join so the warm session is closed cleanly. (Reuse the existing `Session::hangup`/`join`.)

The `dead_air_ms = gemini_connected_at - answered_at` log becomes ≈0 (session already connected); keep or adjust the log to reflect warm-start.

## Component 2 — `answered` gate for the greeting (`gemini_live.rs`)

`start(server, scenario)` → `start(server, scenario, answered)`. `answered` is a shared, level-triggered signal safe against firing before the loop waits on it — an `Arc<tokio::sync::Notify>` paired with an `Arc<AtomicBool>` (check the bool, else await the notify), or an equivalent (`tokio::sync::watch<bool>`); the implementer picks one and documents why it is not edge-loss-prone.

In `run_session` (`gemini_live.rs:150-185`), the greeting currently arms `greet_at = now + greet_after_silence_ms` at session start. Change it so the greet timer is armed **only after `answered` is observed**: until then, the greet `select!` arm is disabled; once answered, compute `greet_at = answered_instant + greet_after_silence_ms` and proceed exactly as today (`!greeted && !had_activity` → send `GREET_CUE`). `greet_after_silence_ms == 0` (reactive-only) and resume sessions keep their current "never proactively greet" behaviour.

During ring, `run_session` still services `audio_in` (the engine's silence) and `transport.recv` (drains any server frames, though with no cue the model stays quiet). Only the greeting arm is gated. This guarantees no greeting audio during ring.

Note: `start` spawns a reconnect loop that may call `run_session` more than once. The `answered` gate applies to the first fresh session's greeting; on reconnect the session is already past answer, so the gate is already open (pass the same `answered` handle, already fired).

## Component 3 — default greet delay (`config.rs`)

`DEFAULT_GREET_AFTER_SILENCE_MS`: `1500` → `400`. Flows to `configs_from_env` and `run_live` (both read the const). No other change; the `--greet-after-silence-ms` flag and `KUTSU_*` overrides still win.

## Error handling & edge cases

- `start` is infallible before its internal spawn, so a connect failure during ring does not surface at the `run_call` level; the session's internal reconnect loop is the resilience mechanism, not a `run_call`-level fallback re-`start()` after answer.
- Callee never answers (`Terminated`/timeout during ring) → `hangup` + join the warm session; outcome unchanged (NoAnswer/Busy/etc. per the existing classification).
- Callee answers while the WSS is still mid-`setup` (very fast pickup) → the `answered` signal fires; the greet timer waits for the session to be usable; audio flows once `setup` completes. No greeting is lost (it is gated after answer, not after setup — if setup lags, the greet timer starts on answer but the first cue only sends once the loop is live).
- The silence feed must stop the instant the bridge starts, so `session.audio_in` has a single writer during the call (no interleaving of engine-silence and phone audio). Structurally: the engine's silence ticks live only inside the ring-wait `select!`, which is exited before the bridge is built.

## Testing

- `run_session` greeting gate (FakeTransport, existing test scaffolding): with `answered` un-fired, no `GREET_CUE` is sent even past `greet_after_silence_ms`; after `answered` fires, `GREET_CUE` is sent after the delay when the callee stays silent; if the model produces output first (`had_activity`), no greeting is sent. These lock in "no greeting during ring".
- Silence-during-ring: the engine pushes silence frames into `session.audio_in` while awaiting answer (unit-level: a fake SipEvent stream that delays `Answered` and asserts silence frames were sent to the session before answer). Where a full engine test is impractical, factor the ring-wait into a testable unit and assert the silence-then-answer ordering.
- Default: a config test asserts `DEFAULT_GREET_AFTER_SILENCE_MS == 400`.

## Extension seams (out of scope now)

- **Option B (pre-generate greeting during ring + buffer + flush on answer)** — removes the remaining ~2 s generation latency; deliberately deferred.
- **Audio-onset metric (answered → first model `OutputAudio`)** — would measure the true dead-air; the current `first transcript` proxy lags. Cheap follow-up, not in this spec.
- **Uplink RMS/level in the quality log** — separate small feature, tracked apart from warm-start.
