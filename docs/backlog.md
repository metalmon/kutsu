# kutsu — backlog

Deferred work items, most recent first. Not a roadmap; a parking lot so nothing
is lost. Move an item into an SDD spec/plan when it's picked up.

## Network-quality gating — refinements

Done: `net_check::preflight` is now enforced in `engine::run_call` (real
`kutsu call` + MCP paths), fail-closed, gated by `net_check.enabled`; plus a
mid-call abort on sustained uplink RTP loss (>10% cumulative after ~5 s).

Done (2026-08-22): rolling-window uplink-loss gate (`window_loss_pct` over an
~8 s deque), tunable threshold (`NetCheckConfig.uplink_loss_abort_pct` /
`KUTSU_UPLINK_LOSS_ABORT_PCT`), and percentile jitter (p95−p50) in preflight.

Remaining (inherent, low priority):
- **Pre-answer callee/RTP leg probe** — preflight can only cover the Gemini leg;
  the callee's cellular leg cannot be probed before answer, only watched mid-call
  (which the rolling-window gate now does).

## Real-time scheduling — v2 (downlink pacer thread)

`realtime::promote_current_thread` currently raises only the `kutsu-sip` OS
thread (RTP send/recv) via `audio_thread_priority` (MMCSS on Windows, SCHED_FIFO
/ rtkit on Linux; best-effort, `KUTSU_RT_PRIORITY` toggle, default on). The
downlink pacer + uplink run as tokio tasks on the shared multi-thread runtime,
so they can't be individually prioritised. v2: move the pacer onto its own
dedicated OS thread and promote it too, or run a small dedicated current-thread
runtime for the audio path. Not urgent — RTP loss is ~0 today.

**Build note (Linux):** `audio_thread_priority`'s default `dbus` feature (rtkit,
lets Linux get RT priority without CAP_SYS_NICE) needs `libdbus-1-dev` at build
time. To drop that build dep, disable default features (falls back to
`sched_setscheduler`, which then needs `CAP_SYS_NICE`/root at runtime).

## Answering-machine / voicemail detection (AMD)

**Problem:** outbound calls that hit voicemail or a carrier auto-answer are
indistinguishable from a human at the SIP layer — both answer with `200 OK`
(confirmed live on Novofon: voicemail and human calls both reached `200 OK`,
`dead_air_ms=0`). The PSTN does not signal "this is voicemail" in SIP. Today the
agent greets and "talks to" the voicemail, wasting a call + a Gemini session.

**What SIP *can* tell us (already mapped in `sip/outcome.rs`):** non-answers —
`486` busy, `480` unavailable, `408` no-answer/timeout, `603` decline, `404`.

**Signals to evaluate (need data — see logging below):**
1. **SIP `183` Session Progress / early media** — carriers sometimes play
   "абонент недоступен / аппарат выключен" as early media before/without `200`.
   ezk's high-level `OutboundCall` hides 18x provisionals (`sip/mod.rs:82`), so
   this needs either ezk trace (`RUST_LOG=ezk_sip_ua=trace,ezk_sip_core=trace`)
   or surfacing provisionals from the vendored `ezk-sip-core`.
2. **Answer latency (INVITE→200)** — a near-instant `200` with no ringing hints
   auto-answer/voicemail. Cheap to log at the engine layer.
3. **Media-level AMD (the real discriminator)** — profile the callee's first
   utterance after answer: a human says a short "алло" (~0.3–0.8 s) then waits;
   a machine plays a long continuous greeting (2–5 s+) then a beep. Building
   blocks already exist: the energy VAD (`src/vad.rs`) + per-call uplink WAV
   dumps (`KUTSU_DUMP_UPLINK_DIR`) for offline profiling.

**Candidate approaches (all probabilistic, ~85–95% at best):**
- Media-AMD heuristic on first-utterance duration/silence/beep → disposition
  `voicemail` / hang up, built on the existing VAD.
- Lean on Gemini: prompt it to recognise a voicemail greeting and either leave a
  message or end with a `voicemail` disposition (reuses the existing bridge).
- Combine: cheap VAD pre-filter + Gemini confirmation.

**Status:** logging added to gather the first-utterance profile signal (see the
`amd_probe` log line in `gemini_live.rs`). Decide the approach once we have data
from real human-vs-voicemail calls.
