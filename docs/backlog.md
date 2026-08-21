# kutsu — backlog

Deferred work items, most recent first. Not a roadmap; a parking lot so nothing
is lost. Move an item into an SDD spec/plan when it's picked up.

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
