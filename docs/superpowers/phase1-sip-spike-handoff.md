# Phase 1 (SIP spike) — handoff ledger

Written 2026-08-14 to hand off to a fresh session. Read this + the memory notes
([[kutsu-current-state]], [[voice-cloud-reference]], [[kutsu-rmcp-tasks]]) before
starting.

## Where the project stands

- **Phase 2 (Gemini Live client) — DONE, live-validated, merged to master.**
  Direct BidiGenerateContent WS client, half/native, session resumption +
  reconnect, single dynamic `end_call`, fail-closed net preflight, `kutsu live`
  harness. Plus (also merged): HTTP-CONNECT proxy, binary WS frames, snake_case
  client keys (`realtime_input`/`tool_response`/`client_content`), rustls ring
  provider, hybrid greeting (`greet_after_silence_ms`). 27 unit tests green.
- **Local SIP test stand — DONE, live-validated** at `dev/sip-test/` (echo 600
  works end to end). See its README.
- `src/sip.rs` is still a **stub**. Phases 3 (audio bridge), 4 (engine+state),
  5 (MCP/Tasks) not started. No SIP trunk contract yet (real PSTN blocked).

## Phase 1 goal (from README Status)

SIP spike: outbound `INVITE` via `ezk-sip-ua` / `ezk-rtc`, and **confirm raw RTP
(G.711) flows both ways**. First milestone is exactly that — one outbound call
against a SIP endpoint with bidirectional raw RTP — before building anything on
top. This is a **spike** (feasibility answer + throwaway code), not the polished
`sip.rs` yet.

## Test environment (ready to use)

- Asterisk runs **natively in WSL2 Ubuntu** (NOT Docker — Docker Desktop on
  Windows can't route RTP; see dev/sip-test/README.md). It's on the host LAN IP
  **192.168.88.243:5060**, G.711 ulaw/alaw.
- Credentials — kutsu (caller): `kutsu` / `kutsupw`. Softphone (callee):
  `callee` / `calleepw`.
- Dialplan: **600 = echo** (call it, send RTP, get the same RTP echoed back —
  the ideal spike target), 601 = playback, 602 = 1 kHz tone, 6001 = ring the
  softphone.
- Control Asterisk in WSL: `sudo systemctl restart asterisk`;
  `sudo asterisk -rx "pjsip show endpoints"`; live SIP trace:
  `sudo asterisk -rx "pjsip set logger on"` then watch the console/journal.

## The spike, concretely

Make kutsu register as `kutsu`/`kutsupw` to `192.168.88.243:5060` (or INVITE
directly if simpler), dial **600**, stream a little G.711 RTP, and verify the
echoed RTP comes back (count frames both directions). Success = bidirectional
RTP confirmed. Then, and only then, design the real `sip.rs`.

## Key references

- Deps already present: `ezk-sip-ua = 0.9`, `ezk-rtc = 0.0.1`. Build needs the
  vendored-OpenSSL toolchain: `cargo build --features vendor-openssl`
  (NASM + LLVM/libclang + Strawberry Perl; `LIBCLANG_PATH` set — all persisted
  to the user env on this machine; details in [[kutsu-rmcp-tasks]]).
- Study `ezk-sip-ua` / `ezk-rtc` docs + examples (docs.rs, the crates' repo) for
  the outbound INVITE + RTP-session API — **the core unknown of this spike**.
- Phone-side audio is **G.711 8 kHz mu-law/PCM**; the bridge (phase 3) will
  convert to Gemini's 16k/24k PCM16. Params/quirks in [[voice-cloud-reference]].

## Recommended process (superpowers)

1. `superpowers:brainstorming` — classify as a **spike**; agree the probe scope
   with the user (2-3 sentences), then investigate cheaply.
2. Resolve the open questions below with the user.
3. Write throwaway spike code (e.g. a `kutsu sip-spike` subcommand or a
   `#[ignore]` integration test) that INVITEs 600 and logs sent/received RTP
   frame counts against the WSL Asterisk.
4. Report: does bidirectional RTP flow? Recommendation.
5. If yes → design real `sip.rs` (architectural: spec → writing-plans → SDD).

## Open questions for brainstorming

- **Register-then-INVITE vs direct INVITE with credentials?** (Real trunks vary;
  Asterisk here accepts a registered `kutsu` endpoint.)
- **Where does the spike process run — Windows host or inside WSL?** Media
  reachability: kutsu must advertise an address Asterisk can send RTP to.
  Running the spike **inside WSL Ubuntu** (same host as Asterisk, 192.168.88.243)
  is likely simplest and avoids the Windows/WSL media-address dance we hit with
  Docker. Decide early.
- **How does `ezk-rtc` expose raw RTP frames** (send/recv API, codec setup for
  G.711)? This is the main thing to learn from its examples.
- **mu-law vs a-law** default; jitter/timing of outbound frames.

## Gotchas already learned (don't repeat)

- Docker Desktop on Windows: SIP registers but **RTP is silent** (container IP in
  SDP). Use WSL-native Asterisk. (Full detail: dev/sip-test/README.md.)
- Desktop softphones squat local UDP 5060 → if kutsu/Asterisk also use 5060 on
  the same host, expect bind conflicts. Keep local ports distinct.
