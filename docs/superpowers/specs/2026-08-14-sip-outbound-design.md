# Design: `src/sip.rs` — outbound SIP/RTP transport

Status: approved for implementation planning
Date: 2026-08-14
Author: handoff from phase-1 SIP spike (`tests/sip_spike.rs`, live-validated)

## 1. Purpose & boundaries

`sip` is kutsu's **SIP signaling + RTP media transport** for outbound calls. It
places one or more concurrent outbound calls over a SIP trunk and exposes, per
call, a bidirectional stream of **raw G.711 RTP payloads** plus call-lifecycle
events, while fully containing the `ezk` stack (which is `!Send`) behind
`Send`-only handles.

It is *only* transport. Explicit non-responsibilities:

- **Codec conversion** (G.711 µ-law/a-law ↔ PCM16, resampling) — owned by
  [`crate::bridge`]. `sip` hands over/accepts raw G.711 payloads unchanged.
- **Call orchestration** (dial → bridge → hang up → finalize) — owned by
  [`crate::engine`].
- **Call state / transcripts** — owned by [`crate::state`].

This boundary is fixed by the existing module docs (`bridge.rs` already commits
to owning µ-law + resampling).

### Feasibility basis

Phase-1 spike (`tests/sip_spike.rs`) proved the full path against the local WSL
Asterisk stand: register-free digest INVITE to echo ext 600, plain RTP/AVP,
codec negotiated PCMU, **150 RTP frames sent / 149 echoed back over 3 s**. This
design productionizes that spike. The spike file is deleted once `sip.rs` lands.

## 2. Scope (this iteration)

In scope — exactly what the stand proves + universal structure:

- Shared `ezk` `Endpoint` with **one UDP transport**.
- `place_call(number)`: **register-free** direct INVITE with **digest auth**
  (Asterisk/trunk identifies the endpoint by the From-user and challenges the
  INVITE — no REGISTER needed for origination).
- Media: **plain RTP/AVP, G.711** (`PCMU`/`PCMA`), 20 ms ptime.
- Per-call bidirectional raw-payload channels + lifecycle events + hangup (BYE),
  and detection of remote BYE.
- Concurrent calls up to `ServerConfig::max_concurrent_channels` (default 3).

Out of scope this iteration (designed as seams, not implemented):

- **REGISTER** binding (some trunks require it — add when the real trunk does).
- **TLS signaling / SRTP media**.
- **IP-based auth** (no digest).
- **Outbound DTMF** (RFC 2833 telephone-event) — for IVR navigation later.
- Inbound calls (kutsu is outbound-only).

Each seam is a documented extension point (§8), not a stub that pretends to work.

## 3. Configuration

New `SipConfig` in `src/config.rs`, referenced from `ServerConfig`.

```rust
pub struct SipConfig {
    /// SIP server / trunk, "host:port". Spike default: 192.168.88.243:5060.
    pub server: String,
    /// Digest username (also the default From-user / caller identity).
    pub username: String,
    /// Digest password.
    pub password: String,
    /// Caller identity user-part in the From header. Default = `username`.
    pub from_user: Option<String>,
    /// Local IP to bind the UDP transport + advertise in SDP. Default: the
    /// route toward `server`, auto-detected (UdpSocket connect trick).
    pub local_ip: Option<IpAddr>,

    // --- extension seams, unused this iteration (documented, not wired) ---
    /// Send a REGISTER binding before calling. Not yet implemented.
    #[serde(default)] pub register: bool,
    /// Signaling/media transport. Only `Udp` implemented. Not yet wired.
    #[serde(default)] pub transport: SipTransportKind, // Udp (default) | Tls
}
```

Values come from the server config file (loading mechanism follows the existing
`ServerConfig` pattern). Secrets: `password` is read from config like the
existing proxy `password: Option<String>`; no new secret-handling machinery.

## 4. Public interface (the `engine`/`bridge` contract)

```rust
/// Process-wide SIP transport. Cheap to clone (Arc). Owns the ezk Endpoint,
/// which lives on the internal SIP runtime thread (§5).
#[derive(Clone)]
pub struct SipTransport { /* Arc<Shared> */ }

impl SipTransport {
    /// Start the SIP runtime thread, bind the UDP transport, build the endpoint.
    /// Call once at startup.
    pub async fn new(cfg: &SipConfig) -> Result<Self, SipError>;

    /// Place an outbound call to `number` on the trunk. Resolves once the INVITE
    /// is accepted into a dialog (ringing or answered) and the per-call worker
    /// is spawned; errors on immediate reject / send failure. Answer, ringing,
    /// and teardown then arrive as `SipEvent`s on `SipCall::events`. The returned
    /// handle is Send and drives no ezk types.
    pub async fn place_call(&self, number: &str) -> Result<SipCall, SipError>;

    /// Number of currently active calls (for `engine` to gate the concurrency
    /// cap; `sip` does not enforce it — §9).
    pub fn active_calls(&self) -> usize;

    /// Graceful shutdown: terminate active calls, stop the runtime thread.
    pub async fn shutdown(self);
}

/// One live outbound call. All-Send. The ezk `Call` loop runs on the SIP thread.
pub struct SipCall {
    pub call_id: String,
    events:  mpsc::Receiver<SipEvent>,
    rtp_in:  mpsc::Receiver<Bytes>,   // inbound G.711 payloads (one RTP payload each)
    rtp_out: mpsc::Sender<Bytes>,     // outbound G.711 payloads (one per 20 ms)
    hangup:  oneshot::Sender<()>,     // drop or send -> BYE
}

impl SipCall {
    pub fn events(&mut self)   -> &mut mpsc::Receiver<SipEvent>; // lifecycle
    pub fn audio_in(&mut self) -> &mut mpsc::Receiver<Bytes>;    // remote -> us
    pub fn audio_out(&self)    -> mpsc::Sender<Bytes>;           // us -> remote (clonable)
    pub async fn hangup(self);                                   // send BYE, await termination
}

pub enum SipEvent {
    Ringing,                              // 0+ provisional responses (180/183)
    Answered { codec: NegotiatedCodec },  // 200 OK + ACK; codec negotiated at answer
    Terminated(TermReason),               // remote BYE / our BYE / media/transport failure
}

pub enum TermReason { RemoteHangup, LocalHangup, Failed(String) }

pub struct NegotiatedCodec { pub pt: u8, pub kind: G711Kind, pub ptime_ms: u32 }
pub enum G711Kind { Ulaw /* pt 0 */, Alaw /* pt 8 */ }
```

Notes:
- `place_call` resolves as soon as the INVITE is accepted into a dialog and the
  worker is spawned; all of `Ringing` / `Answered { codec }` / `Terminated` then
  flow through `events`. Rationale: `state` tracks a `ringing → in_progress`
  lifecycle, and the codec is only known at answer, so answer must be an event
  carrying the codec — not a construction-time field. The engine's flow: `let
  call = place_call().await?;` then consume `events` until `Answered` (start
  bridging, using `codec` to pick µ-law/a-law) or `Terminated`.
- Backpressure: `rtp_in`/`rtp_out` are bounded mpsc (e.g. cap 64 ≈ 1.3 s of
  20 ms frames). If `bridge` stalls, inbound frames drop (logged) rather than
  unbounded-buffer; outbound `send` awaits, naturally pacing the bridge.

## 5. Internal architecture — the SIP runtime thread

The crux of approach **A**: contain everything `!Send`.

- `SipTransport::new` spawns **one dedicated OS thread** running a
  `tokio::runtime::Builder::new_current_thread()` runtime with a `LocalSet`.
- On that thread: build the ezk `Endpoint` (UDP transport, `DialogLayer`,
  `add_allow(INVITE/ACK/CANCEL/BYE/OPTIONS)` — **mandatory**, else ezk panics
  "tried to use empty vector" serializing an empty Allow header), and run a
  **command loop** receiving `Cmd::PlaceCall { number, reply }` over a `Send`
  mpsc from `SipTransport`.
- Each `PlaceCall` is handled by `spawn_local(run_call(...))`. `run_call` owns
  the `!Send` `OutboundCall`/`Call`/`RtpSender`/`RtpReceiver` and never leaves
  the thread. Once the INVITE is accepted into a dialog (ezk `OutboundCall::make`
  returns), it replies to `place_call`'s awaiting caller (via a `Send` oneshot)
  with the assembled `SipCall` handle, then emits `Ringing`/`Answered`/
  `Terminated` over that call's `events` channel as the dialog progresses.
- The whole ezk `Endpoint` internal machinery (its own `tokio::spawn`ed UDP
  receive tasks — those are `Send`) lives on this runtime too.

Files:
- `src/sip/mod.rs` — public types (`SipTransport`, `SipCall`, `SipEvent`,
  `NegotiatedCodec`, `SipError`, config glue) + the thread/command plumbing.
- `src/sip/call.rs` — `run_call`: the per-call ezk driving loop (productionized
  from the spike), and the `RtpPacket ↔ Bytes` + lifecycle translation.

Keep to these two files; if `run_call` grows, split SDP setup into a helper.

## 6. Per-call data flow (`run_call`)

Setup (from spike, generalized):
1. Build `SdpSession` (`OpenSslContext`, `local_ip`, `SdpSessionConfig{
   offer_transport: Rtp, offer_ice: false, offer_avpf: false }`),
   `add_local_media(Audio, PCMU + PCMA, SendRecv)`, `add_media(SendRecv)`,
   `RtcMediaBackend::new`.
2. `OutboundCall::make(endpoint, DigestAuthenticator, id=From(sip:from_user@server),
   contact=sip:from_user@<bound-addr>, target=sip:<number>@server, media)`.
   On success the dialog exists → reply the `SipCall` handle to `place_call` and
   create the `events`/`rtp_in`/`rtp_out` channels. If `make` returned an early
   (ringing) dialog, emit `Ringing`. Then `wait_for_completion()` → `finish()`
   → `Call`.
3. From the first `MediaEvent::SenderAdded/ReceiverAdded`, capture `RtpSender`,
   `RtpReceiver`, and `NegotiatedCodec` (pt 0→Ulaw, 8→Alaw); emit
   `Answered { codec }`.

Run loop (single task, `select!`):
- `call.run()` → drive: `Internal(e)` → `handle_internal_event`; `Media(_)` →
  ignore (already captured); `Terminated` → emit `Terminated(RemoteHangup)`, stop.
- `rtp_out.recv()` → `RtpSender::send(SendRtpPacket::new(now, pt, payload))`.
  (Sender waits for transport-Connected; `call.run()` on the same task supplies
  that state — same interleaving the spike relies on. To avoid the send-blocks-
  the-loop hazard the spike sidestepped with a second task, `run_call` polls the
  sender inside the same `select!` so `call.run()` keeps advancing.)
- `RtpReceiver::recv()` (via the backend) → push payload `Bytes` into `rtp_in`
  (drop-oldest / drop-on-full with a rate-limited warning).
- `hangup` oneshot fired → `call.terminate()` → emit `Terminated(LocalHangup)`.

Frame shape: one RTP payload = one `Bytes` (≈160 B for 8 kHz/20 ms µ-law). No
reframing in `sip`; `bridge` handles sample math.

## 7. Error handling

New `SipError` (thiserror) in `sip/mod.rs`:

```rust
pub enum SipError {
    Bind(io::Error),            // UDP bind / endpoint build
    Invite(StatusLine),         // non-2xx final response
    NotAnswered,                // timeout / cancelled before answer
    NoMediaInAnswer,            // 2xx without usable SDP
    Media(String),              // ezk media backend error
    RuntimeGone,                // SIP thread died / channel closed
}
```

Wire into the crate `Error` enum: `#[error("sip error: {0}")] Sip(#[from] SipError)`.
`place_call` maps ezk `MakeCallError`/`MakeCallCompletionError` onto these.
`Terminated(Failed(_))` carries transport/media failures surfaced mid-call.

## 8. Extension seams (documented, unimplemented)

- **REGISTER**: `SipConfig::register`; a `register()` step before the command
  loop starts accepting calls (ezk `Registration::register`). Not wired.
- **TLS/SRTP**: `SipConfig::transport`; `EndpointBuilder::listen_rustls` +
  `SdpSessionConfig::offer_transport = SdesSrtp/DtlsSrtp`. Not wired.
- **DTMF**: a `SipCall::send_dtmf(&str)` method + telephone-event negotiation.
  Not present this iteration.

## 9. Concurrency & lifecycle

- One SIP thread serves all calls (G.711 at 8 kHz is trivial CPU;
  cooperative scheduling is ample for `max_concurrent_channels` = 3).
- `sip` does **not** enforce the concurrency cap (existing note: the cap is
  "enforced in phase 4/5" by `engine`). `sip` exposes an accurate active-call
  count (`SipTransport::active_calls() -> usize`) for `engine` to gate on.
- `shutdown` stops the runtime thread and joins it. In-flight calls are dropped
  (ezk `Call` teardown attempts a best-effort BYE, not guaranteed on shutdown);
  the engine should `hangup()` active calls first. Graceful shutdown-time BYE
  draining is a future seam.

## 10. Testing

Unit (pure, no network):
- `number → SipUri` / From / Contact construction.
- Codec mapping: pt 0 → `Ulaw`, pt 8 → `Alaw`; `NegotiatedCodec` fields.
- `SipConfig` parse + `from_user` default = `username`; `local_ip` auto-detect.
- `SdpSessionConfig` built for plain-RTP G.711 (assert `offer_ice == false`,
  `offer_transport == Rtp`).

Integration (`#[ignore]`, live stand — replaces the spike):
- `tests/sip_outbound.rs`: `SipTransport::new` → `place_call("600")` → assert
  `Answered`, drive audio: send N µ-law frames on `audio_out`, assert frames
  arrive on `audio_in` (echo), then `hangup()` and assert `Terminated`.
- Same env knobs as the spike (`KUTSU_SIP_*`), same run recipe
  (`cargo test --features vendor-openssl --test sip_outbound -- --ignored`).

The `Send` channel seam means later phases can test `engine`/`bridge` against a
fake `SipCall` (channels) with no ezk.

## 11. Dependency & file changes

- Promote from `[dev-dependencies]` to `[dependencies]`: `bytes`, `bytesstr`,
  `ezk-sip-core`, `ezk-sip-auth`, `ezk-sip-types`. (`ezk-sip-ua`, `ezk-rtc`
  already deps.) Build still requires `--features vendor-openssl`.
- Add `SipConfig` + `SipTransportKind` to `src/config.rs`.
- `src/sip.rs` → `src/sip/{mod.rs, call.rs}`.
- Add `SipError` and `Error::Sip`.
- **Delete** `tests/sip_spike.rs`; add `tests/sip_outbound.rs`.

## 12. Out of scope / explicitly deferred

Inbound calls; REGISTER; TLS/SRTP; IP auth; DTMF; codec transcoding inside
`sip`; call-state persistence; the `engine`/`bridge` wiring itself (later
phases). This spec delivers the transport seam they will consume.
