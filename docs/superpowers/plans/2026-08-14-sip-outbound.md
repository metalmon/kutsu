# SIP Outbound Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `src/sip` — kutsu's outbound SIP/RTP transport: place register-free digest INVITE calls over a SIP trunk and expose per-call raw G.711 payload streams + lifecycle events over `Send` channels.

**Architecture:** One dedicated OS thread runs a `current_thread` tokio runtime + `LocalSet` that hosts the ezk `Endpoint` and every per-call driving loop (ezk `Call`/`SdpSession` are `!Send`). The rest of kutsu talks to it only through `Send` mpsc/oneshot channels, so `bridge`/`engine` never see ezk types. Media is plain RTP/AVP G.711; codec conversion lives in `bridge`.

**Tech Stack:** Rust 2024, tokio, `ezk-sip-ua` 0.9 + `ezk-rtc` 0.0.1 (+ `ezk-sip-core`/`-auth`/`-types`), `bytes`/`bytesstr`. OpenSSL via `--features vendor-openssl`.

**Spec:** `docs/superpowers/specs/2026-08-14-sip-outbound-design.md`

**Provenance:** The ezk call-setup/driving sequence is lifted verbatim from the live-validated phase-1 spike `tests/sip_spike.rs` (call answered, 150 RTP frames sent / 149 echoed). That file is deleted in Task 6.

## Global Constraints

- **All builds/tests require `--features vendor-openssl`** (ezk-rtc → ezk-srtp → openssl-sys). Any `cargo build`/`cargo test` command below implicitly includes this flag. Toolchain (NASM/LLVM/Perl/`LIBCLANG_PATH`) is already persisted on this machine.
- **Register-free outbound only** this iteration: UDP transport, digest auth, direct INVITE, plain RTP/AVP, G.711 (PCMU/PCMA), 20 ms ptime.
- **`sip` is transport only** — no codec conversion, no orchestration, no call-state persistence. Raw G.711 payloads cross the boundary as `bytes::Bytes`.
- **English only** in all code/comments/docs/logs (repo rule).
- **`sip` exposes but does not enforce** the concurrency cap (`active_calls()`); enforcement is `engine`'s job in a later phase.
- Extension seams (REGISTER, TLS/SRTP, IP-auth, DTMF) are documented in config/types but **not implemented**.

## File Structure

- `Cargo.toml` — promote `bytes`, `bytesstr`, `ezk-sip-core`, `ezk-sip-auth`, `ezk-sip-types` from `[dev-dependencies]` to `[dependencies]`.
- `src/config.rs` — add `SipConfig` + `SipTransportKind`.
- `src/error.rs` — add `Error::Sip` (wraps `sip::SipError`).
- `src/sip.rs` → **split** into:
  - `src/sip/mod.rs` — public types (`SipTransport`, `SipCall`, `SipEvent`, `TermReason`, `NegotiatedCodec`, `G711Kind`, `SipError`), the runtime-thread plumbing, and pure helpers.
  - `src/sip/call.rs` — `run_call` (the per-call ezk driving loop) + media/URI/auth builders.
- `tests/sip_spike.rs` — **deleted** (replaced).
- `tests/sip_outbound.rs` — new `#[ignore]` live integration test.
- `src/lib.rs` — unchanged (`pub mod sip;` resolves to `sip/mod.rs`).

---

## Task 1: Dependencies + `SipConfig`

**Files:**
- Modify: `Cargo.toml` (`[dependencies]` / `[dev-dependencies]`)
- Modify: `src/config.rs`
- Test: `src/config.rs` (inline `#[cfg(test)]`)

**Interfaces:**
- Produces: `config::SipConfig { server: String, username: String, password: String, from_user: Option<String>, local_ip: Option<std::net::IpAddr>, register: bool, transport: SipTransportKind }`, method `SipConfig::from_user(&self) -> &str`; `config::SipTransportKind { Udp, Tls }` (`Default = Udp`).

- [ ] **Step 1: Promote deps in `Cargo.toml`.** Move these five lines out of `[dev-dependencies]` and into `[dependencies]` (keep `tempfile` in dev):

```toml
bytes = "1"
bytesstr = "1"
ezk-sip-core = "0.9"
ezk-sip-auth = "0.5"
ezk-sip-types = "0.6"
```

After the edit `[dev-dependencies]` contains only `tempfile = "3"`.

- [ ] **Step 2: Write the failing config test.** Append to the `#[cfg(test)] mod tests` in `src/config.rs`:

```rust
#[test]
fn sip_config_parses_and_from_user_defaults_to_username() {
    let json = r#"{"server":"192.168.88.243:5060","username":"kutsu","password":"kutsupw"}"#;
    let c: SipConfig = serde_json::from_str(json).unwrap();
    assert_eq!(c.server, "192.168.88.243:5060");
    assert_eq!(c.from_user(), "kutsu");
    assert_eq!(c.transport, SipTransportKind::Udp);
    assert!(!c.register);
    assert!(c.local_ip.is_none());
}

#[test]
fn sip_config_from_user_override() {
    let json = r#"{"server":"s:5060","username":"u","password":"p","from_user":"caller"}"#;
    let c: SipConfig = serde_json::from_str(json).unwrap();
    assert_eq!(c.from_user(), "caller");
}
```

- [ ] **Step 3: Run test to verify it fails.**

Run: `cargo test --features vendor-openssl --lib config::tests::sip_config`
Expected: FAIL — `cannot find type SipConfig`.

- [ ] **Step 4: Implement the config types.** Add near the top of `src/config.rs` (add `use std::net::IpAddr;` to the imports):

```rust
/// Signaling/media transport for the SIP trunk. Only `Udp` is implemented this
/// iteration; `Tls` is a documented extension seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SipTransportKind {
    #[default]
    Udp,
    Tls,
}

/// Outbound SIP trunk configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SipConfig {
    /// SIP server / trunk as `host:port`.
    pub server: String,
    /// Digest username (also the default caller identity).
    pub username: String,
    /// Digest password.
    pub password: String,
    /// From-header user-part. Defaults to `username` when absent.
    #[serde(default)]
    pub from_user: Option<String>,
    /// Local IP to bind + advertise in SDP. Auto-detected (route toward
    /// `server`) when absent.
    #[serde(default)]
    pub local_ip: Option<IpAddr>,

    // --- extension seams; parsed but not yet wired ---
    /// Send a REGISTER binding before calling. Not yet implemented.
    #[serde(default)]
    pub register: bool,
    /// Transport kind. Only `Udp` implemented. Not yet wired.
    #[serde(default)]
    pub transport: SipTransportKind,
}

impl SipConfig {
    /// Caller identity user-part: explicit `from_user`, else the digest username.
    pub fn from_user(&self) -> &str {
        self.from_user.as_deref().unwrap_or(&self.username)
    }
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `cargo test --features vendor-openssl --lib config::tests::sip_config`
Expected: PASS (both tests).

- [ ] **Step 6: Commit.**

```bash
git add Cargo.toml Cargo.lock src/config.rs
git commit -m "feat(sip): add SipConfig + promote ezk crates to real deps"
```

---

## Task 2: `SipError` + crate `Error` wiring

**Files:**
- Modify: `src/sip.rs` (still the single-file module at this point)
- Modify: `src/error.rs`
- Test: `src/sip.rs` (inline)

**Interfaces:**
- Produces: `sip::SipError` (thiserror) with variants `Config(&'static str)`, `Bind(#[from] std::io::Error)`, `Invite(String)`, `NotAnswered`, `Media(String)`, `RuntimeGone`. And `error::Error::Sip(#[from] sip::SipError)`.

- [ ] **Step 1: Write the failing test.** Replace the doc-only body of `src/sip.rs` with the doc comment plus:

```rust
/// Errors from the SIP transport layer.
#[derive(Debug, thiserror::Error)]
pub enum SipError {
    #[error("sip config: {0}")]
    Config(&'static str),
    #[error("sip bind/io: {0}")]
    Bind(#[from] std::io::Error),
    #[error("invite rejected: {0}")]
    Invite(String),
    #[error("call not answered")]
    NotAnswered,
    #[error("media error: {0}")]
    Media(String),
    #[error("sip runtime unavailable")]
    RuntimeGone,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sip_error_display_and_crate_error_from() {
        let e = SipError::Invite("486 Busy Here".into());
        assert_eq!(e.to_string(), "invite rejected: 486 Busy Here");
        let ce: crate::error::Error = SipError::NotAnswered.into();
        assert_eq!(ce.to_string(), "sip error: call not answered");
    }
}
```

- [ ] **Step 2: Run test to verify it fails.**

Run: `cargo test --features vendor-openssl --lib sip::tests::sip_error`
Expected: FAIL — `Error` has no `Sip` variant / no `From<SipError>`.

- [ ] **Step 3: Wire into the crate error enum.** In `src/error.rs`, add this variant to `pub enum Error`:

```rust
    #[error("sip error: {0}")]
    Sip(#[from] crate::sip::SipError),
```

- [ ] **Step 4: Run test to verify it passes.**

Run: `cargo test --features vendor-openssl --lib sip::tests::sip_error`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add src/sip.rs src/error.rs
git commit -m "feat(sip): add SipError and wire into crate Error"
```

---

## Task 3: Split module + public types + pure helpers

**Files:**
- Move: `src/sip.rs` → `src/sip/mod.rs`
- Create: `src/sip/call.rs`
- Test: `src/sip/mod.rs` (inline)

**Interfaces:**
- Consumes: `config::SipConfig` (Task 1), `SipError` (Task 2).
- Produces:
  - `sip::G711Kind { Ulaw, Alaw }`, `sip::NegotiatedCodec { pt: u8, kind: G711Kind, ptime_ms: u32 }`.
  - `sip::SipEvent { Ringing, Answered { codec: NegotiatedCodec }, Terminated(TermReason) }`, `sip::TermReason { RemoteHangup, LocalHangup, Failed(String) }`.
  - pure fns: `g711_kind_from_pt(u8) -> Option<G711Kind>`, `detect_local_ip(SocketAddr) -> std::io::Result<IpAddr>`, `plain_rtp_g711_config() -> ezk_rtc::sdp::SdpSessionConfig`.

- [ ] **Step 1: Convert the module to a directory.**

```bash
mkdir src/sip
git mv src/sip.rs src/sip/mod.rs
```

Create an empty `src/sip/call.rs` with just a doc line and declare it from `mod.rs` (add `mod call;` near the top of `src/sip/mod.rs`):

```rust
//! Per-call ezk driving loop (runs on the SIP runtime thread).
```

- [ ] **Step 2: Write the failing tests.** Append to the `#[cfg(test)] mod tests` in `src/sip/mod.rs`:

```rust
#[test]
fn g711_pt_maps_to_kind() {
    assert!(matches!(g711_kind_from_pt(0), Some(G711Kind::Ulaw)));
    assert!(matches!(g711_kind_from_pt(8), Some(G711Kind::Alaw)));
    assert!(g711_kind_from_pt(9).is_none());
}

#[test]
fn plain_rtp_config_is_g711_no_ice_no_srtp() {
    use ezk_rtc::sdp::TransportType;
    let c = plain_rtp_g711_config();
    assert_eq!(c.offer_transport, TransportType::Rtp);
    assert!(!c.offer_ice);
    assert!(!c.offer_avpf);
}

#[test]
fn detect_local_ip_returns_routable_addr() {
    // Loopback target -> loopback source; proves the connect() trick works.
    let ip = detect_local_ip("127.0.0.1:5060".parse().unwrap()).unwrap();
    assert!(ip.is_loopback());
}
```

- [ ] **Step 3: Run tests to verify they fail.**

Run: `cargo test --features vendor-openssl --lib sip::tests`
Expected: FAIL — helpers/types not defined.

- [ ] **Step 4: Implement types + helpers.** Add to `src/sip/mod.rs` (imports: `use std::net::{IpAddr, SocketAddr, Ipv4Addr};`):

```rust
/// Negotiated G.711 flavour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum G711Kind {
    /// PCMU, payload type 0.
    Ulaw,
    /// PCMA, payload type 8.
    Alaw,
}

/// Codec negotiated for a call (what `bridge` needs to decode/encode).
#[derive(Debug, Clone, Copy)]
pub struct NegotiatedCodec {
    pub pt: u8,
    pub kind: G711Kind,
    pub ptime_ms: u32,
}

/// Why a call ended.
#[derive(Debug, Clone)]
pub enum TermReason {
    RemoteHangup,
    LocalHangup,
    Failed(String),
}

/// Lifecycle event for a live call.
#[derive(Debug, Clone)]
pub enum SipEvent {
    Ringing,
    Answered { codec: NegotiatedCodec },
    Terminated(TermReason),
}

/// Map an RTP payload type to a G.711 flavour (0=PCMU, 8=PCMA).
pub(crate) fn g711_kind_from_pt(pt: u8) -> Option<G711Kind> {
    match pt {
        0 => Some(G711Kind::Ulaw),
        8 => Some(G711Kind::Alaw),
        _ => None,
    }
}

/// Detect the local IP that routes toward `server` (no packets sent).
pub(crate) fn detect_local_ip(server: SocketAddr) -> std::io::Result<IpAddr> {
    let sock = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    sock.connect(server)?;
    Ok(sock.local_addr()?.ip())
}

/// SDP session config for plain RTP/AVP G.711 (no ICE, no SRTP).
pub(crate) fn plain_rtp_g711_config() -> ezk_rtc::sdp::SdpSessionConfig {
    use ezk_rtc::sdp::{BundlePolicy, RtcpMuxPolicy, SdpSessionConfig, TransportType};
    SdpSessionConfig {
        offer_transport: TransportType::Rtp,
        offer_ice: false,
        offer_avpf: false,
        rtcp_mux_policy: RtcpMuxPolicy::Negotiate,
        bundle_policy: BundlePolicy::MaxCompat,
        mtu: ezk_rtc::Mtu::new(1400),
    }
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `cargo test --features vendor-openssl --lib sip::tests`
Expected: PASS (all sip unit tests, including Task 2's).

- [ ] **Step 6: Commit.**

```bash
git add src/sip/
git commit -m "feat(sip): split module; add public types + pure helpers"
```

---

## Task 4: `SipTransport` — runtime thread + endpoint

**Files:**
- Modify: `src/sip/mod.rs`
- Modify: `src/sip/call.rs` (add `build_endpoint`)
- Test: `src/sip/mod.rs` (inline, offline — binds loopback UDP only)

**Interfaces:**
- Consumes: `SipConfig`, `SipError`, helpers/types (`SipEvent`) from Task 3.
- Produces:
  - `sip::SipCall { pub call_id: String, .. }` with `events`/`audio_in`/`audio_out`/`hangup`/`from_parts` (defined here because `Cmd`/`place_call` reference it; `from_parts` is unused until Task 5 — a `dead_code` warning here is expected and fine).
  - `sip::SipTransport` (`#[derive(Clone)]`), `SipTransport::new(&SipConfig) -> Result<Self, SipError>`, `SipTransport::place_call(&self, &str) -> Result<SipCall, SipError>`, `SipTransport::active_calls(&self) -> usize`, `SipTransport::shutdown(self)` (async).
  - internal `Cmd` enum, `build_endpoint(IpAddr) -> Result<(ezk_sip_core::Endpoint, SocketAddr), SipError>` in `call.rs`.
  - **Placeholder** `run_call` is added in Task 5; in this task the command loop handles `Cmd::Place` by immediately replying `Err(SipError::Config("not implemented"))` so the module compiles. Task 5 replaces that arm.

- [ ] **Step 1: Write the failing test.** Append to `#[cfg(test)] mod tests` in `src/sip/mod.rs`:

```rust
#[tokio::test]
async fn transport_binds_loopback_and_reports_zero_calls() {
    let cfg = crate::config::SipConfig {
        server: "127.0.0.1:5060".into(),
        username: "u".into(),
        password: "p".into(),
        from_user: None,
        local_ip: Some("127.0.0.1".parse().unwrap()),
        register: false,
        transport: Default::default(),
    };
    let t = SipTransport::new(&cfg).await.expect("bind");
    assert_eq!(t.active_calls(), 0);
    t.shutdown().await;
}
```

- [ ] **Step 2: Run test to verify it fails.**

Run: `cargo test --features vendor-openssl --lib sip::tests::transport_binds`
Expected: FAIL — `SipTransport` not defined.

- [ ] **Step 3: Add `build_endpoint` to `src/sip/call.rs`.**

```rust
use std::net::{IpAddr, SocketAddr};

use ezk_sip_core::Endpoint;
use ezk_sip_types::Method;
use ezk_sip_ua::dialog::DialogLayer;

use crate::sip::SipError;

/// Build the shared endpoint with one UDP transport. `add_allow` is MANDATORY —
/// an empty Allow header panics ezk on serialization ("tried to use empty vector").
pub(crate) async fn build_endpoint(local_ip: IpAddr) -> Result<(Endpoint, SocketAddr), SipError> {
    let mut builder = Endpoint::builder();
    builder.add_layer(DialogLayer::default());
    for m in [
        Method::INVITE,
        Method::ACK,
        Method::CANCEL,
        Method::BYE,
        Method::OPTIONS,
    ] {
        builder.add_allow(m);
    }
    let transport = builder
        .bind_udp(SocketAddr::new(local_ip, 0))
        .await
        .map_err(SipError::Bind)?;
    let bound = transport.bound();
    Ok((builder.build(), bound))
}
```

- [ ] **Step 4: Add `SipCall` + the transport + thread to `src/sip/mod.rs`.** Imports: `use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}; use tokio::sync::{mpsc, oneshot}; use bytes::Bytes;`

First the `SipCall` type (referenced by `Cmd`/`place_call` below; its channel ends are filled by `run_call` in Task 5):

```rust
/// A live outbound call. All fields are Send; the ezk `Call` loop runs on the
/// SIP runtime thread and communicates only through these channels.
pub struct SipCall {
    pub call_id: String,
    events: mpsc::Receiver<SipEvent>,
    rtp_in: mpsc::Receiver<Bytes>,
    rtp_out: mpsc::Sender<Bytes>,
    hangup: Option<oneshot::Sender<()>>,
}

impl SipCall {
    /// Lifecycle events (`Ringing` / `Answered` / `Terminated`).
    pub fn events(&mut self) -> &mut mpsc::Receiver<SipEvent> {
        &mut self.events
    }

    /// Inbound G.711 payloads (remote → us), one RTP payload per item.
    pub fn audio_in(&mut self) -> &mut mpsc::Receiver<Bytes> {
        &mut self.rtp_in
    }

    /// Outbound G.711 payload sink (us → remote). Clone to share.
    pub fn audio_out(&self) -> mpsc::Sender<Bytes> {
        self.rtp_out.clone()
    }

    /// Hang up (send BYE) and wait for termination.
    pub async fn hangup(mut self) {
        if let Some(h) = self.hangup.take() {
            let _ = h.send(());
        }
        while let Some(ev) = self.events.recv().await {
            if matches!(ev, SipEvent::Terminated(_)) {
                break;
            }
        }
    }

    /// Assemble a handle from channel ends (called by `run_call`, Task 5).
    pub(crate) fn from_parts(
        call_id: String,
        events: mpsc::Receiver<SipEvent>,
        rtp_in: mpsc::Receiver<Bytes>,
        rtp_out: mpsc::Sender<Bytes>,
        hangup: oneshot::Sender<()>,
    ) -> Self {
        Self { call_id, events, rtp_in, rtp_out, hangup: Some(hangup) }
    }
}
```

Then the transport + thread:

```rust
/// Command sent from `SipTransport` to the SIP runtime thread.
enum Cmd {
    Place {
        number: String,
        reply: oneshot::Sender<Result<SipCall, SipError>>,
    },
    Shutdown,
}

struct Shared {
    cmd_tx: mpsc::Sender<Cmd>,
    active: Arc<AtomicUsize>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// Process-wide SIP transport. Cheap to clone. Owns the ezk endpoint, which
/// lives on a dedicated single-threaded runtime so all `!Send` ezk state stays
/// off the caller's runtime.
#[derive(Clone)]
pub struct SipTransport {
    inner: Arc<Shared>,
}

impl SipTransport {
    /// Start the SIP runtime thread, bind the UDP transport, build the endpoint.
    pub async fn new(cfg: &crate::config::SipConfig) -> Result<Self, SipError> {
        let server: SocketAddr = cfg
            .server
            .parse()
            .map_err(|_| SipError::Config("server must be host:port"))?;
        let local_ip = match cfg.local_ip {
            Some(ip) => ip,
            None => detect_local_ip(server).map_err(SipError::Bind)?,
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (ready_tx, ready_rx) = oneshot::channel::<Result<(), SipError>>();
        let active = Arc::new(AtomicUsize::new(0));

        let cfg_owned = cfg.clone();
        let active_thread = active.clone();
        let join = std::thread::Builder::new()
            .name("kutsu-sip".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build sip runtime");
                let local = tokio::task::LocalSet::new();
                local.block_on(
                    &rt,
                    sip_thread_main(cfg_owned, server, local_ip, cmd_rx, active_thread, ready_tx),
                );
            })
            .map_err(SipError::Bind)?;

        // Wait for the thread to report the endpoint bind result.
        ready_rx.await.map_err(|_| SipError::RuntimeGone)??;

        Ok(Self {
            inner: Arc::new(Shared {
                cmd_tx,
                active,
                join: Mutex::new(Some(join)),
            }),
        })
    }

    /// Number of currently active calls (for `engine` to gate the cap).
    pub fn active_calls(&self) -> usize {
        self.inner.active.load(Ordering::Relaxed)
    }

    /// Place an outbound call to `number`. Resolves once the dialog is
    /// established; ringing/answer/teardown then arrive on `SipCall::events`.
    pub async fn place_call(&self, number: &str) -> Result<SipCall, SipError> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .cmd_tx
            .send(Cmd::Place {
                number: number.to_owned(),
                reply,
            })
            .await
            .map_err(|_| SipError::RuntimeGone)?;
        rx.await.map_err(|_| SipError::RuntimeGone)?
    }

    /// Terminate active calls and stop the runtime thread.
    pub async fn shutdown(self) {
        let _ = self.inner.cmd_tx.send(Cmd::Shutdown).await;
        let handle = self.inner.join.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = tokio::task::spawn_blocking(move || h.join()).await;
        }
    }
}

/// SIP runtime thread entry point: build the endpoint, then serve commands.
async fn sip_thread_main(
    cfg: crate::config::SipConfig,
    server: SocketAddr,
    local_ip: IpAddr,
    mut cmd_rx: mpsc::Receiver<Cmd>,
    active: Arc<AtomicUsize>,
    ready_tx: oneshot::Sender<Result<(), SipError>>,
) {
    let (endpoint, bound) = match call::build_endpoint(local_ip).await {
        Ok(pair) => {
            let _ = ready_tx.send(Ok(()));
            pair
        }
        Err(e) => {
            let _ = ready_tx.send(Err(e));
            return;
        }
    };

    let mut seq: u64 = 0;
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            Cmd::Place { number, reply } => {
                seq += 1;
                let call_id = format!("kutsu-{seq}");
                // TODO(Task 5): spawn_local(run_call(...)). Placeholder keeps it compiling.
                let _ = (server, bound, &cfg, &endpoint, &number, &call_id, &active);
                let _ = reply.send(Err(SipError::Config("call setup not implemented")));
            }
            Cmd::Shutdown => break,
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes.**

Run: `cargo test --features vendor-openssl --lib sip::tests::transport_binds`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add src/sip/
git commit -m "feat(sip): SipTransport runtime thread + endpoint bind"
```

---

## Task 5: `run_call` + `SipCall` + `place_call` (live)

**Files:**
- Modify: `src/sip/mod.rs` (add `SipCall`; replace the placeholder `Cmd::Place` arm)
- Modify: `src/sip/call.rs` (add `run_call` + media/URI/auth builders)
- Test: `tests/sip_outbound.rs` (new, `#[ignore]`, live stand)

**Interfaces:**
- Consumes: `build_endpoint` (Task 4), helpers/types (Task 3), `Cmd` + `SipCall` (Task 4).
- Produces:
  - `call::run_call(endpoint, cfg, server, bound, number, call_id, reply)` (async, runs on the SIP thread), plus the media/URI/auth builders and `map_make_err` in `call.rs`.
  - The real `Cmd::Place` arm in `sip_thread_main` (replacing Task 4's placeholder).

- [ ] **Step 1: Write the failing live integration test.** Create `tests/sip_outbound.rs`:

```rust
//! Live integration test for the outbound SIP transport. Requires the WSL
//! Asterisk stand (see dev/sip-test/README.md). #[ignore]d; run explicitly:
//!   cargo test --features vendor-openssl --test sip_outbound -- --ignored --nocapture
//! Env overrides: KUTSU_SIP_SERVER / _USER / _PASS / _EXT.

use std::time::Duration;

use bytes::Bytes;
use kutsu::config::SipConfig;
use kutsu::sip::{G711Kind, SipEvent, SipTransport};

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

#[tokio::test]
#[ignore = "requires the live WSL Asterisk stand; run with --ignored"]
async fn outbound_echo_600_bidirectional_rtp() {
    let cfg = SipConfig {
        server: env_or("KUTSU_SIP_SERVER", "192.168.88.243:5060"),
        username: env_or("KUTSU_SIP_USER", "kutsu"),
        password: env_or("KUTSU_SIP_PASS", "kutsupw"),
        from_user: None,
        local_ip: None,
        register: false,
        transport: Default::default(),
    };
    let transport = SipTransport::new(&cfg).await.expect("transport up");
    let mut call = transport
        .place_call(&env_or("KUTSU_SIP_EXT", "600"))
        .await
        .expect("place_call");

    // Wait for answer (skip ringing).
    let codec = loop {
        match call.events().recv().await.expect("event stream open") {
            SipEvent::Ringing => continue,
            SipEvent::Answered { codec } => break codec,
            SipEvent::Terminated(r) => panic!("terminated before answer: {r:?}"),
        }
    };
    assert!(matches!(codec.kind, G711Kind::Ulaw | G711Kind::Alaw));

    // Send ~150 frames of G.711 silence while counting echoed frames.
    let out = call.audio_out();
    let sender = tokio::spawn(async move {
        let payload = Bytes::from(vec![0xFFu8; 160]);
        let mut ticker = tokio::time::interval(Duration::from_millis(20));
        for _ in 0..150 {
            ticker.tick().await;
            if out.send(payload.clone()).await.is_err() {
                break;
            }
        }
    });

    let mut received = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        tokio::select! {
            frame = call.audio_in().recv() => {
                if frame.is_some() { received += 1; if received >= 50 { break; } }
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    sender.abort();

    eprintln!("[sip_outbound] codec={:?} received={received}", codec.kind);
    assert!(received > 0, "no echoed RTP frames — bidirectional media failed");

    call.hangup().await;
    transport.shutdown().await;
}
```

- [ ] **Step 2: Run it to confirm it fails to compile/link.**

Run: `cargo test --features vendor-openssl --test sip_outbound -- --ignored`
Expected: FAIL — `SipCall` has no `events`/`audio_in`/`audio_out`/`hangup`.

- [ ] **Step 3: Confirm `SipCall` (already defined in Task 4).** No new code: the `SipCall` struct, its `events`/`audio_in`/`audio_out`/`hangup` accessors, and the `pub(crate) from_parts` constructor were all added in Task 4 Step 4 so the module compiled. `run_call` (Step 5) calls `SipCall::from_parts(...)`. When you add the `use crate::sip::{...}` line in `call.rs` (Step 5), **consolidate it with the `use crate::sip::SipError;` that Task 4 already put at the top of `call.rs`** — a duplicate import of `SipError` (or of the `std::net` types Task 4 imported) is a compile error. There should be exactly one `use crate::sip::{...}` and one `use std::net::{...}` in `call.rs`.

- [ ] **Step 4: Replace the `Cmd::Place` placeholder in `sip_thread_main`.** Swap the placeholder arm for:

```rust
            Cmd::Place { number, reply } => {
                seq += 1;
                let call_id = format!("kutsu-{seq}");
                let endpoint = endpoint.clone();
                let cfg = cfg.clone();
                let active = active.clone();
                active.fetch_add(1, Ordering::Relaxed);
                tokio::task::spawn_local(async move {
                    call::run_call(endpoint, cfg, server, bound, number, call_id, reply).await;
                    active.fetch_sub(1, Ordering::Relaxed);
                });
            }
```

- [ ] **Step 5: Implement `run_call` + builders in `src/sip/call.rs`.** Add imports:

```rust
use std::time::Instant;

use bytes::Bytes;
use ezk_rtc::rtp_session::SendRtpPacket;
use ezk_rtc::sdp::{Codec, Codecs, Direction, MediaType, SdpSession};
use ezk_rtc::OpenSslContext;
use ezk_sip_auth::{DigestAuthenticator, DigestCredentials, DigestUser};
use ezk_sip_types::header::typed::Contact;
use ezk_sip_types::uri::{NameAddr, SipUri};
use bytesstr::BytesStr;
use ezk_sip_ua::{CallEvent, MediaEvent, OutboundCall, RtcMediaBackend};
use tokio::sync::{mpsc, oneshot};

use crate::config::SipConfig;
use crate::sip::{
    g711_kind_from_pt, plain_rtp_g711_config, G711Kind, NegotiatedCodec, SipCall, SipError,
    SipEvent, TermReason,
};
```

Then:

```rust
fn make_auth(cfg: &SipConfig) -> DigestAuthenticator {
    let mut creds = DigestCredentials::new();
    creds.set_default(DigestUser::new(
        cfg.username.clone(),
        cfg.password.clone().into_bytes(),
    ));
    DigestAuthenticator::new(creds)
}

fn build_media(local_ip: IpAddr) -> Result<RtcMediaBackend, SipError> {
    let mut sdp = SdpSession::new(
        OpenSslContext::try_new().map_err(|e| SipError::Media(e.to_string()))?,
        local_ip,
        plain_rtp_g711_config(),
    );
    let lm = sdp
        .add_local_media(
            Codecs::new(MediaType::Audio)
                .with_codec(Codec::PCMU)
                .with_codec(Codec::PCMA),
            Direction::SendRecv,
        )
        .ok_or(SipError::Media("add_local_media failed".into()))?;
    sdp.add_media(lm, Direction::SendRecv, None, None);
    Ok(RtcMediaBackend::new(sdp))
}

fn make_uris(
    server: SocketAddr,
    from_user: &str,
    bound: SocketAddr,
    number: &str,
) -> (NameAddr, Contact, SipUri) {
    let id = NameAddr::uri(SipUri::new(server.into()).user(BytesStr::from(from_user)));
    let contact = Contact::new(NameAddr::uri(
        SipUri::new(bound.into()).user(BytesStr::from(from_user)),
    ));
    let target = SipUri::new(server.into()).user(BytesStr::from(number));
    (id, contact, target)
}

fn neg_codec(c: &ezk_sip_ua::Codec) -> NegotiatedCodec {
    NegotiatedCodec {
        pt: c.pt,
        kind: g711_kind_from_pt(c.pt).unwrap_or(G711Kind::Ulaw),
        ptime_ms: 20,
    }
}

/// Drive one outbound call to completion. Owns all `!Send` ezk state; runs on
/// the SIP runtime thread. Adapted from the phase-1 spike (tests/sip_spike.rs).
pub(crate) async fn run_call(
    endpoint: Endpoint,
    cfg: SipConfig,
    server: SocketAddr,
    bound: SocketAddr,
    number: String,
    call_id: String,
    reply: oneshot::Sender<Result<SipCall, SipError>>,
) {
    let local_ip = bound.ip();
    let media = match build_media(local_ip) {
        Ok(m) => m,
        Err(e) => {
            let _ = reply.send(Err(e));
            return;
        }
    };
    let (id, contact, target) = make_uris(server, cfg.from_user(), bound, &number);

    let mut outbound = match OutboundCall::make(endpoint, make_auth(&cfg), id, contact, target, media)
        .await
    {
        Ok(o) => o,
        Err(e) => {
            let _ = reply.send(Err(map_make_err(&e)));
            return;
        }
    };

    // Dialog established -> hand the caller a live SipCall handle.
    let (ev_tx, ev_rx) = mpsc::channel::<SipEvent>(16);
    let (in_tx, in_rx) = mpsc::channel::<Bytes>(64);
    let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(64);
    let (hup_tx, mut hup_rx) = oneshot::channel::<()>();
    let handle = SipCall::from_parts(call_id, ev_rx, in_rx, out_tx, hup_tx);
    if reply.send(Ok(handle)).is_err() {
        let _ = outbound.cancel().await;
        return;
    }

    let unacked = match outbound.wait_for_completion().await {
        Ok(u) => u,
        Err(e) => {
            let _ = ev_tx
                .send(SipEvent::Terminated(TermReason::Failed(e.to_string())))
                .await;
            return;
        }
    };
    let mut call = match unacked.finish().await {
        Ok(c) => c,
        Err(e) => {
            let _ = ev_tx
                .send(SipEvent::Terminated(TermReason::Failed(e.to_string())))
                .await;
            return;
        }
    };

    // Capture the RTP sender/receiver + codec from the first media events.
    let mut sender = None;
    let mut receiver = None;
    let mut codec = None;
    while sender.is_none() || receiver.is_none() {
        match call.run().await {
            Ok(CallEvent::Internal(e)) => {
                if call.handle_internal_event(e).await.is_err() {
                    let _ = ev_tx
                        .send(SipEvent::Terminated(TermReason::Failed("internal".into())))
                        .await;
                    return;
                }
            }
            Ok(CallEvent::Media(MediaEvent::SenderAdded { sender: s, codec: c })) => {
                codec = Some(neg_codec(&c));
                sender = Some(s);
            }
            Ok(CallEvent::Media(MediaEvent::ReceiverAdded { receiver: r, .. })) => {
                receiver = Some(r);
            }
            Ok(CallEvent::Terminated) => {
                let _ = ev_tx
                    .send(SipEvent::Terminated(TermReason::RemoteHangup))
                    .await;
                return;
            }
            Err(e) => {
                let _ = ev_tx
                    .send(SipEvent::Terminated(TermReason::Failed(e.to_string())))
                    .await;
                return;
            }
        }
    }
    let mut sender = sender.unwrap();
    let mut receiver = receiver.unwrap();
    let codec = codec.unwrap();
    let pt = codec.pt;
    let _ = ev_tx.send(SipEvent::Answered { codec }).await;

    // Outbound forwarder on its own local task (proven pattern from the spike:
    // RtpSender::send waits for transport-Connected, which the main call.run()
    // loop supplies — so the two must run concurrently).
    let out_task = tokio::task::spawn_local(async move {
        while let Some(payload) = out_rx.recv().await {
            if sender
                .send(SendRtpPacket::new(Instant::now(), pt, payload))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Main loop: pump SIP/media, forward inbound audio, honour hangup.
    let reason = loop {
        tokio::select! {
            r = call.run() => match r {
                Ok(CallEvent::Internal(e)) => {
                    if call.handle_internal_event(e).await.is_err() {
                        break TermReason::Failed("internal".into());
                    }
                }
                Ok(CallEvent::Media(_)) => {}
                Ok(CallEvent::Terminated) => break TermReason::RemoteHangup,
                Err(e) => break TermReason::Failed(e.to_string()),
            },
            Some(rtp) = receiver.recv() => {
                // Drop-on-full: never block the media loop on a stalled bridge.
                let _ = in_tx.try_send(rtp.payload);
            }
            _ = &mut hup_rx => {
                let _ = call.terminate().await;
                break TermReason::LocalHangup;
            }
        }
    };
    out_task.abort();
    let _ = ev_tx.send(SipEvent::Terminated(reason)).await;
}

fn map_make_err<A: std::fmt::Debug, M: std::fmt::Debug>(
    e: &ezk_sip_ua::MakeCallError<M, A>,
) -> SipError {
    use ezk_sip_ua::MakeCallError;
    match e {
        MakeCallError::Failed(line) => SipError::Invite(format!("{line:?}")),
        MakeCallError::Core(ezk_sip_core::Error::RequestTimedOut) => SipError::NotAnswered,
        other => SipError::Media(format!("{other:?}")),
    }
}
```

> Note: `RtpReceiver` derefs to `mpsc::Receiver<RtpPacket>`, so `receiver.recv()` yields `Option<RtpPacket>` and `rtp.payload` is the `Bytes` payload. `MakeCallError`/`Method`/`Endpoint`/`SocketAddr`/`IpAddr` are already imported at the top of `call.rs` (Task 4 added the net + `Method` imports; add `ezk_sip_ua::MakeCallError` and `ezk_sip_core::Error` are referenced by path here).

- [ ] **Step 6: Verify the crate + test compile.**

Run: `cargo test --features vendor-openssl --test sip_outbound --no-run`
Expected: PASS (compiles). If `map_make_err` generic bounds don't line up with the concrete `OutboundCall::make` error type, adjust the call site to `map_make_err(&e)` with the inferred `M = RtcMediaBackendError`.

- [ ] **Step 7: Run the live test against the WSL stand.**

Prereq: confirm Asterisk is up — `wsl.exe -- pgrep -a asterisk`.
Run: `cargo test --features vendor-openssl --test sip_outbound -- --ignored --nocapture`
Expected: PASS — prints `codec=Ulaw received=50` (or ≥1), call answered and echoed.

- [ ] **Step 8: Run the full unit suite (no regressions).**

Run: `cargo test --features vendor-openssl --lib`
Expected: PASS (all prior unit tests green).

- [ ] **Step 9: Commit.**

```bash
git add src/sip/ tests/sip_outbound.rs
git commit -m "feat(sip): outbound call driving loop + SipCall (live-validated)"
```

---

## Task 6: Retire the spike

**Files:**
- Delete: `tests/sip_spike.rs`

**Interfaces:** none.

- [ ] **Step 1: Delete the throwaway spike.**

```bash
git rm tests/sip_spike.rs
```

- [ ] **Step 2: Confirm nothing references it and the suite still builds.**

Run: `cargo test --features vendor-openssl --no-run`
Expected: PASS (compiles; `sip_spike` gone).

- [ ] **Step 3: Commit.**

```bash
git commit -m "chore(sip): remove phase-1 throwaway spike (superseded by src/sip)"
```

---

## Self-Review

**Spec coverage:**
- §1 boundaries / raw-G.711 seam → `SipCall` audio channels carry `Bytes` (Task 5). ✔
- §2 scope (UDP, digest, register-free INVITE, plain RTP G.711) → `build_endpoint` (Task 4) + `build_media`/`run_call` (Task 5). ✔
- §3 `SipConfig` + seams → Task 1. ✔
- §4 public interface (`SipTransport`/`SipCall`/`SipEvent`/`NegotiatedCodec`/`active_calls`) → Tasks 3–5. ✔
- §5 dedicated SIP thread + `LocalSet` + command loop → Task 4. ✔
- §6 per-call data flow (make → reply → wait/finish → capture → forward) → `run_call` Task 5. ✔
- §7 `SipError` + `Error::Sip` → Task 2; `map_make_err` maps ezk errors. ✔
- §8 extension seams documented (config `register`/`transport`) → Task 1. ✔
- §9 concurrency: `active_calls()` exposed, cap not enforced → Task 4/5. ✔
- §10 tests: pure unit (Tasks 1,3), offline transport (Task 4), live `#[ignore]` integration replacing the spike (Tasks 5,6). ✔
- §11 dep promotion + module split + delete spike → Tasks 1,3,6. ✔

**Placeholder scan:** The only intentional placeholder is the Task 4 `Cmd::Place` stub, explicitly replaced in Task 5 Step 4. No "TBD"/"add error handling"/uncoded steps remain.

**Type consistency:** `SipTransport::{new,place_call,active_calls,shutdown}`, `SipCall::{events,audio_in,audio_out,hangup,from_parts}`, `SipEvent::{Ringing,Answered{codec},Terminated}`, `NegotiatedCodec{pt,kind,ptime_ms}`, `run_call(endpoint,cfg,server,bound,number,call_id,reply)` — signatures match across the `Cmd::Place` call site (Task 5 Step 4) and the `run_call` definition (Step 5). `SipCall::from_parts` field order matches the struct. Channel element types (`SipEvent`, `Bytes`) are consistent between producer (`run_call`) and consumers (`SipCall` accessors, integration test).
