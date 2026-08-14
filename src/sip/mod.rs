//! SIP user agent (`ezk-sip-ua` + `ezk-rtc`).
//!
//! Registers against a configured SIP trunk, places outbound `INVITE`s,
//! negotiates SDP for G.711, and exposes raw RTP frame streams in both
//! directions so [`crate::bridge`] can feed/consume audio without owning
//! any physical audio device.
//!
//! Not yet implemented. First milestone is a spike: complete one outbound
//! call against a real trunk and confirm raw RTP frames flow both ways
//! before building anything on top of this.

mod call;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

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
}
