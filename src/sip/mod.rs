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
}
