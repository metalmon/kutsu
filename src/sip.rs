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
