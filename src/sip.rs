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
