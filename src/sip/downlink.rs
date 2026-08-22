//! Downlink RTP quality (us -> callee), derived from the RTCP receiver reports
//! the callee sends back. Unlike the uplink path (which kutsu computes itself
//! from arriving sequence numbers), these numbers come from ezk-rtc's outbound
//! stream stats and are only present when the carrier actually emits RR.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// A point-in-time downlink quality snapshot (our audio -> callee), as reported
/// by the remote via RTCP RR. `present` is false until the first RR arrives (or
/// forever, if the carrier never sends RR) — the gate must ignore samples that
/// are not present rather than read a `0%` loss as "healthy".
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DownlinkQuality {
    pub present: bool,
    pub loss_pct: f32,
    pub jitter_ms: u32,
    pub rtt_ms: u32,
}

/// Lock-free downlink stats published by the SIP call task (which owns the ezk
/// `Call` and samples its outbound RR stats) and read by the engine. Single
/// writer, single reader, mirroring [`super::uplink::UplinkQualityShared`].
#[derive(Default)]
pub struct DownlinkQualityShared {
    present: AtomicBool,
    loss_bits: AtomicU32,
    jitter_ms: AtomicU32,
    rtt_ms: AtomicU32,
}

impl DownlinkQualityShared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn publish(&self, q: &DownlinkQuality) {
        self.loss_bits
            .store(q.loss_pct.to_bits(), Ordering::Relaxed);
        self.jitter_ms.store(q.jitter_ms, Ordering::Relaxed);
        self.rtt_ms.store(q.rtt_ms, Ordering::Relaxed);
        // Publish `present` last so a reader that sees it true also sees the
        // values above (Relaxed is fine here: single writer, and the gate
        // tolerates a one-tick-stale value).
        self.present.store(q.present, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> DownlinkQuality {
        DownlinkQuality {
            present: self.present.load(Ordering::Relaxed),
            loss_pct: f32::from_bits(self.loss_bits.load(Ordering::Relaxed)),
            jitter_ms: self.jitter_ms.load(Ordering::Relaxed),
            rtt_ms: self.rtt_ms.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_defaults_to_absent() {
        let shared = DownlinkQualityShared::new();
        let q = shared.snapshot();
        assert!(!q.present);
        assert_eq!(q.loss_pct, 0.0);
    }

    #[test]
    fn shared_roundtrips_published_snapshot() {
        let shared = DownlinkQualityShared::new();
        let q = DownlinkQuality {
            present: true,
            loss_pct: 12.5,
            jitter_ms: 30,
            rtt_ms: 120,
        };
        shared.publish(&q);
        assert_eq!(shared.snapshot(), q);
    }
}
