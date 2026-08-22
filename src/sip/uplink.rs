//! Uplink RTP quality: loss/reorder accounting (RFC 3550-style) over the
//! 16-bit sequence space. Pure, single-writer; the SIP receive loop feeds it
//! each arriving sequence number.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A point-in-time uplink quality snapshot (phone -> us).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UplinkQuality {
    pub received: u64,
    pub lost: u64,
    pub reordered: u64,
}

/// Accumulates RTP sequence numbers into loss/reorder counts. `lost` is
/// derived as `(max_ext - first_ext + 1) - received`, correct under
/// reordering (a late packet raises `received`, not the span).
#[derive(Debug)]
pub struct UplinkStats {
    started: bool,
    cycles: u64,
    max16: u16,
    first_ext: u64,
    received: u64,
    reordered: u64,
}

impl Default for UplinkStats {
    fn default() -> Self {
        Self::new()
    }
}

impl UplinkStats {
    pub fn new() -> Self {
        Self {
            started: false,
            cycles: 0,
            max16: 0,
            first_ext: 0,
            received: 0,
            reordered: 0,
        }
    }

    /// Classifies `seq` relative to the running max via a single `0x8000`
    /// forward/backward split (RFC 3550 §A.1-style), with no warmup and no
    /// MAX_DROPOUT/MAX_MISORDER guard: a genuine forward jump >32768 or a
    /// reorder as the very second packet is misclassified. Negligible at
    /// telephony packet rates (~50 pkt/s) — acceptable for this diagnostic.
    pub fn observe(&mut self, seq: u16) {
        if !self.started {
            self.started = true;
            self.max16 = seq;
            self.first_ext = seq as u64; // cycle 0 baseline
            self.received = 1;
            return;
        }
        self.received += 1;
        let forward = seq.wrapping_sub(self.max16); // distance max -> seq, forward
        if forward == 0 {
            // Duplicate packet: already counted in `received` above; do not
            // touch max16/cycles/reordered.
        } else if forward < 0x8000 {
            // Forward progress, possibly across a 16-bit wrap.
            if seq < self.max16 {
                self.cycles += 1;
            }
            self.max16 = seq;
        } else {
            // seq is behind max -> a reordered (late) packet.
            self.reordered += 1;
        }
    }

    pub fn snapshot(&self) -> UplinkQuality {
        if !self.started {
            return UplinkQuality::default();
        }
        let max_ext = self.cycles * 65_536 + self.max16 as u64;
        let span = max_ext - self.first_ext + 1;
        UplinkQuality {
            received: self.received,
            lost: span.saturating_sub(self.received),
            reordered: self.reordered,
        }
    }
}

/// Lock-free uplink counters published by the SIP receive loop and read by
/// the engine, mirroring `bridge::QualityShared`. Single writer, single reader.
#[derive(Default)]
pub struct UplinkQualityShared {
    received: AtomicU64,
    lost: AtomicU64,
    reordered: AtomicU64,
}

impl UplinkQualityShared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn publish(&self, q: &UplinkQuality) {
        self.received.store(q.received, Ordering::Relaxed);
        self.lost.store(q.lost, Ordering::Relaxed);
        self.reordered.store(q.reordered, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> UplinkQuality {
        UplinkQuality {
            received: self.received.load(Ordering::Relaxed),
            lost: self.lost.load(Ordering::Relaxed),
            reordered: self.reordered.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(seqs: &[u16]) -> UplinkQuality {
        let mut s = UplinkStats::new();
        for &q in seqs {
            s.observe(q);
        }
        s.snapshot()
    }

    #[test]
    fn empty_is_zero() {
        assert_eq!(run(&[]), UplinkQuality::default());
    }

    #[test]
    fn in_order_has_no_loss() {
        let q = run(&[100, 101, 102, 103, 104]);
        assert_eq!(
            q,
            UplinkQuality {
                received: 5,
                lost: 0,
                reordered: 0
            }
        );
    }

    #[test]
    fn single_gap_counts_one_lost() {
        // 102 missing.
        let q = run(&[100, 101, 103, 104]);
        assert_eq!(
            q,
            UplinkQuality {
                received: 4,
                lost: 1,
                reordered: 0
            }
        );
    }

    #[test]
    fn wider_gap_counts_all_lost() {
        // 101..104 missing (4 lost).
        let q = run(&[100, 105]);
        assert_eq!(
            q,
            UplinkQuality {
                received: 2,
                lost: 4,
                reordered: 0
            }
        );
    }

    #[test]
    fn reorder_does_not_inflate_loss() {
        // 102 arrives late, after 103.
        let q = run(&[100, 101, 103, 102, 104]);
        assert_eq!(
            q,
            UplinkQuality {
                received: 5,
                lost: 0,
                reordered: 1
            }
        );
    }

    #[test]
    fn duplicate_is_counted_not_lost() {
        let q = run(&[100, 101, 101, 102]);
        // received 4, span 100..102 = 3, saturating -> lost 0.
        assert_eq!(q.lost, 0);
        assert_eq!(q.received, 4);
    }

    #[test]
    fn wraparound_has_no_loss() {
        let q = run(&[65534, 65535, 0, 1]);
        assert_eq!(
            q,
            UplinkQuality {
                received: 4,
                lost: 0,
                reordered: 0
            }
        );
    }

    #[test]
    fn shared_roundtrips_published_snapshot() {
        let shared = UplinkQualityShared::new();
        assert_eq!(shared.snapshot(), UplinkQuality::default());
        let q = UplinkQuality {
            received: 50,
            lost: 3,
            reordered: 2,
        };
        shared.publish(&q);
        assert_eq!(shared.snapshot(), q);
    }
}
