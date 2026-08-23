//! Circuit breaker for the shared Gemini leg. Repeated preflight failures trip
//! it and hold the whole dispatch queue for a cooldown; on expiry it admits a
//! single half-open probe whose outcome closes or re-trips it.

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// What the dispatcher should do with the next eligible entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Channel healthy — dispatch normally.
    Dispatch,
    /// Cooldown elapsed — dispatch exactly one probe, then call
    /// [`Breaker::mark_probe_dispatched`].
    Probe,
    /// Holding — do not dispatch; park until this epoch-ms (or a wake).
    Hold(u64),
}

#[derive(Default)]
struct State {
    consec_failures: u32,
    open_until_ms: u64,
    probe_in_flight: bool,
    probe_deadline_ms: u64,
}

pub struct Breaker {
    threshold: u32,
    cooldown_ms: u64,
    wake: Arc<Notify>,
    state: Mutex<State>,
}

impl Breaker {
    /// `threshold == 0` disables the breaker (always [`Decision::Dispatch`]).
    pub fn new(threshold: u32, cooldown_ms: u64, wake: Arc<Notify>) -> Self {
        Self {
            threshold,
            cooldown_ms,
            wake,
            state: Mutex::new(State::default()),
        }
    }

    /// Non-mutating decision for the dispatcher at `now` (epoch-ms).
    pub fn decision(&self, now: u64) -> Decision {
        if self.threshold == 0 {
            return Decision::Dispatch;
        }
        let s = self.state.lock().unwrap();
        if s.open_until_ms == 0 {
            return Decision::Dispatch;
        }
        if now < s.open_until_ms {
            return Decision::Hold(s.open_until_ms);
        }
        if s.probe_in_flight && now < s.probe_deadline_ms {
            return Decision::Hold(s.probe_deadline_ms);
        }
        Decision::Probe
    }

    /// Record that the half-open probe was spawned at `now`; sets a wedge-safety
    /// deadline so a probe that never reports cannot block dispatch forever.
    pub fn mark_probe_dispatched(&self, now: u64) {
        let mut s = self.state.lock().unwrap();
        s.probe_in_flight = true;
        s.probe_deadline_ms = now.saturating_add(self.cooldown_ms.saturating_mul(2));
    }

    /// Report a preflight outcome at `now`. Success closes; failure trips (or
    /// re-trips) at the threshold. Wakes the dispatcher to re-evaluate.
    pub fn on_result(&self, ok: bool, now: u64) {
        if self.threshold == 0 {
            return;
        }
        {
            let mut s = self.state.lock().unwrap();
            s.probe_in_flight = false;
            if ok {
                s.consec_failures = 0;
                s.open_until_ms = 0;
            } else {
                s.consec_failures = s.consec_failures.saturating_add(1);
                if s.consec_failures >= self.threshold {
                    s.open_until_ms = now.saturating_add(self.cooldown_ms);
                }
            }
        }
        self.wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Notify;

    fn br(threshold: u32, cooldown: u64) -> Breaker {
        Breaker::new(threshold, cooldown, Arc::new(Notify::new()))
    }

    #[test]
    fn closed_dispatches() {
        let b = br(3, 60_000);
        assert_eq!(b.decision(0), Decision::Dispatch);
    }

    #[test]
    fn trips_after_threshold_failures() {
        let b = br(3, 60_000);
        b.on_result(false, 1000);
        b.on_result(false, 1000);
        assert_eq!(b.decision(1000), Decision::Dispatch); // 2 < 3
        b.on_result(false, 1000);
        assert_eq!(b.decision(1000), Decision::Hold(61_000));
        assert_eq!(b.decision(60_999), Decision::Hold(61_000));
    }

    #[test]
    fn half_open_admits_one_probe_then_holds() {
        let b = br(1, 10_000);
        b.on_result(false, 0); // trips: open_until = 10_000
        assert_eq!(b.decision(10_000), Decision::Probe);
        b.mark_probe_dispatched(10_000);
        // probe in flight -> hold until the wedge-safety deadline (now + 2*cooldown)
        assert_eq!(b.decision(10_000), Decision::Hold(30_000));
    }

    #[test]
    fn probe_success_closes() {
        let b = br(1, 10_000);
        b.on_result(false, 0);
        let _ = b.decision(10_000);
        b.mark_probe_dispatched(10_000);
        b.on_result(true, 11_000);
        assert_eq!(b.decision(11_000), Decision::Dispatch);
    }

    #[test]
    fn probe_failure_retrips() {
        let b = br(1, 10_000);
        b.on_result(false, 0);
        let _ = b.decision(10_000);
        b.mark_probe_dispatched(10_000);
        b.on_result(false, 11_000);
        assert_eq!(b.decision(11_000), Decision::Hold(21_000));
    }

    #[test]
    fn stale_probe_is_re_admitted() {
        let b = br(1, 10_000);
        b.on_result(false, 0);
        let _ = b.decision(10_000);
        b.mark_probe_dispatched(10_000); // deadline = 30_000
        assert_eq!(b.decision(30_000), Decision::Probe);
    }

    #[test]
    fn threshold_zero_disables() {
        let b = br(0, 60_000);
        b.on_result(false, 0);
        b.on_result(false, 0);
        b.on_result(false, 0);
        assert_eq!(b.decision(0), Decision::Dispatch);
    }
}
