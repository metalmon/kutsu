//! Pure reconnect policy for the Live session (no IO): exponential backoff and
//! the "drop the resumption handle after N consecutive failures" rule.

use std::time::Duration;

pub struct Backoff {
    current_ms: u64,
    base_ms: u64,
    max_ms: u64,
}

impl Backoff {
    pub fn new(base_ms: u64, max_ms: u64) -> Self {
        Backoff { current_ms: base_ms, base_ms, max_ms }
    }

    pub fn next_delay(&mut self) -> Duration {
        let d = Duration::from_millis(self.current_ms);
        self.current_ms = (self.current_ms * 2).min(self.max_ms);
        d
    }

    pub fn reset(&mut self) {
        self.current_ms = self.base_ms;
    }
}

pub struct ReconnectState {
    fails: u32,
    reset_handle_after: u32,
}

impl ReconnectState {
    pub fn new(reset_handle_after: u32) -> Self {
        ReconnectState { fails: 0, reset_handle_after }
    }

    pub fn on_success(&mut self) {
        self.fails = 0;
    }

    /// Record a failed connect/session. Returns true when the caller should drop
    /// the (likely stale) resumption handle and start a fresh session.
    pub fn on_failure(&mut self) -> bool {
        self.fails += 1;
        self.reset_handle_after != 0 && self.fails >= self.reset_handle_after
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn backoff_doubles_and_caps_then_resets() {
        let mut b = Backoff::new(300, 5000);
        assert_eq!(b.next_delay(), Duration::from_millis(300));
        assert_eq!(b.next_delay(), Duration::from_millis(600));
        assert_eq!(b.next_delay(), Duration::from_millis(1200));
        assert_eq!(b.next_delay(), Duration::from_millis(2400));
        assert_eq!(b.next_delay(), Duration::from_millis(4800));
        assert_eq!(b.next_delay(), Duration::from_millis(5000)); // capped
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_millis(300));
    }

    #[test]
    fn drops_handle_after_four_consecutive_failures() {
        let mut s = ReconnectState::new(4);
        assert!(!s.on_failure()); // 1
        assert!(!s.on_failure()); // 2
        assert!(!s.on_failure()); // 3
        assert!(s.on_failure());  // 4 -> drop handle
        s.on_success();
        assert!(!s.on_failure()); // counter reset by success
    }
}
