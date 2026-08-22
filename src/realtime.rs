//! Best-effort real-time scheduling for the SIP/RTP thread.
//!
//! The `kutsu-sip` thread carries the realtime-critical RTP send/receive path.
//! Under scheduler jitter it can drain the UDP socket late (OS buffer overflow →
//! packet loss) or pace egress unevenly. Promoting it to real-time priority
//! (MMCSS "Pro Audio" on Windows, SCHED_FIFO / rtkit on Linux, via
//! `audio_thread_priority`) reserves CPU for it. Best-effort: on any failure
//! (e.g. Linux without CAP_SYS_NICE and no rtkit) we log and continue at normal
//! priority — never fatal. Toggle with `KUTSU_RT_PRIORITY` (default on).

/// Parse the `KUTSU_RT_PRIORITY` toggle. Default ON; only an explicit
/// `0`/`false`/`no`/`off` (case-insensitive) disables it. Unset or empty = on.
pub fn rt_enabled(var: Option<String>) -> bool {
    match var.as_deref().map(|s| s.trim().to_ascii_lowercase()) {
        Some(v) => !matches!(v.as_str(), "0" | "false" | "no" | "off"),
        None => true,
    }
}

/// Promote the current thread to real-time priority (best-effort). Returns a
/// handle that MUST be held for as long as the priority should last (dropping it
/// reverts). `enabled == false`, or any OS failure, yields `None` without
/// failing the caller.
#[must_use]
pub fn promote_current_thread(
    label: &str,
    enabled: bool,
) -> Option<audio_thread_priority::RtPriorityHandle> {
    if !enabled {
        return None;
    }
    // 20 ms G.711 frames @ 8 kHz = 160 frames per RTP packet.
    match audio_thread_priority::promote_current_thread_to_real_time(160, 8000) {
        Ok(handle) => {
            tracing::info!(thread = label, "thread promoted to real-time priority");
            Some(handle)
        }
        Err(e) => {
            tracing::warn!(
                thread = label,
                error = ?e,
                "could not raise thread to real-time priority; continuing at normal priority"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rt_priority_defaults_on_when_unset_or_empty() {
        assert!(rt_enabled(None));
        assert!(rt_enabled(Some(String::new())));
        assert!(rt_enabled(Some("   ".into())));
    }

    #[test]
    fn rt_priority_explicit_off_values_disable() {
        for v in ["0", "false", "no", "off", "OFF", "False"] {
            assert!(!rt_enabled(Some(v.into())), "{v} should disable");
        }
    }

    #[test]
    fn rt_priority_other_values_stay_on() {
        for v in ["1", "true", "yes", "on"] {
            assert!(rt_enabled(Some(v.into())), "{v} should stay on");
        }
    }
}
