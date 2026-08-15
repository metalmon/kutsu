//! Structured dial result, parsed from the SIP response status code. This is
//! the fine-grained outcome of an attempt; `CallState` stays coarse.

/// Terminal outcome of one dial attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallOutcome {
    /// A connected call ended normally (model or caller hung up).
    Completed,
    /// 486 Busy Here.
    Busy,
    /// 408 request timeout, 480 temporarily unavailable, or ring timeout.
    NoAnswer,
    /// 603 decline, 403 forbidden, other 6xx.
    Rejected,
    /// 404 not found, 484 address incomplete.
    NotFound,
    /// 503 service unavailable, other 5xx.
    Unavailable,
    /// Technical failure (media, bridge, gemini) or an unmapped code.
    Failed,
}

impl CallOutcome {
    /// Transient outcomes worth an automatic or scheduled retry.
    pub fn retryable(&self) -> bool {
        matches!(self, CallOutcome::Busy | CallOutcome::NoAnswer)
    }
}

/// Map a SIP response status code to a `CallOutcome`.
pub fn outcome_from_status(code: u16) -> CallOutcome {
    match code {
        200..=299 => CallOutcome::Completed,
        404 | 484 => CallOutcome::NotFound,
        486 | 600 => CallOutcome::Busy,      // 486 Busy Here, 600 Busy Everywhere
        408 | 480 => CallOutcome::NoAnswer,
        403 | 603 => CallOutcome::Rejected,
        500..=599 => CallOutcome::Unavailable,
        _ => CallOutcome::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_codes() {
        assert_eq!(outcome_from_status(486), CallOutcome::Busy);
        assert_eq!(outcome_from_status(600), CallOutcome::Busy);
        assert_eq!(outcome_from_status(404), CallOutcome::NotFound);
        assert_eq!(outcome_from_status(484), CallOutcome::NotFound);
        assert_eq!(outcome_from_status(408), CallOutcome::NoAnswer);
        assert_eq!(outcome_from_status(480), CallOutcome::NoAnswer);
        assert_eq!(outcome_from_status(603), CallOutcome::Rejected);
        assert_eq!(outcome_from_status(403), CallOutcome::Rejected);
        assert_eq!(outcome_from_status(503), CallOutcome::Unavailable);
        assert_eq!(outcome_from_status(200), CallOutcome::Completed);
    }

    #[test]
    fn unmapped_code_is_failed() {
        assert_eq!(outcome_from_status(481), CallOutcome::Failed);
        assert_eq!(outcome_from_status(100), CallOutcome::Failed);
    }

    #[test]
    fn retryable_only_busy_and_no_answer() {
        assert!(CallOutcome::Busy.retryable());
        assert!(CallOutcome::NoAnswer.retryable());
        for o in [CallOutcome::Completed, CallOutcome::Rejected, CallOutcome::NotFound, CallOutcome::Unavailable, CallOutcome::Failed] {
            assert!(!o.retryable());
        }
    }
}
