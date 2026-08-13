//! Typed errors for the Gemini Live client.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("config error: {0}")]
    Config(String),
    #[error("network unstable: {0}")]
    NetworkUnstable(String),
    #[error("connect error: {0}")]
    Connect(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("session closed")]
    SessionClosed,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_stable() {
        assert_eq!(Error::Protocol("bad frame".into()).to_string(), "protocol error: bad frame");
        assert_eq!(Error::NetworkUnstable("rtt_p95=800ms".into()).to_string(),
                   "network unstable: rtt_p95=800ms");
        assert_eq!(Error::SessionClosed.to_string(), "session closed");
    }

    #[test]
    fn from_json_error_converts() {
        let e: Error = serde_json::from_str::<serde_json::Value>("{").unwrap_err().into();
        assert!(matches!(e, Error::Json(_)));
    }
}
