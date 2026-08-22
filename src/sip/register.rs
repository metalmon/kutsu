//! REGISTER support: bind the account to the trunk (digest auth) and keep the
//! binding refreshed for the process lifetime.
//!
//! Required by registration-based trunks (login/password), e.g. Novofon's
//! standard SIP account. Pure IP-authorized trunks leave `register` off and
//! never touch this path.

use std::net::SocketAddr;
use std::time::Duration;

use ezk_sip_core::Endpoint;
use ezk_sip_types::uri::SipUri;
use ezk_sip_ua::{RegistrarConfig, Registration};

use crate::config::SipConfig;
use crate::sip::SipError;
use crate::sip::call::{make_auth, uri_host_port};

/// Send an initial REGISTER (handling the `401`/`407` digest challenge via the
/// same credentials used for `INVITE`) and, on success, return the live
/// [`Registration`]. ezk spawns a background task that refreshes the binding
/// until the handle is dropped, so the caller MUST keep it alive for as long as
/// calls should be placeable; dropping it sends an unregister.
///
/// `server` supplies the port and the numeric fallback host; when
/// `cfg.sip_domain` is set it is the registrar/URI domain (resolved via DNS by
/// the stack), matching how the trunk expects to be addressed.
pub(crate) async fn start_registration(
    endpoint: Endpoint,
    cfg: &SipConfig,
    server: SocketAddr,
) -> Result<Registration, SipError> {
    let registrar = SipUri::new(uri_host_port(server, cfg.sip_domain.as_deref()));
    let mut config = RegistrarConfig::new(cfg.username.clone(), registrar);
    if let Some(secs) = cfg.register_expiry_secs {
        config = config.with_custom_expiry(Duration::from_secs(secs));
    }
    Registration::register(endpoint, config, make_auth(cfg))
        .await
        .map_err(|e| SipError::Register(e.to_string()))
}
