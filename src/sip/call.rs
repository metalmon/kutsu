//! Per-call ezk driving loop (runs on the SIP runtime thread).

use std::net::{IpAddr, SocketAddr};

use ezk_sip_core::Endpoint;
use ezk_sip_types::Method;
use ezk_sip_ua::dialog::DialogLayer;

use crate::sip::SipError;

/// Build the shared endpoint with one UDP transport. `add_allow` is MANDATORY —
/// an empty Allow header panics ezk on serialization ("tried to use empty vector").
pub(crate) async fn build_endpoint(local_ip: IpAddr) -> Result<(Endpoint, SocketAddr), SipError> {
    let mut builder = Endpoint::builder();
    builder.add_layer(DialogLayer::default());
    for m in [
        Method::INVITE,
        Method::ACK,
        Method::CANCEL,
        Method::BYE,
        Method::OPTIONS,
    ] {
        builder.add_allow(m);
    }
    let transport = builder
        .bind_udp(SocketAddr::new(local_ip, 0))
        .await
        .map_err(SipError::Bind)?;
    let bound = transport.bound();
    Ok((builder.build(), bound))
}
