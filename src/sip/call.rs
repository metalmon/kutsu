//! Per-call ezk driving loop (runs on the SIP runtime thread).

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use bytes::Bytes;
use bytesstr::BytesStr;
use ezk_rtc::OpenSslContext;
use ezk_rtc::rtp_session::SendRtpPacket;
use ezk_rtc::sdp::{Codec, Codecs, Direction, MediaType, SdpSession};
use ezk_sip_auth::{DigestAuthenticator, DigestCredentials, DigestUser};
use ezk_sip_core::Endpoint;
use ezk_sip_types::Method;
use ezk_sip_types::header::typed::Contact;
use ezk_sip_types::host::{Host, HostPort};
use ezk_sip_types::uri::{NameAddr, SipUri};
use ezk_sip_ua::dialog::DialogLayer;
use ezk_sip_ua::{CallEvent, MediaEvent, OutboundCall, RtcMediaBackend};
use tokio::sync::{mpsc, oneshot};

use crate::config::SipConfig;
use crate::sip::{
    CallOutcome, G711Kind, NegotiatedCodec, SipCall, SipError, SipEvent, TermReason,
    UplinkQualityShared, UplinkStats, g711_kind_from_pt, plain_rtp_g711_config,
};

/// Build the shared endpoint with one UDP transport. `add_allow` is MANDATORY —
/// an empty Allow header panics ezk on serialization ("tried to use empty vector").
/// `local_port` of `0` binds an OS-chosen ephemeral port; a non-zero value pins
/// the SIP source port (required by trunks that authorize by static `IP:port`).
pub(crate) async fn build_endpoint(
    local_ip: IpAddr,
    local_port: u16,
) -> Result<(Endpoint, SocketAddr), SipError> {
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
        .bind_udp(SocketAddr::new(local_ip, local_port))
        .await
        .map_err(SipError::Bind)?;
    let bound = transport.bound();
    Ok((builder.build(), bound))
}

/// Build the SIP-URI host:port for request/To/From URIs and the registrar.
/// With a configured `domain` (a DNS name the stack resolves), the URIs carry
/// that domain (required by trunks that route/authorize by SIP domain, e.g.
/// Novofon `sip.novofon.ru`); otherwise they fall back to the numeric `server`
/// host (backwards-compatible IP behaviour). The port always comes from `server`.
pub(crate) fn uri_host_port(server: SocketAddr, domain: Option<&str>) -> HostPort {
    match domain {
        Some(d) if !d.is_empty() => HostPort {
            host: Host::Name(BytesStr::from(d)),
            port: Some(server.port()),
        },
        _ => server.into(),
    }
}

pub(crate) fn make_auth(cfg: &SipConfig) -> DigestAuthenticator {
    let mut creds = DigestCredentials::new();
    creds.set_default(DigestUser::new(
        cfg.username.clone(),
        cfg.password.clone().into_bytes(),
    ));
    DigestAuthenticator::new(creds)
}

fn build_media(local_ip: IpAddr) -> Result<RtcMediaBackend, SipError> {
    let mut sdp = SdpSession::new(
        OpenSslContext::try_new().map_err(|e| SipError::Media(e.to_string()))?,
        local_ip,
        plain_rtp_g711_config(),
    );
    let lm = sdp
        .add_local_media(
            Codecs::new(MediaType::Audio)
                .with_codec(Codec::PCMU)
                .with_codec(Codec::PCMA),
            Direction::SendRecv,
        )
        .ok_or(SipError::Media("add_local_media failed".into()))?;
    sdp.add_media(lm, Direction::SendRecv, None, None);
    Ok(RtcMediaBackend::new(sdp))
}

fn make_uris(
    uri_host: HostPort,
    from_user: &str,
    bound: SocketAddr,
    number: &str,
) -> (NameAddr, Contact, SipUri) {
    let id = NameAddr::uri(SipUri::new(uri_host.clone()).user(BytesStr::from(from_user)));
    let contact = Contact::new(NameAddr::uri(
        SipUri::new(bound.into()).user(BytesStr::from(from_user)),
    ));
    let target = SipUri::new(uri_host).user(BytesStr::from(number));
    (id, contact, target)
}

fn neg_codec(c: &ezk_sip_ua::Codec) -> NegotiatedCodec {
    NegotiatedCodec {
        pt: c.pt,
        kind: g711_kind_from_pt(c.pt).unwrap_or(G711Kind::Ulaw),
        ptime_ms: 20,
    }
}

/// Drive one outbound call to completion. Owns all `!Send` ezk state; runs on
/// the SIP runtime thread.
pub(crate) async fn run_call(
    endpoint: Endpoint,
    cfg: SipConfig,
    server: SocketAddr,
    bound: SocketAddr,
    number: String,
    call_id: String,
    reply: oneshot::Sender<Result<SipCall, SipError>>,
) {
    let local_ip = bound.ip();
    let media = match build_media(local_ip) {
        Ok(m) => m,
        Err(e) => {
            let _ = reply.send(Err(e));
            return;
        }
    };
    let uri_host = uri_host_port(server, cfg.sip_domain.as_deref());
    let (id, contact, target) = make_uris(uri_host, cfg.from_user(), bound, &number);

    let mut outbound =
        match OutboundCall::make(endpoint, make_auth(&cfg), id, contact, target, media).await {
            Ok(o) => o,
            Err(e) => {
                let _ = reply.send(Err(map_make_err(&e)));
                return;
            }
        };

    // Dialog established -> hand the caller a live SipCall handle.
    let (ev_tx, ev_rx) = mpsc::channel::<SipEvent>(16);
    let (in_tx, in_rx) = mpsc::channel::<Bytes>(64);
    let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(64);
    let (hup_tx, mut hup_rx) = oneshot::channel::<()>();
    let uplink_quality = UplinkQualityShared::new();
    let handle = SipCall::from_parts(
        call_id,
        ev_rx,
        in_rx,
        out_tx,
        hup_tx,
        uplink_quality.clone(),
    );
    if reply.send(Ok(handle)).is_err() {
        let _ = outbound.cancel().await;
        return;
    }

    let unacked = match outbound.wait_for_completion().await {
        Ok(u) => u,
        Err(e) => {
            let (outcome, detail) = classify_completion_err(&e);
            let _ = ev_tx
                .send(SipEvent::Terminated(TermReason::Failed { outcome, detail }))
                .await;
            return;
        }
    };
    let mut call = match unacked.finish().await {
        Ok(c) => c,
        Err(e) => {
            let (outcome, detail) = classify_completion_err(&e);
            let _ = ev_tx
                .send(SipEvent::Terminated(TermReason::Failed { outcome, detail }))
                .await;
            return;
        }
    };

    // From here on the INVITE has already completed successfully (200 OK), so every
    // TermReason::Failed { outcome: CallOutcome::Failed, .. } below is post-answer.
    // Busy/NoAnswer are pre-answer INVITE outcomes only (see classify_completion_err
    // above); a connected call that drops is classified Failed here. If this ever
    // needs to emit Busy, the engine busy-retry wiring (engine.rs) must be revisited.
    // Capture the RTP sender/receiver + codec from the first media events.
    let mut sender = None;
    let mut receiver = None;
    let mut codec = None;
    let media_deadline = tokio::time::sleep(std::time::Duration::from_secs(10));
    tokio::pin!(media_deadline);
    while sender.is_none() || receiver.is_none() {
        tokio::select! {
            r = call.run() => match r {
                Ok(CallEvent::Internal(e)) => {
                    if call.handle_internal_event(e).await.is_err() {
                        let _ = ev_tx
                            .send(SipEvent::Terminated(TermReason::Failed {
                                outcome: CallOutcome::Failed,
                                detail: "internal".into(),
                            }))
                            .await;
                        return;
                    }
                }
                Ok(CallEvent::Media(MediaEvent::SenderAdded { sender: s, codec: c })) => {
                    codec = Some(neg_codec(&c));
                    sender = Some(s);
                }
                Ok(CallEvent::Media(MediaEvent::ReceiverAdded { receiver: r, .. })) => {
                    receiver = Some(r);
                }
                Ok(CallEvent::Terminated) => {
                    let _ = ev_tx
                        .send(SipEvent::Terminated(TermReason::RemoteHangup))
                        .await;
                    return;
                }
                Err(e) => {
                    let _ = ev_tx
                        .send(SipEvent::Terminated(TermReason::Failed {
                            outcome: CallOutcome::Failed,
                            detail: e.to_string(),
                        }))
                        .await;
                    return;
                }
            },
            _ = &mut media_deadline => {
                let _ = ev_tx
                    .send(SipEvent::Terminated(TermReason::Failed {
                        outcome: CallOutcome::Failed,
                        detail: "answered but media was not negotiated in time".into(),
                    }))
                    .await;
                let _ = call.terminate().await;
                return;
            }
        }
    }
    let mut sender = sender.unwrap();
    let mut receiver = receiver.unwrap();
    let codec = codec.unwrap();
    let pt = codec.pt;
    let _ = ev_tx.send(SipEvent::Answered { codec }).await;

    // Outbound forwarder on its own local task (proven pattern from the spike:
    // RtpSender::send waits for transport-Connected, which the main call.run()
    // loop supplies — so the two must run concurrently).
    let out_task = tokio::task::spawn_local(async move {
        // RTP media time = a MONOTONIC 20 ms/frame clock, NOT `Instant::now()`.
        // ezk derives the packet's RTP timestamp from `media_time`; the downlink
        // is a continuous 20 ms G.711 stream (the pacer emits one frame — audio
        // or silence — per tick), so the timestamp must advance by exactly one
        // frame (160 samples @ 8 kHz) per packet. Stamping `Instant::now()` per
        // packet folds the pacer/scheduler jitter into the RTP timestamps, which
        // a tolerant LAN PBX ignored but a real carrier's jitter buffer renders
        // as stutter/dropouts. Anchoring to a fixed clock removes that.
        //
        // `send_at` also defaults to `media_time`; with the pacer's
        // `MissedTickBehavior::Delay` the real cadence only ever drifts *slower*
        // than 20 ms, so `media_time` stays at/behind `now` and packets still go
        // out immediately (no added latency).
        let frame = Duration::from_millis(20);
        let mut media_time: Option<Instant> = None;
        while let Some(payload) = out_rx.recv().await {
            let mt = match media_time {
                Some(prev) => prev + frame,
                None => Instant::now(),
            };
            media_time = Some(mt);
            if sender
                .send(SendRtpPacket::new(mt, pt, payload))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Inbound receiver on its own local task. ezk-rtc's inbound jitter buffer is
    // a tiny fixed 10 ms window (`RX_BUFFER_DURATION` in ezk-rtc's queue.rs) that
    // drops packets when the consumer falls behind. Draining `receiver.recv()` in
    // a dedicated tight loop — rather than sharing a `select!` with `call.run()`,
    // where it is polled only once per loop iteration — keeps the buffer emptied
    // promptly so packets don't age out of the window (≈14% uplink loss observed
    // otherwise, all inside ezk, not on the wire). Mirrors the outbound `out_task`.
    let in_task = tokio::task::spawn_local(async move {
        let mut uplink_stats = UplinkStats::new();
        while let Some(rtp) = receiver.recv().await {
            uplink_stats.observe(rtp.sequence_number.0);
            uplink_quality.publish(&uplink_stats.snapshot());
            // Drop-on-full: never block the receive loop on a stalled bridge.
            let _ = in_tx.try_send(rtp.payload);
        }
    });

    // Main loop: pump SIP/media and honour hangup. Inbound RTP is drained by
    // `in_task` above; outbound by `out_task`.
    let reason = loop {
        tokio::select! {
            r = call.run() => match r {
                Ok(CallEvent::Internal(e)) => {
                    if call.handle_internal_event(e).await.is_err() {
                        break TermReason::Failed {
                            outcome: CallOutcome::Failed,
                            detail: "internal".into(),
                        };
                    }
                }
                Ok(CallEvent::Media(_)) => {}
                Ok(CallEvent::Terminated) => break TermReason::RemoteHangup,
                Err(e) => break TermReason::Failed {
                    outcome: CallOutcome::Failed,
                    detail: e.to_string(),
                },
            },
            _ = &mut hup_rx => {
                let _ = call.terminate().await;
                break TermReason::LocalHangup;
            }
        }
    };
    out_task.abort();
    in_task.abort();
    let _ = ev_tx.send(SipEvent::Terminated(reason)).await;
}

fn map_make_err<A: std::fmt::Debug, M: std::fmt::Debug>(
    e: &ezk_sip_ua::MakeCallError<M, A>,
) -> SipError {
    use ezk_sip_ua::MakeCallError;
    match e {
        MakeCallError::Failed(line) => SipError::Invite(format!("{line:?}")),
        MakeCallError::Core(ezk_sip_core::Error::RequestTimedOut) => SipError::NotAnswered,
        other => SipError::Media(format!("{other:?}")),
    }
}

/// Classify a post-INVITE completion failure (`OutboundCall::wait_for_completion`
/// / `UnacknowledgedCall::finish`) into a `CallOutcome`. Note this is a distinct
/// error type from `MakeCallError` above (`MakeCallCompletionError` has no `Auth`
/// variant and adds `MissingSdpInResponse`), even though both carry a `StatusLine`
/// on the same-shaped `Failed` variant.
fn classify_completion_err<M: std::fmt::Debug>(
    e: &ezk_sip_ua::MakeCallCompletionError<M>,
) -> (CallOutcome, String) {
    use crate::sip::outcome_from_status;
    use ezk_sip_ua::MakeCallCompletionError;
    match e {
        MakeCallCompletionError::Failed(line) => {
            let code = line.code.into_u16();
            (outcome_from_status(code), format!("{line:?}"))
        }
        MakeCallCompletionError::Core(ezk_sip_core::Error::RequestTimedOut) => {
            (CallOutcome::NoAnswer, "request timed out".into())
        }
        other => (CallOutcome::Failed, format!("{other:?}")),
    }
}
