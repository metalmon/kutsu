//! THROWAWAY SPIKE (phase 1) — feasibility probe, NOT part of the shipping build.
//!
//! Question it answers: can `ezk-sip-ua` + `ezk-rtc` place one outbound SIP call
//! and exchange raw G.711 RTP in BOTH directions? Success = we send >0 RTP frames
//! to Asterisk extension 600 (echo) and receive >0 echoed frames back.
//!
//! This is intentionally disposable. Once the answer is known, the real `sip.rs`
//! gets designed properly (spec -> plan -> SDD) and this file is deleted.
//!
//! Requires the local WSL Asterisk test stand (see `dev/sip-test/README.md`),
//! reachable on the host LAN. It is `#[ignore]`d so `cargo test` never runs it by
//! accident. Run it explicitly (needs the OpenSSL toolchain feature that ezk-rtc
//! pulls in):
//!
//!   cargo test --features vendor-openssl --test sip_spike -- --ignored --nocapture
//!
//! Override defaults via env: KUTSU_SIP_SERVER (host:port), KUTSU_SIP_USER,
//! KUTSU_SIP_PASS, KUTSU_SIP_EXT.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use bytesstr::BytesStr;
use ezk_rtc::rtp_session::SendRtpPacket;
use ezk_rtc::sdp::{
    BundlePolicy, Codec, Codecs, Direction, MediaType, RtcpMuxPolicy, SdpSession, SdpSessionConfig,
    TransportType,
};
use ezk_rtc::{Mtu, OpenSslContext};
use ezk_sip_auth::{DigestAuthenticator, DigestCredentials, DigestUser};
use ezk_sip_core::Endpoint;
use ezk_sip_types::header::typed::Contact;
use ezk_sip_types::uri::{NameAddr, SipUri};
use ezk_sip_types::Method;
use ezk_sip_ua::dialog::DialogLayer;
use ezk_sip_ua::{CallEvent, MediaEvent, OutboundCall, RtcMediaBackend};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

#[tokio::test]
#[ignore = "requires the live WSL Asterisk test stand; run with --ignored"]
async fn sip_spike_echo_600_bidirectional_rtp() {
    // The whole probe is time-boxed: a broken environment should fail fast, not hang.
    tokio::time::timeout(Duration::from_secs(25), run_spike())
        .await
        .expect("spike timed out (no server? firewall blocking UDP?)");
}

async fn run_spike() {
    let server: SocketAddr = env_or("KUTSU_SIP_SERVER", "192.168.88.243:5060")
        .parse()
        .expect("KUTSU_SIP_SERVER must be host:port");
    let user = env_or("KUTSU_SIP_USER", "kutsu");
    let pass = env_or("KUTSU_SIP_PASS", "kutsupw");
    let ext = env_or("KUTSU_SIP_EXT", "600");

    // Discover the local IP that actually routes toward the server, so the SIP
    // Contact/Via and the SDP `c=` line advertise an address Asterisk can reach.
    let local_ip = {
        let probe = std::net::UdpSocket::bind(("0.0.0.0", 0)).unwrap();
        probe.connect(server).expect("cannot route to SIP server");
        probe.local_addr().unwrap().ip()
    };
    eprintln!("[spike] server={server} local_ip={local_ip} user={user} ext={ext}");

    // --- SIP endpoint: UDP transport + dialog layer -----------------------------
    let mut builder = Endpoint::builder();
    builder.add_layer(DialogLayer::default());
    // Declare our capabilities. Without at least one Allow method, the endpoint
    // inserts an empty Allow-header vector into REGISTER/INVITE, which panics on
    // serialization ("tried to use empty vector").
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
        .expect("bind UDP");
    let local_contact_addr = transport.bound();
    let endpoint = builder.build();

    // Fresh digest authenticator per request-flow (consumed by value).
    let mk_auth = || {
        let mut creds = DigestCredentials::new();
        creds.set_default(DigestUser::new(user.clone(), pass.clone().into_bytes()));
        DigestAuthenticator::new(creds)
    };

    // No REGISTER: for an OUTBOUND call Asterisk identifies the `kutsu` endpoint
    // by the From username and challenges the INVITE with digest auth directly.
    // (Registration is only needed to RECEIVE calls.) This also sidesteps the
    // AOR-name mismatch that 404'd the REGISTER, and matches how most trunks
    // accept originations.
    //
    //   From identity : sip:kutsu@<server>   (drives endpoint identification)
    //   Contact       : sip:kutsu@<our bound addr>   (where in-dialog requests go)
    //   Target        : sip:600@<server>     (echo extension)
    let id = NameAddr::uri(SipUri::new(server.into()).user(BytesStr::from(user.as_str())));
    let contact = Contact::new(NameAddr::uri(
        SipUri::new(local_contact_addr.into()).user(BytesStr::from(user.as_str())),
    ));
    let target = SipUri::new(server.into()).user(BytesStr::from(ext.as_str()));

    // --- Plain-RTP G.711 media backend -----------------------------------------
    let config = SdpSessionConfig {
        offer_transport: TransportType::Rtp, // RTP/AVP, no SRTP
        offer_ice: false,                    // classic SIP, no ICE
        offer_avpf: false,
        rtcp_mux_policy: RtcpMuxPolicy::Negotiate,
        bundle_policy: BundlePolicy::MaxCompat,
        mtu: Mtu::new(1400),
    };
    let mut sdp = SdpSession::new(OpenSslContext::try_new().unwrap(), local_ip, config);
    let local_media = sdp
        .add_local_media(
            Codecs::new(MediaType::Audio)
                .with_codec(Codec::PCMU)
                .with_codec(Codec::PCMA),
            Direction::SendRecv,
        )
        .expect("add_local_media");
    sdp.add_media(local_media, Direction::SendRecv, None, None);
    let media = RtcMediaBackend::new(sdp);

    // --- Place the call to extension 600 (echo) --------------------------------
    let mut outbound = OutboundCall::make(endpoint.clone(), mk_auth(), id, contact, target, media)
        .await
        .expect("INVITE failed");
    let unacked = outbound
        .wait_for_completion()
        .await
        .expect("no final response");
    let mut call = unacked.finish().await.expect("call not answered");
    eprintln!("[spike] call answered");

    // Drive the call event loop until the media backend hands us both the RTP
    // sender and receiver (emitted right after SDP negotiation).
    let mut sender = None;
    let mut receiver = None;
    let mut send_pt = 0u8;
    while sender.is_none() || receiver.is_none() {
        match call.run().await.expect("call.run") {
            CallEvent::Internal(e) => call.handle_internal_event(e).await.expect("internal event"),
            CallEvent::Media(MediaEvent::SenderAdded { sender: s, codec }) => {
                eprintln!("[spike] sender ready: codec={} pt={}", codec.name, codec.pt);
                send_pt = codec.pt;
                sender = Some(s);
            }
            CallEvent::Media(MediaEvent::ReceiverAdded { receiver: r, codec }) => {
                eprintln!("[spike] receiver ready: codec={} pt={}", codec.name, codec.pt);
                receiver = Some(r);
            }
            CallEvent::Terminated => panic!("call terminated before media was set up"),
        }
    }
    let mut sender = sender.unwrap();
    let mut receiver = receiver.unwrap();

    // Traffic generator: send 20 ms G.711 frames and count echoed frames for ~3 s.
    // Runs on its own task (RtpSender/RtpReceiver are Send); `call` stays on this
    // task and must keep being polled to pump media + drive the transport state.
    let traffic = tokio::spawn(async move {
        let payload = Bytes::from(vec![0xFFu8; 160]); // 20 ms of mu-law silence
        let mut sent = 0usize;
        let mut received = 0usize;
        let started = Instant::now();
        let window = Duration::from_secs(3);
        let mut ticker = tokio::time::interval(Duration::from_millis(20));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if started.elapsed() >= window {
                        break;
                    }
                    let pkt = SendRtpPacket::new(Instant::now(), send_pt, payload.clone());
                    if sender.send(pkt).await.is_err() {
                        break;
                    }
                    sent += 1;
                }
                pkt = receiver.recv() => {
                    match pkt {
                        Some(_) => received += 1,
                        None => break, // receiver closed
                    }
                }
            }
        }
        (sent, received)
    });
    tokio::pin!(traffic);

    // Keep the call/media loop alive until the traffic task is done.
    let (sent, received) = loop {
        tokio::select! {
            r = call.run() => match r.expect("call.run") {
                CallEvent::Internal(e) => call.handle_internal_event(e).await.expect("internal event"),
                CallEvent::Media(_) => {}
                CallEvent::Terminated => panic!("call terminated mid-traffic"),
            },
            joined = &mut traffic => break joined.expect("traffic task panicked"),
        }
    };

    eprintln!("[spike] RESULT: pt={send_pt} sent={sent} received={received}");
    let _ = call.terminate().await;

    assert!(sent > 0, "sent no RTP frames — outbound path broken");
    assert!(
        received > 0,
        "received no echoed RTP frames — bidirectional RTP FAILED (SDP/media reachability)"
    );
}
