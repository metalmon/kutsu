//! Audio bridge between the SIP RTP session and the Gemini Live WebSocket.
//!
//! Phone-side audio is G.711 mu-law/PCM at 8kHz; Gemini Live expects PCM16
//! at 16kHz in and produces PCM16 at 24kHz out. This module depacketizes
//! RTP, decodes/encodes mu-law, and resamples in both directions.
//!
//! Not yet implemented.

mod g711;
mod resample;
mod pace;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::gemini_live::Event;
use crate::sip::G711Kind;

/// Everything the bridge needs for one call. The engine builds this from a
/// `SipCall` (phone side) and a gemini `Session` (gemini side).
pub struct BridgePorts {
    pub codec: G711Kind,
    /// Inbound G.711 payloads from the phone (remote -> us).
    pub phone_in: mpsc::Receiver<Bytes>,
    /// Outbound G.711 payloads to the phone (us -> remote).
    pub phone_out: mpsc::Sender<Bytes>,
    /// Uplink sink to Gemini (PCM16 @ 16 kHz).
    pub gemini_in: mpsc::Sender<Vec<i16>>,
    /// Downlink + control events from Gemini.
    pub gemini_events: mpsc::Receiver<Event>,
    /// Non-audio events forwarded to the engine.
    pub events_out: mpsc::Sender<Event>,
}

/// Why the bridge stopped.
#[derive(Debug)]
pub enum BridgeEnd {
    /// The phone side ended (RTP receiver closed, or send to phone failed).
    PhoneClosed,
    /// The Gemini side ended (event stream closed).
    GeminiClosed,
}

/// Bridge one call until a side ends. Does not hang up or join either side —
/// the engine owns lifecycle. Cancel-safe at its await points.
pub async fn run(ports: BridgePorts) -> BridgeEnd {
    let BridgePorts {
        codec,
        mut phone_in,
        phone_out,
        gemini_in,
        mut gemini_events,
        events_out,
    } = ports;

    // Uplink: transparent, continuous, never-manipulated forward (spec §2).
    // Separate task so its backpressure `await` can never stall the downlink.
    let uplink = tokio::spawn(async move {
        while let Some(payload) = phone_in.recv().await {
            let pcm8 = g711::decode(codec, &payload);
            let pcm16 = resample::up_8k_16k(&pcm8);
            if gemini_in.send(pcm16).await.is_err() {
                break; // gemini sink closed
            }
        }
    });
    tokio::pin!(uplink);

    // Downlink: 24 kHz buffer + 20 ms pacer + barge-in, on this task.
    let mut downlink = pace::Downlink::new();
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(20));

    loop {
        tokio::select! {
            _ = &mut uplink => {
                // Uplink task ended: the phone stopped feeding us (hang-up).
                return BridgeEnd::PhoneClosed;
            }
            ev = gemini_events.recv() => match ev {
                Some(Event::OutputAudio(pcm24)) => downlink.push(&pcm24),
                Some(Event::Interrupted) => downlink.clear(),
                Some(other) => {
                    if events_out.send(other).await.is_err() {
                        // Engine dropped its event receiver; keep bridging audio.
                    }
                }
                None => return BridgeEnd::GeminiClosed,
            },
            _ = ticker.tick() => {
                let pcm8 = downlink.next_frame();
                let payload = g711::encode(codec, &pcm8);
                if phone_out.send(Bytes::from(payload)).await.is_err() {
                    return BridgeEnd::PhoneClosed;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gemini_live::Event;
    use crate::sip::G711Kind;
    use bytes::Bytes;
    use tokio::sync::mpsc;

    // Build ports + keep the far ends for the test to drive.
    struct Ends {
        phone_in_tx: mpsc::Sender<Bytes>,
        phone_out_rx: mpsc::Receiver<Bytes>,
        gemini_in_rx: mpsc::Receiver<Vec<i16>>,
        gemini_events_tx: mpsc::Sender<Event>,
        events_out_rx: mpsc::Receiver<Event>,
    }

    fn wire(codec: G711Kind) -> (BridgePorts, Ends) {
        let (phone_in_tx, phone_in) = mpsc::channel(64);
        let (phone_out, phone_out_rx) = mpsc::channel(64);
        let (gemini_in, gemini_in_rx) = mpsc::channel(64);
        let (gemini_events_tx, gemini_events) = mpsc::channel(64);
        let (events_out, events_out_rx) = mpsc::channel(64);
        (
            BridgePorts { codec, phone_in, phone_out, gemini_in, gemini_events, events_out },
            Ends { phone_in_tx, phone_out_rx, gemini_in_rx, gemini_events_tx, events_out_rx },
        )
    }

    #[tokio::test(start_paused = true)]
    async fn uplink_forwards_every_frame_transparently() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        // Send 5 frames of 160 G.711 silence bytes.
        for _ in 0..5 {
            ends.phone_in_tx.send(Bytes::from(vec![0xFFu8; 160])).await.unwrap();
        }
        // Each becomes one PCM16 16 kHz frame of 320 samples, in order, none dropped.
        for _ in 0..5 {
            let frame = ends.gemini_in_rx.recv().await.unwrap();
            assert_eq!(frame.len(), 320);
        }
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn downlink_paces_frames_to_phone() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        // Push 60 ms of loud audio.
        ends.gemini_events_tx.send(Event::OutputAudio(vec![8000i16; 480 * 3])).await.unwrap();
        // Advance three 20 ms ticks; expect three 160-byte frames.
        for _ in 0..3 {
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
            let frame = ends.phone_out_rx.recv().await.unwrap();
            assert_eq!(frame.len(), 160);
        }
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn barge_in_silences_the_phone() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        ends.gemini_events_tx.send(Event::OutputAudio(vec![8000i16; 480 * 4])).await.unwrap();
        ends.gemini_events_tx.send(Event::Interrupted).await.unwrap();
        // Drain several 20 ms ticks. The `select!` picks a ready branch at random
        // each iteration, so a single `yield_now` cannot deterministically guarantee
        // that BOTH `OutputAudio` and `Interrupted` have been processed before the
        // first tick fires. Instead, advance many ticks: once `Interrupted` has been
        // processed (which it must be, eventually, since the mpsc channel is FIFO and
        // the loop keeps running), the buffer is cleared and no further audio arrives,
        // so every frame from that point on is deterministically silence (modulo the
        // downsampler's ~8-sample filter ring-out). Asserting on a frame several ticks
        // in — well past both the event processing and the ring-out — is robust
        // regardless of event/tick interleaving order.
        let mut last_frame = Vec::new();
        for _ in 0..8 {
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
            last_frame = ends.phone_out_rx.recv().await.unwrap().to_vec();
        }
        let pcm = g711::decode(G711Kind::Ulaw, &last_frame);
        assert!(pcm.iter().all(|&s| s.abs() < 64), "expected silence after barge-in");
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn non_audio_events_forwarded_to_engine() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        ends.gemini_events_tx.send(Event::TurnComplete).await.unwrap();
        let got = ends.events_out_rx.recv().await.unwrap();
        assert!(matches!(got, Event::TurnComplete));
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn ends_when_gemini_closes() {
        let (ports, ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        // Drop the gemini events sender -> gemini_events.recv() returns None.
        drop(ends.gemini_events_tx);
        let end = h.await.unwrap();
        assert!(matches!(end, BridgeEnd::GeminiClosed));
    }
}
