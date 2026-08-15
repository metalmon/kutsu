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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::gemini_live::Event;
use crate::sip::G711Kind;

/// Shared, lock-free call-quality counters published by the downlink loop and
/// read by the engine (e.g. for periodic state snapshots) without blocking
/// either side.
#[derive(Default)]
pub struct QualityShared {
    underruns: AtomicU64,
    starved_ms: AtomicU64,
    max_gap_ms: AtomicU64,
}

impl QualityShared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> crate::state::CallQuality {
        crate::state::CallQuality {
            underruns: self.underruns.load(Ordering::Relaxed),
            starved_ms: self.starved_ms.load(Ordering::Relaxed),
            max_gap_ms: self.max_gap_ms.load(Ordering::Relaxed),
            ..Default::default()
        }
    }
}

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
    /// Downlink prefill target before (re)starting playout, in ms.
    pub prebuffer_ms: u32,
    /// Downlink refill target after a mid-turn underrun, in ms.
    pub resume_ms: u32,
    /// Shared call-quality counters, published once per pacer tick.
    pub quality: Arc<QualityShared>,
    /// Call id, used to name uplink dump files.
    pub call_id: String,
    /// If set, directory to write per-call uplink WAV dumps into.
    pub uplink_dump: Option<std::path::PathBuf>,
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
        prebuffer_ms,
        resume_ms,
        quality,
        call_id,
        uplink_dump,
    } = ports;

    // Uplink: transparent, continuous, never-manipulated forward (spec §2).
    // Separate task so its backpressure `await` can never stall the downlink.
    let uplink = tokio::spawn(async move {
        let mut dump8 = uplink_dump.as_ref().and_then(|dir| {
            crate::audio_file::Pcm16Writer::create(&dir.join(format!("{call_id}-uplink-8k.wav")), 8000).ok()
        });
        let mut dump16 = uplink_dump.as_ref().and_then(|dir| {
            crate::audio_file::Pcm16Writer::create(&dir.join(format!("{call_id}-uplink-16k.wav")), 16000).ok()
        });
        while let Some(payload) = phone_in.recv().await {
            let pcm8 = g711::decode(codec, &payload);
            let pcm16 = resample::up_8k_16k(&pcm8);
            if let Some(w) = dump8.as_mut() { let _ = w.write(&pcm8); }
            if let Some(w) = dump16.as_mut() { let _ = w.write(&pcm16); }
            if gemini_in.send(pcm16).await.is_err() {
                break; // gemini sink closed
            }
        }
        if let Some(w) = dump8.take() { let _ = w.finalize(); }
        if let Some(w) = dump16.take() { let _ = w.finalize(); }
    });
    tokio::pin!(uplink);

    // Downlink: 24 kHz buffer + 20 ms pacer + barge-in, on this task.
    let mut downlink = pace::Downlink::new(prebuffer_ms, resume_ms);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(20));

    // Inter-chunk gap clock: wall-clock time between consecutive OutputAudio
    // events within a turn, tracked for the `max_gap_ms` quality metric.
    let mut last_audio: Option<tokio::time::Instant> = None;
    let mut max_gap_ms = 0u64;

    // Every loop exit goes through `break` so a single `uplink.abort()` below
    // covers every path -- dropping a JoinHandle detaches it (leak) instead of
    // cancelling it, so we must abort explicitly rather than just `return`.
    let end = loop {
        tokio::select! {
            _ = &mut uplink => {
                // Uplink task ended: the phone stopped feeding us (hang-up).
                break BridgeEnd::PhoneClosed;
            }
            ev = gemini_events.recv() => match ev {
                Some(Event::OutputAudio(pcm24)) => {
                    downlink.set_expecting(true);
                    let now = tokio::time::Instant::now();
                    if let Some(prev) = last_audio {
                        let gap = now.duration_since(prev).as_millis() as u64;
                        if gap > max_gap_ms {
                            max_gap_ms = gap;
                        }
                    }
                    last_audio = Some(now);
                    downlink.push(&pcm24);
                }
                Some(Event::Interrupted) => {
                    downlink.set_expecting(false);
                    last_audio = None;
                    downlink.clear();
                }
                Some(Event::TurnComplete) => {
                    downlink.set_expecting(false);
                    last_audio = None;
                    if events_out.send(Event::TurnComplete).await.is_err() {
                        // Engine dropped its event receiver; keep bridging audio.
                    }
                }
                Some(other) => {
                    if events_out.send(other).await.is_err() {
                        // Engine dropped its event receiver; keep bridging audio.
                    }
                }
                None => break BridgeEnd::GeminiClosed,
            },
            _ = ticker.tick() => {
                let pcm8 = downlink.next_frame();
                let payload = g711::encode(codec, &pcm8);
                quality.underruns.store(downlink.underruns(), Ordering::Relaxed);
                quality.starved_ms.store(downlink.starved_ms(), Ordering::Relaxed);
                quality.max_gap_ms.store(max_gap_ms, Ordering::Relaxed);
                if phone_out.send(Bytes::from(payload)).await.is_err() {
                    break BridgeEnd::PhoneClosed;
                }
            }
        }
    };
    // Harmless no-op if the uplink task already finished (the `&mut uplink`
    // branch above); required to stop it on the other two exit paths.
    uplink.abort();
    end
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
            BridgePorts {
                codec,
                phone_in,
                phone_out,
                gemini_in,
                gemini_events,
                events_out,
                prebuffer_ms: 140,
                resume_ms: 60,
                quality: QualityShared::new(),
                call_id: "c1".to_string(),
                uplink_dump: None,
            },
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

        // `tokio::time::interval`'s very first `tick()` resolves immediately
        // (no time needs to pass). Force + discard that free tick now, on an
        // empty buffer (deterministically silence), before pushing anything,
        // so every tick from here on sits on the regular 20 ms cadence --
        // same reasoning as `barge_in_silences_the_phone`.
        tokio::task::yield_now().await;
        let _ = ends.phone_out_rx.recv().await.unwrap();

        // Push 200 ms of loud audio -- above wire()'s 140 ms prefill target,
        // so playout actually starts instead of holding for prefill.
        ends.gemini_events_tx.send(Event::OutputAudio(vec![8000i16; 480 * 10])).await.unwrap();
        tokio::task::yield_now().await;

        // Advance three 20 ms ticks; expect three 160-byte frames carrying
        // real (non-silent) audio -- proves playout is actually pacing
        // buffered audio to the phone, not just holding silence for prefill.
        for _ in 0..3 {
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
            let frame = ends.phone_out_rx.recv().await.unwrap();
            assert_eq!(frame.len(), 160);
            let pcm = g711::decode(G711Kind::Ulaw, &frame);
            assert!(pcm.iter().any(|&s| s.abs() > 1000), "expected paced audio, not silence");
        }
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn barge_in_silences_the_phone() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));

        // `tokio::time::interval`'s very first `tick()` resolves immediately (no
        // time needs to pass), independent of any events. Force + discard that
        // free tick now, on an empty buffer (deterministically silence), before
        // pushing anything, so every tick from here on sits on the regular 20 ms
        // cadence and `select!` only has one ready branch at a time below.
        tokio::task::yield_now().await;
        let _ = ends.phone_out_rx.recv().await.unwrap();

        // Push a large burst (~400 ms) of loud audio.
        ends.gemini_events_tx.send(Event::OutputAudio(vec![8000i16; 480 * 20])).await.unwrap();
        // The next regular tick is a full 20 ms away (Pending) -> `select!` can
        // only take the `gemini_events` branch, so this push is processed
        // deterministically before any tick fires.
        tokio::task::yield_now().await;

        // Advance one tick and confirm audio is actively playing: proves the
        // burst is really queued (buffer far from drained), not a stale/empty
        // pacer that would be silent regardless of barge-in working.
        tokio::time::advance(std::time::Duration::from_millis(20)).await;
        let frame = ends.phone_out_rx.recv().await.unwrap();
        let pcm = g711::decode(G711Kind::Ulaw, &frame);
        assert!(pcm.iter().any(|&s| s.abs() > 1000), "expected audio to be playing before barge-in");

        // Barge-in while ~18 more frames (360 ms) of loud audio remain buffered.
        ends.gemini_events_tx.send(Event::Interrupted).await.unwrap();
        // Same reasoning: the next tick is a full 20 ms away, so this is
        // processed deterministically before any further tick -- `clear()` runs
        // before the next `next_frame()` call.
        tokio::task::yield_now().await;

        // The tick immediately after `clear()` still carries the downsampler's
        // FIR ring-out (~8 samples of decaying signal at the head of the frame;
        // see pace.rs's own `clear_flushes_pending_audio` test, which discards
        // this exact frame for the same reason). Discard it, then check the next
        // one. With no further audio queued, that second frame must be clean
        // silence. Without a working `clear()` the buffer would still hold ~17
        // frames of loud audio at this point, so it would still be loud --
        // silence here is proof `clear()` actually dropped the pending audio,
        // not a vacuous check (the buffer is nowhere near draining naturally).
        tokio::time::advance(std::time::Duration::from_millis(20)).await;
        let _ = ends.phone_out_rx.recv().await.unwrap(); // ring-out frame, discarded

        tokio::time::advance(std::time::Duration::from_millis(20)).await;
        let frame = ends.phone_out_rx.recv().await.unwrap();
        let pcm = g711::decode(G711Kind::Ulaw, &frame);
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
    async fn quality_shared_records_underruns_while_expecting() {
        let q = super::QualityShared::new();
        let (mut ports, mut ends) = wire(G711Kind::Ulaw);
        ports.prebuffer_ms = 0; // disable prefill so the test drains deterministically
        ports.resume_ms = 0;
        ports.quality = q.clone();
        let h = tokio::spawn(run(ports));
        // Model turn starts, one 20 ms chunk, then goes quiet mid-turn (no TurnComplete).
        ends.gemini_events_tx.send(Event::OutputAudio(vec![8000i16; 480])).await.unwrap();
        tokio::task::yield_now().await;
        // Drain several ticks: after the one buffered frame, empty-while-expecting = underruns.
        for _ in 0..4 {
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
            let _ = ends.phone_out_rx.recv().await;
        }
        assert!(q.snapshot().underruns >= 1, "expected underruns recorded while expecting");
        drop(ends);
        let _ = h.await;
    }

    #[tokio::test]
    async fn uplink_dump_writes_both_wavs() {
        let dir = std::env::temp_dir().join(format!("kutsu-uplink-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let (mut ports, ends) = wire(G711Kind::Ulaw);
        ports.call_id = "c1".to_string();
        ports.uplink_dump = Some(dir.clone());
        let h = tokio::spawn(run(ports));

        for _ in 0..5 {
            ends.phone_in_tx.send(Bytes::from(vec![0xFFu8; 160])).await.unwrap();
        }
        // Close only the phone side; the uplink task drains the buffered
        // frames, finalizes both WAV writers, and ends -- which the bridge
        // observes as `PhoneClosed` and returns. Keep `gemini_events_tx`
        // (inside `ends`) alive until then so the bridge doesn't race to a
        // `GeminiClosed` exit first and abort the uplink task mid-drain.
        drop(ends.phone_in_tx);
        let _ = h.await;

        let eight = dir.join("c1-uplink-8k.wav");
        let sixteen = dir.join("c1-uplink-16k.wav");
        assert!(eight.exists(), "8k dump missing");
        assert!(sixteen.exists(), "16k dump missing");
        assert!(!crate::audio_file::read_pcm16(&eight, 8000).unwrap().is_empty());
        assert!(!crate::audio_file::read_pcm16(&sixteen, 16000).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
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
