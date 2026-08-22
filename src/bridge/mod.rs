//! Audio bridge between the SIP RTP session and the Gemini Live WebSocket.
//!
//! Phone-side audio is G.711 mu-law/PCM at 8kHz; Gemini Live expects PCM16
//! at 16kHz in and produces PCM16 at 24kHz out. This module depacketizes
//! RTP, decodes/encodes mu-law, and resamples in both directions.
//!
//! Not yet implemented.

mod agc;
mod g711;
mod pace;
mod resample;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::gemini_live::Event;
use crate::sip::G711Kind;

/// How long after the last model audio frame the downlink still counts as
/// "active" for the end-of-call drain. Must exceed the largest normal
/// inter-chunk gap on the Gemini path so a momentary empty buffer mid-goodbye
/// doesn't end the drain early. Well under a second so a truly finished turn
/// tears down promptly.
const DOWNLINK_GRACE_MS: u64 = 600;

/// Shared, lock-free call-quality counters published by the downlink loop and
/// read by the engine (e.g. for periodic state snapshots) without blocking
/// either side.
#[derive(Default)]
pub struct QualityShared {
    underruns: AtomicU64,
    starved_ms: AtomicU64,
    max_gap_ms: AtomicU64,
    /// Running sum of squares of decoded uplink PCM16 samples, and the sample
    /// count — snapshot derives the RMS level. Published by the uplink task.
    uplink_sumsq: AtomicU64,
    uplink_samples: AtomicU64,
    /// True while the downlink is still delivering a turn: either the pacer
    /// holds buffered audio, or model audio arrived within the last grace
    /// window (so a brief inter-chunk gap doesn't read as "done"). Published
    /// every pacer tick; the engine polls it to drain the model's closing
    /// audio to the phone before hanging up.
    downlink_active: AtomicBool,
}

impl QualityShared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> crate::state::CallQuality {
        let sumsq = self.uplink_sumsq.load(Ordering::Relaxed);
        let samples = self.uplink_samples.load(Ordering::Relaxed);
        let uplink_rms = sumsq
            .checked_div(samples)
            .map_or(0, |ms| (ms as f64).sqrt() as u64);
        crate::state::CallQuality {
            underruns: self.underruns.load(Ordering::Relaxed),
            starved_ms: self.starved_ms.load(Ordering::Relaxed),
            max_gap_ms: self.max_gap_ms.load(Ordering::Relaxed),
            uplink_rms,
            ..Default::default()
        }
    }

    /// Publish whether the downlink is still delivering a turn (buffered audio
    /// or recent model audio). Called every pacer tick from `run`.
    fn set_downlink_active(&self, active: bool) {
        self.downlink_active.store(active, Ordering::Relaxed);
    }

    /// True while the downlink is still delivering a turn. The engine polls this
    /// on a model-initiated end to drain the goodbye before teardown.
    pub fn downlink_active(&self) -> bool {
        self.downlink_active.load(Ordering::Relaxed)
    }

    /// Accumulate one decoded uplink PCM16 frame into the running RMS level.
    /// Sum-of-squares in u64: max 32768^2 per sample, so `u64::MAX / 32768^2`
    /// ≈ 1.7e10 samples (~24 days at 8 kHz) before overflow — far beyond any call.
    fn add_uplink_level(&self, pcm: &[i16]) {
        let sq: u64 = pcm.iter().map(|&s| (s as i64 * s as i64) as u64).sum();
        self.uplink_sumsq.fetch_add(sq, Ordering::Relaxed);
        self.uplink_samples
            .fetch_add(pcm.len() as u64, Ordering::Relaxed);
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
    /// If set, directory to write per-call downlink WAV dumps into
    /// (`<call>-downlink-24k.wav` from Gemini + `<call>-downlink-8k.wav` to phone).
    pub downlink_dump: Option<std::path::PathBuf>,
    /// Adaptive gain applied to the uplink before it reaches Gemini.
    pub agc_cfg: crate::config::AgcConfig,
    /// Teardown signal from the engine: when it fires, the bridge stops and
    /// returns [`BridgeEnd::Cancelled`] (replaces aborting the task).
    pub cancel: tokio::sync::oneshot::Receiver<()>,
}

/// Why the bridge stopped.
#[derive(Debug, PartialEq, Eq)]
pub enum BridgeEnd {
    /// The phone side ended (RTP receiver closed, or send to phone failed).
    PhoneClosed,
    /// The Gemini side ended (event stream closed).
    GeminiClosed,
    /// The engine asked the bridge to stop (call teardown).
    Cancelled,
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
        downlink_dump,
        agc_cfg,
        mut cancel,
    } = ports;

    // Uplink: transparent, continuous, never-manipulated forward (spec §2).
    // Separate task so its backpressure `await` can never stall the downlink.
    let uplink_quality = quality.clone();
    // `call_id` is moved into the uplink `async move` below; keep a copy for
    // the downlink dump filenames on this task.
    let call_id_dl = call_id.clone();
    let uplink = tokio::spawn(async move {
        let mut dump8 = uplink_dump.as_ref().and_then(|dir| {
            let path = dir.join(format!("{call_id}-uplink-8k.wav"));
            match crate::audio_file::Pcm16Writer::create(&path, 8000) {
                Ok(w) => Some(w),
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "failed to open uplink dump");
                    None
                }
            }
        });
        let mut dump16 = uplink_dump.as_ref().and_then(|dir| {
            let path = dir.join(format!("{call_id}-uplink-16k.wav"));
            match crate::audio_file::Pcm16Writer::create(&path, 16000) {
                Ok(w) => Some(w),
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "failed to open uplink dump");
                    None
                }
            }
        });
        let mut agc = agc::Agc::new(agc_cfg);
        while let Some(payload) = phone_in.recv().await {
            let mut pcm8 = g711::decode(codec, &payload);
            // Normalise the (often quiet) real-trunk uplink toward a target level
            // so Gemini's speech detection fires reliably; everything downstream
            // (level metric, resample, dumps, VAD, Gemini) sees the result.
            agc.process(&mut pcm8);
            uplink_quality.add_uplink_level(&pcm8);
            let pcm16 = resample::up_8k_16k(&pcm8);
            // Synchronous `hound` writes on this async uplink task, between
            // `recv` and `gemini_in.send`: negligible under normal disk I/O,
            // and only exercised at all when dumping is enabled.
            if let Some(w) = dump8.as_mut() {
                let _ = w.write(&pcm8);
            }
            if let Some(w) = dump16.as_mut() {
                let _ = w.write(&pcm16);
            }
            if gemini_in.send(pcm16).await.is_err() {
                break; // gemini sink closed
            }
        }
        if let Some(w) = dump8.take() {
            let _ = w.finalize();
        }
        if let Some(w) = dump16.take() {
            let _ = w.finalize();
        }
    });
    tokio::pin!(uplink);

    // Downlink: 24 kHz buffer + 20 ms pacer + barge-in, on this task.
    let mut downlink = pace::Downlink::new(prebuffer_ms, resume_ms);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(20));
    // Media pacer: on a late tick, resume the 20 ms cadence from now rather than
    // firing a catch-up burst (the default `Burst`). Bursting bunches RTP egress
    // and makes the callee's jitter buffer stutter even at 0 packet loss.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Inter-chunk gap clock: wall-clock time between consecutive OutputAudio
    // events within a turn, tracked for the `max_gap_ms` quality metric.
    let mut last_audio: Option<tokio::time::Instant> = None;
    let mut max_gap_ms = 0u64;
    // Per-turn downlink latency: time from the first OutputAudio of a turn to the
    // first real (buffered) frame actually paced to the phone -- i.e. our
    // prebuffer/resume cost, the latency lever we control (vs the external
    // Gemini/proxy round-trip). Timed off the pace buffer's playing false->true
    // edge; reset each turn on TurnComplete/Interrupted.
    let mut turn_recv_at: Option<tokio::time::Instant> = None;
    let mut turn_onset_logged = false;
    let mut max_onset_ms = 0u64;
    let mut onset_count = 0u64;
    // Downlink SEND-timing diagnostic: wall-clock gap between consecutive pacer
    // ticks (should be ~20ms). A default `tokio::interval` uses
    // MissedTickBehavior::Burst, so a missed tick under load fires a catch-up
    // burst → uneven RTP egress → jittery playout even at 0 packet loss.
    let mut last_tick: Option<tokio::time::Instant> = None;
    let mut max_tick_gap_ms = 0u64;
    let mut late_ticks = 0u64;
    let mut tick_count = 0u64;

    // Optional downlink dump: `-downlink-24k.wav` is the raw model audio from
    // Gemini (pre-resample); `-downlink-8k.wav` is what we send to the phone
    // (post-resample). Both live on this task, so they finalize on loop exit.
    let open_dl = |suffix: &str, rate: u32| {
        downlink_dump.as_ref().and_then(|dir| {
            let path = dir.join(format!("{call_id_dl}-downlink-{suffix}.wav"));
            match crate::audio_file::Pcm16Writer::create(&path, rate) {
                Ok(w) => Some(w),
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "failed to open downlink dump");
                    None
                }
            }
        })
    };
    let mut dl_dump24 = open_dl("24k", 24000);
    let mut dl_dump8 = open_dl("8k", 8000);

    // Every loop exit goes through `break` so a single `uplink.abort()` below
    // covers every path -- dropping a JoinHandle detaches it (leak) instead of
    // cancelling it, so we must abort explicitly rather than just `return`.
    let end = loop {
        tokio::select! {
            _ = &mut cancel => {
                // Engine teardown: stop bridging (fires on send() or sender drop).
                break BridgeEnd::Cancelled;
            }
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
                    if turn_recv_at.is_none() {
                        turn_recv_at = Some(now);
                        turn_onset_logged = false;
                    }
                    if let Some(w) = dl_dump24.as_mut() { let _ = w.write(&pcm24); }
                    downlink.push(&pcm24);
                }
                Some(Event::Interrupted) => {
                    downlink.set_expecting(false);
                    last_audio = None;
                    turn_recv_at = None;
                    turn_onset_logged = false;
                    // Fade the buffered head to silence (25 ms) instead of a hard
                    // cut, so barge-in/reconnect-flush stops without a click.
                    downlink.interrupt_fade(25);
                }
                Some(Event::TurnComplete) => {
                    downlink.set_expecting(false);
                    last_audio = None;
                    turn_recv_at = None;
                    turn_onset_logged = false;
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
                let tnow = tokio::time::Instant::now();
                if let Some(prev) = last_tick {
                    let g = tnow.duration_since(prev).as_millis() as u64;
                    if g > max_tick_gap_ms { max_tick_gap_ms = g; }
                    if g >= 30 { late_ticks += 1; }
                }
                last_tick = Some(tnow);
                tick_count += 1;
                let before_playing = downlink.playing();
                let pcm8 = downlink.next_frame();
                // playing false->true = prefill met, first real frame of a turn
                // reaches the phone now. Time it against when the turn's audio
                // first arrived from Gemini.
                if !before_playing && downlink.playing()
                    && let (Some(t0), false) = (turn_recv_at, turn_onset_logged) {
                        let onset = tnow.duration_since(t0).as_millis() as u64;
                        if onset > max_onset_ms { max_onset_ms = onset; }
                        onset_count += 1;
                        tracing::info!(call_id = %call_id_dl, downlink_onset_ms = onset, "downlink audio onset");
                        turn_onset_logged = true;
                    }
                if let Some(w) = dl_dump8.as_mut() { let _ = w.write(&pcm8); }
                let payload = g711::encode(codec, &pcm8);
                quality.underruns.store(downlink.underruns(), Ordering::Relaxed);
                quality.starved_ms.store(downlink.starved_ms(), Ordering::Relaxed);
                quality.max_gap_ms.store(max_gap_ms, Ordering::Relaxed);
                // "Active" = buffer non-empty OR model audio arrived within the
                // grace window. The grace bridges inter-chunk gaps (and audio
                // that streams in just after `end_call`) so the drain doesn't
                // stop on a momentary empty buffer mid-goodbye.
                let recent_audio = last_audio.is_some_and(|t| {
                    tokio::time::Instant::now().duration_since(t)
                        < std::time::Duration::from_millis(DOWNLINK_GRACE_MS)
                });
                quality.set_downlink_active(downlink.pending() || recent_audio);
                if phone_out.send(Bytes::from(payload)).await.is_err() {
                    break BridgeEnd::PhoneClosed;
                }
            }
        }
    };
    // Harmless no-op if the uplink task already finished (the `&mut uplink`
    // branch above); required to stop it on the other two exit paths.
    tracing::info!(
        max_tick_gap_ms,
        late_ticks,
        tick_count,
        max_onset_ms,
        onset_count,
        "downlink pacer send-timing"
    );
    uplink.abort();
    if let Some(w) = dl_dump24.take() {
        let _ = w.finalize();
    }
    if let Some(w) = dl_dump8.take() {
        let _ = w.finalize();
    }
    end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gemini_live::Event;
    use crate::sip::G711Kind;
    use bytes::Bytes;
    use tokio::sync::mpsc;

    #[test]
    fn uplink_rms_is_zero_before_any_audio_and_matches_a_constant_tone() {
        let q = QualityShared::new();
        assert_eq!(q.snapshot().uplink_rms, 0); // no samples -> 0, not a divide-by-zero
        // A constant-amplitude signal has RMS == its amplitude.
        q.add_uplink_level(&[1000i16; 400]);
        q.add_uplink_level(&[1000i16; 400]);
        assert_eq!(q.snapshot().uplink_rms, 1000);
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_signal_ends_the_bridge_as_cancelled() {
        let (ports, ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        // No side closes; only the engine's teardown signal fires.
        ends.cancel_tx.send(()).unwrap();
        assert_eq!(h.await.unwrap(), BridgeEnd::Cancelled);
    }

    // Build ports + keep the far ends for the test to drive.
    struct Ends {
        phone_in_tx: mpsc::Sender<Bytes>,
        phone_out_rx: mpsc::Receiver<Bytes>,
        gemini_in_rx: mpsc::Receiver<Vec<i16>>,
        gemini_events_tx: mpsc::Sender<Event>,
        events_out_rx: mpsc::Receiver<Event>,
        cancel_tx: tokio::sync::oneshot::Sender<()>,
    }

    fn wire(codec: G711Kind) -> (BridgePorts, Ends) {
        let (phone_in_tx, phone_in) = mpsc::channel(64);
        let (phone_out, phone_out_rx) = mpsc::channel(64);
        let (gemini_in, gemini_in_rx) = mpsc::channel(64);
        let (gemini_events_tx, gemini_events) = mpsc::channel(64);
        let (events_out, events_out_rx) = mpsc::channel(64);
        let (cancel_tx, cancel) = tokio::sync::oneshot::channel();
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
                downlink_dump: None,
                agc_cfg: crate::config::AgcConfig {
                    enabled: false,
                    ..Default::default()
                },
                cancel,
            },
            Ends {
                phone_in_tx,
                phone_out_rx,
                gemini_in_rx,
                gemini_events_tx,
                events_out_rx,
                cancel_tx,
            },
        )
    }

    #[tokio::test(start_paused = true)]
    async fn uplink_forwards_every_frame_transparently() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        // Send 5 frames of 160 G.711 silence bytes.
        for _ in 0..5 {
            ends.phone_in_tx
                .send(Bytes::from(vec![0xFFu8; 160]))
                .await
                .unwrap();
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
        ends.gemini_events_tx
            .send(Event::OutputAudio(vec![8000i16; 480 * 10]))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Advance three 20 ms ticks; expect three 160-byte frames carrying
        // real (non-silent) audio -- proves playout is actually pacing
        // buffered audio to the phone, not just holding silence for prefill.
        for _ in 0..3 {
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
            let frame = ends.phone_out_rx.recv().await.unwrap();
            assert_eq!(frame.len(), 160);
            let pcm = g711::decode(G711Kind::Ulaw, &frame);
            assert!(
                pcm.iter().any(|&s| s.abs() > 1000),
                "expected paced audio, not silence"
            );
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
        ends.gemini_events_tx
            .send(Event::OutputAudio(vec![8000i16; 480 * 20]))
            .await
            .unwrap();
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
        assert!(
            pcm.iter().any(|&s| s.abs() > 1000),
            "expected audio to be playing before barge-in"
        );

        // Barge-in while ~18 more frames (360 ms) of loud audio remain buffered.
        // `interrupt_fade` ramps the buffered head down to silence over ~25 ms and
        // DROPS the ~17 remaining frames, so the phone hears a short fade then
        // clean silence — not the full 360 ms burst.
        ends.gemini_events_tx
            .send(Event::Interrupted)
            .await
            .unwrap();
        tokio::task::yield_now().await;

        // Within a few frames the level must decay to silence (proving the bulk
        // was dropped — without a working interrupt_fade it would stay loud for
        // ~17 frames), and at most a couple of frames may still carry the fading
        // head (proving it's a fade, and that the bulk is gone).
        let mut reached_silence = false;
        let mut loud_frames = 0;
        for _ in 0..6 {
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
            let frame = ends.phone_out_rx.recv().await.unwrap();
            let pcm = g711::decode(G711Kind::Ulaw, &frame);
            let peak = pcm.iter().map(|&s| s.abs()).max().unwrap_or(0);
            if peak < 64 {
                reached_silence = true;
                break;
            }
            if peak > 1000 {
                loud_frames += 1;
            }
        }
        assert!(
            reached_silence,
            "audio must fade to silence within a few frames of barge-in"
        );
        assert!(
            loud_frames <= 2,
            "barge-in must drop the buffered bulk (only a short fade remains), got {loud_frames} loud frames"
        );

        drop(ends);
        let _ = h.await;
    }

    #[tokio::test(start_paused = true)]
    async fn non_audio_events_forwarded_to_engine() {
        let (ports, mut ends) = wire(G711Kind::Ulaw);
        let h = tokio::spawn(run(ports));
        ends.gemini_events_tx
            .send(Event::TurnComplete)
            .await
            .unwrap();
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
        ends.gemini_events_tx
            .send(Event::OutputAudio(vec![8000i16; 480]))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        // Drain several ticks: after the one buffered frame, empty-while-expecting = underruns.
        for _ in 0..4 {
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
            let _ = ends.phone_out_rx.recv().await;
        }
        assert!(
            q.snapshot().underruns >= 1,
            "expected underruns recorded while expecting"
        );
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
            ends.phone_in_tx
                .send(Bytes::from(vec![0xFFu8; 160]))
                .await
                .unwrap();
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
        assert!(
            !crate::audio_file::read_pcm16(&eight, 8000)
                .unwrap()
                .is_empty()
        );
        assert!(
            !crate::audio_file::read_pcm16(&sixteen, 16000)
                .unwrap()
                .is_empty()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn downlink_dump_writes_both_wavs() {
        let dir = std::env::temp_dir().join(format!("kutsu-downlink-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let (mut ports, ends) = wire(G711Kind::Ulaw);
        ports.call_id = "d1".to_string();
        ports.downlink_dump = Some(dir.clone());
        let h = tokio::spawn(run(ports));

        // Model audio (24k) -> the 24k dump; the 20ms pacer ticks -> the 8k dump.
        for _ in 0..3 {
            ends.gemini_events_tx
                .send(Event::OutputAudio(vec![1000i16; 480 * 4]))
                .await
                .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(120)).await; // let several ticks fire
        // Close the gemini side; the bridge returns `GeminiClosed` and finalizes
        // both writers on the way out. Keep the rest of `ends` alive so phone_out
        // sends keep succeeding.
        drop(ends.gemini_events_tx);
        let _ = h.await;

        let k24 = dir.join("d1-downlink-24k.wav");
        let k8 = dir.join("d1-downlink-8k.wav");
        assert!(k24.exists(), "24k downlink dump missing");
        assert!(k8.exists(), "8k downlink dump missing");
        assert!(
            !crate::audio_file::read_pcm16(&k24, 24000)
                .unwrap()
                .is_empty()
        );
        assert!(!crate::audio_file::read_pcm16(&k8, 8000).unwrap().is_empty());
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
