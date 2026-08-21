//! Adaptive gain control (AGC) for the uplink (callee → Gemini).
//!
//! Real trunks arrive quiet (~-32 dBFS), which under-triggers Gemini's speech
//! detection and stalls turn-taking. This lifts sustained speech toward a target
//! level so it is reliably detected, while HOLDING gain on near-silence (below
//! the noise floor) so background noise is never amplified into false speech.
//! Gain moves smoothly (fast attack down to avoid clipping, slow release up) and
//! is bounded by a ceiling; output is clamped to the PCM range.

use crate::config::AgcConfig;

/// Per-call adaptive uplink gain. Cheap (one pass per 20 ms frame), stateful in
/// the current gain only.
pub struct Agc {
    cfg: AgcConfig,
    target_rms: f32,
    max_gain: f32,
    gain: f32,
}

impl Agc {
    pub fn new(cfg: AgcConfig) -> Self {
        let target_rms = 32768.0 * 10f32.powf(cfg.target_dbfs / 20.0);
        let max_gain = 10f32.powf(cfg.max_gain_db / 20.0);
        Self { cfg, target_rms, max_gain, gain: 1.0 }
    }

    /// Apply adaptive gain in place to one PCM16 frame.
    pub fn process(&mut self, frame: &mut [i16]) {
        if !self.cfg.enabled || frame.is_empty() {
            return;
        }
        let sq: f64 = frame.iter().map(|&s| (s as f64) * (s as f64)).sum();
        let rms = (sq / frame.len() as f64).sqrt() as f32;

        // Adapt only on frames with real signal (above the noise floor); hold on
        // near-silence so background is not driven up toward the target.
        if rms >= self.cfg.noise_floor_rms && rms > 0.0 {
            let desired = (self.target_rms / rms).clamp(0.0, self.max_gain);
            // Asymmetric smoothing: fast attack DOWN (limit clipping quickly),
            // slow release UP (avoid pumping between words).
            let coeff = if desired < self.gain { 0.3 } else { 0.05 };
            self.gain += (desired - self.gain) * coeff;
        }

        if (self.gain - 1.0).abs() < 1e-3 {
            return; // effectively unity — skip the multiply
        }
        for s in frame.iter_mut() {
            let v = (*s as f32 * self.gain).round();
            *s = v.clamp(i16::MIN as f32, i16::MAX as f32) as i16;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AgcConfig;

    fn rms(frame: &[i16]) -> f32 {
        let sq: f64 = frame.iter().map(|&s| (s as f64) * (s as f64)).sum();
        (sq / frame.len() as f64).sqrt() as f32
    }
    fn target_rms(c: &AgcConfig) -> f32 {
        32768.0 * 10f32.powf(c.target_dbfs / 20.0)
    }
    /// Feed `n` frames of constant amplitude `amp` (so frame RMS == |amp|),
    /// return the last output frame's RMS.
    fn run(agc: &mut Agc, amp: i16, n: usize) -> f32 {
        let mut last = 0.0;
        for _ in 0..n {
            let mut f = vec![amp; 160]; // 20 ms @ 8 kHz
            agc.process(&mut f);
            last = rms(&f);
        }
        last
    }

    #[test]
    fn quiet_speech_is_lifted_toward_target() {
        let c = AgcConfig::default(); // -18 dBFS, +30 dB cap, floor 200
        let mut agc = Agc::new(c);
        let out = run(&mut agc, 500, 400); // rms 500 (> floor), quiet speech
        let t = target_rms(&c);
        assert!((out - t).abs() < t * 0.15, "expected ~{t}, got {out}");
    }

    #[test]
    fn loud_speech_is_attenuated_toward_target_without_clipping() {
        let c = AgcConfig::default();
        let mut agc = Agc::new(c);
        let mut f = vec![12000i16; 160];
        for _ in 0..400 {
            f = vec![12000i16; 160];
            agc.process(&mut f);
        }
        let t = target_rms(&c);
        assert!(f.iter().all(|&s| s.abs() < 32767), "must not clip");
        assert!((rms(&f) - t).abs() < t * 0.2, "expected ~{t}, got {}", rms(&f));
    }

    #[test]
    fn near_silence_is_held_not_amplified() {
        let c = AgcConfig::default();
        let mut agc = Agc::new(c);
        let out = run(&mut agc, 50, 400); // rms 50 (< floor 200) => hold
        assert!(out < 200.0, "silence/noise must not be boosted, got {out}");
    }

    #[test]
    fn gain_respects_max_cap() {
        let c = AgcConfig { max_gain_db: 6.0, ..AgcConfig::default() }; // ~2x cap
        let mut agc = Agc::new(c);
        let out = run(&mut agc, 500, 400); // desired ~8x, capped at 2x -> ~1000
        assert!(out <= 500.0 * 2.0 * 1.05, "gain must cap at ~2x, got {out}");
        assert!(out > 500.0 * 1.5, "gain should apply up to the cap, got {out}");
    }

    #[test]
    fn disabled_passes_through_unchanged() {
        let c = AgcConfig { enabled: false, ..AgcConfig::default() };
        let mut agc = Agc::new(c);
        let out = run(&mut agc, 500, 50);
        assert!((out - 500.0).abs() < 1.0, "disabled AGC must not alter the signal");
    }
}
