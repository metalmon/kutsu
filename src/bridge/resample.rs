//! Sample-rate conversion. `up_8k_16k` is a pure per-frame linear upsampler (the
//! uplink to Gemini — a robust listener). `Downsampler` is stateful (carries FIR
//! history across the pacer's 20 ms blocks) so the human-audible downlink has no
//! periodic edge transient. Both sit behind these boundaries so a higher-quality
//! impl (e.g. `rubato`) can replace them without touching callers.

use std::sync::OnceLock;

/// Upsample 8 kHz -> 16 kHz by linear interpolation. Output length = 2 * input.
pub fn up_8k_16k(input: &[i16]) -> Vec<i16> {
    let mut out = Vec::with_capacity(input.len() * 2);
    for i in 0..input.len() {
        let cur = input[i] as i32;
        let next = if i + 1 < input.len() {
            input[i + 1] as i32
        } else {
            cur // hold the last sample (frame boundary)
        };
        out.push(cur as i16);
        out.push(((cur + next) / 2) as i16);
    }
    out
}

/// Windowed-sinc low-pass FIR (cutoff ~3.4 kHz at 24 kHz), computed once.
fn lowpass() -> &'static [f32] {
    static C: OnceLock<Vec<f32>> = OnceLock::new();
    C.get_or_init(|| {
        const N: usize = 23; // taps (odd)
        let fc_norm = 3400.0f32 / 24000.0;
        let mid = (N as f32 - 1.0) / 2.0;
        let pi = std::f32::consts::PI;
        let mut h = vec![0f32; N];
        let mut sum = 0f32;
        for i in 0..N {
            let x = i as f32 - mid;
            let sinc = if x == 0.0 {
                2.0 * fc_norm
            } else {
                (2.0 * pi * fc_norm * x).sin() / (pi * x)
            };
            let w = 0.54 - 0.46 * (2.0 * pi * i as f32 / (N as f32 - 1.0)).cos(); // Hamming
            h[i] = sinc * w;
            sum += h[i];
        }
        for v in &mut h {
            *v /= sum; // normalize DC gain to 1
        }
        h
    })
}

/// Stateful 24 kHz -> 8 kHz downsampler: FIR low-pass then /3 decimation.
pub struct Downsampler {
    win: Vec<i16>, // sliding window of the last `taps` samples
    phase: usize,  // input-sample counter mod 3; emit when 0
}

impl Downsampler {
    pub fn new() -> Self {
        Self {
            win: vec![0; lowpass().len()],
            phase: 0,
        }
    }

    /// Feed a block of 24 kHz samples; return the decimated 8 kHz samples.
    pub fn process(&mut self, block: &[i16]) -> Vec<i16> {
        let h = lowpass();
        let taps = h.len();
        let mut out = Vec::with_capacity(block.len() / 3 + 1);
        for &s in block {
            self.win.copy_within(1..taps, 0);
            self.win[taps - 1] = s;
            if self.phase == 0 {
                let mut acc = 0f32;
                for j in 0..taps {
                    acc += h[j] * self.win[j] as f32;
                }
                out.push(acc.round().clamp(-32768.0, 32767.0) as i16);
            }
            self.phase = (self.phase + 1) % 3;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsample_doubles_and_interpolates() {
        let out = up_8k_16k(&[0, 100]);
        // [a, mean(a,b), b, hold(b)]
        assert_eq!(out, vec![0, 50, 100, 100]);
        assert_eq!(up_8k_16k(&[0; 160]).len(), 320);
    }

    #[test]
    fn upsample_preserves_dc() {
        let out = up_8k_16k(&[1000; 10]);
        assert!(out.iter().all(|&s| s == 1000));
    }

    #[test]
    fn downsample_thirds_length_and_dc() {
        let mut d = Downsampler::new();
        let out = d.process(&[2000i16; 480]);
        assert_eq!(out.len(), 160);
        // After the filter warms up, DC passes through (gain ~1).
        let tail = &out[80..];
        assert!(tail.iter().all(|&s| (s - 2000).abs() < 60), "DC not preserved: {tail:?}");
    }

    fn tone(freq: f32, n: usize) -> Vec<i16> {
        (0..n)
            .map(|i| {
                let t = i as f32 / 24000.0;
                ((2.0 * std::f32::consts::PI * freq * t).sin() * 10000.0) as i16
            })
            .collect()
    }

    fn rms(s: &[i16]) -> f32 {
        (s.iter().map(|&x| (x as f32).powi(2)).sum::<f32>() / s.len() as f32).sqrt()
    }

    #[test]
    fn downsample_is_antialiasing() {
        // A 1 kHz tone (in-band) passes; a 6 kHz tone (above the 4 kHz Nyquist of
        // 8 kHz) is strongly attenuated.
        let mut d1 = Downsampler::new();
        let pass = d1.process(&tone(1000.0, 2400));
        let mut d2 = Downsampler::new();
        let block = d2.process(&tone(6000.0, 2400));
        // Compare steady-state RMS (skip warm-up).
        let pass_rms = rms(&pass[40..]);
        let block_rms = rms(&block[40..]);
        assert!(pass_rms > 3000.0, "in-band tone attenuated: {pass_rms}");
        assert!(block_rms < pass_rms * 0.2, "out-of-band not attenuated: pass={pass_rms} block={block_rms}");
    }

    #[test]
    fn downsample_is_continuous_across_blocks() {
        // Processing [A|B] in two calls must equal processing A++B in one call.
        let a = tone(1000.0, 480);
        let b = tone(1000.0, 480);
        let mut split = Downsampler::new();
        let mut out_split = split.process(&a);
        out_split.extend(split.process(&b));

        let mut whole = Downsampler::new();
        let combined: Vec<i16> = a.iter().chain(b.iter()).copied().collect();
        let out_whole = whole.process(&combined);

        assert_eq!(out_split, out_whole);
    }
}
