//! PCM16 audio file I/O for the dev harness (WAV via `hound`, or raw `.pcm`).
//! Not used by the production audio path — the phase-3 bridge handles real audio.

use std::path::Path;

use crate::error::{Error, Result};

/// Read a mono PCM16 file. `.wav` is parsed and validated against `expected_rate`;
/// any other extension is treated as headerless raw PCM16 at the expected rate.
pub fn read_pcm16(path: &Path, expected_rate: u32) -> Result<Vec<i16>> {
    let is_wav = path
        .extension()
        .map(|e| e.eq_ignore_ascii_case("wav"))
        .unwrap_or(false);
    if is_wav {
        let reader = hound::WavReader::open(path).map_err(hound_err)?;
        let spec = reader.spec();
        if spec.channels != 1 || spec.bits_per_sample != 16 {
            return Err(Error::Config(format!(
                "expected mono PCM16, got {} channels / {} bits",
                spec.channels, spec.bits_per_sample
            )));
        }
        if spec.sample_rate != expected_rate {
            return Err(Error::Config(format!(
                "expected {} Hz, got {} Hz (resampling is not this layer's job)",
                expected_rate, spec.sample_rate
            )));
        }
        reader
            .into_samples::<i16>()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(hound_err)
    } else {
        let bytes = std::fs::read(path)?;
        if bytes.len() % 2 != 0 {
            return Err(Error::Config("raw PCM file has odd byte length".into()));
        }
        Ok(bytes
            .chunks_exact(2)
            .map(|b| i16::from_le_bytes([b[0], b[1]]))
            .collect())
    }
}

/// Streaming WAV writer for mono PCM16.
pub struct Pcm16Writer {
    inner: hound::WavWriter<std::io::BufWriter<std::fs::File>>,
}

impl Pcm16Writer {
    pub fn create(path: &Path, rate: u32) -> Result<Self> {
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let inner = hound::WavWriter::create(path, spec).map_err(hound_err)?;
        Ok(Pcm16Writer { inner })
    }

    pub fn write(&mut self, samples: &[i16]) -> Result<()> {
        for &s in samples {
            self.inner.write_sample(s).map_err(hound_err)?;
        }
        Ok(())
    }

    pub fn finalize(self) -> Result<()> {
        self.inner.finalize().map_err(hound_err)
    }
}

fn hound_err(e: hound::Error) -> Error {
    match e {
        hound::Error::IoError(io) => Error::Io(io),
        other => Error::Config(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn wav_round_trip_16k() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("a.wav");
        let samples: Vec<i16> = (0..320).map(|i| (i as i16) - 160).collect();

        let mut w = Pcm16Writer::create(&path, 16000).unwrap();
        w.write(&samples).unwrap();
        w.finalize().unwrap();

        let back = read_pcm16(&path, 16000).unwrap();
        assert_eq!(back, samples);
    }

    #[test]
    fn wrong_rate_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("b.wav");
        let mut w = Pcm16Writer::create(&path, 8000).unwrap();
        w.write(&[1, 2, 3]).unwrap();
        w.finalize().unwrap();

        let err = read_pcm16(&path, 16000).unwrap_err();
        assert!(matches!(err, crate::error::Error::Config(_)));
    }
}
