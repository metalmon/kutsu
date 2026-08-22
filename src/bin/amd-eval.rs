//! Offline AMD harness (dev only; built with `--features amd-eval`). Runs a
//! labeled corpus of callee-audio WAVs through a detector and reports how well
//! it separates a live human from a machine.

use std::path::PathBuf;

use clap::Parser;
use kutsu::amd::detector::{AmdDetector, HeuristicDetector, HeuristicParams};
use kutsu::amd::features::extract_profile;
use kutsu::amd::framer::{EnergyFramer, SpeechFramer};
use kutsu::amd::{AmdClass, FrameClass};
use kutsu::config::VadConfig;

#[derive(Parser)]
#[command(name = "amd-eval", about = "Offline AMD detector evaluation harness")]
struct Cli {
    /// Directory holding the corpus WAV files.
    #[arg(long)]
    corpus: PathBuf,
    /// labels.toml mapping WAV file name -> class.
    #[arg(long)]
    labels: PathBuf,
    /// Sample rate of the corpus WAVs (energy path expects 8000).
    #[arg(long, default_value = "8000")]
    rate: u32,
    /// Framer backend.
    #[arg(long, default_value = "energy")]
    framer: String,
}

fn make_framer(name: &str, sample_rate: u32) -> anyhow::Result<Box<dyn SpeechFramer>> {
    let _ = sample_rate; // used only by the silero backend
    match name {
        "energy" => Ok(Box::new(EnergyFramer::new(VadConfig::default()))),
        #[cfg(feature = "amd-silero")]
        "silero" => Ok(Box::new(kutsu::amd::framer::SileroFramer::new(
            sample_rate,
        )?)),
        #[cfg(not(feature = "amd-silero"))]
        "silero" => anyhow::bail!("silero framer needs a build with --features amd-silero"),
        other => anyhow::bail!("unknown framer: {other}"),
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let labels = kutsu::amd::corpus::load_labels(&cli.labels)?;
    let frame_len = (cli.rate / 50) as usize; // 20 ms
    let detector = HeuristicDetector::new(HeuristicParams::default());

    let mut pairs: Vec<(AmdClass, AmdClass)> = Vec::new();
    for (file, actual) in &labels {
        let samples = kutsu::audio_file::read_pcm16(&cli.corpus.join(file), cli.rate)?;
        let mut framer = make_framer(&cli.framer, cli.rate)?;
        let frames: Vec<FrameClass> = samples
            .chunks_exact(frame_len)
            .map(|c| framer.classify(c))
            .collect();
        // Diagnostic: raw speech-probability distribution (framers that provide
        // one, i.e. Silero) — reveals whether the model is firing at all.
        let probs: Vec<f32> = frames.iter().filter_map(|f| f.speech_prob).collect();
        if !probs.is_empty() {
            let n = probs.len() as f32;
            let mean = probs.iter().sum::<f32>() / n;
            let max = probs.iter().cloned().fold(f32::MIN, f32::max);
            let over = probs.iter().filter(|&&p| p > 0.5).count();
            eprintln!(
                "  [prob] frames={} mean={mean:.3} max={max:.3} over0.5={over}",
                probs.len()
            );
        }
        let profile = extract_profile(&frames, 20, VadConfig::default().min_rms as f32);
        let verdict = detector.classify(&profile);
        println!(
            "{file}: predicted={:?} actual={:?} (onset={:?} first_utt={}ms hold_ratio={:?})",
            verdict.class,
            actual,
            profile.onset_ms,
            profile.first_utterance_ms,
            profile.nonspeech_energy_ratio
        );
        pairs.push((verdict.class, *actual));
    }

    println!(
        "\n{}",
        kutsu::amd::eval::render_confusion(&kutsu::amd::eval::confusion(&pairs))
    );
    let m = kutsu::amd::eval::binary_metrics(&pairs);
    println!(
        "machine-vs-human: precision={:.3} recall={:.3} f1={:.3} (n={})",
        m.precision,
        m.recall,
        m.f1,
        pairs.len()
    );
    Ok(())
}
