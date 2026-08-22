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

fn make_framer(name: &str) -> anyhow::Result<Box<dyn SpeechFramer>> {
    match name {
        "energy" => Ok(Box::new(EnergyFramer::new(VadConfig::default()))),
        // The Silero backend is deferred (see the amd-harness plan, Task 7).
        "silero" => anyhow::bail!("silero framer not yet implemented"),
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
        let mut framer = make_framer(&cli.framer)?;
        let frames: Vec<FrameClass> = samples
            .chunks_exact(frame_len)
            .map(|c| framer.classify(c))
            .collect();
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
