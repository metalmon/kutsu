//! Kutsu CLI entrypoint and MCP server bootstrap.
//!
//! Scaffold only: the `mcp` subcommand is wired up but not yet implemented.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "kutsu",
    version,
    about = "Outbound SIP calling MCP server, bridging phone calls to Gemini Live"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the MCP server.
    Mcp {
        /// Transport: `stdio` or `streamable-http`.
        #[arg(long, default_value = "stdio", env = "KUTSU_MCP_TRANSPORT")]
        transport: String,
        /// Bind address for the `streamable-http` transport.
        #[arg(long, default_value = "127.0.0.1:8090", env = "KUTSU_MCP_BIND")]
        bind: String,
    },
    /// Run one conversation against Gemini Live from a scenario + audio file (dev harness).
    Live {
        /// Scenario JSON: { system_prompt, goal_schema, context? }.
        #[arg(long)]
        scenario: std::path::PathBuf,
        /// Input audio: mono PCM16 WAV or raw .pcm at 16 kHz.
        #[arg(long = "audio-in")]
        audio_in: std::path::PathBuf,
        /// Output WAV (model speech, 24 kHz).
        #[arg(long = "audio-out")]
        audio_out: Option<std::path::PathBuf>,
        /// Transcript JSONL output.
        #[arg(long)]
        transcript: Option<std::path::PathBuf>,
        /// Filled goal JSON output.
        #[arg(long = "goal-out")]
        goal_out: Option<std::path::PathBuf>,
        /// Override model: half | native.
        #[arg(long)]
        model: Option<String>,
        /// Override voice.
        #[arg(long)]
        voice: Option<String>,
        /// Seconds to keep the session open after input ends.
        #[arg(long, default_value = "8")]
        tail: u64,
        /// Skip the network preflight (offline debugging only).
        #[arg(long = "no-net-check")]
        no_net_check: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let cli = Cli::parse();
    match cli.command {
        Some(Command::Mcp { transport, bind }) => {
            let _ = (transport, bind);
            anyhow::bail!(
                "kutsu {} — MCP server not implemented yet (scaffold stage)",
                kutsu::version()
            );
        }
        Some(Command::Live {
            scenario,
            audio_in,
            audio_out,
            transcript,
            goal_out,
            model,
            voice,
            tail,
            no_net_check,
        }) => {
            let rt = tokio::runtime::Runtime::new()?;
            let code = rt.block_on(run_live(
                scenario, audio_in, audio_out, transcript, goal_out, model, voice, tail,
                no_net_check,
            ))?;
            std::process::exit(code);
        }
        None => {
            println!(
                "kutsu {} — outbound SIP calling MCP server (scaffold, not yet functional)",
                kutsu::version()
            );
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_live(
    scenario_path: std::path::PathBuf,
    audio_in: std::path::PathBuf,
    audio_out: Option<std::path::PathBuf>,
    transcript: Option<std::path::PathBuf>,
    goal_out: Option<std::path::PathBuf>,
    model: Option<String>,
    voice: Option<String>,
    tail: u64,
    no_net_check: bool,
) -> anyhow::Result<i32> {
    // 1. Load scenario + server config (env: GEMINI_API_KEY, proxy).
    let scenario: kutsu::config::ScenarioConfig =
        serde_json::from_slice(&std::fs::read(&scenario_path)?)?;
    let api_key = std::env::var("GEMINI_API_KEY")
        .map_err(|_| anyhow::anyhow!("GEMINI_API_KEY not set"))?;
    let model = match model.as_deref() {
        Some("native") => kutsu::config::Model::NativeAudio,
        _ => kutsu::config::Model::HalfCascade,
    };
    let server = kutsu::config::ServerConfig {
        api_key,
        proxy: None,
        model,
        voice: voice.unwrap_or_else(|| "Autonoe".into()),
        language: "ru-RU".into(),
        net_check: kutsu::config::NetCheckConfig::default(),
        max_concurrent_channels: 3,
    };

    // 2. Preflight (fail closed).
    if !no_net_check {
        let health = kutsu::net_check::preflight(&server).await?;
        eprintln!("net: {}", health.summary());
        if matches!(
            kutsu::net_check::verdict(&health, &server.net_check),
            kutsu::net_check::Verdict::Unusable
        ) {
            eprintln!("network unusable — refusing to place the call");
            return Ok(2);
        }
    }

    // 3. Start session.
    let mut session = kutsu::gemini_live::start(&server, &scenario).await?;

    // 4. Feed audio (32 ms frames at real-time pace) in a task.
    let samples = kutsu::audio_file::read_pcm16(&audio_in, 16000)?;
    // Input is 16 kHz mono PCM16; used below to extend the session deadline
    // so it stays open long enough to stream the whole input plus the tail.
    let input_len_secs = samples.len() as u64 / 16000;
    let audio_tx = session.audio_in.clone();
    tokio::spawn(async move {
        for chunk in samples.chunks(512) {
            if audio_tx.send(chunk.to_vec()).await.is_err() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(32)).await;
        }
    });

    // 5. Consume events until EndCall or tail timeout.
    let mut out = audio_out
        .map(|p| kutsu::audio_file::Pcm16Writer::create(&p, 24000))
        .transpose()?;
    let mut transcript_file = transcript.map(std::fs::File::create).transpose()?;
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(input_len_secs)
        + std::time::Duration::from_secs(tail);
    let mut goal: Option<serde_json::Value> = None;
    loop {
        let ev = tokio::time::timeout_at(deadline, session.events.recv()).await;
        match ev {
            Ok(Some(kutsu::gemini_live::Event::OutputAudio(pcm))) => {
                if let Some(w) = out.as_mut() {
                    w.write(&pcm)?;
                }
            }
            Ok(Some(kutsu::gemini_live::Event::Transcript { role, text, final_ })) => {
                println!("[{:?}] {}", role, text);
                if let Some(f) = transcript_file.as_mut() {
                    use std::io::Write;
                    writeln!(
                        f,
                        "{}",
                        serde_json::json!({"role":format!("{role:?}"),"text":text,"final":final_})
                    )?;
                }
            }
            Ok(Some(kutsu::gemini_live::Event::EndCall { goal: g })) => {
                goal = Some(g);
                break;
            }
            Ok(Some(kutsu::gemini_live::Event::Warning(w))) => eprintln!("warn: {w}"),
            Ok(Some(_)) => {}
            Ok(None) => break,   // session task ended
            Err(_) => {
                session.hangup().await;
                break;
            } // tail timeout
        }
    }
    if let Some(w) = out {
        w.finalize()?;
    }
    if let Some(g) = &goal {
        if let Some(p) = goal_out {
            std::fs::write(p, serde_json::to_vec_pretty(g)?)?;
        }
    }

    let outcome = session.join().await;
    eprintln!("ended_by={:?} goal={}", outcome.ended_by, goal.is_some());
    Ok(match outcome.ended_by {
        kutsu::gemini_live::EndedBy::Error => 1,
        _ => 0,
    })
}
