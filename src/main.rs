//! Kutsu CLI entrypoint and MCP server bootstrap.
//!
//! The `mcp` subcommand runs the MCP server (stdio or streamable-http
//! transport) and is fully implemented; see [`kutsu::mcp`] and
//! [`kutsu::mcp_http`].

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "kutsu",
    version,
    about = "Outbound SIP calling MCP server, bridging phone calls to Gemini Live"
)]
struct Cli {
    /// Log output format: text (dev, default) or json (machine-parseable / SIEM).
    #[arg(
        long = "log-format",
        env = "KUTSU_LOG_FORMAT",
        default_value = "text",
        global = true
    )]
    log_format: String,
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
        /// Bearer token required on the streamable-http transport (env KUTSU_MCP_TOKEN).
        #[arg(long = "auth-token", env = "KUTSU_MCP_TOKEN")]
        auth_token: Option<String>,
    },
    /// Run one conversation against Gemini Live from a scenario + audio file (dev harness).
    Live {
        /// Scenario JSON: { goal_schema, context?, prompt_override? }.
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
        /// Wait this many ms for the callee before the agent greets first;
        /// 0 disables the proactive greeting (purely reactive).
        #[arg(long = "greet-after-silence-ms")]
        greet_after_silence_ms: Option<u64>,
    },
    /// Place one outbound call: dial <number>, bridge to Gemini, print the transcript.
    Call {
        /// Number/extension to dial.
        number: String,
        /// Optional scenario JSON file (goal_schema, context?, prompt_override?). Uses a default if absent.
        #[arg(long)]
        scenario: Option<std::path::PathBuf>,
    },
    /// Write a documented default config to `kutsu.toml` (does not overwrite unless --force).
    Init {
        /// Path to write. Defaults to `kutsu.toml` in the current directory.
        #[arg(long, default_value = "kutsu.toml")]
        path: std::path::PathBuf,
        /// Overwrite an existing file.
        #[arg(long)]
        force: bool,
    },
}

/// The documented default `kutsu.toml`, embedded so `kutsu init` and the
/// in-repo example never drift.
const EXAMPLE_TOML: &str = include_str!("../kutsu.example.toml");

/// On Windows the default multimedia timer resolution is ~15.6 ms, which
/// quantises the bridge's 20 ms media pacer into uneven ticks → jittery RTP
/// egress and audible stutter at the callee even with zero packet loss. Raise
/// it to 1 ms for the process lifetime. No-op on other platforms.
#[cfg(windows)]
fn raise_timer_resolution() {
    #[link(name = "winmm")]
    unsafe extern "system" {
        fn timeBeginPeriod(u_period: u32) -> u32;
    }
    // 1 ms period, never released — kutsu is a real-time media server for its
    // whole lifetime, so `timeEndPeriod` on exit is unnecessary.
    unsafe {
        timeBeginPeriod(1);
    }
}
#[cfg(not(windows))]
fn raise_timer_resolution() {}

fn main() -> anyhow::Result<()> {
    raise_timer_resolution();
    let cli = Cli::parse();

    // Bridge the `log` facade into `tracing` so ezk-sip-core's `sip_progress`
    // call-progress patch (emitted via `log::info!`) reaches the subscriber and
    // the EnvFilter below. Idempotent; ignore the "already set" error.
    let _ = tracing_log::LogTracer::init();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    match cli.log_format.as_str() {
        "json" => {
            let _ = builder.json().try_init();
        }
        _ => {
            let _ = builder.try_init();
        }
    }
    match cli.command {
        Some(Command::Mcp {
            transport,
            bind,
            auth_token,
        }) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(run_mcp(transport, bind, auth_token))
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
            greet_after_silence_ms,
        }) => {
            let rt = tokio::runtime::Runtime::new()?;
            let code = rt.block_on(run_live(
                scenario,
                audio_in,
                audio_out,
                transcript,
                goal_out,
                model,
                voice,
                tail,
                no_net_check,
                greet_after_silence_ms,
            ))?;
            std::process::exit(code);
        }
        Some(Command::Call { number, scenario }) => {
            let rt = tokio::runtime::Runtime::new()?;
            let code = rt.block_on(run_call_cli(number, scenario));
            std::process::exit(code);
        }
        Some(Command::Init { path, force }) => run_init(&path, force),
        None => {
            println!(
                "kutsu {} — outbound SIP calling MCP server. Subcommands: `mcp` (run the MCP server), \
                 `call` (place one call from the CLI), `live` (dev harness against a scenario + audio file), \
                 `init` (write a default kutsu.toml). Run with --help for details.",
                kutsu::version()
            );
            Ok(())
        }
    }
}

/// Write the documented default config to `path`. Refuses to clobber an
/// existing file unless `force`, so a populated `kutsu.toml` is never lost.
fn run_init(path: &std::path::Path, force: bool) -> anyhow::Result<()> {
    if path.exists() && !force {
        anyhow::bail!(
            "{} already exists; pass --force to overwrite",
            path.display()
        );
    }
    std::fs::write(path, EXAMPLE_TOML)?;
    println!("wrote {}", path.display());
    Ok(())
}

/// Run the MCP server: build the engine from env config, serve the chosen
/// transport until it returns (client disconnect / EOF for stdio, a shutdown
/// signal for streamable-http — see [`kutsu::mcp_http::shutdown_signal`]),
/// then hang up any still-live calls and best-effort shut down the engine.
async fn run_mcp(
    transport: String,
    bind: String,
    auth_token: Option<String>,
) -> anyhow::Result<()> {
    let (server_cfg, sip_cfg) = kutsu::main_support::configs_from_env()?;
    if server_cfg.max_concurrent_channels == 0 {
        tracing::warn!(
            "max_concurrent_channels is 0 — every placed call will queue forever and never dial; \
             raise the server config's max_concurrent_channels to enable calls"
        );
    }
    let engine = std::sync::Arc::new(
        kutsu::engine::Engine::new(std::sync::Arc::new(server_cfg), &sip_cfg).await?,
    );

    match transport.as_str() {
        "stdio" => {
            use rmcp::{ServiceExt, transport::io::stdio};
            let handler = kutsu::mcp::KutsuServer::new(engine.clone());
            let service = handler.serve(stdio()).await?;
            service.waiting().await?;
        }
        "streamable-http" => {
            // rmcp's default StreamableHttpServerConfig::allowed_hosts is
            // loopback-only, so a non-loopback --bind will have its /mcp
            // requests Host-rejected unless something in front of it (a
            // reverse proxy) rewrites the Host header to a loopback value.
            // This is informational only — we still bind and serve.
            let is_loopback = bind.starts_with("127.")
                || bind.starts_with("localhost")
                || bind.starts_with("[::1]");
            if !is_loopback {
                tracing::warn!(
                    %bind,
                    "streamable-http bound to a non-loopback address; rmcp's default \
                     allowed_hosts is loopback-only, so /mcp requests may be rejected \
                     unless fronted by a reverse proxy that sets a loopback Host header"
                );
            }
            kutsu::mcp_http::serve(engine.clone(), &bind, auth_token).await?;
        }
        // The bail short-circuits before teardown runs below, but that's
        // fine: no transport ever started serving, so no calls were placed
        // and nothing needs a BYE — the (unused) SIP socket is reclaimed on
        // process exit like any other early-error path.
        other => anyhow::bail!("unknown transport: {other}"),
    }

    // Graceful teardown after the transport returns (client disconnect / EOF
    // for stdio, a shutdown signal for streamable-http): hang up every live
    // call, then best-effort shut the engine's SIP transport down. Extracted
    // to `mcp_http::graceful_teardown` so it's unit-testable — see
    // `mcp_http::tests::graceful_teardown_cancels_live_calls`.
    //
    // Bounded, then force-exit: when the MCP client disconnects (stdio EOF) the
    // process MUST release its resources — especially the fixed SIP port — and
    // exit promptly. A host that churns MCP connections (spawn, probe, close)
    // would otherwise leave a zombie holding the port, so the next spawn fails
    // its bind (EADDRINUSE) and the tools go "unknown". A teardown that hangs
    // must not keep the process alive, so cap it and then exit unconditionally.
    tracing::info!("mcp transport closed — tearing down (<=5s) then exiting");
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        kutsu::mcp_http::graceful_teardown(engine),
    )
    .await;
    std::process::exit(0)
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
    greet_after_silence_ms: Option<u64>,
) -> anyhow::Result<i32> {
    // 1. Load scenario + server config (env: GEMINI_API_KEY, proxy).
    let scenario: kutsu::config::ScenarioConfig =
        serde_json::from_slice(&std::fs::read(&scenario_path)?)?;
    // Server config comes from the single env source (`configs_from_env`), so
    // every KUTSU_* knob (quality, retry, transcript/dump dirs, voice, ...)
    // takes effect here too. The CLI args below override only what this
    // subcommand sets explicitly; the SipConfig is unused on this path.
    let (mut server, _sip) = kutsu::main_support::configs_from_env()?;
    if let Some(m) = model.as_deref() {
        server.model = match m {
            "native" => kutsu::config::Model::NativeAudio,
            "half" => kutsu::config::Model::HalfCascade,
            x => anyhow::bail!("unknown model '{}' (expected 'half' or 'native')", x),
        };
    }
    if let Some(v) = voice {
        server.voice = v;
    }
    if let Some(ms) = greet_after_silence_ms {
        server.greet_after_silence_ms = ms;
    }

    // 2. Preflight (fail closed).
    if !no_net_check {
        let health = kutsu::net_check::preflight(&server, &scenario).await?;
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
    // Pre-answered stub: this CLI path has no ring/answer signal of its own.
    let mut session = kutsu::gemini_live::start(&server, &scenario, {
        let (_tx, rx) = tokio::sync::watch::channel(true);
        rx
    })
    .await?;

    // 4. Feed audio (32 ms frames at real-time pace) in a task.
    let samples = kutsu::audio_file::read_pcm16(&audio_in, 16000)?;
    // Input is 16 kHz mono PCM16; used below to extend the session deadline
    // so it stays open long enough to stream the whole input plus the tail.
    let input_len_secs = (samples.len() as u64 / 16000).max(1);
    let audio_tx = session.audio_in.clone();
    let tail_secs = tail;
    tokio::spawn(async move {
        for chunk in samples.chunks(512) {
            if audio_tx.send(chunk.to_vec()).await.is_err() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(32)).await;
        }
        // Keep the stream continuous with trailing silence so the model's
        // automatic VAD can detect end-of-speech and take its turn — a real
        // phone line never stops sending. Roughly `tail` seconds of silence.
        let silence = vec![0i16; 512];
        for _ in 0..(tail_secs * 1000 / 32) {
            if audio_tx.send(silence.clone()).await.is_err() {
                return;
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
            Ok(Some(_)) => {}
            Ok(None) => break, // session task ended
            Err(_) => {
                session.hangup().await;
                break;
            } // tail timeout
        }
    }
    if let Some(w) = out {
        w.finalize()?;
    }
    if let Some(g) = &goal
        && let Some(p) = goal_out
    {
        std::fs::write(p, serde_json::to_vec_pretty(g)?)?;
    }

    let outcome = session.join().await;
    eprintln!("ended_by={:?} goal={}", outcome.ended_by, goal.is_some());
    Ok(match outcome.ended_by {
        kutsu::gemini_live::EndedBy::Error => 1,
        _ => 0,
    })
}

async fn run_call_cli(number: String, scenario_path: Option<std::path::PathBuf>) -> i32 {
    let (server, sip_cfg) = match kutsu::main_support::configs_from_env() {
        Ok(x) => x,
        Err(e) => {
            eprintln!("config error: {e}");
            return 1;
        }
    };
    let scenario = match scenario_path {
        Some(p) => {
            let text = match std::fs::read_to_string(&p) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("failed to read scenario {p:?}: {e}");
                    return 1;
                }
            };
            match serde_json::from_str(&text) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("invalid scenario JSON in {p:?}: {e}");
                    return 1;
                }
            }
        }
        None => kutsu::main_support::default_scenario(),
    };

    let engine = match kutsu::engine::Engine::new(std::sync::Arc::new(server), &sip_cfg).await {
        Ok(e) => e,
        Err(e) => {
            eprintln!("engine init failed: {e}");
            return 1;
        }
    };
    let id = engine.place_call(number, scenario).await;

    // Poll the store until terminal; print transcript lines as they grow.
    let mut printed = 0usize;
    let final_state = loop {
        if let Some(rec) = engine.store().get(&id) {
            for entry in rec.transcript.iter().skip(printed) {
                println!("[{:?}] {}", entry.role, entry.text);
            }
            printed = rec.transcript.len();
            if matches!(rec.state, kutsu::state::CallState::Ended) {
                break rec.state;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };

    // Surface the resolved disposition + SIP/technical detail, not just the
    // coarse CallState — this is what tells busy/no-answer/not-found apart.
    let rec = engine.store().get(&id);
    let disposition = rec.as_ref().and_then(|r| r.disposition);
    let detail = rec.as_ref().and_then(|r| r.error.clone());
    println!(
        "call ended: {final_state:?}{}{}",
        disposition
            .map(|d| format!(" (disposition: {d:?})"))
            .unwrap_or_default(),
        detail.map(|d| format!(" — {d}")).unwrap_or_default(),
    );
    engine.shutdown().await;
    // Exit non-zero for any technical/dial failure so scripts can branch on it.
    // This preserves the pre-disposition CLI contract, where every unanswered
    // dial outcome collapsed into `CallState::Failed` -> exit 1. Answered
    // outcomes (Completed/Voicemail/Announcement/Ivr/Hold) and an operator
    // Cancelled are exit 0.
    use kutsu::state::Disposition;
    match disposition {
        Some(
            Disposition::Failed
            | Disposition::Busy
            | Disposition::NoAnswer
            | Disposition::Rejected
            | Disposition::NotFound
            | Disposition::Unavailable,
        ) => 1,
        _ => 0,
    }
}
