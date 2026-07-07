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
        None => {
            println!(
                "kutsu {} — outbound SIP calling MCP server (scaffold, not yet functional)",
                kutsu::version()
            );
            Ok(())
        }
    }
}
