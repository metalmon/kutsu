# Getting started

This guide takes you from a source checkout to a first call.

## Build

```bash
cargo build --release
```

The binary is `target/release/kutsu` (`target\release\kutsu.exe` on Windows).
Some platforms need a vendored OpenSSL for the TLS/WebSocket path:

```bash
cargo build --release --features vendor-openssl
```

## Configure

Write a starter config and edit it:

```bash
kutsu init          # -> ./kutsu.toml
```

Fill in your `[sip]` trunk. Then export the secrets (never put them in the
file):

```bash
export GEMINI_API_KEY="..."
export KUTSU_SIP_PASS="..."
# optional egress proxy for the Gemini WebSocket:
# export PROXY_URL="http://proxy:8080"
```

kutsu finds the file at `$KUTSU_CONFIG`, else `kutsu.toml` in the working
directory. See [configuration.md](configuration.md) for every knob and the
default → file → env → CLI precedence.

## Place a call from the CLI

```bash
kutsu call +15551234567 --scenario ./scenario.json
```

`scenario.json` carries only the per-call layer — the objective and optional
context; the persona comes from config:

```json
{
  "goal_schema": {
    "type": "object",
    "required": ["disposition"],
    "properties": {
      "disposition": { "type": "string",
        "description": "Did the customer confirm the appointment?" }
    }
  },
  "context": { "name": "Ivan", "appointment": "2026-09-01 14:00" }
}
```

Why only the objective and context? See [prompts.md](prompts.md).

## Run as an MCP server

```bash
kutsu mcp                                 # stdio, for a local IDE/agent
kutsu mcp --transport streamable-http     # HTTP for remote clients
```

Agents call the `place_call` tool with the same `goal_schema` + `context`. See
[mcp.md](mcp.md) for the tool list, the call lifecycle, and ops endpoints.

## Iterate without a phone line

The `kutsu live` harness runs one conversation against Gemini Live from an audio
file — no SIP — which is the fastest way to tune prompts:

```bash
kutsu live --scenario ./scenario.json --audio-in ./in.wav --audio-out ./out.wav
```

See [cli.md](cli.md) for all flags.

## Next steps

- [configuration.md](configuration.md) — every setting, env overlay, secrets
- [prompts.md](prompts.md) — persona vs. per-call objective
- [mcp.md](mcp.md) — MCP tools and deployment
- [cli.md](cli.md) — all subcommands
