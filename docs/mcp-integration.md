# MCP integration

How to register the kutsu MCP server in popular MCP clients and install the
`place-call` skill. For the tools themselves and the call lifecycle, see
[mcp.md](mcp.md); for every config knob and the full `KUTSU_*` reference, see
[configuration.md](configuration.md).

## Transports

kutsu speaks MCP over two transports:

- **stdio** — the client spawns `kutsu mcp` as a subprocess. Use this when the
  client runs on the same host as kutsu (the common case).
- **streamable-http** — `kutsu mcp --transport streamable-http --bind
  127.0.0.1:8090 [--auth-token <token>]`. Use this when the client and kutsu run
  on different hosts.

## Prerequisites

- The `kutsu` binary on `PATH` (or use an absolute path in `command`).
- Required environment for the server process:
  - `GEMINI_API_KEY` — required.
  - `PROXY_URL` / `PROXY_USER` / `PROXY_PASSWORD` — if Gemini is geo-blocked.
  - `KUTSU_SIP_SERVER`, `KUTSU_SIP_DOMAIN`, `KUTSU_SIP_USER`, `KUTSU_SIP_PASS`,
    `KUTSU_SIP_REGISTER`, `KUTSU_SIP_LOCAL_IP`, `KUTSU_SIP_LOCAL_PORT` — the SIP
    trunk. See [configuration.md](configuration.md).
  - Optional: `KUTSU_LANGUAGE`, `KUTSU_VOICE_GENDER`, or `KUTSU_CONFIG` pointing
    to a `kutsu.toml` for non-secret knobs (keep secrets in env, not in a
    committed config).

> **Secrets:** many of the config files below are committed to a repo
> (`.mcp.json`, `.vscode/mcp.json`, `.cursor/mcp.json`). Do not put secrets in a
> tracked file — inject them from the environment, a gitignored config, or the
> client's secret-input mechanism.

## The common stdio config

Most clients use the same JSON shape (`mcpServers` map). This block is reused
below; only the file location differs:

```json
{
  "mcpServers": {
    "kutsu": {
      "command": "kutsu",
      "args": ["mcp"],
      "env": {
        "GEMINI_API_KEY": "…",
        "KUTSU_SIP_SERVER": "203.0.113.10:5060",
        "KUTSU_SIP_DOMAIN": "sip.example.com",
        "KUTSU_SIP_USER": "…",
        "KUTSU_SIP_PASS": "…",
        "KUTSU_SIP_REGISTER": "true"
      }
    }
  }
}
```

## Per-client setup

### Claude Code

CLI (recommended):

```bash
claude mcp add kutsu \
  --env GEMINI_API_KEY=… --env KUTSU_SIP_USER=… --env KUTSU_SIP_PASS=… \
  -- kutsu mcp
```

Or commit a project-scoped `.mcp.json` at the repo root using the common
`mcpServers` block above. HTTP transport:

```bash
claude mcp add --transport http kutsu http://127.0.0.1:8090/mcp \
  --header "Authorization: Bearer <token>"
```

### Claude Desktop

Edit `claude_desktop_config.json` (macOS: `~/Library/Application
Support/Claude/`, Windows: `%APPDATA%\Claude\`) and add the common `mcpServers`
block. Restart Claude Desktop.

### Cursor

Create `.cursor/mcp.json` in the project (or `~/.cursor/mcp.json` globally) with
the common `mcpServers` block.

### VS Code (GitHub Copilot agent mode)

Create `.vscode/mcp.json`. VS Code uses a `servers` key with an explicit `type`:

```json
{
  "servers": {
    "kutsu": {
      "type": "stdio",
      "command": "kutsu",
      "args": ["mcp"],
      "env": { "GEMINI_API_KEY": "…", "KUTSU_SIP_USER": "…", "KUTSU_SIP_PASS": "…" }
    }
  }
}
```

### Windsurf

Edit `~/.codeium/windsurf/mcp_config.json` and add the common `mcpServers`
block.

### Cline (VS Code extension)

Open Cline → MCP Servers → Configure, which edits `cline_mcp_settings.json`; add
the common `mcpServers` block.

### Generic MCP client

Spawn `kutsu mcp` as a stdio subprocess (JSON-RPC over stdin/stdout), or point
the client at `http://<host>:8090/mcp` for the streamable-http transport (send
`Authorization: Bearer <token>` when `--auth-token` is set).

## Installing the place-call skill

The repository ships a ready skill at
[`skills/place-call/SKILL.md`](../skills/place-call/SKILL.md) — how an agent
should drive the tools (place → poll status → read disposition/goal), what the
dispositions mean, and that retries (except `busy`) are the agent's job.

- **Claude Code / Claude agents:** copy the `place-call/` directory into the
  agent's skills directory (e.g. a project `.claude/skills/` or the agent's
  configured skills path) so it is discovered as a skill.
- **Other clients** (Cursor, VS Code, …) have no "skill" concept — paste the
  body of `SKILL.md` into the client's rules / system-prompt so the model knows
  how to use the tools.

## Agent-runtime note (bundled runtimes)

Some agent runtimes scope MCP servers and skills per agent (e.g. a
`servers`/`bundles` model plus a per-agent tool allow-list). To wire kutsu into
one such agent:

1. Register the server (`kutsu mcp`, stdio) with the required `env`.
2. Add it to the agent's MCP scope (bundle/allow-list) so the agent sees the
   `place_call` / `get_call_status` / `get_call_transcript` / `end_call` tools.
3. Install the skill into that agent's skills directory.
4. Decide autonomy: `place_call` and `end_call` place/stop **real, paid** calls —
   prefer requiring confirmation unless the agent is trusted to dial
   autonomously.

## Caveats

- **SIP singleton.** A running kutsu MCP server holds a UDP port
  (`KUTSU_SIP_LOCAL_PORT`, default 5060) and one trunk registration for its
  lifetime. Do not run a second kutsu on the same host/port/account in parallel
  (bind conflict + last-registration-wins).
- **Real calls.** The agent can place billed outbound calls. Gate `place_call`
  behind confirmation for untrusted agents.
