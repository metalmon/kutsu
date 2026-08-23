# kutsu documentation

User-facing documentation for kutsu — an outbound SIP calling MCP server that
bridges phone calls to Gemini Live.

## Guides

| Document | Audience | Summary |
|----------|----------|---------|
| [getting-started.md](getting-started.md) | Everyone | Build, `kutsu init`, first call (CLI + MCP) |
| [configuration.md](configuration.md) | Operators | Config file, env overlay, secrets, every knob, full `KUTSU_*` reference |
| [prompts.md](prompts.md) | Integrators | Persona (config) vs. per-call objective (`goal_schema` + `context`); assembly order; localization |
| [mcp.md](mcp.md) | Agent integrators | MCP tools (`place_call`, …), call lifecycle, transports, ops endpoints |
| [mcp-integration.md](mcp-integration.md) | Agent integrators | Register kutsu in popular MCP clients (Claude Code/Desktop, Cursor, VS Code, Windsurf, Cline) + install the `place-call` skill |
| [cli.md](cli.md) | Everyone | `init`, `mcp`, `call`, `live` subcommands |

## Reference files

| Path | What |
|------|------|
| [../kutsu.example.toml](../kutsu.example.toml) | Documented default config (what `kutsu init` writes) |
| [examples/scenario.json](examples/scenario.json) | Example per-call scenario |
| [../skills/place-call/SKILL.md](../skills/place-call/SKILL.md) | Ready-to-install agent skill for driving the call tools |

## Related

- [../README.md](../README.md) — project overview and quickstart
