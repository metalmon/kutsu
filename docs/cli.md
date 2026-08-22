# CLI reference

`kutsu` has four subcommands. Global flag `--log-format text|json`
(`KUTSU_LOG_FORMAT`) applies to all of them.

All subcommands read the same layered config (see
[configuration.md](configuration.md)); `GEMINI_API_KEY` is always required.

## `kutsu init`

Write a documented default config.

```bash
kutsu init                              # -> ./kutsu.toml (refuses to clobber)
kutsu init --force                      # overwrite
kutsu init --path /etc/kutsu/kutsu.toml
```

The content is [`kutsu.example.toml`](../kutsu.example.toml), embedded at build
time so the CLI output and the in-repo example never drift.

## `kutsu mcp`

Run the MCP server. See [mcp.md](mcp.md).

```bash
kutsu mcp
kutsu mcp --transport streamable-http --bind 0.0.0.0:8090 --auth-token "$TOKEN"
```

| Flag | Env | Default |
|------|-----|---------|
| `--transport` | `KUTSU_MCP_TRANSPORT` | `stdio` |
| `--bind` | `KUTSU_MCP_BIND` | `127.0.0.1:8090` |
| `--auth-token` | `KUTSU_MCP_TOKEN` | *(none)* |

## `kutsu call`

Place one outbound call from the CLI and print the transcript.

```bash
kutsu call +15551234567
kutsu call +15551234567 --scenario ./scenario.json
```

`--scenario` is an optional JSON file with the per-call layer; without it a
minimal default (empty goal schema, no context) is used:

```json
{
  "goal_schema": { "type": "object", "required": ["disposition"],
    "properties": { "disposition": { "type": "string",
      "description": "Call outcome" } } },
  "context": { "name": "Alex Carter" },
  "prompt_override": null
}
```

The persona comes from config, not the scenario file — see
[prompts.md](prompts.md).

## `kutsu live`

Developer harness: run one conversation against Gemini Live from a scenario plus
an audio file (no SIP). Useful for iterating on prompts and audio handling.

```bash
kutsu live --scenario ./scenario.json --audio-in ./in.wav \
  --audio-out ./out.wav --transcript ./t.jsonl --goal-out ./goal.json
```

| Flag | Meaning |
|------|---------|
| `--scenario` | Scenario JSON (`goal_schema`, `context?`, `prompt_override?`). |
| `--audio-in` | Mono PCM16 WAV or raw `.pcm` at 16 kHz. |
| `--audio-out` | Output WAV (model speech, 24 kHz). |
| `--transcript` | Transcript JSONL output. |
| `--goal-out` | Filled goal JSON output. |
| `--model` | Override model: `half` \| `native`. |
| `--voice` | Override voice. |
| `--tail` | Seconds to keep the session open after input ends (default 8). |
| `--no-net-check` | Skip the network preflight (offline debugging). |
| `--greet-after-silence-ms` | Override the greet delay (0 = reactive). |
