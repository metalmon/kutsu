# Configuration

This guide is for **operators** running kutsu. It covers the config file, the
environment overlay, secrets, and every tunable knob.

For the prompt/scenario model (persona vs. per-call goal), see
[prompts.md](prompts.md). For the MCP tool surface, see [mcp.md](mcp.md).

## Layering

kutsu resolves each setting from four sources, lowest to highest precedence:

```
built-in default  <  kutsu.toml  <  KUTSU_* environment variable  <  CLI flag
```

- **Built-in defaults** — every field has one; kutsu runs with no config at all
  (except the required secrets below).
- **`kutsu.toml`** — the file, discovered at `$KUTSU_CONFIG`, else `kutsu.toml`
  in the working directory. A missing file is fine; a partial file overrides
  only the keys it names.
- **`KUTSU_*` environment** — overrides any file/default value. Names are flat
  and unchanged from earlier releases (see the [reference table](#environment-variable-reference)).
- **CLI flags** — a few `kutsu live` flags (e.g. `--model`, `--voice`,
  `--greet-after-silence-ms`) override the resolved config for that one
  invocation. See [cli.md](cli.md).

Generate a documented starter file:

```bash
kutsu init                 # writes ./kutsu.toml (refuses to clobber)
kutsu init --force         # overwrite
kutsu init --path /etc/kutsu/kutsu.toml
```

The template `kutsu init` writes is [`kutsu.example.toml`](../kutsu.example.toml)
in the repo — the two never drift (the CLI embeds it at build time).

## Secrets

Secrets are **never read from `kutsu.toml`** — only from the environment, so the
committed file is safe to share. Put the real `kutsu.toml` (if it holds any
site-specific values you'd rather not commit) in `.gitignore`; ship
`kutsu.example.toml`.

| Variable | Purpose | Required |
|----------|---------|----------|
| `GEMINI_API_KEY` | Gemini Live API key | Yes |
| `KUTSU_SIP_PASS` | SIP digest password | For real calls |
| `PROXY_URL` / `PROXY_USER` / `PROXY_PASSWORD` | Egress proxy for the Gemini WebSocket | No |

## File structure

`kutsu.toml` has two top-level tables, `[server]` and `[sip]`, with `[server]`
carrying nested sub-tables. Every key is optional.

```toml
[server]
model = "half-cascade"          # or "native-audio"
voice = "Autonoe"
voice_gender = "female"         # male | female | neutral
language = "en-US"              # BCP-47
max_concurrent_channels = 3
greet_after_silence_ms = 1000
max_call_secs = 600
# transcript_dir = "./transcripts"
# dump_uplink_dir = "./uplink-dump"
# dump_downlink_dir = "./downlink-dump"

[server.net_check]              # pre-dial preflight + mid-call RTP-loss gate
enabled = true
samples = 10
max_rtt_ms = 300
max_jitter_ms = 50
max_loss_pct = 2.0
uplink_loss_abort_pct = 10.0

[server.quality]                # downlink playout pacing
prebuffer_ms = 800
resume_ms = 400
abort_underruns = 40

[server.retry]                  # transient dial-outcome retries
busy_max_attempts = 3
busy_retry_interval_ms = 300000

[server.vad]                    # callee speech-onset detection
min_rms = 200
ratio = 3.0
onset_frames = 3
warmup_frames = 10

[server.agc]                    # adaptive uplink gain
enabled = true
target_dbfs = -18.0
max_gain_db = 30.0
noise_floor_rms = 200.0

[server.prompts]                # all prompt text (see prompts.md)
# base_system_prompt = "..."
# closing = "..."
# ...

[sip]
server = "sip.example.com:5060"
username = "your-sip-login"
register = false
# from_user = "caller-id"
# local_ip = "203.0.113.10"
# local_port = 5060
# sip_domain = "sip.example.com"
# register_expiry_secs = 120
```

## Section reference

### `[server]`

| Key | Default | Meaning |
|-----|---------|---------|
| `model` | `half-cascade` | `half-cascade` (reliable telephone ASR) or `native-audio` (lower latency, ignores the structured language code). |
| `voice` | `Autonoe` | Gemini voice name. |
| `voice_gender` | `female` | Grammatical gender the agent refers to itself in — `male`/`female`/`neutral`. Match it to the voice. |
| `language` | `en-US` | Spoken language, BCP-47. Also pinned into the prompt (see [prompts.md](prompts.md)). |
| `max_concurrent_channels` | `3` | Simultaneous calls; extras queue. `0` queues everything forever (never dials). |
| `greet_after_silence_ms` | `1000` | Silence window after answer before the agent greets first. `0` = purely reactive. |
| `max_call_secs` | `600` | Hard cap on one call's duration. |
| `transcript_dir` | *(none)* | Directory for finalized per-call `CallRecord` JSON. |
| `dump_uplink_dir` / `dump_downlink_dir` | *(none)* | Per-call WAV dumps for offline analysis. |

### `[server.net_check]`

Pre-dial network preflight (Gemini leg) plus a mid-call rolling-window uplink
RTP-loss abort (callee leg).

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `true` | Run the pre-dial preflight; fail-closed if it fails. |
| `samples` | `10` | Ping samples for the preflight probe. |
| `max_rtt_ms` | `300` | Fail preflight above this median RTT. |
| `max_jitter_ms` | `50` | Fail preflight above this p95−p50 jitter. |
| `max_loss_pct` | `2.0` | Fail preflight above this ping-loss %. |
| `uplink_loss_abort_pct` | `10.0` | Abort a live call above this rolling (~8 s window) uplink RTP loss %. |
| `downlink_loss_abort_pct` | `10.0` | Abort above this loss % the callee reports via RTCP receiver reports (our audio → callee). Best-effort: only active when the carrier sends RR. |

### `[server.quality]`

Downlink playout pacing — the latency-vs-glitch tradeoff.

| Key | Default | Meaning |
|-----|---------|---------|
| `prebuffer_ms` | `800` | Buffer target before (re)starting playout. |
| `resume_ms` | `400` | Faster re-arm target after a mid-turn underrun. |
| `abort_underruns` | `40` | Cumulative underruns that abort a call as unusable. `0` = never. |

### `[server.retry]`

| Key | Default | Meaning |
|-----|---------|---------|
| `busy_max_attempts` | `3` | Dial attempts for a busy number, including the first. |
| `busy_retry_interval_ms` | `300000` | Delay before a busy retry (5 min). |

### `[server.vad]`

Energy-VAD tuning for detecting that the callee has started speaking.

| Key | Default | Meaning |
|-----|---------|---------|
| `min_rms` | `200` | Absolute RMS floor; below this is never speech. |
| `ratio` | `3.0` | Speech = RMS ≥ max(`min_rms`, noise_floor × `ratio`). |
| `onset_frames` | `3` | Consecutive speech frames to confirm onset (rejects clicks). |
| `warmup_frames` | `10` | Startup window (~200 ms at 20 ms/frame) for early-onset detection. |

### `[server.agc]`

Adaptive gain on the uplink (callee → Gemini); real trunks arrive quiet.

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `true` | Master toggle; when off the uplink is untouched. |
| `target_dbfs` | `-18.0` | Level sustained speech is driven toward. |
| `max_gain_db` | `30.0` | Ceiling on applied gain. |
| `noise_floor_rms` | `200.0` | Below this a frame is treated as silence; gain is held. |

### `[server.prompts]`

All prompt text. Every key has an English-only default; see
[prompts.md](prompts.md) for what each one does and the assembly order.

| Key | Overridable via env |
|-----|---------------------|
| `base_system_prompt` | `KUTSU_SYSTEM_PROMPT` |
| `goal_preamble` | — |
| `closing` | — |
| `greet_cue` | `KUTSU_GREET_CUE` |
| `resume_cue` | `KUTSU_RESUME_CUE` |
| `gender_female` / `gender_male` | — |
| `language_template` | — |
| `amd_instruction` | — |

### `[sip]`

| Key | Default | Meaning |
|-----|---------|---------|
| `server` | *(empty)* | Trunk as `host:port`. |
| `username` | *(empty)* | Digest username (also the default caller identity). |
| *(password)* | — | From `KUTSU_SIP_PASS` only. |
| `register` | `false` | Send REGISTER before calling (login/password trunks). |
| `from_user` | *(username)* | From-header user-part. |
| `local_ip` | *(auto)* | Bind + advertise in SDP; auto-detected toward `server`. |
| `local_port` | *(ephemeral)* | Fixed source port for `IP:port`-authorized trunks. |
| `sip_domain` | *(server host)* | SIP domain for URIs/REGISTER (domain-routed trunks). |
| `register_expiry_secs` | *(stack default)* | Requested REGISTER binding expiry. |

## Environment variable reference

Every `KUTSU_*` variable overrides the corresponding config field. Only
variables that are set take effect, so the file/default shows through otherwise.

| Variable | Field |
|----------|-------|
| `KUTSU_CONFIG` | Path to the TOML file (default `kutsu.toml`). |
| `KUTSU_MODEL` | `server.model` |
| `KUTSU_VOICE` | `server.voice` |
| `KUTSU_VOICE_GENDER` | `server.voice_gender` |
| `KUTSU_LANGUAGE` | `server.language` |
| `KUTSU_MAX_CONCURRENT_CHANNELS` | `server.max_concurrent_channels` |
| `KUTSU_GREET_AFTER_SILENCE_MS` | `server.greet_after_silence_ms` |
| `KUTSU_MAX_CALL_SECS` | `server.max_call_secs` |
| `KUTSU_TRANSCRIPT_DIR` | `server.transcript_dir` |
| `KUTSU_DUMP_UPLINK_DIR` / `KUTSU_DUMP_DOWNLINK_DIR` | audio dump dirs |
| `KUTSU_NETCHECK_ENABLED` | `server.net_check.enabled` |
| `KUTSU_NETCHECK_SAMPLES` | `server.net_check.samples` |
| `KUTSU_NETCHECK_MAX_RTT_MS` | `server.net_check.max_rtt_ms` |
| `KUTSU_NETCHECK_MAX_JITTER_MS` | `server.net_check.max_jitter_ms` |
| `KUTSU_NETCHECK_MAX_LOSS_PCT` | `server.net_check.max_loss_pct` |
| `KUTSU_UPLINK_LOSS_ABORT_PCT` | `server.net_check.uplink_loss_abort_pct` |
| `KUTSU_DOWNLINK_LOSS_ABORT_PCT` | `server.net_check.downlink_loss_abort_pct` |
| `KUTSU_QUALITY_PREBUFFER_MS` | `server.quality.prebuffer_ms` |
| `KUTSU_QUALITY_RESUME_MS` | `server.quality.resume_ms` |
| `KUTSU_QUALITY_ABORT_UNDERRUNS` | `server.quality.abort_underruns` |
| `KUTSU_BUSY_MAX_ATTEMPTS` | `server.retry.busy_max_attempts` |
| `KUTSU_BUSY_RETRY_INTERVAL_MS` | `server.retry.busy_retry_interval_ms` |
| `KUTSU_VAD_MIN_RMS` | `server.vad.min_rms` |
| `KUTSU_VAD_RATIO` | `server.vad.ratio` |
| `KUTSU_VAD_ONSET_FRAMES` | `server.vad.onset_frames` |
| `KUTSU_VAD_WARMUP_FRAMES` | `server.vad.warmup_frames` |
| `KUTSU_AGC_ENABLED` | `server.agc.enabled` |
| `KUTSU_AGC_TARGET_DBFS` | `server.agc.target_dbfs` |
| `KUTSU_AGC_MAX_GAIN_DB` | `server.agc.max_gain_db` |
| `KUTSU_AGC_NOISE_FLOOR_RMS` | `server.agc.noise_floor_rms` |
| `KUTSU_SYSTEM_PROMPT` | `server.prompts.base_system_prompt` |
| `KUTSU_GREET_CUE` | `server.prompts.greet_cue` |
| `KUTSU_RESUME_CUE` | `server.prompts.resume_cue` |
| `KUTSU_SIP_SERVER` | `sip.server` |
| `KUTSU_SIP_USER` | `sip.username` |
| `KUTSU_SIP_PASS` | `sip.password` *(secret)* |
| `KUTSU_SIP_FROM_USER` | `sip.from_user` |
| `KUTSU_SIP_DOMAIN` | `sip.sip_domain` |
| `KUTSU_SIP_LOCAL_IP` | `sip.local_ip` |
| `KUTSU_SIP_LOCAL_PORT` | `sip.local_port` |
| `KUTSU_SIP_REGISTER` | `sip.register` |
| `KUTSU_SIP_REGISTER_EXPIRY` | `sip.register_expiry_secs` |

Booleans accept `1/true/yes/on` and `0/false/no/off`.

### Runtime-only variables

Read directly at startup, not part of the config file:

| Variable | Default | Meaning |
|----------|---------|---------|
| `KUTSU_LOG_FORMAT` | `text` | `text` (dev) or `json` (SIEM-parseable). Also `--log-format`. |
| `KUTSU_RT_PRIORITY` | `on` | Raise the RTP send/recv thread to real-time priority (best-effort). |
| `KUTSU_MCP_TRANSPORT` | `stdio` | `stdio` or `streamable-http`. Also `--transport`. |
| `KUTSU_MCP_BIND` | `127.0.0.1:8090` | Bind address for `streamable-http`. Also `--bind`. |
| `KUTSU_MCP_TOKEN` | *(none)* | Bearer token required on `/mcp`. Also `--auth-token`. |

See [mcp.md](mcp.md) for the server transport and ops endpoints.
