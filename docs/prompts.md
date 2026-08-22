# Prompts & scenarios

This guide explains how kutsu builds a call's system instruction, and the split
between the **deployment-stable persona** (config) and the **per-call intent**
(the `place_call` arguments).

For where the prompt text lives and how to override it, see
[configuration.md § `[server.prompts]`](configuration.md#serverprompts).

## Two layers

kutsu deliberately separates what is stable across a campaign from what changes
per call:

| Layer | Where it lives | Changes per call? |
|-------|----------------|-------------------|
| **Persona / behaviour** | `[server.prompts].base_system_prompt` (config) | No — set once per deployment |
| **Protocol scaffolding** | `[server.prompts]` (closing, gender, language, cues) | No |
| **Objective** | `goal_schema` passed to `place_call` | **Yes** |
| **Lead / context** | `context` passed to `place_call` | **Yes** |
| **Persona override** | `prompt_override` passed to `place_call` (optional) | Yes (rare) |

The caller does **not** send a full system prompt on every call. It sends only
the call's objective (`goal_schema`) and, optionally, `context` and a one-off
`prompt_override`.

## The objective *is* the goal_schema

kutsu has no separate free-text "goal" field. The call's objective is carried by
the `goal_schema` — a JSON Schema — in two ways:

1. **As the `end_call` tool parameters.** The agent fills and submits it to end
   the call; the filled value is the call's structured outcome (disposition,
   collected fields, …).
2. **Injected into the system prompt.** The schema JSON is rendered into the
   prompt after `goal_preamble`, so the model knows what to *steer the
   conversation toward and gather* — not only how to shape the final payload.

Because the schema is the single source of truth for the objective, **write good
`description` fields**: they are what tell the model what each value means and,
implicitly, what the call is for.

```json
{
  "type": "object",
  "required": ["disposition"],
  "properties": {
    "disposition": {
      "type": "string",
      "enum": ["renewed", "declined", "callback"],
      "description": "Outcome: did the customer renew, decline, or ask for a callback?"
    },
    "callback_time": {
      "type": "string",
      "description": "If disposition is callback, the requested time in the caller's words."
    }
  }
}
```

## Assembly order

[`ServerConfig::assemble_system_instruction`] composes the final system
instruction in this order:

```
1. prompt_override  ??  base_system_prompt      # persona
2. goal_preamble + <goal_schema as pretty JSON> # objective
3. "# Contact context" + context                # if context present
4. gender_female | gender_male | ""             # per voice_gender
5. closing                                       # end_call directive
6. language_template with {language} substituted # spoken-language pin
```

The `goal_schema` also travels separately to the crate as the `end_call` tool
schema; the language directive is repeated in-prompt because native-audio
ignores the structured language code.

## Runtime cues

Two prompt strings are sent as user turns *during* a call, not part of the
system instruction:

- **`greet_cue`** — handed to the model when the callee stays silent past
  `greet_after_silence_ms`, so it greets first. The wording of the greeting
  itself comes from the persona; the cue only hands over the turn.
- **`resume_cue`** — sent after a reconnect that lost context mid-exchange, so
  the model asks the other party to repeat.

## Localization & clean-OSS

The shipped prompt defaults are **English-only**. kutsu speaks whatever language
you configure (`language`), but the *default scaffolding text* stays English so
the repository has no embedded target-language content.

Language-specific wording belongs in your **local `kutsu.toml`**, not in the
code. Many languages inflect verbs/adjectives by speaker gender; anchor the
correct forms for your target language in a local override (add your language's
gendered examples where the placeholder is):

```toml
[server.prompts]
gender_female = """

# Your voice
You speak with a FEMALE voice. Always use feminine grammatical forms
(verbs, adjectives, participles) - for example: <your-language forms here>.
Never use masculine self-reference.
"""
```

Since `kutsu.toml` is not committed, target-language text lives only in your
deployment, keeping the repository English-only.

## Per-call examples

Minimal (persona from config, objective only):

```jsonc
// place_call arguments
{
  "to_number": "+15551234567",
  "goal_schema": { "type": "object", "required": ["disposition"],
    "properties": { "disposition": { "type": "string",
      "description": "Did they confirm the appointment?" } } }
}
```

With context and a one-off persona override:

```jsonc
{
  "to_number": "+15551234567",
  "goal_schema": { "...": "..." },
  "context": { "name": "Alex Carter", "plan": "Pro", "renews_on": "2026-09-01" },
  "prompt_override": "You are a debt collector. Be firm but polite."
}
```

See [mcp.md](mcp.md) for the full `place_call` contract.
