//! MCP server layer (`rmcp`).
//!
//! Planned tools, each a thin wrapper over [`crate::engine`] / [`crate::state`]:
//! - `place_call(to_number, system_prompt, lead)` — spawns the call in the
//!   background and returns a `call_id` immediately (async execution model,
//!   chosen to avoid MCP client tool-call timeouts on calls that run minutes).
//! - `get_call_status(call_id)`
//! - `get_call_transcript(call_id)`
//! - `end_call(call_id)`
//! - `list_calls()`
//!
//! Not yet implemented. See the project plan for the `glossa`-style
//! `#[tool_router]` pattern this will follow.
