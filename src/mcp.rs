//! MCP server layer (`rmcp`).
//!
//! Planned tools, each a thin wrapper over [`crate::engine`] / [`crate::state`]:
//! - `place_call(to_number, system_prompt, lead)` — spawns the call in the
//!   background and returns a `call_id` immediately (async execution model,
//!   chosen to avoid MCP client tool-call timeouts on calls that run minutes).
//! - `get_call_status(call_id)`
//! - `get_call_transcript(call_id)`
//! - `end_call(call_id)`
//!
//! Enumerating all calls is deliberately *not* an MCP tool: the client that
//! placed a call already holds its `call_id` and polls that one call. A global
//! listing is a server-side operational concern (admin/HTTP), not a capability
//! the model needs to drive a call — mirroring the MCP Tasks extension, which
//! dropped `tasks/list` for the same reason.
//!
//! Not yet implemented. See the project plan for the `glossa`-style
//! `#[tool_router]` pattern this will follow.
