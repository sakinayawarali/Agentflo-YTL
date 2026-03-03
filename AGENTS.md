# Agentflo EBM Agent Architecture

This file is the map of how the Agentflo EBM WhatsApp agent is wired. Use it to
orient new engineers, debug issues quickly, and keep system changes consistent.

How to use this file
- Start with "Request Flow" to trace a message end-to-end.
- Use "Key Modules" to jump into the right file when debugging or extending.
- Follow "Tool Contracts" when adding or modifying tools.
- Check "State and Storage" when investigating missing sessions, carts, or auth.

## Request Flow (Happy Path)
1) Incoming webhook -> Flask `app.py` -> `RouteHandler` in `agents/helpers/route_handlers.py`.
2) `RouteHandler` validates, dedupes, and normalizes the message.
3) `RouteHandler` calls `ADKHelper.handle_message(...)`.
4) `ADKHelper` acquires the per-user singleflight lock, authenticates the user,
   and ensures session + customer metadata.
5) `ADKHelper` runs the LLM via Google ADK `Runner` (see `agents/agent.py`).
6) The LLM calls tools (search, cart, templates, order, etc.) as needed.
7) `ADKHelper` sends the response via WhatsApp (Meta or Twilio transport).

## Key Modules
- `app.py`
  Flask entrypoint, exposes `/webhook` and Twilio status hooks.

- `agents/helpers/route_handlers.py`
  Webhook parsing, inbound dedupe/staleness checks, onboarding + invoice flow,
  media handling, and main routing to `ADKHelper`.

- `agents/helpers/adk_helper.py`
  Core orchestrator: authentication, session creation/rotation, singleflight lock,
  LLM invocation, WhatsApp send logic, and VN/TTS handling.

- `agents/helpers/adk/mixins/*`
  Shared mixins for session, WhatsApp, TTS, and runner integration.

- `agents/agent.py`
  Defines the `LlmAgent`, system prompt assembly, and tool list.

- `agents/prompt/prompt_template.txt`
  The system prompt template. Language personas are injected at runtime via
  `agents/prompt/prompt_creator.py`.

- `agents/tools/*`
  Tool implementations: product search, cart, order, templates, and sales intel.

## Tool Contracts
- API tools (`agents/tools/api_tools.py`) return a strict ToolResponse:
  `{success, data, error{code,message,retryable}, source{system,timestamp}}`
  Always read `data` only when `success == true`.

- Template tools (e.g., `order_draft_template`) return fully formatted text.
  The agent should output the tool result as-is.

- Cart tool (`agentflo_cart_tool`) returns a Firestore-backed cart with pricing
  and promotions refreshed from Sales Intelligence.

## State and Storage (Firestore)
All per-user documents live under:
`tenants/{tenant_id}/users/{user_id}`

Common collections:
- `sessions_meta` (session lifecycle state + counters)
- `conversations` (conversation lifecycle state)
- `store_carts` (draft order/cart per store)
- `inbound_messages` (idempotency records)
- `message_locks` (singleflight lock for processing)

## External Services
- WhatsApp Cloud API (Meta) or Twilio (transport toggle via env)
- Google ADK + Vertex AI sessions
- Lambda APIs for customer/product lookup
- Sales Intelligence API for pricing/promotions
- ElevenLabs for voice notes
- S3 for VN storage (meta path)

## Voice / VN Flow
- Audio inbound is transcribed to text and sent through the same agent flow.
- TTS is generated after text response when enabled (ElevenLabs).

## Extending the Agent
- New tool: implement in `agents/tools`, add to `agents/agent.py` tool list,
  and follow ToolResponse conventions for any API call tools.
- Prompt changes: update `agents/prompt/prompt_template.txt` (do not edit
  compiled prompts directly).
- New language packs: add config + persona under `agents/prompt/languages/`.

## Debugging Tips
- Tool failures: check API credentials (`API_JWT_TOKEN`, endpoints).
- Wrong draft/cart: inspect Firestore `store_carts` and message lock behavior.
- Malformed tool calls: see `_recover_malformed_template_call` in
  `agents/helpers/adk_helper.py`.
  
