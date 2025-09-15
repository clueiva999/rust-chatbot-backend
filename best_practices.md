# 📘 Project Best Practices

## 1. Project Purpose
Rust-based, fully async WebSocket backend using Axum to power an AI assistant with streaming responses from Groq's OpenAI-compatible API, including optional vision (image) input. The service authenticates users against a Next.js app (via bearer token), gates features to Pro users, maintains short chat history per connection, and streams assistant deltas over WebSocket.

## 2. Project Structure
- Root
  - Cargo.toml / Cargo.lock: Rust package and dependency manifest.
  - Dockerfile: Multi-stage build using cargo-chef for cached dependency layers; minimal Debian runtime.
  - fly.toml: Fly.io deployment config (ports, health checks, concurrency, VM size, env).
  - .env: Local environment variables (not committed by default best practice).
  - README.md: Minimal instructions; extend as needed.
  - static/
    - index.html: Demo UI that connects via WebSocket. Note: current demo sends raw strings and a custom "!context" command; backend expects JSON protocol—see Protocol section below.
- src/
  - main.rs: Single-file backend containing:
    - Type definitions (AuthenticatedUser, SessionContext, WsMsg enum)
    - Shared state with RwLock<HashMap<sid, SessionContext>>
    - WebSocket upgrade and main loop
    - Handlers: handle_chat (text) and handle_image_chat (vision)
    - Auth helpers (fetch_user_context)
    - Constants (MODEL_ID, HOUSE_RULES_PROMPT)
  - enterprise.md: Duplicate/system prompt variant kept as reference. Prefer single source of truth.
  - main.rs.bkp: Prior version kept for reference; do not ship to production.

Recommended refactor (when scope grows):
- src/
  - lib.rs (optional)
  - config.rs (env parsing, model selection)
  - auth.rs (authenticate_user, fetch_user_context)
  - ws.rs (WsMsg, ws_handler, ws_loop)
  - handlers/
    - chat.rs (handle_chat)
    - image_chat.rs (handle_image_chat)
  - llm.rs (trait + impl over async-openai Client for testability)
  - state.rs (AppState, Shared aliases)
  - prompts/
    - house_rules.md (externalized prompt content)

Entry points and configuration
- Binary crate entry: src/main.rs
- Env vars used:
  - GROQ_API_KEY (required)
  - NEXTJS_URL (defaults to https://clueiva.com)
  - PORT=8080 (Fly)
- Network: binds 0.0.0.0:8080 (container-friendly)

## 3. Test Strategy
Frameworks and tools
- Built-in: cargo test
- Async: #[tokio::test] for async unit/integration tests
- HTTP/Router: axum testing with tower::Service or axum-test helpers
- Mocking external HTTP: wiremock or httpmock for fetch_user_context (reqwest)
- WebSockets: tokio-tungstenite for client-side integration tests to the in-process server

Organization
- Unit tests colocated in each module (mod tests {})
- Integration tests in tests/ directory
- Use feature flags or a trait abstraction to mock LLM client:
  - Define a trait LlmClient { fn stream_chat(...); fn stream_vision(...); }
  - Provide a real impl wrapping async-openai Client
  - Provide a mock impl for tests to return deterministic deltas

Guidelines
- Unit tests
  - auth: validate Pro/Non-Pro logic, error surfaces for missing aiContext
  - guard_pro: correct gating by Pro flag and auth state
  - message building: correct inclusion of system prompt, trimming history to MAX_TURNS
- Integration tests
  - WS happy path: authenticate -> chat -> stream deltas -> done
  - Error conditions: unauthenticated chat, non-Pro access, invalid token from NEXTJS_URL
  - Vision path: send image_chat payload using reasonable image URL and verify streaming
- Determinism
  - Fix model + low temperature in tests
  - Mock LLM streams to deterministic token sequences
- Coverage expectations
  - Target: core paths ≥80% (auth helpers, guards, handlers, ws loop branches)
- CI
  - Run cargo fmt -- --check and cargo clippy -- -D warnings before cargo test

## 4. Code Style
Language/async
- Use anyhow::Result in application boundaries; convert/display user-safe errors on the wire
- Avoid unwrap()/expect() in production paths; prefer ? with context
- Keep lock scopes minimal: acquire RwLock just long enough, never hold across .await unless necessary
- Prefer cloning small values over holding locks while building requests

Naming conventions
- Modules, files, functions: snake_case
- Types and traits: PascalCase
- Constants: SCREAMING_SNAKE_CASE
- JSON field naming: use serde rename to match external API contracts (e.g., isPro, type)

Comments and documentation
- Document public functions and types with concise rustdoc
- Keep embedded prompts in separate file when they grow; reference path in code

Error handling
- Convert internal errors to WsMsg::Error with concise messages
- Do not leak secrets or stack traces over WebSocket
- Log errors with tracing::error and include correlation (sid) where feasible

Formatting and linting
- Enforce rustfmt and clippy
- Prefer small, testable functions in modules rather than a monolithic main.rs

Security
- Validate and sanitize inputs (message size, image URL format)
- Consider max message size and rate limiting per connection
- Ensure tokens are only kept in memory; do not log token values

## 5. Common Patterns
- WebSocket loop pattern
  - Deserialize WsMsg from incoming JSON
  - Authenticate then gate functionality via guard_pro
  - Stream assistant deltas and send final done=true marker
- Session management with RwLock<HashMap>
  - Keyed by randomly generated sid (Uuid)
  - Store minimal fields; limit chat_history to MAX_TURNS*2
- LLM request composition
  - System message = HOUSE_RULES_PROMPT + per-user ai_context
  - Maintain recent history, append user, stream assistant, then append assistant
- Vision messages (image_chat)
  - async-openai content parts: text + image_url with ImageDetail::High
- Deployment
  - cargo-chef cached dependencies; small Debian runtime
  - Fly concurrency as connections; http+https ports configured with health checks

## 6. Do's and Don'ts
Do
- Externalize large prompts and configuration
- Abstract the LLM client behind a trait to enable mocking in tests
- Bound memory usage: cap chat turns and payload sizes
- Use structured logging (tracing) and avoid logging secrets
- Keep lock lifetimes short and avoid holding across awaits
- Validate auth before state mutation and before any paid LLM call
- Prefer explicit protocol types (WsMsg) with serde tagging

Don't
- Mix multiple responsibilities in main.rs as complexity grows
- Leak internal error details or stack traces to clients
- Depend directly on external services in tests without mocks
- Keep unbounded history or accept unbounded inputs over WebSocket
- Diverge frontend protocol from backend contract

## 7. Tools & Dependencies
Key libraries
- axum: Web framework + ws extractors
- tokio: Async runtime
- futures-util: Stream utilities
- tracing, tracing-subscriber: Logging
- serde, serde_json: Serialization
- reqwest: HTTP client for Next.js user context
- async-openai: OpenAI-compatible client (pointed to Groq)
- uuid, chrono: IDs, timestamps
- dotenvy, anyhow: Env management and error handling

Setup and run
- Local
  - Set env: GROQ_API_KEY, optionally NEXTJS_URL
  - cargo run
  - WS endpoint: ws://localhost:8080/ws
- Docker
  - docker build -t rust-chatbot-groq .
  - docker run -e GROQ_API_KEY=... -p 8080:8080 rust-chatbot-groq
- Fly.io
  - fly launch (already configured: app name, region, ports)
  - fly deploy

Configuration patterns
- Keep MODEL_ID configurable (env: MODEL_ID) to avoid rebuilds for model changes
- Define a Config struct that reads env once at startup (with sane defaults)

## 8. Other Notes
Protocol contract (backend)
- WebSocket expects JSON with tagged enum:
  - {"type":"authenticate","token":"..."}
  - {"type":"update_context","context":"..."}
  - {"type":"chat","message":"..."}
  - {"type":"image_chat","message":"...","image_data":"..."}
- Responses include:
  - auth_response, system_message, chat_delta { delta, done }, error
- The current static/index.html sends plain strings and a "!context ..." command; align UI to JSON protocol above before relying on it in production.

Model and prompts
- MODEL_ID is currently hardcoded (e.g., openai/gpt-oss-120b in main.rs). Keep consistent across environments via env.
- HOUSE_RULES_PROMPT is large; externalize to a file and load at startup for easier edits and review.

Auth and gating
- authenticate_user fetches context from NEXTJS_URL/api/user/context; ensure HTTPS in production
- Non-Pro users get an upgrade message; Pro users require non-empty ai_context
- guard_pro returns WsMsg::Error and prevents LLM calls if not authed or not Pro

Stability and resilience
- Add graceful shutdown (signal handling) and in-flight request draining
- Consider per-connection timeouts and heartbeat/ping for stale sockets
- Implement periodic cleanup of authenticated sessions using authenticated_at TTL

LLM usage
- Stream tokens and cap max_tokens; adjust temperature/top_p per environment
- For images, prefer signed URLs over embedding large base64 strings; enforce size limits

Logging/observability
- Attach sid to log lines; consider tracing spans per connection
- Optionally enable JSON logs in production for easy aggregation

Security and privacy
- Do not store or log access tokens
- Sanitize user-supplied text before logging
- Validate image_data is a URL (or well-formed data URL) and consider allowlist
