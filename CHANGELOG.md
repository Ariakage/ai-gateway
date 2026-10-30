# Changelog

## [0.1.0] - 2026-10-30

Initial public release.

### Added

- A single-binary Rust gateway for OpenAI Chat Completions, OpenAI Responses, and Anthropic Messages.
- Bidirectional protocol translation, SSE text / tool deltas, reasoning and refusal text, and completion-state mapping.
- Multi-provider model aliases and six routing strategies with price cards, cache-aware estimates, TTFT tracking, and health cooldowns.
- Bounded in-memory Responses conversation history for cross-route continuation.
- SQLite usage accounting, per-currency cost aggregation, and an embedded bilingual light / dark dashboard.
- Configuration validation, backup, hot apply, upstream model discovery, and synchronization.
- Optional inference / admin authentication, Host / Origin checks, stream timeouts, heartbeats, and response-size limits.
- Unit, mock-upstream smoke, and official OpenAI / Anthropic SDK compatibility tests.
- Bilingual project documentation, real dashboard screenshots using mock data, and GitHub Actions CI.

### Scope

This is an early-stage release for trusted local and small self-hosted deployments. Provider-specific features are not universally portable, and local mock / SDK compatibility tests are not live-provider certification. See the README for security requirements and known limitations.

[0.1.0]: https://github.com/Ariakage/ai-gateway/releases/tag/v0.1.0
