# Security Policy

## Supported versions

The project is early-stage. Security fixes target the latest published release and the current `main` branch; older versions do not have a separate maintenance commitment.

## Reporting a vulnerability

Please use [GitHub private vulnerability reporting](https://github.com/Ariakage/ai-gateway/security/advisories/new) when it is available. Include the affected version, prerequisites, minimal reproduction, impact, and a suggested fix if you have one.

Do not include real API keys, conversation histories, private provider URLs, or production databases. Avoid posting working exploits in public Issues before maintainers have had a chance to respond. No response-time or bounty commitment is currently offered.

## Deployment notes

- The default configuration is for a trusted local machine, not an unauthenticated public endpoint.
- For remote access, set **both** inference keys (`server.api_keys`) and an admin token (`server.admin_token`). Use TLS at a reverse proxy and configure the intended Host / Origin allowlists.
- Admin endpoints can read and replace configuration, including upstream credentials. Restrict admin access accordingly.
- Prefer environment variable references for provider credentials. Never commit `config.toml`, `.env` files, backups, or usage databases.
- The gateway sends inference requests to the configured upstream. Local-first operation does not remove the upstream's data-processing role.
- SQLite normally stores request metadata, not conversation bodies. `server.store_error_details = true` may persist prompt or tool data echoed in upstream errors.
- Responses conversation state lives in bounded memory and disappears on restart. Do not rely on it as durable storage or a multi-tenant isolation mechanism.
- Review upstream URLs and headers before loading third-party configuration.

## 发布前检查 / Before publishing

检查 Git 暂存区，而不只是工作目录：密钥、私人配置、备份、数据库、截图和日志都可能泄露信息。示例与截图应只使用模拟数据。请勿把未经检查的请求记录或浏览器存储上传至公开仓库。
