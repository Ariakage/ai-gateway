# AI Gateway 使用手册

[← 返回项目首页](../README.md) · [English overview](../README.en.md)

一个用 Rust 写的本地 AI API 网关：把多个上游（OpenAI / Anthropic / OpenAI Responses / 任意 OpenAI 兼容服务）聚合成一个本地端口，同时对外提供 **OpenAI Chat Completions**、**OpenAI Responses** 和 **Anthropic Messages** 三种 API 格式，并自动做跨格式转换、模型名映射和 token 用量/速度统计。

不需要账号系统，单个二进制，配置一个 TOML 文件即可运行。

```
                          ┌──────────────────────────────┐
  OpenAI Chat SDK ───────▶│  POST /v1/chat/completions   │
  OpenAI Responses SDK ──▶│  POST /v1/responses          │──▶ OpenAI Chat 上游
  Anthropic SDK ─────────▶│  POST /v1/messages           │──▶ OpenAI Responses 上游
  Claude Code / Codex ───▶│  POST /v1/messages           │──▶ Anthropic 上游
  curl / WebUI ──────────▶│  GET  /v1/models             │──▶ 任意兼容上游
                          └──────────────────────────────┘
                                     │
                                     ▼
                     SQLite（用量/速度）+ 内置面板
```

## 功能

- **多上游、多密钥**：任意数量的 provider，每个配置 `base_url`、`api_key`、格式和模型列表。
- **三种入站格式**：同一端口同时接受 OpenAI Chat Completions、OpenAI Responses 和 Anthropic Messages。
- **三种上游格式**：`format = "openai" | "responses" | "anthropic"`，任意入站格式可路由到任意上游格式。
- **双向自动转换**：
  - OpenAI Chat ↔ Anthropic Messages
  - OpenAI Chat ↔ OpenAI Responses
  - Responses ↔ Anthropic（组合转换）
  - SSE 流式响应实时转换（文本、thinking、工具调用参数增量、Responses 事件流）
  - 非流式响应转换（文本、工具调用、图片、usage、stop reason）
- **Responses API 状态**：`previous_response_id` 在同一原生 Responses 路由下透传上游 ID；跨路由或非原生会话用本地历史展开，因此切换路由仍能继续多轮对话。
- **上游噪声清洗**：静默移除上游偶发注入的 `##ERROR##` 和 `<ds_safety>…</ds_safety>` 标记；流式响应中被拆到多个 chunk 的标记也能识别并清理。
- **模型映射**：别名、`provider/model` 直连、参数默认值、跨格式按模型名路由。
- **多路由与价格策略**：同一模型可配置多个 provider、多种 API 格式；支持分时价格、缓存命中、成本/延迟/优先级/轮询/手动路由，面板给出切换建议。
- **用量与速度统计**：
  - 输入/输出 token、请求数、成功率、错误数
  - TTFT（首字延迟）、总耗时、输出速度 tok/s
  - 按模型、按 provider、按时间分桶统计，SQLite 持久化
  - 上游没返回 usage 时自动估算，并在面板标记 `est`
- **内置监控面板**：暖纸色编辑式排版、明暗主题、编号分节，无需前端构建；中英文界面可随时切换（`?lang=zh` / `?lang=en`），选择会保存在浏览器。
- **在线配置编辑**：面板内置 TOML 编辑器，支持校验、保存、备份和热应用；host/port/database 变更会提示重启。
- **上游模型同步**：面板“获取模型”读取各 provider 的模型目录，列出缺失模型，可单个或全部写回 provider 的 `models` 数组；写入前校验并自动备份。
- **配置热重载**：`POST /admin/api/reload`。
- **可选鉴权**：客户端 API key、admin token；默认仅同源浏览器可访问，跨来源需显式配置 `allowed_origins`，监听非回环地址必须设置 `admin_token`。
- **不落盘对话内容**：SQLite 只保存请求元数据、token 和状态码，错误正文默认不落库；Responses 会话缓存仅存在于内存，进程退出即消失。

## 快速开始

```bash
cp config.example.toml config.toml
# 编辑 config.toml，填入 base_url / api_key / 模型

cargo run --release
# 或者
cargo build --release && ./target/release/ai-gateway --config config.toml
```

启动输出示例：

```
  AI Gateway  ·  3 provider(s)  ·  8 model(s)
  ─────────────────────────────────────────────────────────
  OpenAI      POST http://127.0.0.1:8787/v1/chat/completions
  Anthropic   POST http://127.0.0.1:8787/v1/messages
  Responses   POST http://127.0.0.1:8787/v1/responses
  Embeddings  POST http://127.0.0.1:8787/v1/embeddings
  Models      GET  http://127.0.0.1:8787/v1/models
  Dashboard   http://127.0.0.1:8787/
  Config      config.toml
  Usage DB    gateway.db
  Models      fast, claude, smart, resp, gpt-4o, ...
```

命令行参数：

```bash
ai-gateway --config /path/to/config.toml --host 127.0.0.1 --port 8787
```

## 配置

完整示例见 [`config.example.toml`](../config.example.toml)。

### 服务端

```toml
[server]
host = "127.0.0.1"   # IPv6 可写 "::1" 或 "[::1]"
port = 8787
database = "gateway.db"   # 相对路径按配置文件所在目录解析

# Responses API 的 previous_response_id 会话缓存（只在内存中，默认 128 条 / 16 MiB / 1 小时）
response_state = true
# response_state_max_entries = 128
# response_state_max_bytes = 16777216
# response_state_max_entry_bytes = 2097152
# 网页端「请求上下文」保留的最近上游请求 JSON（只在内存中，默认 20 条，0 = 关闭）
# request_context_limit = 20
# 单条记录的上游 JSON 与可用的本地重建历史合计超过该字节数时只保留元数据
# request_context_max_entry_bytes = 1048576
# 流式上游空闲超时（秒），0 = 关闭
# stream_idle_timeout_secs = 120
# 流式请求等待响应头的超时（秒），0 = 关闭
# stream_headers_timeout_secs = 30
# 读取上游错误正文的超时与大小上限
# error_body_timeout_secs = 10
# error_body_max_bytes = 262144
# Responses 转换保留的最大生成字节数
# stream_output_max_bytes = 8388608
# 非流式成功响应体上限（0 表示不限制）
# response_body_max_bytes = 33554432
# 上游流空闲时向下游发送 SSE 注释心跳的间隔（0 表示关闭）
# stream_heartbeat_secs = 15

# 允许跨来源调用网关的浏览器 Origin。留空 = 仅同源；其余跨来源请求返回 403。
# allowed_origins = ["http://localhost:3000"]
# 额外允许的 Host 头（反向代理/公网域名）
# allowed_hosts = ["gateway.example.com"]

# 是否把上游错误正文写入 SQLite。默认 false，只保存状态码，避免上游回显的提示词落库。
# store_error_details = false

# 可选：客户端必须携带其中一个 key
# 支持 Authorization: Bearer <key> 和 x-api-key: <key>
api_keys = []

# 可选：保护 /admin/*（面板数据）
# 未设置时仅允许同源/非浏览器访问；绑定非 127.0.0.1 地址会拒绝启动。
# admin_token = "change-me"
```

安全默认值：
- 默认不发送任何 CORS 允许头，跨来源浏览器请求会被 `Origin` 校验拒绝。
- Host 头必须是回环地址、实际监听地址、`server.allowed_hosts` 或 `server.allowed_origins` 中的主机名，防止 DNS 重绑定。
- 需要跨来源调用时，把完整 Origin 写入 `server.allowed_origins`；CORS 头和 Origin 校验每次请求都读取当前配置，无需重启。
- 管理接口在未设置 `admin_token` 时仍只接受同源或本机非浏览器请求；监听非回环地址必须设置 `admin_token`。
- 配置保存、重载、上游模型同步共用同一套暴露检查；校验失败时保留当前运行配置，且不写入同步结果。

### 路由

```toml
[routing]
default_strategy = "auto"     # auto | manual | priority | round_robin | cheapest | fastest
prefer_native = true          # auto 策略优先使用同格式路由
failure_cooldown_secs = 30    # 连续失败后的冷却时间
```

单个模型可以用 `strategy` / `route` 覆盖全局默认值，详见下面的“多路由、分时价格与转发策略”。

### Provider

```toml
[[providers]]
id = "openai"
format = "openai"                         # openai | responses | anthropic
base_url = "https://api.openai.com/v1"    # 需要包含版本段
api_key = "${OPENAI_API_KEY}"             # 支持 ${ENV_VAR}
models = ["gpt-4o", "gpt-4o-mini"]

# 可选
# models_url = "https://api.openai.com/v1/models"  # 模型目录地址，默认 {base_url}/models
# timeout_secs = 600        # 非流式请求超时
# stream_usage = true       # 流式时注入 stream_options.include_usage
```

- `base_url` 必须带版本路径（`/v1`）。如果 URL 完全没有路径，会自动补 `/v1`。
- `format` 决定上游协议和请求路径：
  - `openai` → `{base_url}/chat/completions`
  - `responses` → `{base_url}/responses`
  - `anthropic` → `{base_url}/messages`
- 任意 OpenAI 兼容服务都可以：DeepSeek、Qwen、OpenRouter、Ollama、LM Studio、vLLM、one-api / new-api 等。
- Responses-only 模型（例如 Codex 系列）请使用 `format = "responses"` 的 provider。
- `models_url` 用于模型目录和“Test upstreams”。默认是 `{base_url}/models`；如果模型目录与推理前缀不同（例如 OpenCode Go 的 `https://opencode.ai/inference/go/v1/models`），可以单独指定。
- 自定义请求头：

```toml
[providers.headers]
"HTTP-Referer" = "https://example.com"
"X-Title" = "My Gateway"
```

（`[providers.headers]` 作用于最近一个 `[[providers]]` 块。）

### 模型映射

```toml
[[models]]
name = "fast"                 # 客户端看到的模型名
provider = "deepseek"
upstream = "deepseek-chat"    # 实际发给上游的模型名
aliases = ["cheap"]           # 额外可用名称
description = "Low latency"

[models.params]               # 作为默认参数合并，客户端显式传值优先
temperature = 0.3
```

解析优先级：

1. `provider/model` 显式写法，例如 `openai/gpt-4o-mini`；
2. `[[models]]` 的 `name` 或 `aliases`；
3. provider 的 `models` 数组（同格式优先）。

如果某个模型只在 provider 的 `models` 数组里，也可以用原名直接调用；网关会按入站格式优先选择同格式的 provider，因此天然支持“用 OpenAI 格式调用 Claude”或“用 Anthropic 格式调用 GPT”。

## 多路由、分时价格与转发策略

一个客户端模型名可以对应多条路由。每条路由可以是一个 provider、一种 API 格式、一个上游模型和一张价格卡：

```toml
[[models]]
name = "portable"
strategy = "auto"                 # auto | manual | priority | round_robin | cheapest | fastest
description = "Same client model, multiple upstreams"

[[models.routes]]
id = "portable-chat"
provider = "openai"
upstream = "gpt-4o"
format = "openai"                 # 可选；默认用 provider.format
priority = 10
weight = 1
[models.routes.pricing]
currency = "USD"
input = 2.0                       # 每 1M input token
output = 8.0
cache_read = 0.5

[[models.routes]]
id = "portable-anth"
provider = "anthropic"
upstream = "claude-sonnet-4-5-20250929"
format = "anthropic"
priority = 20
[models.routes.pricing]
input = 3.0
output = 15.0
cache_read = 0.3
[[models.routes.pricing.schedules]]
label = "off-peak"
days = ["mon", "tue", "wed", "thu", "fri"]
start = "00:00"
end = "08:00"
multiplier = 0.5                  # 基础价的 50%

[[models.routes]]
id = "portable-deepseek"
provider = "deepseek"
upstream = "deepseek-chat"
priority = 30
```

- 同一 provider 可以派生多条路由，用 `format` 覆盖协议；如果 OpenAI 兼容服务同时支持 Chat 和 Responses，可以直接写两条路由。
- 价格卡是可选的；没有价格的路由仍可用于优先级、延迟、轮询和手动策略，只是不会参与成本比较。
- 分时价格按网关所在机器的**本地时间**匹配；`start > end` 表示跨午夜；`multiplier` 作用于最终价格。
- `cache_read` / `cache_write` 是缓存读写单价；路由选择会参考近期缓存命中率，实际请求成本会记入统计。

### 策略

| strategy | 选择方式 |
| --- | --- |
| `auto`（默认） | 优先与入站格式一致的路由，再按当前有效成本、priority；没有同格式路由时跨格式比较。候选路线币种不一致时不做成本比较，回退到 priority |
| `priority` | 按 `priority` 升序，跳过冷却中的路由 |
| `manual` | 固定使用 `route = "<route id>"`；不因价格/健康状态切换。隐式 provider 模型会选择优先级最高的一条路由作为回退 |
| `round_robin` | 在健康路由之间按 `weight` 加权轮询 |
| `cheapest` | 按当前时段、缓存命中率折算后的单次请求成本选择；可比路线币种不一致时返回明确错误，不做跨币种比较 |
| `fastest` | 按近期 TTFT EWMA 选择；未采样路由会先以乐观延迟探测一次，4xx 探测失败会退避。同一路由的在途探测有限，全部线路都在探测时额外请求会按在途数分散 |

成本比较只会在同一币种内进行；不同币种需要显式汇率/统一计价配置（当前未实现）。

连续 3 次 5xx / 429 / 408 会把路由放入冷却（默认 30 秒，`routing.failure_cooldown_secs`），冷却结束后自动半开重试。延迟和缓存命中率使用内存 EWMA，重启后从 0 开始；历史用量在 SQLite 中。

单次请求也可以强制指定路由：

```bash
curl http://127.0.0.1:8787/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'x-gateway-route: portable-anth' \
  -d '{"model":"portable","messages":[{"role":"user","content":"hi"}]}'
```

响应头会返回 `x-gateway-route`、`x-gateway-provider`、`x-gateway-format`，方便确认实际走了哪条路由。

### 面板与建议

面板的 **Routing** 区域会列出有多条路由的模型，并展示每条路由的：

- 当前时段价格（每 1M input / output）
- 历史缓存命中率
- 平均 TTFT
- 典型请求的估算成本
- 健康状态（正常 / 冷却中）
- 当前策略对 chat / responses / anthropic 三种入站格式分别会选择哪条路由

如果当前路由明显更贵（≥10%）或明显更慢（TTFT 低 20% 以上），会显示建议切换到的路由和原因，并提供 **Copy strategy** 按钮，复制可直接粘贴到模型块的 `strategy` / `route` 两行。

> 价格比较假设所有价格使用同一种货币；网关不做汇率换算。`/admin/api/routing` 返回完整的路由、价格、缓存、延迟和建议数据。

## 客户端接入

### OpenAI SDK / OpenAI 兼容客户端

Base URL 填 `http://127.0.0.1:8787/v1`，API Key 随便填（除非配置了 `api_keys`）。

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8787/v1", api_key="local")
resp = client.chat.completions.create(
    model="claude",                       # 可以是任意映射后的名字
    messages=[{"role": "user", "content": "你好"}],
)

# 流式
for chunk in client.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "讲个笑话"}],
    stream=True,
):
    for choice in chunk.choices:
        print(choice.delta.content or "", end="")
```

适用于 OpenWebUI、Cline、Continue、LobeChat、NextChat 等所有支持自定义 OpenAI Base URL 的客户端。

### OpenAI Responses SDK / Codex / Agents SDK

Base URL 同样是 `http://127.0.0.1:8787/v1`，使用 `client.responses.create(...)`。

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8787/v1", api_key="local")

resp = client.responses.create(
    model="resp",                 # 可以指向 responses / openai / anthropic 任意上游
    input="你好",
)
print(resp.output_text)

# 流式
with client.responses.create(model="fast", input="讲个笑话", stream=True) as stream:
    for event in stream:
        if event.type == "response.output_text.delta":
            print(event.delta, end="", flush=True)
```

`previous_response_id` 可以直接使用：

```python
first = client.responses.create(model="fast", input="第一轮")
second = client.responses.create(
    model="fast",
    input="第二轮",
    previous_response_id=first.id,
)
```

网关会维护上一轮上下文（默认最多 128 条 / 16 MiB、1 小时）。同一原生 Responses 路由继续透传 `previous_response_id`；切换路由或上游不是 Responses 时，用本地历史展开后转发。这样故障切换或手动切换路由后仍能继续多轮对话。

> Responses 的 `retrieve` / `cancel` / background 模式需要网关持久化完整响应对象，当前未实现；流式和非流式 `create` 不受影响。

### Anthropic SDK / Claude Code

Base URL 填 `http://127.0.0.1:8787`（SDK 会自己拼 `/v1/messages`）。

```python
from anthropic import Anthropic

client = Anthropic(base_url="http://127.0.0.1:8787", api_key="local")
resp = client.messages.create(
    model="fast",                         # 也可以用 OpenAI 上游
    max_tokens=1024,
    messages=[{"role": "user", "content": "你好"}],
)
print(resp.content[0].text)
```

Claude Code：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
export ANTHROPIC_AUTH_TOKEN=local
claude
```

> Claude Code 会使用类似 `claude-sonnet-4-5-20250929` 的模型名。你可以在 `[[models]]` 里把该名字映射到任意 provider，或者直接把它列在某个 provider 的 `models` 里。

### curl

```bash
# OpenAI 格式
curl http://127.0.0.1:8787/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"fast","messages":[{"role":"user","content":"hi"}]}'

# Anthropic 格式（流式）
curl -N http://127.0.0.1:8787/v1/messages \
  -H 'content-type: application/json' \
  -d '{"model":"claude","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"hi"}]}'
```

## 协议转换说明

| 能力 | Chat ↔ Anthropic | Chat ↔ Responses | Responses ↔ Anthropic |
| --- | --- | --- | --- |
| system / developer 消息 | → `system` / system message | `instructions` 互转 | 组合转换 |
| 文本 | ✅ | ✅ | ✅ |
| 图片 | `image_url` ↔ image block | `input_image` ↔ `image_url` | ✅ |
| 工具调用 | `tools` / `tool_calls` / `tool` ↔ `tools` / `tool_use` / `tool_result` | `function_call` ↔ `tool_calls` | ✅ |
| 工具选择 | `auto` / `required` / `none` / 指定函数 | ✅ | ✅ |
| 流式文本 | ✅ | ✅ | ✅ |
| 流式工具参数 | ✅ | ✅ | ✅ |
| thinking / reasoning | `reasoning_content` ↔ thinking 增量（签名不保留） | reasoning summary → `reasoning_content` | 同左 |
| parallel_tool_calls | `tool_choice.disable_parallel_tool_use` 双向映射 | ↔ | 串行工具调用约束会保留 |
| finish_reason | `length` ↔ `max_tokens` | incomplete / max_output_tokens → `length` | 截断优先于 `tool_calls` |
| stop reason | `stop` / `length` / `tool_calls` | `status` / `incomplete_details` 互转 | ✅ |
| usage | input/output token 互转 | `input_tokens` / `output_tokens` 互转 | ✅ |
| `previous_response_id` | — | 同原生路由透传；跨路由本地展开 | 同左 |

默认会为转发到 **OpenAI Chat 上游**的流式请求注入 `stream_options.include_usage = true`，从而拿到精确 usage。若某个上游不接受该字段，设置 `stream_usage = false`，网关会退化为估算。Anthropic 和 Responses 上游本身会返回 usage，无需注入。

### 跨格式转换会不会影响实际使用？

文本、流式响应和普通 function calling 有本地模拟上游及官方 SDK 测试覆盖，但这不等于所有客户端、模型和供应商均完全兼容。跨格式转换会丢失部分供应商专有语义；依赖这些能力时，应优先选择原生格式路由。

实际影响可以按下面几类看：

| 限制 | 是否有实际影响 | 说明 |
| --- | --- | --- |
| 文本、流式、function calling、多轮工具结果 | 通常可用 | 有官方 OpenAI / Anthropic SDK 配合模拟上游的测试覆盖；真实供应商仍需按实际模型验证。 |
| `n > 1`、`logprobs` | 很小 | 只有需要并行生成多个候选或展示 token 概率的客户端会用到；普通聊天和 Agent 不使用。 |
| Anthropic thinking 签名 | 中等 | 原生 Anthropic → Anthropic 是透传，完全不受影响；只有把带 thinking 的历史发给非 Anthropic 上游时会丢失签名，可能降低跨模型续聊质量。 |
| 严格 JSON Schema | 中等 | 转给 OpenAI Chat 上游会映射为 `response_format`；转给 Anthropic 只能把 schema 写进 system prompt，**不保证**严格符合 schema。需要强约束时请使用 OpenAI Chat / Responses 上游。 |
| OpenAI 托管工具（web_search、file_search、computer use、MCP） | **较大** | 这些不是普通 function tool，无法翻译成 Chat / Anthropic 工具。需要托管工具时，请把请求发给 `format = "responses"` 的 OpenAI 上游（Responses → Responses 是透传）。 |
| Responses `retrieve` / `cancel` / background | 较大 | `retrieve` 返回 501；没有实现 `cancel` 接口，也没有完整的后台任务生命周期支持。原生上游可能接受 background 字段，但不能据此依赖网关进行后台任务管理。 |
| `previous_response_id` | 小 | 同一原生 Responses 路由透传；跨路由由网关内存展开（默认 128 条 / 16 MiB / 1 小时 / 重启丢失）。超长会话或频繁重启时，客户端应改用完整历史。 |
| prompt caching / cache_control | 成本层面 | Anthropic 的 `cache_control` 在转成 Chat 时会被丢弃；原生透传保留。影响的是费用和首字延迟，不是回答正确性。 |
| 图片 URL | 小 | 跨格式时 URL 直传；如果 Anthropic 无法访问该 URL，请改用 base64 data URI。 |
| `count_tokens` | 小 | 是估算值，不是官方 tokenizer；用于上下文管理时可能有几个百分点的偏差，实际 usage 仍以响应为准。 |
| 采样参数（top_k、thinking.budget_tokens、service_tier 等） | 小 | 跨格式时会被忽略；同格式透传保留。 |

接入新客户端时，建议先验证文本、流式、多轮工具结果及所需的专有字段。托管工具和严格 Structured Outputs 应使用支持这些能力的原生上游；后台任务的完整生命周期与响应检索不在当前网关支持范围内。

## 在线编辑配置

面板的 **Configuration** 标签页直接编辑 `config.toml`：

- **Validate** 只解析并校验 TOML，不写盘。
- **Save & apply** 校验通过后写入 `config.toml.bak` 备份，再原子替换配置文件，并立即热加载 providers / models / routing / pricing。
- 编辑器支持行号、未保存标记、`Cmd/Ctrl+S` 保存。
- `host`、`port`、`database` 变更会显示“重启后生效”；`admin_token` 变更会提示下一次请求需要新 token。
- 原始配置包含 API Key。默认只监听 `127.0.0.1`；如果暴露到其他机器，请先设置 `admin_token`。

对应接口：

```bash
GET  /admin/api/config/raw          # 原始 TOML
POST /admin/api/config/validate     # 只校验
PUT  /admin/api/config              # 校验 + 备份 + 保存 + 热加载
```

## 统计与面板

浏览器打开 `http://127.0.0.1:8787/`：

- 请求数、成功率、输入/输出 token、平均速度、平均 TTFT
- 缓存读取 / 写入 token、估算成本（按配置价格）
- token 用量时间序列（1h / 24h / 7d / 30d / all）
- 按模型、按 provider、按路由的聚合
- 最近请求明细（模型、路由、格式、token、tok/s、TTFT、耗时、成本、状态）
- 路由价格、缓存命中、TTFT、健康状态与切换建议
- 上游连通性测试、配置热重载

统计口径：

- **token 用量**优先使用上游返回的 `usage`；缺失时按字符估算，并标记 `est`。
- **缓存 token** 来自 OpenAI `prompt_tokens_details.cached_tokens`、Anthropic `cache_read_input_tokens` / `cache_creation_input_tokens`、Responses `input_tokens_details`。
- **成本**优先按请求使用的完整价格卡和 token 明细计算；没有完整价格卡时，可回退到上游报告的费用。两者均不可用时显示 `—`。
- **tok/s** 只对流式请求有意义：从第一个内容 token 到流结束的生成窗口计算。非流式显示 `—`。
- **TTFT** 只对流式请求记录。非流式显示 `—`。
- 失败的请求 token 和成本记为 0，默认只保留状态码；设置 `server.store_error_details = true` 才会把上游错误正文写入 SQLite。
- 客户端取消（状态 499）会保留已采集/估算的 token 和成本，并用 `estimated` 标记不确定性。
- 数据保存在 SQLite（默认 `gateway.db`），可以随时删除重建。
- Responses 会话缓存只在内存中，不写入 SQLite。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/chat/completions` | OpenAI Chat Completions |
| POST | `/v1/responses` | OpenAI Responses（含流式事件） |
| POST | `/v1/responses/input_tokens` | Responses 输入 token 估算；`previous_response_id` 会展开本地缓存历史 |
| POST | `/v1/messages` | Anthropic Messages |
| POST | `/v1/messages/count_tokens` | Anthropic token 计数（估算） |
| POST | `/v1/embeddings` | OpenAI Embeddings（仅 openai 格式上游） |
| GET | `/v1/models` | 模型列表（同时兼容两家 SDK） |
| GET | `/v1/models/{id}` | 单个模型 |
| GET | `/healthz` | 健康检查 |
| GET | `/` | 监控面板 |
| GET | `/admin/api/overview` | 汇总统计 |
| GET | `/admin/api/timeseries` | 时间序列 |
| GET | `/admin/api/models` | 按模型聚合 |
| GET | `/admin/api/providers` | 按 provider 聚合 |
| GET | `/admin/api/routing` | 路由、价格、缓存、延迟与切换建议 |
| GET | `/admin/api/requests` | 请求明细（分页） |
| GET | `/admin/api/config` | 脱敏后的配置 |
| GET | `/admin/api/config/raw` | 原始 `config.toml`（含密钥，受 admin_token 保护） |
| POST | `/admin/api/config/validate` | 仅校验 TOML，不写盘 |
| PUT | `/admin/api/config` | 校验 → 备份 → 原子写入 → 热加载 |
| GET | `/admin/api/health` | 探测所有上游 |
| POST | `/admin/api/reload` | 重新加载磁盘上的配置文件 |

`/admin/*` 在配置了 `admin_token` 时需要携带 `x-admin-token: <token>`（或 `Authorization: Bearer <token>`）。面板会在收到 401 时弹出输入框。

## 本地开发与测试

```bash
# 1. 启动模拟上游（同时实现 Chat / Responses / Anthropic 接口）
python3 dev/mock_upstream.py 19100

# 2. 启动网关（使用仓库内的测试配置）
cargo run -- --config dev/test-config.toml

# 3. 端到端测试
python3 dev/smoke_test.py
```

`dev/smoke_test.py` 覆盖三种格式的双向非流式/流式、工具调用、`previous_response_id`、多路由/价格/策略、配置编辑、模型列表、错误传播、统计接口。`dev/sdk_test.py` 使用官方 `openai` 和 `anthropic` Python SDK 验证兼容性（需先安装这两个包）。实际检查数量以脚本输出为准，所有这些测试使用本地模拟上游。

单元测试：

```bash
cargo test
```

## 已知限制

- 网关不会自动跟随上游 HTTP 重定向，以避免泄露 API key 或自定义认证头。请将 `base_url` 和 `models_url` 配置为最终地址。
- Anthropic 工具结果中的图片转到 Chat 上游时，会放在工具结果之后的 user 消息中；上游模型需要支持图片输入。

- Responses 的 `retrieve` / `cancel` / background 模式未实现（`create` 和流式正常）。
- OpenAI 托管工具（web_search、file_search、computer use、MCP）只在 Responses → Responses 透传时可用；跨格式时会被忽略。
- OpenAI `n > 1`、`logprobs` 等少数字段在跨格式转换时会被忽略。
- Anthropic `thinking` 的签名无法在跨格式转换中保留，转到 OpenAI / Responses 时以 reasoning 文本输出；Chat `reasoning_content` 转到 Responses 时映射为 reasoning summary item。
- 拒答内容在 Chat 与 Responses 之间双向映射（`message.refusal` / `delta.refusal` ↔ refusal 内容块）；转到 Anthropic 时作为可见文本输出。
- Chat/Responses → Anthropic 的 usage 会把 `cached_tokens` 拆到 `cache_read_input_tokens`，`input_tokens` 只包含未缓存的普通输入。
- Anthropic/Responses → Chat 的流式最终 usage 同样返回 `prompt_tokens_details.cached_tokens`，与非流式一致。
- Responses 的 `previous_response_id` 依赖内存缓存：默认最多 128 条 / 16 MiB、保留 1 小时、重启后丢失。可用 `response_state = false` 关闭。
- 单轮历史超过缓存上限时，响应 `metadata.gateway_cache` 会标记 `not_stored_too_large`，后续使用该 ID 会收到明确错误。
- Native Responses 会话在同一路由下继续透传上游 ID；切换路由时才使用本地历史重放。
- 仪表盘的「请求上下文」只保留最近 `server.request_context_limit`（默认 20）条实际发往上游的请求体，仅存于内存、重启即清空；上游 JSON 和可用的本地重建历史合计超过 `server.request_context_max_entry_bytes`（默认 1 MiB）时只列元数据。原生 Responses 使用 `previous_response_id` 时，上游持有的历史不一定可在本地获取；可获取时会单独标明本地重建结果。降低单条上限会立即清除超限记录的内容。将条数设为 0 可关闭该功能。
- 流式请求默认 30 秒内拿不到上游响应头、或 120 秒无数据即判定超时；可用 `server.stream_headers_timeout_secs` / `server.stream_idle_timeout_secs` 调整或设为 0 关闭。
- Responses 转换需要保留完整输出时默认最多 8 MiB；超过 `server.stream_output_max_bytes` 会返回失败而不是继续增长内存。
- 非流式成功响应默认最多 32 MiB；超过 `server.response_body_max_bytes` 会返回 502。设为 0 可关闭限制。
- 流式上游空闲超过 `server.stream_heartbeat_secs`（默认 15 秒）时，网关会向下游发送 `: keep-alive` SSE 注释心跳；设为 0 可关闭。
- 网关生成的 SSE 事件预算为 `stream_output_max_bytes + 4 MiB`；清洗阶段超限会输出协议错误并把请求记为 502，不会静默丢弃结束事件。
- 价格是网关侧的估算：按每 1M token 配置，不做货币换算；所有参与比较的路由应使用同一币种。
- 统计面板按币种分别汇总成本；不同币种的金额不会相加，也不会自动换汇。
- 分时价格按网关所在机器的本地时间匹配；延迟 / 缓存命中率的 EWMA 只在内存中，重启后从 0 开始（历史用量仍在 SQLite）。
- 路由健康只根据 5xx / 429 / 408 冷却，不感知供应商的并发配额、余额或限流窗口；`manual` 策略会按配置强制使用指定路由，即使它处于冷却状态。
- `count_tokens` / `input_tokens` 是估算值，不是官方精确计数；`previous_response_id` 历史不可用时返回 400，不会只计新输入。
- 跨格式转换时，图片 URL 直接透传；Anthropic 要求可访问的图片 URL，必要时请改用 base64 data URI。
- 网关默认面向本机使用。若暴露到公网，请务必设置 `api_keys` 和 `admin_token`，并放在反向代理 / TLS 后面。

## License

MIT
