#!/usr/bin/env python3
"""End-to-end smoke test against a running gateway + dev/mock_upstream.py."""

import json
import sys
import time
import urllib.error
import urllib.request

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:18787").rstrip("/")
PASS = 0


def check(name, cond, detail=""):
    global PASS
    if not cond:
        raise AssertionError(f"FAIL {name}: {detail}")
    PASS += 1
    print(f"  ok  {name}")


def post(path, body, headers=None):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method="POST",
        headers={"content-type": "application/json", **(headers or {})},
    )
    return urllib.request.urlopen(req, timeout=30)


def get(path, headers=None):
    req = urllib.request.Request(BASE + path, headers=headers or {})
    return urllib.request.urlopen(req, timeout=30)


def put(path, body, headers=None):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method="PUT",
        headers={"content-type": "application/json", **(headers or {})},
    )
    return urllib.request.urlopen(req, timeout=30)


def read_sse(resp):
    events = []
    for raw in resp:
        line = raw.decode("utf-8", "replace").strip()
        if line.startswith("data:"):
            events.append(line[5:].strip())
    return events


def collect_oai_stream(resp):
    content = ""
    tool_args = ""
    tool_name = None
    finish = None
    usage = None
    done = False
    for data in read_sse(resp):
        if data == "[DONE]":
            done = True
            continue
        obj = json.loads(data)
        if obj.get("usage"):
            usage = obj["usage"]
        for choice in obj.get("choices", []):
            delta = choice.get("delta") or {}
            content += delta.get("content") or ""
            for tc in delta.get("tool_calls") or []:
                fn = tc.get("function") or {}
                if fn.get("name"):
                    tool_name = fn["name"]
                tool_args += fn.get("arguments") or ""
            if choice.get("finish_reason"):
                finish = choice["finish_reason"]
    return {
        "content": content,
        "tool_name": tool_name,
        "tool_args": tool_args,
        "finish": finish,
        "usage": usage,
        "done": done,
    }


def collect_anth_stream(resp):
    events = []
    for raw in resp:
        line = raw.decode("utf-8", "replace").strip()
        if line.startswith("event:"):
            events.append(line[6:].strip())
    return events


def collect_responses_stream(resp):
    names = []
    text = ""
    usage = None
    output = []
    for raw in resp:
        line = raw.decode("utf-8", "replace").strip()
        if line.startswith("event:"):
            names.append(line[6:].strip())
        elif line.startswith("data:"):
            try:
                obj = json.loads(line[5:].strip())
            except Exception:
                continue
            if obj.get("type") == "response.output_text.delta":
                text += obj.get("delta", "")
            elif obj.get("type") == "response.completed":
                usage = obj["response"].get("usage")
                output = obj["response"].get("output", [])
    return names, text, usage, output


def responses_text(data):
    text = ""
    for item in data.get("output", []):
        if item.get("type") == "message":
            for part in item.get("content", []):
                if part.get("type") == "output_text":
                    text += part.get("text", "")
    return text


def anthropic_text(resp):
    data = json.loads(resp.read())
    return data, "".join(
        b.get("text", "") for b in data.get("content", []) if b.get("type") == "text"
    )


TOOLS_OAI = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get weather",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
            },
        },
    }
]
TOOLS_ANTH = [
    {
        "name": "get_weather",
        "description": "Get weather",
        "input_schema": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
        },
    }
]


def main():
    print(f"gateway: {BASE}")

    # --- OpenAI in / OpenAI out -------------------------------------
    r = post(
        "/v1/chat/completions",
        {"model": "fast", "messages": [{"role": "user", "content": "hi"}]},
    )
    data = json.loads(r.read())
    check("openai->openai json", "OpenAI mock" in data["choices"][0]["message"]["content"])
    check("openai->openai usage", data["usage"]["total_tokens"] == 19)
    check("openai->openai model alias", data["model"] == "fast")

    r = post(
        "/v1/chat/completions",
        {"model": "fast", "stream": True, "messages": [{"role": "user", "content": "hi"}]},
    )
    s = collect_oai_stream(r)
    check("openai->openai stream text", "OpenAI mock" in s["content"])
    check("openai->openai stream done", s["done"])
    check("openai->openai stream usage", s["usage"] and s["usage"]["completion_tokens"] == 7)
    check("openai->openai stream finish", s["finish"] == "stop")

    # --- OpenAI in / Anthropic out ----------------------------------
    r = post(
        "/v1/chat/completions",
        {"model": "claude", "messages": [{"role": "user", "content": "hi"}]},
    )
    data = json.loads(r.read())
    check("openai->anth json", "Anthropic mock" in data["choices"][0]["message"]["content"])
    check("openai->anth usage", data["usage"]["total_tokens"] == 20)
    check("openai->anth finish", data["choices"][0]["finish_reason"] == "stop")

    r = post(
        "/v1/chat/completions",
        {"model": "claude", "stream": True, "messages": [{"role": "user", "content": "hi"}]},
    )
    s = collect_oai_stream(r)
    check("openai->anth stream text", "Anthropic mock" in s["content"])
    check("openai->anth stream done", s["done"])
    check("openai->anth stream usage", s["usage"] and s["usage"]["completion_tokens"] == 9)
    check("openai->anth stream finish", s["finish"] == "stop")

    # --- Anthropic in / OpenAI out ----------------------------------
    r = post(
        "/v1/messages",
        {
            "model": "fast",
            "max_tokens": 128,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    data, text = anthropic_text(r)
    check("anth->openai json", "OpenAI mock" in text)
    check("anth->openai type", data["type"] == "message")
    check("anth->openai usage", data["usage"]["input_tokens"] == 8)
    check("anth->openai cache usage", data["usage"]["cache_read_input_tokens"] == 4)
    check("anth->openai stop", data["stop_reason"] == "end_turn")

    r = post(
        "/v1/messages",
        {
            "model": "fast",
            "max_tokens": 128,
            "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    events = collect_anth_stream(r)
    check("anth->openai stream start", events and events[0] == "message_start")
    check("anth->openai stream delta", "content_block_delta" in events)
    check("anth->openai stream stop", events[-2:] == ["message_delta", "message_stop"])

    # --- Anthropic in / Anthropic out -------------------------------
    r = post(
        "/v1/messages",
        {
            "model": "claude",
            "max_tokens": 128,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    data, text = anthropic_text(r)
    check("anth->anth json", "Anthropic mock" in text)
    check("anth->anth usage", data["usage"]["output_tokens"] == 9)

    r = post(
        "/v1/messages",
        {
            "model": "claude",
            "max_tokens": 128,
            "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    events = collect_anth_stream(r)
    check("anth->anth stream", events and events[0] == "message_start" and events[-1] == "message_stop")

    # --- tool calling, both directions ------------------------------
    r = post(
        "/v1/chat/completions",
        {
            "model": "claude",
            "messages": [{"role": "user", "content": "weather in paris"}],
            "tools": TOOLS_OAI,
        },
    )
    data = json.loads(r.read())
    msg = data["choices"][0]["message"]
    check("anth tool -> openai", msg["tool_calls"][0]["function"]["name"] == "get_weather")
    check(
        "anth tool args -> openai",
        json.loads(msg["tool_calls"][0]["function"]["arguments"]) == {"city": "Paris"},
    )
    check("anth tool finish", data["choices"][0]["finish_reason"] == "tool_calls")

    r = post(
        "/v1/chat/completions",
        {
            "model": "claude",
            "stream": True,
            "messages": [{"role": "user", "content": "weather in paris"}],
            "tools": TOOLS_OAI,
        },
    )
    s = collect_oai_stream(r)
    check("anth tool stream name", s["tool_name"] == "get_weather")
    check("anth tool stream args", json.loads(s["tool_args"]) == {"city": "Paris"})
    check("anth tool stream finish", s["finish"] == "tool_calls")

    r = post(
        "/v1/messages",
        {
            "model": "fast",
            "max_tokens": 128,
            "messages": [{"role": "user", "content": "weather in paris"}],
            "tools": TOOLS_ANTH,
        },
    )
    data = json.loads(r.read())
    tool_use = [b for b in data["content"] if b["type"] == "tool_use"]
    check("openai tool -> anth", tool_use and tool_use[0]["name"] == "get_weather")
    check("openai tool args -> anth", tool_use[0]["input"] == {"city": "Paris"})
    check("openai tool stop -> anth", data["stop_reason"] == "tool_use")

    r = post(
        "/v1/messages",
        {
            "model": "fast",
            "max_tokens": 128,
            "stream": True,
            "messages": [{"role": "user", "content": "weather in paris"}],
            "tools": TOOLS_ANTH,
        },
    )
    events = collect_anth_stream(r)
    check("openai tool stream -> anth", "content_block_start" in events and "message_stop" in events)

    # --- models / count_tokens / errors -----------------------------
    models = json.loads(get("/v1/models").read())
    check("models list", any(m["id"] == "fast" for m in models["data"]))
    check("models anthropic-compatible", "has_more" in models)

    r = post(
        "/v1/messages/count_tokens",
        {"model": "claude", "messages": [{"role": "user", "content": "hello world"}]},
    )
    check("count_tokens", json.loads(r.read())["input_tokens"] > 0)

    try:
        post("/v1/chat/completions", {"model": "nope", "messages": []})
        check("unknown model 404", False)
    except urllib.error.HTTPError as e:
        body = json.loads(e.read())
        check("unknown model 404", e.code == 404 and "available models" in body["error"]["message"])

    # --- upstream error propagation ---------------------------------
    try:
        post(
            "/v1/chat/completions",
            {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]},
        )
        check("upstream 429 same format", False)
    except urllib.error.HTTPError as e:
        body = json.loads(e.read())
        check(
            "upstream 429 same format",
            e.code == 429 and body["error"]["type"] == "rate_limit_error",
        )

    try:
        post(
            "/v1/messages",
            {
                "model": "gpt-4o-mini",
                "max_tokens": 16,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        check("upstream 429 cross format", False)
    except urllib.error.HTTPError as e:
        body = json.loads(e.read())
        check(
            "upstream 429 cross format",
            e.code == 429 and body["type"] == "error" and "rate limit" in body["error"]["message"],
        )

    try:
        post(
            "/v1/chat/completions",
            {"model": "broken/x", "messages": [{"role": "user", "content": "hi"}]},
        )
        check("unreachable upstream 502", False)
    except urllib.error.HTTPError as e:
        body = json.loads(e.read())
        check("unreachable upstream 502", e.code == 502 and "error" in body)

    # --- OpenAI Responses API ---------------------------------------
    r = post("/v1/responses", {"model": "fast", "input": "hi"})
    data = json.loads(r.read())
    check(
        "responses->chat json",
        data["object"] == "response" and "OpenAI mock" in data["output_text"],
    )
    check("responses->chat usage", data["usage"]["total_tokens"] == 19)

    r = post("/v1/responses", {"model": "claude", "input": "hi"})
    data = json.loads(r.read())
    check(
        "responses->anth json",
        data["object"] == "response" and "Anthropic mock" in data["output_text"],
    )

    r = post("/v1/responses", {"model": "resp", "input": "hi"})
    data = json.loads(r.read())
    check(
        "responses->responses json",
        "Responses mock" in responses_text(data) and data["model"] == "resp",
    )

    r = post("/v1/responses", {"model": "fast", "input": "hi", "stream": True})
    names, text, usage, _ = collect_responses_stream(r)
    check("responses stream start", names and names[0] == "response.created")
    check("responses stream text", "OpenAI mock" in text)
    check("responses stream end", names[-1] == "response.completed" and usage is not None)

    r = post("/v1/responses", {"model": "resp", "input": "hi", "stream": True})
    names, text, usage, _ = collect_responses_stream(r)
    check(
        "responses passthrough stream",
        "Responses mock" in text and names[-1] == "response.completed",
    )

    r = post(
        "/v1/chat/completions",
        {"model": "resp", "messages": [{"role": "user", "content": "hi"}]},
    )
    data = json.loads(r.read())
    check(
        "chat->responses",
        "Responses mock" in data["choices"][0]["message"]["content"],
    )
    check("chat->responses usage", data["usage"]["total_tokens"] == 21)

    r = post(
        "/v1/messages",
        {
            "model": "resp",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    data, _ = anthropic_text(r)
    check("anth->responses", "Responses mock" in data["content"][0]["text"])

    r = post(
        "/v1/responses",
        {
            "model": "fast",
            "input": "weather in paris",
            "tools": [
                {
                    "type": "function",
                    "name": "get_weather",
                    "description": "Get weather",
                    "parameters": {
                        "type": "object",
                        "properties": {"city": {"type": "string"}},
                    },
                }
            ],
        },
    )
    data = json.loads(r.read())
    calls = [o for o in data["output"] if o["type"] == "function_call"]
    check(
        "responses tool -> chat",
        bool(calls)
        and calls[0]["name"] == "get_weather"
        and json.loads(calls[0]["arguments"]) == {"city": "Paris"},
    )

    r = post(
        "/v1/chat/completions",
        {
            "model": "resp",
            "messages": [{"role": "user", "content": "weather in paris"}],
            "tools": TOOLS_OAI,
        },
    )
    data = json.loads(r.read())
    check(
        "chat tool -> responses",
        data["choices"][0]["message"]["tool_calls"][0]["function"]["name"]
        == "get_weather",
    )

    r = post("/v1/responses/input_tokens", {"model": "fast", "input": "hello world"})
    check("responses input_tokens", json.loads(r.read())["input_tokens"] > 0)

    r1 = json.loads(
        post("/v1/responses", {"model": "fast", "input": "first question"}).read()
    )
    post(
        "/v1/responses",
        {
            "model": "fast",
            "input": "second question",
            "previous_response_id": r1["id"],
        },
    ).read()
    last = json.loads(
        urllib.request.urlopen("http://127.0.0.1:19100/debug/last").read()
    )
    roles = [m["role"] for m in last["body"]["messages"]]
    check("previous_response_id expansion", roles == ["user", "assistant", "user"])

    small_count = json.loads(
        post(
            "/v1/responses/input_tokens",
            {"model": "fast", "input": "second question"},
        ).read()
    )["input_tokens"]
    history_count = json.loads(
        post(
            "/v1/responses/input_tokens",
            {
                "model": "fast",
                "input": "second question",
                "previous_response_id": r1["id"],
            },
        ).read()
    )["input_tokens"]
    check("input_tokens includes cached history", history_count > small_count)
    try:
        post(
            "/v1/responses/input_tokens",
            {
                "model": "fast",
                "input": "x",
                "previous_response_id": "resp_missing",
            },
        )
        check("input_tokens missing history", False)
    except urllib.error.HTTPError as e:
        check("input_tokens missing history", e.code == 400)

    try:
        get("/v1/responses/resp_missing")
        check("responses retrieve unsupported", False)
    except urllib.error.HTTPError as e:
        check("responses retrieve unsupported", e.code == 501)

    # --- multi-route / pricing / strategy ---------------------------
    r = post(
        "/v1/chat/completions",
        {"model": "multi", "messages": [{"role": "user", "content": "hi"}]},
    )
    data = json.loads(r.read())
    check("multi auto native", "OpenAI mock" in data["choices"][0]["message"]["content"])
    check("multi auto route header", r.headers.get("x-gateway-route") == "multi-chat")

    r = post(
        "/v1/chat/completions",
        {"model": "multi", "messages": [{"role": "user", "content": "hi"}]},
        headers={"x-gateway-route": "multi-responses"},
    )
    data = json.loads(r.read())
    check("route override", "Responses mock" in data["choices"][0]["message"]["content"])
    check("route override header", r.headers.get("x-gateway-route") == "multi-responses")

    try:
        post(
            "/v1/chat/completions",
            {"model": "multi", "messages": [{"role": "user", "content": "hi"}]},
            headers={"x-gateway-route": "does-not-exist"},
        )
        check("unknown route 400", False)
    except urllib.error.HTTPError as e:
        check("unknown route 400", e.code == 400)

    routing = json.loads(get("/admin/api/routing?range=30d").read())
    multi = next((m for m in routing["models"] if m["model"] == "multi"), None)
    check("routing multi routes", multi is not None and len(multi["routes"]) == 3)
    check("routing native choice", multi["choices"]["openai"] == "multi-chat")
    check("routing responses choice", multi["choices"]["responses"] == "multi-responses")
    check("routing anthropic choice", multi["choices"]["anthropic"] == "multi-anth")
    check(
        "routing suggestion",
        multi["suggested_strategy"] == "cheapest"
        and multi["suggested_route"] == "multi-responses",
    )

    cheap = next((m for m in routing["models"] if m["model"] == "cheap"), None)
    check("routing cheapest", cheap["choices"]["openai"] == "cheap-responses")
    manual = next((m for m in routing["models"] if m["model"] == "manual"), None)
    check("routing manual", manual["choices"]["openai"] == "manual-responses")

    # --- admin ------------------------------------------------------
    time.sleep(0.4)
    cfg = json.loads(get("/admin/api/config").read())
    check(
        "admin config",
        len(cfg["providers"]) == 4
        and len(cfg["models"]) == 7
        and cfg["routing"]["default_strategy"] == "auto",
    )
    multi_cfg = next(m for m in cfg["models"] if m["name"] == "multi")
    check("admin config routes", len(multi_cfg["routes"]) == 3)

    raw = json.loads(get("/admin/api/config/raw").read())
    check("admin config raw", "[server]" in raw["content"] and raw["path"].endswith(".toml"))

    validated = json.loads(
        post("/admin/api/config/validate", {"content": raw["content"]}).read()
    )
    check(
        "admin config validate",
        validated["ok"] and validated["providers"] == 4 and validated["models"] == 11,
    )

    invalid = json.loads(
        post("/admin/api/config/validate", {"content": "[server]\nport = 99999\n"}).read()
    )
    check("admin config validate invalid", not invalid["ok"])

    saved = json.loads(put("/admin/api/config", {"content": raw["content"]}).read())
    check(
        "admin config save roundtrip",
        saved["ok"] and not saved["restart_required"] and saved["backup"] is not None,
    )

    overview = json.loads(get("/admin/api/overview?range=24h").read())
    check("admin overview requests", overview["stats"]["requests"] >= 12)
    check("admin overview tokens", overview["stats"]["completion_tokens"] > 0)
    check("admin overview cached", overview["stats"]["cached_tokens"] > 0)
    check(
        "admin overview cost",
        overview["stats"]["cost"] > 0 and overview["stats"]["cost_currency"] == "USD",
    )

    series = json.loads(get("/admin/api/timeseries?range=24h").read())
    check("admin timeseries", len(series["buckets"]) > 0)

    models_stats = json.loads(get("/admin/api/models?range=24h").read())
    check("admin model stats", len(models_stats["models"]) >= 2)

    reqs = json.loads(get("/admin/api/requests?limit=100").read())
    check("admin request log", reqs["total"] >= 12)
    check("admin request usage", any(r["completion_tokens"] > 0 for r in reqs["requests"]))
    check("admin request tps", any((r["tokens_per_sec"] or 0) > 0 for r in reqs["requests"]))
    check("admin request ttft", any(r["ttft_ms"] is not None for r in reqs["requests"]))

    health = json.loads(get("/admin/api/health").read())
    health_by_id = {p["id"]: p for p in health["providers"]}
    check(
        "admin health",
        all(
            health_by_id[x]["ok"]
            for x in ["mock-openai", "mock-anthropic", "mock-responses"]
        ),
    )
    check("admin health detects down", not health_by_id["broken"]["ok"])

    print(f"\n{PASS} checks passed")


if __name__ == "__main__":
    main()
