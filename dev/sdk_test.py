#!/usr/bin/env python3
"""Validate the gateway with the official OpenAI and Anthropic Python SDKs."""

import json
import sys

from anthropic import Anthropic
from openai import OpenAI

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:18787"

oai = OpenAI(base_url=BASE + "/v1", api_key="dummy")
anth = Anthropic(base_url=BASE, api_key="dummy")

passed = 0


def check(name, cond, detail=""):
    global passed
    if not cond:
        raise AssertionError(f"FAIL {name}: {detail}")
    passed += 1
    print(f"  ok  {name}")


# --- OpenAI SDK, OpenAI upstream -----------------------------------------
r = oai.chat.completions.create(
    model="fast", messages=[{"role": "user", "content": "hi"}]
)
check("openai sdk json", "OpenAI mock" in r.choices[0].message.content)
check("openai sdk usage", r.usage.total_tokens == 19)

stream = oai.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "hi"}],
    stream=True,
)
text = ""
usage = None
for chunk in stream:
    if chunk.usage:
        usage = chunk.usage
    for choice in chunk.choices:
        text += choice.delta.content or ""
check("openai sdk stream", "OpenAI mock" in text)
check("openai sdk stream usage", usage is not None and usage.completion_tokens == 7)

# --- OpenAI SDK, Anthropic upstream (format translation) -----------------
r = oai.chat.completions.create(
    model="claude", messages=[{"role": "user", "content": "hi"}]
)
check("openai sdk -> anth", "Anthropic mock" in r.choices[0].message.content)
check("openai sdk -> anth usage", r.usage.total_tokens == 20)

stream = oai.chat.completions.create(
    model="claude",
    messages=[{"role": "user", "content": "hi"}],
    stream=True,
)
text = ""
tool_name = None
for chunk in stream:
    for choice in chunk.choices:
        text += choice.delta.content or ""
        if choice.delta.tool_calls:
            tool_name = choice.delta.tool_calls[0].function.name
check("openai sdk -> anth stream", "Anthropic mock" in text)

# --- Anthropic SDK, Anthropic upstream -----------------------------------
r = anth.messages.create(
    model="claude",
    max_tokens=64,
    messages=[{"role": "user", "content": "hi"}],
)
check("anthropic sdk json", "Anthropic mock" in r.content[0].text)
check("anthropic sdk usage", r.usage.output_tokens == 9)

with anth.messages.stream(
    model="claude",
    max_tokens=64,
    messages=[{"role": "user", "content": "hi"}],
) as stream:
    text = stream.get_final_text()
check("anthropic sdk stream", "Anthropic mock" in text)

# --- Anthropic SDK, OpenAI upstream (format translation) -----------------
r = anth.messages.create(
    model="fast",
    max_tokens=64,
    messages=[{"role": "user", "content": "hi"}],
)
check("anthropic sdk -> openai", "OpenAI mock" in r.content[0].text)
check("anthropic sdk -> openai stop", r.stop_reason == "end_turn")

with anth.messages.stream(
    model="fast",
    max_tokens=64,
    messages=[{"role": "user", "content": "hi"}],
) as stream:
    text = stream.get_final_text()
check("anthropic sdk -> openai stream", "OpenAI mock" in text)

# --- tools through both SDKs --------------------------------------------
tools = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get weather",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}},
        },
    }
]
r = oai.chat.completions.create(
    model="claude",
    messages=[{"role": "user", "content": "weather in paris"}],
    tools=tools,
)
check("openai sdk tool call", r.choices[0].message.tool_calls[0].function.name == "get_weather")
check("openai sdk tool finish", r.choices[0].finish_reason == "tool_calls")

r = anth.messages.create(
    model="fast",
    max_tokens=64,
    messages=[{"role": "user", "content": "weather in paris"}],
    tools=[
        {
            "name": "get_weather",
            "description": "Get weather",
            "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}},
        }
    ],
)
blocks = [b for b in r.content if b.type == "tool_use"]
check("anthropic sdk tool call", blocks and blocks[0].name == "get_weather")
check("anthropic sdk tool input", blocks[0].input == {"city": "Paris"})
check("anthropic sdk tool stop", r.stop_reason == "tool_use")

# --- OpenAI Responses API ------------------------------------------------
r = oai.responses.create(model="fast", input="hi")
check("responses sdk json", "OpenAI mock" in r.output_text)
check("responses sdk usage", r.usage.total_tokens == 19)

r = oai.responses.create(model="claude", input="hi")
check("responses sdk -> anth", "Anthropic mock" in r.output_text)

r = oai.responses.create(model="resp", input="hi")
check("responses sdk passthrough", "Responses mock" in r.output_text)

types = []
text = ""
for ev in oai.responses.create(model="fast", input="hi", stream=True):
    types.append(ev.type)
    if ev.type == "response.output_text.delta":
        text += ev.delta
check("responses sdk stream", "OpenAI mock" in text)
check(
    "responses sdk stream end",
    types[0] == "response.created" and types[-1] == "response.completed",
)

types = []
text = ""
for ev in oai.responses.create(model="resp", input="hi", stream=True):
    types.append(ev.type)
    if ev.type == "response.output_text.delta":
        text += ev.delta
check(
    "responses sdk passthrough stream",
    "Responses mock" in text and types[-1] == "response.completed",
)

r = oai.chat.completions.create(
    model="resp", messages=[{"role": "user", "content": "hi"}]
)
check("chat sdk -> responses", "Responses mock" in r.choices[0].message.content)

r = anth.messages.create(
    model="resp",
    max_tokens=64,
    messages=[{"role": "user", "content": "hi"}],
)
check("anthropic sdk -> responses", "Responses mock" in r.content[0].text)

resp_tools = [
    {
        "type": "function",
        "name": "get_weather",
        "description": "Get weather",
        "parameters": {"type": "object", "properties": {"city": {"type": "string"}}},
        "strict": False,
    }
]
r = oai.responses.create(model="fast", input="weather", tools=resp_tools)
calls = [o for o in r.output if o.type == "function_call"]
check(
    "responses sdk tool",
    bool(calls)
    and calls[0].name == "get_weather"
    and json.loads(calls[0].arguments) == {"city": "Paris"},
)

stream_calls = []
for ev in oai.responses.create(
    model="fast", input="weather", tools=resp_tools, stream=True
):
    if ev.type == "response.output_item.done" and ev.item.type == "function_call":
        stream_calls.append((ev.item.name, ev.item.arguments))
check(
    "responses sdk stream tool",
    bool(stream_calls) and stream_calls[0][0] == "get_weather",
)

r1 = oai.responses.create(model="fast", input="first question")
r2 = oai.responses.create(
    model="fast", input="second question", previous_response_id=r1.id
)
check("responses previous_response_id", "OpenAI mock" in r2.output_text)

count = oai.responses.input_tokens.count(model="fast", input="hello world")
check(
    "responses input tokens sdk",
    count.input_tokens > 0 and count.object == "response.input_tokens",
)

try:
    oai.responses.retrieve(r1.id)
    check("responses retrieve unsupported", False)
except Exception as e:  # noqa: BLE001
    check("responses retrieve unsupported", "not supported" in str(e).lower())

# --- model listing -------------------------------------------------------
models = oai.models.list()
check("openai sdk models", any(m.id == "fast" for m in models.data))

amodels = anth.models.list()
check("anthropic sdk models", any(m.id == "claude" for m in amodels.data))

print(f"\n{passed} SDK checks passed")
