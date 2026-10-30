#!/usr/bin/env python3
"""Tiny OpenAI + Anthropic mock upstream used to exercise the gateway locally.

    python3 dev/mock_upstream.py 19100
"""

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 19100
TICK = 0.03
LAST = {"path": None, "body": None}


def encode(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # noqa: N802
        sys.stderr.write("[mock] " + fmt % args + "\n")

    # -- helpers ---------------------------------------------------------
    def read_json(self):
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def send_json(self, obj, status=200):
        data = encode(obj)
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def start_sse(self):
        self.send_response(200)
        self.send_header("content-type", "text/event-stream; charset=utf-8")
        self.send_header("cache-control", "no-cache")
        self.send_header("connection", "close")
        self.end_headers()
        self.close_connection = True

    def sse(self, payload: bytes, event: str | None = None):
        if event:
            self.wfile.write(f"event: {event}\n".encode())
        self.wfile.write(b"data: " + payload + b"\n\n")
        self.wfile.flush()
        time.sleep(TICK)

    # -- routes ----------------------------------------------------------
    def do_GET(self):  # noqa: N802
        if self.path.rstrip("/").endswith("/debug/last"):
            self.send_json(LAST)
            return
        if self.path.rstrip("/").endswith("/models"):
            self.send_json(
                {
                    "object": "list",
                    "data": [
                        {"id": "gpt-4o", "object": "model", "created": 0, "owned_by": "mock"},
                        {"id": "claude-sonnet", "object": "model", "created": 0, "owned_by": "mock"},
                    ],
                }
            )
            return
        self.send_json({"error": {"message": "not found"}}, 404)

    def do_POST(self):  # noqa: N802
        global LAST
        path = self.path.split("?")[0].rstrip("/")
        req = self.read_json()
        LAST = {"path": path, "body": req}
        if path.endswith("/chat/completions"):
            self.openai_chat(req)
        elif path.endswith("/responses"):
            self.responses_api(req)
        elif path.endswith("/messages"):
            self.anthropic_messages(req)
        elif path.endswith("/embeddings"):
            self.send_json(
                {
                    "object": "list",
                    "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2, 0.3]}],
                    "model": req.get("model", "mock-embed"),
                    "usage": {"prompt_tokens": 4, "total_tokens": 4},
                }
            )
        else:
            self.send_json({"error": {"message": f"unknown path {path}"}}, 404)

    # -- OpenAI ----------------------------------------------------------
    def openai_chat(self, req):
        model = req.get("model", "gpt-4o")
        tools = req.get("tools") or []
        created = int(time.time())

        if model == "gpt-4o-mini":
            self.send_json(
                {
                    "error": {
                        "message": "mock rate limit",
                        "type": "rate_limit_error",
                        "code": "rate_limit_exceeded",
                    }
                },
                429,
            )
            return

        if req.get("stream"):
            self.start_sse()

            def chunk(delta, finish=None):
                self.sse(
                    encode(
                        {
                            "id": "chatcmpl-mock",
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model,
                            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
                        }
                    )
                )

            chunk({"role": "assistant", "content": ""})
            if tools:
                name = tools[0].get("function", {}).get("name", "tool")
                chunk(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call_mock_1",
                                "type": "function",
                                "function": {"name": name, "arguments": ""},
                            }
                        ]
                    }
                )
                chunk({"tool_calls": [{"index": 0, "function": {"arguments": '{"city":'}}]})
                chunk({"tool_calls": [{"index": 0, "function": {"arguments": '"Paris"}'}}]})
                chunk({}, "tool_calls")
            else:
                for piece in ["Hello ", "from ", "the ", "OpenAI ", "mock."]:
                    chunk({"content": piece})
                chunk({}, "stop")

            self.sse(
                encode(
                    {
                        "id": "chatcmpl-mock",
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model,
                        "choices": [],
                        "usage": {
                            "prompt_tokens": 12,
                            "completion_tokens": 7,
                            "total_tokens": 19,
                            "prompt_tokens_details": {"cached_tokens": 4},
                        },
                    }
                )
            )
            self.sse(b"[DONE]")
            return

        if tools:
            name = tools[0].get("function", {}).get("name", "tool")
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_mock_1",
                        "type": "function",
                        "function": {"name": name, "arguments": '{"city":"Paris"}'},
                    }
                ],
            }
            finish = "tool_calls"
        else:
            message = {"role": "assistant", "content": "Hello from the OpenAI mock."}
            finish = "stop"

        self.send_json(
            {
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 7, "total_tokens": 19, "prompt_tokens_details": {"cached_tokens": 4}},
            }
        )

    # -- OpenAI Responses ------------------------------------------------
    def responses_api(self, req):
        model = req.get("model", "gpt-5-codex")
        tools = req.get("tools") or []
        created = int(time.time())
        resp_id = "resp_mock"
        base = {
            "id": resp_id,
            "object": "response",
            "created_at": created,
            "status": "in_progress",
            "model": model,
            "output": [],
            "parallel_tool_calls": True,
            "tool_choice": req.get("tool_choice", "auto"),
            "tools": tools,
            "error": None,
            "incomplete_details": None,
            "instructions": req.get("instructions"),
            "metadata": None,
            "previous_response_id": req.get("previous_response_id"),
            "reasoning": None,
            "store": False,
            "temperature": None,
            "text": {"format": {"type": "text"}},
            "top_p": None,
            "truncation": "disabled",
            "usage": None,
            "user": None,
        }
        usage = {
            "input_tokens": 13,
            "input_tokens_details": {"cached_tokens": 5, "cache_write_tokens": 0},
            "output_tokens": 8,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 21,
        }

        if req.get("stream"):
            self.start_sse()
            seq = [0]

            def emit(typ, **kwargs):
                payload = {"type": typ, "sequence_number": seq[0], **kwargs}
                seq[0] += 1
                self.sse(encode(payload), typ)

            emit("response.created", response=base)
            emit("response.in_progress", response=base)

            if tools:
                item = {
                    "type": "function_call",
                    "id": "fc_mock",
                    "call_id": "call_mock_resp",
                    "name": tools[0].get("name", "tool"),
                    "arguments": "",
                    "status": "in_progress",
                }
                emit("response.output_item.added", output_index=0, item=item)
                for piece in ['{"city":', '"Paris"}']:
                    emit(
                        "response.function_call_arguments.delta",
                        item_id="fc_mock",
                        output_index=0,
                        delta=piece,
                    )
                full_args = '{"city":"Paris"}'
                emit(
                    "response.function_call_arguments.done",
                    item_id="fc_mock",
                    output_index=0,
                    arguments=full_args,
                )
                done_item = {**item, "arguments": full_args, "status": "completed"}
                emit("response.output_item.done", output_index=0, item=done_item)
                output = [done_item]
            else:
                text = "Hello from the Responses mock."
                emit(
                    "response.output_item.added",
                    output_index=0,
                    item={
                        "type": "message",
                        "id": "msg_mock",
                        "status": "in_progress",
                        "role": "assistant",
                        "content": [],
                    },
                )
                emit(
                    "response.content_part.added",
                    item_id="msg_mock",
                    output_index=0,
                    content_index=0,
                    part={"type": "output_text", "text": "", "annotations": []},
                )
                for piece in ["Hello ", "from ", "the ", "Responses ", "mock."]:
                    emit(
                        "response.output_text.delta",
                        item_id="msg_mock",
                        output_index=0,
                        content_index=0,
                        delta=piece,
                        logprobs=[],
                    )
                emit(
                    "response.output_text.done",
                    item_id="msg_mock",
                    output_index=0,
                    content_index=0,
                    text=text,
                    logprobs=[],
                )
                part = {"type": "output_text", "text": text, "annotations": []}
                emit(
                    "response.content_part.done",
                    item_id="msg_mock",
                    output_index=0,
                    content_index=0,
                    part=part,
                )
                done_item = {
                    "type": "message",
                    "id": "msg_mock",
                    "status": "completed",
                    "role": "assistant",
                    "content": [part],
                }
                emit("response.output_item.done", output_index=0, item=done_item)
                output = [done_item]

            final = {
                **base,
                "status": "completed",
                "completed_at": int(time.time()),
                "output": output,
                "usage": usage,
            }
            emit("response.completed", response=final)
            return

        if tools:
            output = [
                {
                    "type": "function_call",
                    "id": "fc_mock",
                    "call_id": "call_mock_resp",
                    "name": tools[0].get("name", "tool"),
                    "arguments": '{"city":"Paris"}',
                    "status": "completed",
                }
            ]
        else:
            output = [
                {
                    "type": "message",
                    "id": "msg_mock",
                    "status": "completed",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "Hello from the Responses mock.",
                            "annotations": [],
                        }
                    ],
                }
            ]
        self.send_json(
            {
                **base,
                "status": "completed",
                "completed_at": int(time.time()),
                "output": output,
                "usage": usage,
            }
        )

    # -- Anthropic -------------------------------------------------------
    def anthropic_messages(self, req):
        model = req.get("model", "claude-sonnet")
        tools = req.get("tools") or []
        message_id = "msg_mock"

        if req.get("stream"):
            self.start_sse()
            self.sse(
                encode(
                    {
                        "type": "message_start",
                        "message": {
                            "id": message_id,
                            "type": "message",
                            "role": "assistant",
                            "model": model,
                            "content": [],
                            "stop_reason": None,
                            "stop_sequence": None,
                            "usage": {"input_tokens": 11, "output_tokens": 1},
                        },
                    }
                ),
                "message_start",
            )
            if tools:
                name = tools[0].get("name", "tool")
                self.sse(
                    encode(
                        {
                            "type": "content_block_start",
                            "index": 0,
                            "content_block": {
                                "type": "tool_use",
                                "id": "toolu_mock_1",
                                "name": name,
                                "input": {},
                            },
                        }
                    ),
                    "content_block_start",
                )
                for piece in ['{"city":', '"Paris"}']:
                    self.sse(
                        encode(
                            {
                                "type": "content_block_delta",
                                "index": 0,
                                "delta": {"type": "input_json_delta", "partial_json": piece},
                            }
                        ),
                        "content_block_delta",
                    )
                self.sse(
                    encode({"type": "content_block_stop", "index": 0}),
                    "content_block_stop",
                )
                stop_reason = "tool_use"
            else:
                self.sse(
                    encode(
                        {
                            "type": "content_block_start",
                            "index": 0,
                            "content_block": {"type": "text", "text": ""},
                        }
                    ),
                    "content_block_start",
                )
                for piece in ["Hello ", "from ", "the ", "Anthropic ", "mock."]:
                    self.sse(
                        encode(
                            {
                                "type": "content_block_delta",
                                "index": 0,
                                "delta": {"type": "text_delta", "text": piece},
                            }
                        ),
                        "content_block_delta",
                    )
                self.sse(encode({"type": "ping"}), "ping")
                self.sse(
                    encode({"type": "content_block_stop", "index": 0}),
                    "content_block_stop",
                )
                stop_reason = "end_turn"

            self.sse(
                encode(
                    {
                        "type": "message_delta",
                        "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                        "usage": {"output_tokens": 9},
                    }
                ),
                "message_delta",
            )
            self.sse(encode({"type": "message_stop"}), "message_stop")
            return

        if tools:
            name = tools[0].get("name", "tool")
            content = [
                {
                    "type": "tool_use",
                    "id": "toolu_mock_1",
                    "name": name,
                    "input": {"city": "Paris"},
                }
            ]
            stop_reason = "tool_use"
        else:
            content = [{"type": "text", "text": "Hello from the Anthropic mock."}]
            stop_reason = "end_turn"

        self.send_json(
            {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": content,
                "stop_reason": stop_reason,
                "stop_sequence": None,
                "usage": {"input_tokens": 11, "output_tokens": 9},
            }
        )


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"mock upstream listening on http://127.0.0.1:{PORT}", flush=True)
    server.serve_forever()
