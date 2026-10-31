//! Bidirectional translation between the OpenAI Chat Completions API and the
//! Anthropic Messages API. Only the fields that matter for chat/tool use are
//! translated; unknown vendor fields are dropped when crossing formats.

use serde_json::{json, Map, Value};

use crate::util::{estimate_value_tokens, now_secs};

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

fn text_of(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Array(parts) => {
            let mut out = String::new();
            for p in parts {
                if let Some(t) = p.get("text").and_then(|t| t.as_str()) {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(t);
                }
            }
            out
        }
        other => other.to_string(),
    }
}

fn data_uri_parts(uri: &str) -> Option<(String, String)> {
    let rest = uri.strip_prefix("data:")?;
    let (meta, data) = rest.split_once(',')?;
    let media = meta.split(';').next().unwrap_or("image/png").to_string();
    Some((media, data.to_string()))
}

fn openai_image_part_to_anthropic(part: &Value) -> Option<Value> {
    let url = part
        .get("image_url")
        .and_then(|i| i.get("url").and_then(|u| u.as_str()).or_else(|| i.as_str()))
        .or_else(|| part.get("url").and_then(|u| u.as_str()))?;
    if let Some((media_type, data)) = data_uri_parts(url) {
        Some(json!({
            "type": "image",
            "source": {"type": "base64", "media_type": media_type, "data": data}
        }))
    } else {
        Some(json!({
            "type": "image",
            "source": {"type": "url", "url": url}
        }))
    }
}

fn anthropic_image_to_openai(block: &Value) -> Option<Value> {
    let source = block.get("source")?;
    let url = match source.get("type").and_then(|t| t.as_str()) {
        Some("base64") => {
            let media = source
                .get("media_type")
                .and_then(|m| m.as_str())
                .unwrap_or("image/png");
            let data = source.get("data").and_then(|d| d.as_str())?;
            format!("data:{media};base64,{data}")
        }
        Some("url") => source.get("url").and_then(|u| u.as_str())?.to_string(),
        _ => return None,
    };
    Some(json!({"type": "image_url", "image_url": {"url": url}}))
}

/// Split a content field into Anthropic-style blocks.
fn to_anthropic_blocks(content: &Value) -> Vec<Value> {
    match content {
        Value::String(s) => {
            if s.is_empty() {
                Vec::new()
            } else {
                vec![json!({"type": "text", "text": s})]
            }
        }
        Value::Array(parts) => {
            let mut out = Vec::new();
            for p in parts {
                match p.get("type").and_then(|t| t.as_str()) {
                    Some("text") | Some("input_text") => {
                        if let Some(t) = p.get("text").and_then(|t| t.as_str()) {
                            if !t.is_empty() {
                                out.push(json!({"type": "text", "text": t}));
                            }
                        }
                    }
                    Some("image_url") | Some("image") => {
                        if let Some(img) = openai_image_part_to_anthropic(p) {
                            out.push(img);
                        }
                    }
                    _ => {}
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

fn push_anthropic_message(msgs: &mut Vec<Value>, role: &str, blocks: Vec<Value>) {
    if blocks.is_empty() {
        return;
    }
    if let Some(last) = msgs.last_mut() {
        if last.get("role").and_then(|r| r.as_str()) == Some(role) {
            if let Some(arr) = last.get_mut("content").and_then(|c| c.as_array_mut()) {
                arr.extend(blocks);
                return;
            }
        }
    }
    msgs.push(json!({"role": role, "content": blocks}));
}

fn push_tool_result(msgs: &mut Vec<Value>, block: Value) {
    if let Some(last) = msgs.last_mut() {
        if last.get("role").and_then(|r| r.as_str()) == Some("user") {
            let tool_only = last
                .get("content")
                .and_then(|c| c.as_array())
                .map(|a| {
                    !a.is_empty()
                        && a.iter()
                            .all(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_result"))
                })
                .unwrap_or(false);
            if tool_only {
                if let Some(arr) = last.get_mut("content").and_then(|c| c.as_array_mut()) {
                    arr.push(block);
                    return;
                }
            }
        }
    }
    msgs.push(json!({"role": "user", "content": [block]}));
}

pub fn openai_finish_to_anthropic(reason: Option<&str>) -> &'static str {
    map_oai_finish_to_anth(reason)
}

pub fn anthropic_stop_to_openai(reason: Option<&str>) -> &'static str {
    map_anth_stop_to_oai(reason)
}

fn map_oai_finish_to_anth(reason: Option<&str>) -> &'static str {
    match reason.unwrap_or("stop") {
        "length" => "max_tokens",
        "tool_calls" | "function_call" => "tool_use",
        "content_filter" => "end_turn",
        _ => "end_turn",
    }
}

fn map_anth_stop_to_oai(reason: Option<&str>) -> &'static str {
    match reason.unwrap_or("end_turn") {
        "max_tokens" => "length",
        "tool_use" => "tool_calls",
        _ => "stop",
    }
}

pub fn anthropic_usage(v: &Value) -> (u64, u64, u64) {
    let u = &v["usage"];
    let input = u.get("input_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
    let cache_read = u
        .get("cache_read_input_tokens")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let cache_write = u
        .get("cache_creation_input_tokens")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let output = u.get("output_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
    (input + cache_read + cache_write, output, cache_read)
}

pub fn openai_usage(v: &Value) -> (u64, u64) {
    let u = &v["usage"];
    (
        u.get("prompt_tokens").and_then(|x| x.as_u64()).unwrap_or(0),
        u.get("completion_tokens")
            .and_then(|x| x.as_u64())
            .unwrap_or(0),
    )
}

// ---------------------------------------------------------------------------
// requests
// ---------------------------------------------------------------------------

pub fn openai_to_anthropic_request(req: &Value, upstream_model: &str) -> Value {
    let messages = req
        .get("messages")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();

    let mut system = String::new();
    let mut anth_messages: Vec<Value> = Vec::new();

    for msg in &messages {
        let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
        match role {
            "system" | "developer" => {
                let t = text_of(&msg["content"]);
                if !t.trim().is_empty() {
                    if !system.is_empty() {
                        system.push_str("\n\n");
                    }
                    system.push_str(&t);
                }
            }
            "user" => {
                let blocks = to_anthropic_blocks(&msg["content"]);
                push_anthropic_message(&mut anth_messages, "user", blocks);
            }
            "assistant" => {
                let mut blocks = to_anthropic_blocks(&msg["content"]);
                if let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                    for (i, tc) in tool_calls.iter().enumerate() {
                        let f = &tc["function"];
                        let args = f.get("arguments").and_then(|a| a.as_str()).unwrap_or("{}");
                        let input: Value = serde_json::from_str(args).unwrap_or_else(|_| json!({}));
                        blocks.push(json!({
                            "type": "tool_use",
                            "id": tc.get("id").and_then(|x| x.as_str()).map(|s| s.to_string())
                                .unwrap_or_else(|| format!("call_{i}")),
                            "name": f.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                            "input": input
                        }));
                    }
                }
                // legacy single function_call
                if let Some(fc) = msg.get("function_call") {
                    let args = fc.get("arguments").and_then(|a| a.as_str()).unwrap_or("{}");
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": "call_legacy",
                        "name": fc.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                        "input": serde_json::from_str::<Value>(args).unwrap_or_else(|_| json!({}))
                    }));
                }
                push_anthropic_message(&mut anth_messages, "assistant", blocks);
            }
            "tool" | "function" => {
                let id = msg
                    .get("tool_call_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let content = text_of(&msg["content"]);
                push_tool_result(
                    &mut anth_messages,
                    json!({"type": "tool_result", "tool_use_id": id, "content": content}),
                );
            }
            _ => {}
        }
    }

    let mut out = Map::new();
    out.insert("model".into(), json!(upstream_model));
    out.insert("messages".into(), json!(anth_messages));

    if !system.is_empty() {
        out.insert("system".into(), json!(system));
    }

    // Only translate a client-provided limit. Route defaults and the final
    // 4096 fallback are applied after params are merged by the gateway.
    if let Some(max_tokens) = req
        .get("max_completion_tokens")
        .or_else(|| req.get("max_tokens"))
        .and_then(|v| v.as_u64())
    {
        out.insert("max_tokens".into(), json!(max_tokens));
    }

    for key in ["temperature", "top_p"] {
        if let Some(v) = req.get(key) {
            if !v.is_null() {
                out.insert(key.into(), v.clone());
            }
        }
    }

    if let Some(stop) = req.get("stop") {
        let seq: Vec<Value> = match stop {
            Value::String(s) => vec![json!(s)],
            Value::Array(a) => a.clone(),
            _ => Vec::new(),
        };
        if !seq.is_empty() {
            out.insert("stop_sequences".into(), json!(seq));
        }
    }

    if let Some(tools) = req.get("tools").and_then(|t| t.as_array()) {
        let converted: Vec<Value> = tools
            .iter()
            .filter(|t| t.get("type").and_then(|x| x.as_str()) == Some("function"))
            .filter_map(|t| {
                let f = t.get("function")?;
                Some(json!({
                    "name": f.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                    "description": f.get("description").cloned().unwrap_or(Value::Null),
                    "input_schema": f.get("parameters").cloned()
                        .unwrap_or_else(|| json!({"type": "object", "properties": {}}))
                }))
            })
            .collect();
        if !converted.is_empty() {
            out.insert("tools".into(), json!(converted));
            match req.get("tool_choice") {
                Some(Value::String(s)) => match s.as_str() {
                    "auto" => {
                        out.insert("tool_choice".into(), json!({"type": "auto"}));
                    }
                    "required" => {
                        out.insert("tool_choice".into(), json!({"type": "any"}));
                    }
                    "none" => {
                        // Anthropic has no explicit "never"; drop the tools.
                        out.remove("tools");
                    }
                    _ => {}
                },
                Some(v) if v.get("type").and_then(|t| t.as_str()) == Some("function") => {
                    if let Some(name) = v["function"].get("name").and_then(|n| n.as_str()) {
                        out.insert("tool_choice".into(), json!({"type": "tool", "name": name}));
                    }
                }
                _ => {}
            }
            if out.contains_key("tools") {
                if let Some(parallel) = req.get("parallel_tool_calls").and_then(|p| p.as_bool()) {
                    if !parallel {
                        match out.get_mut("tool_choice").and_then(|c| c.as_object_mut()) {
                            Some(choice) => {
                                choice.insert("disable_parallel_tool_use".into(), json!(true));
                            }
                            None => {
                                out.insert(
                                    "tool_choice".into(),
                                    json!({
                                        "type": "auto",
                                        "disable_parallel_tool_use": true
                                    }),
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    if req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false) {
        out.insert("stream".into(), json!(true));
    }

    if let Some(user) = req.get("user").and_then(|u| u.as_str()) {
        out.insert("metadata".into(), json!({"user_id": user}));
    }

    // JSON modes are not native to Anthropic; nudge the model via the system prompt.
    if let Some(format) = req
        .get("response_format")
        .and_then(|r| r.get("type"))
        .and_then(|t| t.as_str())
    {
        let extra = match format {
            "json_object" => {
                Some("Respond with a single valid JSON object and nothing else.".to_string())
            }
            "json_schema" => req
                .get("response_format")
                .and_then(|r| r.get("json_schema"))
                .and_then(|s| s.get("schema"))
                .map(|schema| {
                    format!(
                        "Respond with a single valid JSON value that conforms to this JSON Schema. \
                         Return only the JSON value and nothing else:\n{}",
                        schema
                    )
                }),
            _ => None,
        };
        if let Some(extra) = extra {
            let merged = if system.is_empty() {
                extra
            } else {
                format!("{system}\n\n{extra}")
            };
            out.insert("system".into(), json!(merged));
        }
    }

    Value::Object(out)
}

pub fn anthropic_to_openai_request(req: &Value, upstream_model: &str) -> Value {
    let mut messages: Vec<Value> = Vec::new();

    if let Some(system) = req.get("system") {
        let t = text_of(system);
        if !t.trim().is_empty() {
            messages.push(json!({"role": "system", "content": t}));
        }
    }

    if let Some(msgs) = req.get("messages").and_then(|m| m.as_array()) {
        for msg in msgs {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
            let blocks: Vec<Value> = match &msg["content"] {
                Value::String(s) => vec![json!({"type": "text", "text": s})],
                Value::Array(a) => a.clone(),
                _ => Vec::new(),
            };

            if role == "assistant" {
                let mut texts: Vec<Value> = Vec::new();
                let mut tool_calls: Vec<Value> = Vec::new();
                for b in &blocks {
                    match b.get("type").and_then(|t| t.as_str()) {
                        Some("text") => {
                            texts.push(json!({"type": "text", "text": b["text"]}));
                        }
                        Some("tool_use") => {
                            tool_calls.push(json!({
                                "id": b.get("id").and_then(|x| x.as_str()).unwrap_or("call_0"),
                                "type": "function",
                                "function": {
                                    "name": b.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                                    "arguments": b.get("input").cloned().unwrap_or_else(|| json!({})).to_string()
                                }
                            }));
                        }
                        _ => {}
                    }
                }
                if texts.is_empty() && tool_calls.is_empty() {
                    continue;
                }
                let content = if texts.is_empty() {
                    Value::Null
                } else if texts.len() == 1 {
                    texts[0]["text"].clone()
                } else {
                    json!(texts)
                };
                let mut m = json!({"role": "assistant", "content": content});
                if !tool_calls.is_empty() {
                    m["tool_calls"] = json!(tool_calls);
                }
                messages.push(m);
            } else {
                let mut parts: Vec<Value> = Vec::new();
                let mut tool_msgs: Vec<Value> = Vec::new();
                for b in &blocks {
                    match b.get("type").and_then(|t| t.as_str()) {
                        Some("text") => {
                            if let Some(text) = b["text"].as_str().filter(|text| !text.is_empty()) {
                                parts.push(json!({"type": "text", "text": text}));
                            }
                        }
                        Some("image") => {
                            if let Some(img) = anthropic_image_to_openai(b) {
                                parts.push(img);
                            }
                        }
                        Some("tool_result") => {
                            let id = b.get("tool_use_id").and_then(|x| x.as_str()).unwrap_or("");
                            if let Some(content) = b.get("content").and_then(|v| v.as_array()) {
                                for block in content {
                                    if block.get("type").and_then(|v| v.as_str()) == Some("image") {
                                        if let Some(image) = anthropic_image_to_openai(block) {
                                            parts.push(image);
                                        }
                                    }
                                }
                            }
                            tool_msgs.push(json!({
                                "role": "tool",
                                "tool_call_id": id,
                                "content": text_of(&b["content"])
                            }));
                        }
                        _ => {}
                    }
                }
                for tm in tool_msgs {
                    messages.push(tm);
                }
                if !parts.is_empty() {
                    let content = if parts.len() == 1
                        && parts[0].get("type").and_then(|t| t.as_str()) == Some("text")
                    {
                        parts[0]["text"].clone()
                    } else {
                        json!(parts)
                    };
                    messages.push(json!({"role": "user", "content": content}));
                } else if messages.is_empty() {
                    messages.push(json!({"role": "user", "content": ""}));
                }
            }
        }
    }

    let mut out = Map::new();
    out.insert("model".into(), json!(upstream_model));
    out.insert("messages".into(), json!(messages));

    if let Some(mt) = req.get("max_tokens").and_then(|v| v.as_u64()) {
        out.insert("max_tokens".into(), json!(mt));
    }
    for key in ["temperature", "top_p"] {
        if let Some(v) = req.get(key) {
            if !v.is_null() {
                out.insert(key.into(), v.clone());
            }
        }
    }
    if let Some(stop) = req.get("stop_sequences") {
        out.insert("stop".into(), stop.clone());
    }
    if let Some(tools) = req.get("tools").and_then(|t| t.as_array()) {
        let converted: Vec<Value> = tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                        "description": t.get("description").cloned().unwrap_or(Value::Null),
                        "parameters": t.get("input_schema").cloned()
                            .unwrap_or_else(|| json!({"type": "object", "properties": {}}))
                    }
                })
            })
            .collect();
        if !converted.is_empty() {
            out.insert("tools".into(), json!(converted));
            if let Some(tc) = req.get("tool_choice") {
                match tc.get("type").and_then(|t| t.as_str()) {
                    Some("auto") => {
                        out.insert("tool_choice".into(), json!("auto"));
                    }
                    Some("any") => {
                        out.insert("tool_choice".into(), json!("required"));
                    }
                    Some("none") => {
                        out.insert("tool_choice".into(), json!("none"));
                    }
                    Some("tool") => {
                        if let Some(name) = tc.get("name").and_then(|n| n.as_str()) {
                            out.insert(
                                "tool_choice".into(),
                                json!({"type": "function", "function": {"name": name}}),
                            );
                        }
                    }
                    _ => {}
                }
                if let Some(disable) = tc
                    .get("disable_parallel_tool_use")
                    .and_then(|v| v.as_bool())
                {
                    out.insert("parallel_tool_calls".into(), json!(!disable));
                }
            }
        }
    }
    if req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false) {
        out.insert("stream".into(), json!(true));
    }
    if let Some(user) = req
        .get("metadata")
        .and_then(|m| m.get("user_id"))
        .and_then(|u| u.as_str())
    {
        out.insert("user".into(), json!(user));
    }

    Value::Object(out)
}

// ---------------------------------------------------------------------------
// responses
// ---------------------------------------------------------------------------

pub fn anthropic_to_openai_response(resp: &Value, alias: &str) -> Value {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();

    if let Some(blocks) = resp.get("content").and_then(|c| c.as_array()) {
        for b in blocks {
            match b.get("type").and_then(|t| t.as_str()) {
                Some("text") => text.push_str(b.get("text").and_then(|t| t.as_str()).unwrap_or("")),
                Some("thinking") => {
                    if let Some(t) = b.get("thinking").and_then(|t| t.as_str()) {
                        reasoning.push_str(t);
                    }
                }
                Some("tool_use") => {
                    tool_calls.push(json!({
                        "id": b.get("id").and_then(|x| x.as_str()).unwrap_or("call_0"),
                        "type": "function",
                        "function": {
                            "name": b.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                            "arguments": b.get("input").cloned().unwrap_or_else(|| json!({})).to_string()
                        }
                    }));
                }
                _ => {}
            }
        }
    }

    let (prompt, completion, cached) = anthropic_usage(resp);
    let mut message = json!({
        "role": "assistant",
        "content": if text.is_empty() && !tool_calls.is_empty() { Value::Null } else { json!(text) }
    });
    if !tool_calls.is_empty() {
        message["tool_calls"] = json!(tool_calls);
    }
    if !reasoning.is_empty() {
        message["reasoning_content"] = json!(reasoning);
    }

    json!({
        "id": resp.get("id").and_then(|x| x.as_str()).unwrap_or("chatcmpl-gateway"),
        "object": "chat.completion",
        "created": now_secs(),
        "model": alias,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": map_anth_stop_to_oai(resp.get("stop_reason").and_then(|s| s.as_str())),
            "logprobs": Value::Null
        }],
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": prompt + completion,
            "prompt_tokens_details": {"cached_tokens": cached}
        }
    })
}

pub fn openai_to_anthropic_response(resp: &Value, alias: &str) -> Value {
    let choice = &resp["choices"][0];
    let message = &choice["message"];

    let text = text_of(&message["content"]);
    let mut content: Vec<Value> = Vec::new();
    // Reasoning becomes a thinking block. Signatures cannot be preserved, so
    // the field is explicitly empty rather than fabricated.
    if let Some(reasoning) = message
        .get("reasoning_content")
        .or_else(|| message.get("reasoning"))
        .and_then(|r| r.as_str())
    {
        if !reasoning.is_empty() {
            content.push(json!({
                "type": "thinking",
                "thinking": reasoning,
                "signature": ""
            }));
        }
    }
    if !text.is_empty() {
        content.push(json!({"type": "text", "text": text}));
    }
    // OpenAI Chat exposes refusals as a separate field; Anthropic clients
    // should still see the refusal text.
    if let Some(refusal) = message.get("refusal").and_then(|r| r.as_str()) {
        if !refusal.is_empty() {
            content.push(json!({"type": "text", "text": refusal}));
        }
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(|t| t.as_array()) {
        for tc in tool_calls {
            let f = &tc["function"];
            let args = f.get("arguments").and_then(|a| a.as_str()).unwrap_or("{}");
            content.push(json!({
                "type": "tool_use",
                "id": tc.get("id").and_then(|x| x.as_str()).unwrap_or("call_0"),
                "name": f.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                "input": serde_json::from_str::<Value>(args).unwrap_or_else(|_| json!({}))
            }));
        }
    }
    if content.is_empty() {
        content.push(json!({"type": "text", "text": ""}));
    }

    let (prompt, completion) = openai_usage(resp);
    let cached = resp["usage"]["prompt_tokens_details"]["cached_tokens"]
        .as_u64()
        .unwrap_or(0)
        .min(prompt);
    // Anthropic input_tokens excludes cache reads; the cache is reported
    // separately so clients do not charge cached input at the full rate.
    let input_tokens = prompt.saturating_sub(cached);
    let raw_id = resp.get("id").and_then(|x| x.as_str()).unwrap_or("gateway");
    let clean_id = raw_id
        .trim_start_matches("msg_")
        .trim_start_matches("chatcmpl-")
        .trim_start_matches("resp_");
    json!({
        "id": format!("msg_{clean_id}"),
        "type": "message",
        "role": "assistant",
        "model": alias,
        "content": content,
        "stop_reason": map_oai_finish_to_anth(choice.get("finish_reason").and_then(|r| r.as_str())),
        "stop_sequence": Value::Null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": completion,
            "cache_read_input_tokens": cached,
            "cache_creation_input_tokens": 0
        }
    })
}

// ---------------------------------------------------------------------------
// OpenAI Responses API
// ---------------------------------------------------------------------------

pub fn responses_usage(v: &Value) -> (u64, u64, u64) {
    let u = &v["usage"];
    let input = u.get("input_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
    let output = u.get("output_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
    let cached = u
        .get("input_tokens_details")
        .and_then(|d| d.get("cached_tokens"))
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    (input, output, cached)
}

/// Normalize the `input` field of a Responses request to an item array.
pub fn responses_input_items(req: &Value) -> Vec<Value> {
    match req.get("input") {
        Some(Value::String(s)) => vec![json!({"role": "user", "content": s})],
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

pub fn responses_output_text(v: &Value) -> String {
    let mut out = String::new();
    if let Some(items) = v.get("output").and_then(|o| o.as_array()) {
        for item in items {
            if item.get("type").and_then(|t| t.as_str()) != Some("message") {
                continue;
            }
            if let Some(parts) = item.get("content").and_then(|c| c.as_array()) {
                for part in parts {
                    if part.get("type").and_then(|t| t.as_str()) == Some("output_text") {
                        out.push_str(part.get("text").and_then(|t| t.as_str()).unwrap_or(""));
                    }
                }
            }
        }
    }
    out
}

fn responses_refusal_text(v: &Value) -> String {
    let mut out = String::new();
    if let Some(items) = v.get("output").and_then(|o| o.as_array()) {
        for item in items {
            if item.get("type").and_then(|t| t.as_str()) != Some("message") {
                continue;
            }
            if let Some(parts) = item.get("content").and_then(|c| c.as_array()) {
                for part in parts {
                    if part.get("type").and_then(|t| t.as_str()) == Some("refusal") {
                        out.push_str(part.get("refusal").and_then(|t| t.as_str()).unwrap_or(""));
                    }
                }
            }
        }
    }
    out
}

fn responses_reasoning_text(v: &Value) -> String {
    let mut out = String::new();
    if let Some(items) = v.get("output").and_then(|o| o.as_array()) {
        for item in items {
            if item.get("type").and_then(|t| t.as_str()) != Some("reasoning") {
                continue;
            }
            if let Some(summary) = item.get("summary").and_then(|s| s.as_array()) {
                for part in summary {
                    if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                        out.push_str(text);
                    }
                }
            }
        }
    }
    out
}

fn responses_tool_calls(v: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(items) = v.get("output").and_then(|o| o.as_array()) {
        for item in items {
            if item.get("type").and_then(|t| t.as_str()) != Some("function_call") {
                continue;
            }
            out.push(json!({
                "id": item
                    .get("call_id")
                    .and_then(|x| x.as_str())
                    .or_else(|| item.get("id").and_then(|x| x.as_str()))
                    .unwrap_or("call_0"),
                "type": "function",
                "function": {
                    "name": item.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                    "arguments": item.get("arguments").and_then(|x| x.as_str()).unwrap_or("{}")
                }
            }));
        }
    }
    out
}

fn responses_text_of(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(text);
                } else if let Some(inner) = item.get("content") {
                    let text = responses_text_of(inner);
                    if !text.is_empty() {
                        if !out.is_empty() {
                            out.push('\n');
                        }
                        out.push_str(&text);
                    }
                }
            }
            out
        }
        _ => String::new(),
    }
}

fn responses_content_to_chat(content: &Value) -> Value {
    match content {
        Value::String(s) => json!(s),
        Value::Array(items) => {
            let mut parts: Vec<Value> = Vec::new();
            for item in items {
                match item.get("type").and_then(|t| t.as_str()) {
                    Some("input_text") | Some("output_text") | Some("text") => {
                        if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                            if !text.is_empty() {
                                parts.push(json!({"type": "text", "text": text}));
                            }
                        }
                    }
                    Some("input_image") => {
                        let url = item
                            .get("image_url")
                            .and_then(|u| u.as_str())
                            .or_else(|| item.get("url").and_then(|u| u.as_str()));
                        if let Some(url) = url {
                            parts.push(json!({"type": "image_url", "image_url": {"url": url}}));
                        }
                    }
                    Some("refusal") => {
                        if let Some(text) = item.get("refusal").and_then(|t| t.as_str()) {
                            parts.push(json!({"type": "text", "text": text}));
                        }
                    }
                    _ => {}
                }
            }
            if parts.len() == 1 && parts[0].get("type").and_then(|t| t.as_str()) == Some("text") {
                parts[0]["text"].clone()
            } else {
                json!(parts)
            }
        }
        _ => json!(""),
    }
}

fn push_chat_tool_call(messages: &mut Vec<Value>, tc: Value) {
    if let Some(last) = messages.last_mut() {
        if last.get("role").and_then(|r| r.as_str()) == Some("assistant") {
            if let Some(obj) = last.as_object_mut() {
                if obj.contains_key("tool_calls") {
                    if let Some(Value::Array(calls)) = obj.get_mut("tool_calls") {
                        calls.push(tc);
                        return;
                    }
                } else {
                    obj.insert("tool_calls".into(), json!([tc]));
                    return;
                }
            }
        }
    }
    messages.push(json!({"role": "assistant", "content": Value::Null, "tool_calls": [tc]}));
}

fn tool_output_text(output: &Value) -> String {
    match output {
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| item.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn responses_to_chat_request(req: &Value, upstream_model: &str) -> Value {
    let mut messages: Vec<Value> = Vec::new();

    let instructions = req
        .get("instructions")
        .map(responses_text_of)
        .unwrap_or_default();
    if !instructions.trim().is_empty() {
        messages.push(json!({"role": "system", "content": instructions}));
    }

    for item in responses_input_items(req) {
        match item
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("message")
        {
            "message" | "" => {
                let role = match item.get("role").and_then(|r| r.as_str()).unwrap_or("user") {
                    "system" | "developer" => "system",
                    "assistant" => "assistant",
                    _ => "user",
                };
                let content = responses_content_to_chat(&item["content"]);
                messages.push(json!({"role": role, "content": content}));
            }
            "function_call" => {
                let call_id = item
                    .get("call_id")
                    .and_then(|x| x.as_str())
                    .or_else(|| item.get("id").and_then(|x| x.as_str()))
                    .unwrap_or("call_0");
                let tc = json!({
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": item.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                        "arguments": item.get("arguments").and_then(|x| x.as_str()).unwrap_or("{}")
                    }
                });
                push_chat_tool_call(&mut messages, tc);
            }
            "function_call_output" => {
                let call_id = item.get("call_id").and_then(|x| x.as_str()).unwrap_or("");
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": tool_output_text(&item["output"])
                }));
            }
            _ => {}
        }
    }

    let mut out = Map::new();
    out.insert("model".into(), json!(upstream_model));

    // Chat APIs expect `content: null` (not `[]`) when an assistant turn only
    // contains tool calls.
    for msg in &mut messages {
        if msg.get("role").and_then(|r| r.as_str()) == Some("assistant") {
            let empty = msg
                .get("content")
                .and_then(|c| c.as_array())
                .map(|a| a.is_empty())
                .unwrap_or(false);
            if empty {
                msg["content"] = if msg.get("tool_calls").is_some() {
                    Value::Null
                } else {
                    json!("")
                };
            }
        }
    }

    out.insert("messages".into(), json!(messages));

    if let Some(max) = req.get("max_output_tokens").and_then(|v| v.as_u64()) {
        out.insert("max_tokens".into(), json!(max));
    }
    for key in ["temperature", "top_p"] {
        if let Some(v) = req.get(key) {
            if !v.is_null() {
                out.insert(key.into(), v.clone());
            }
        }
    }
    if let Some(effort) = req
        .get("reasoning")
        .and_then(|r| r.get("effort"))
        .and_then(|e| e.as_str())
    {
        out.insert("reasoning_effort".into(), json!(effort));
    }
    if let Some(parallel) = req.get("parallel_tool_calls").and_then(|p| p.as_bool()) {
        out.insert("parallel_tool_calls".into(), json!(parallel));
    }

    if let Some(tools) = req.get("tools").and_then(|t| t.as_array()) {
        let converted: Vec<Value> = tools
            .iter()
            .filter(|t| t.get("type").and_then(|x| x.as_str()) == Some("function"))
            .map(|t| {
                let mut function = Map::new();
                function.insert(
                    "name".into(),
                    t.get("name").cloned().unwrap_or_else(|| json!("tool")),
                );
                if let Some(desc) = t.get("description") {
                    if !desc.is_null() {
                        function.insert("description".into(), desc.clone());
                    }
                }
                function.insert(
                    "parameters".into(),
                    t.get("parameters")
                        .cloned()
                        .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
                );
                if let Some(strict) = t.get("strict").and_then(|s| s.as_bool()) {
                    function.insert("strict".into(), json!(strict));
                }
                json!({"type": "function", "function": Value::Object(function)})
            })
            .collect();
        if !converted.is_empty() {
            out.insert("tools".into(), json!(converted));
            match req.get("tool_choice") {
                Some(Value::String(s)) => {
                    out.insert("tool_choice".into(), json!(s));
                }
                Some(v) if v.get("type").and_then(|t| t.as_str()) == Some("function") => {
                    if let Some(name) = v.get("name").and_then(|n| n.as_str()) {
                        out.insert(
                            "tool_choice".into(),
                            json!({"type": "function", "function": {"name": name}}),
                        );
                    }
                }
                _ => {}
            }
        }
    }

    if let Some(format) = req
        .get("text")
        .and_then(|t| t.get("format"))
        .and_then(|f| f.as_object())
    {
        match format.get("type").and_then(|t| t.as_str()) {
            Some("json_object") => {
                out.insert("response_format".into(), json!({"type": "json_object"}));
            }
            Some("json_schema") => {
                out.insert(
                    "response_format".into(),
                    json!({
                        "type": "json_schema",
                        "json_schema": {
                            "name": format.get("name").and_then(|n| n.as_str()).unwrap_or("output"),
                            "schema": format.get("schema").cloned().unwrap_or_else(|| json!({"type": "object"})),
                            "strict": format.get("strict").and_then(|s| s.as_bool()).unwrap_or(false)
                        }
                    }),
                );
            }
            _ => {}
        }
    }

    if req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false) {
        out.insert("stream".into(), json!(true));
    }
    if let Some(key) = req.get("prompt_cache_key").and_then(|k| k.as_str()) {
        out.insert("prompt_cache_key".into(), json!(key));
    } else if let Some(user) = req.get("user").and_then(|u| u.as_str()) {
        out.insert("user".into(), json!(user));
    }
    if let Some(tier) = req.get("service_tier") {
        if !tier.is_null() {
            out.insert("service_tier".into(), tier.clone());
        }
    }
    if let Some(metadata) = req.get("metadata") {
        if !metadata.is_null() {
            out.insert("metadata".into(), metadata.clone());
        }
    }

    Value::Object(out)
}

fn chat_content_to_responses(content: &Value) -> Value {
    match content {
        Value::String(s) => json!(s),
        Value::Array(parts) => {
            let mut out: Vec<Value> = Vec::new();
            for part in parts {
                match part.get("type").and_then(|t| t.as_str()) {
                    Some("text") => {
                        if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                            if !text.is_empty() {
                                out.push(json!({"type": "input_text", "text": text}));
                            }
                        }
                    }
                    Some("image_url") => {
                        let url = part.get("image_url").and_then(|i| {
                            i.get("url").and_then(|u| u.as_str()).or_else(|| i.as_str())
                        });
                        if let Some(url) = url {
                            out.push(json!({
                                "type": "input_image",
                                "image_url": url,
                                "detail": "auto"
                            }));
                        }
                    }
                    _ => {}
                }
            }
            if out.len() == 1 && out[0].get("type").and_then(|t| t.as_str()) == Some("input_text") {
                out[0]["text"].clone()
            } else {
                json!(out)
            }
        }
        _ => json!(""),
    }
}

pub fn chat_to_responses_request(req: &Value, upstream_model: &str) -> Value {
    let mut instructions = String::new();
    let mut input: Vec<Value> = Vec::new();

    if let Some(messages) = req.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            match msg.get("role").and_then(|r| r.as_str()).unwrap_or("user") {
                "system" | "developer" => {
                    let text = text_of(&msg["content"]);
                    if !text.is_empty() {
                        if !instructions.is_empty() {
                            instructions.push_str("\n\n");
                        }
                        instructions.push_str(&text);
                    }
                }
                "user" => {
                    input.push(json!({
                        "role": "user",
                        "content": chat_content_to_responses(&msg["content"])
                    }));
                }
                "assistant" => {
                    let text = text_of(&msg["content"]);
                    if !text.is_empty() {
                        input.push(json!({
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": text}]
                        }));
                    }
                    if let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                        for tc in tool_calls {
                            let args = tc["function"]["arguments"].as_str().unwrap_or("{}");
                            input.push(json!({
                                "type": "function_call",
                                "call_id": tc.get("id").and_then(|x| x.as_str()).unwrap_or("call_0"),
                                "name": tc["function"]["name"].as_str().unwrap_or("tool"),
                                "arguments": args
                            }));
                        }
                    }
                }
                "tool" => {
                    input.push(json!({
                        "type": "function_call_output",
                        "call_id": msg.get("tool_call_id").and_then(|x| x.as_str()).unwrap_or(""),
                        "output": text_of(&msg["content"])
                    }));
                }
                _ => {}
            }
        }
    }

    if input.is_empty() {
        input.push(json!({"role": "user", "content": ""}));
    }

    let mut out = Map::new();
    out.insert("model".into(), json!(upstream_model));
    out.insert("input".into(), json!(input));
    if !instructions.is_empty() {
        out.insert("instructions".into(), json!(instructions));
    }

    let max = req
        .get("max_completion_tokens")
        .or_else(|| req.get("max_tokens"))
        .and_then(|v| v.as_u64());
    if let Some(max) = max {
        out.insert("max_output_tokens".into(), json!(max));
    }
    for key in ["temperature", "top_p"] {
        if let Some(v) = req.get(key) {
            if !v.is_null() {
                out.insert(key.into(), v.clone());
            }
        }
    }
    if let Some(effort) = req.get("reasoning_effort").and_then(|e| e.as_str()) {
        out.insert("reasoning".into(), json!({"effort": effort}));
    }
    if let Some(parallel) = req.get("parallel_tool_calls").and_then(|p| p.as_bool()) {
        out.insert("parallel_tool_calls".into(), json!(parallel));
    }

    if let Some(tools) = req.get("tools").and_then(|t| t.as_array()) {
        let converted: Vec<Value> = tools
            .iter()
            .filter(|t| t.get("type").and_then(|x| x.as_str()) == Some("function"))
            .map(|t| {
                let f = &t["function"];
                json!({
                    "type": "function",
                    "name": f.get("name").and_then(|x| x.as_str()).unwrap_or("tool"),
                    "description": f.get("description").cloned().unwrap_or(Value::Null),
                    "parameters": f.get("parameters").cloned()
                        .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
                    "strict": f.get("strict").and_then(|s| s.as_bool()).unwrap_or(false)
                })
            })
            .collect();
        if !converted.is_empty() {
            out.insert("tools".into(), json!(converted));
            match req.get("tool_choice") {
                Some(Value::String(s)) => {
                    out.insert("tool_choice".into(), json!(s));
                }
                Some(v) if v.get("type").and_then(|t| t.as_str()) == Some("function") => {
                    if let Some(name) = v["function"].get("name").and_then(|n| n.as_str()) {
                        out.insert(
                            "tool_choice".into(),
                            json!({"type": "function", "name": name}),
                        );
                    }
                }
                _ => {}
            }
        }
    }

    if let Some(rf) = req.get("response_format") {
        let format = match rf.get("type").and_then(|t| t.as_str()) {
            Some("json_object") => Some(json!({"type": "json_object"})),
            Some("json_schema") => {
                let js = &rf["json_schema"];
                Some(json!({
                    "type": "json_schema",
                    "name": js.get("name").and_then(|n| n.as_str()).unwrap_or("output"),
                    "schema": js.get("schema").cloned().unwrap_or_else(|| json!({"type": "object"})),
                    "strict": js.get("strict").and_then(|s| s.as_bool()).unwrap_or(false)
                }))
            }
            _ => None,
        };
        if let Some(format) = format {
            out.insert("text".into(), json!({"format": format}));
        }
    }

    if req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false) {
        out.insert("stream".into(), json!(true));
    }
    if let Some(user) = req.get("user").and_then(|u| u.as_str()) {
        out.insert("user".into(), json!(user));
    }
    if let Some(metadata) = req.get("metadata") {
        if !metadata.is_null() {
            out.insert("metadata".into(), metadata.clone());
        }
    }
    // Chat clients are stateless; never ask the upstream to persist.
    out.insert("store".into(), json!(false));

    Value::Object(out)
}

pub struct ResponseEnvelope {
    pub id: String,
    pub model: String,
    pub created_at: i64,
    pub status: String,
    pub output: Vec<Value>,
    pub usage: Option<(u64, u64, u64, u64)>,
    pub incomplete_reason: Option<String>,
    pub instructions: Option<Value>,
    pub tools: Vec<Value>,
    pub tool_choice: Value,
    pub parallel_tool_calls: bool,
    pub previous_response_id: Option<String>,
    pub metadata: Option<Value>,
}

impl ResponseEnvelope {
    pub fn to_json(&self) -> Value {
        let output_text: String = self
            .output
            .iter()
            .filter(|item| item.get("type").and_then(|t| t.as_str()) == Some("message"))
            .flat_map(|item| {
                item.get("content")
                    .and_then(|c| c.as_array())
                    .cloned()
                    .unwrap_or_default()
            })
            .filter(|part| part.get("type").and_then(|t| t.as_str()) == Some("output_text"))
            .filter_map(|part| {
                part.get("text")
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        let usage = self.usage.map(|(input, output, cached, cache_write)| {
            json!({
                "input_tokens": input,
                "input_tokens_details": {
                    "cached_tokens": cached,
                    "cache_write_tokens": cache_write
                },
                "output_tokens": output,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": input + output
            })
        });

        json!({
            "id": self.id,
            "object": "response",
            "created_at": self.created_at,
            "status": self.status,
            "model": self.model,
            "output": self.output,
            "output_text": output_text,
            "error": Value::Null,
            "incomplete_details": self.incomplete_reason.as_ref()
                .map(|reason| json!({"reason": reason}))
                .unwrap_or(Value::Null),
            "instructions": self.instructions.clone().unwrap_or(Value::Null),
            "metadata": self.metadata.clone().unwrap_or(Value::Null),
            "max_output_tokens": Value::Null,
            "parallel_tool_calls": self.parallel_tool_calls,
            "previous_response_id": self.previous_response_id,
            "reasoning": Value::Null,
            "store": false,
            "temperature": Value::Null,
            "text": {"format": {"type": "text"}},
            "tool_choice": self.tool_choice,
            "tools": self.tools,
            "top_p": Value::Null,
            "truncation": "disabled",
            "usage": usage,
            "user": Value::Null
        })
    }
}

pub fn chat_to_responses_response(resp: &Value, alias: &str) -> Value {
    let choice = &resp["choices"][0];
    let message = &choice["message"];
    let text = text_of(&message["content"]);
    let reasoning = message
        .get("reasoning_content")
        .or_else(|| message.get("reasoning"))
        .and_then(|r| r.as_str())
        .unwrap_or("");

    let mut output: Vec<Value> = Vec::new();
    if !reasoning.is_empty() {
        output.push(json!({
            "type": "reasoning",
            "id": format!("rs_{}", uuid::Uuid::new_v4().simple()),
            "summary": [{"type": "summary_text", "text": reasoning}]
        }));
    }
    let refusal = message
        .get("refusal")
        .and_then(|r| r.as_str())
        .unwrap_or("");
    if !text.is_empty() || !refusal.is_empty() {
        let mut content = Vec::new();
        if !text.is_empty() {
            content.push(json!({
                "type": "output_text",
                "text": text,
                "annotations": []
            }));
        }
        if !refusal.is_empty() {
            content.push(json!({"type": "refusal", "refusal": refusal}));
        }
        output.push(json!({
            "type": "message",
            "id": format!("msg_{}", uuid::Uuid::new_v4().simple()),
            "status": "completed",
            "role": "assistant",
            "content": content
        }));
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(|t| t.as_array()) {
        for tc in tool_calls {
            let call_id = tc
                .get("id")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4().simple()));
            output.push(json!({
                "type": "function_call",
                "id": format!("fc_{}", uuid::Uuid::new_v4().simple()),
                "call_id": call_id,
                "name": tc["function"]["name"].as_str().unwrap_or("tool"),
                "arguments": tc["function"]["arguments"].as_str().unwrap_or("{}"),
                "status": "completed"
            }));
        }
    }
    if output.is_empty() {
        output.push(json!({
            "type": "message",
            "id": format!("msg_{}", uuid::Uuid::new_v4().simple()),
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "", "annotations": []}]
        }));
    }

    let (prompt, completion) = openai_usage(resp);
    let cached = resp["usage"]["prompt_tokens_details"]["cached_tokens"]
        .as_u64()
        .unwrap_or(0);
    let (status, incomplete_reason) = match choice.get("finish_reason").and_then(|r| r.as_str()) {
        Some("length") => ("incomplete", Some("max_output_tokens".to_string())),
        _ => ("completed", None),
    };

    ResponseEnvelope {
        id: format!("resp_{}", uuid::Uuid::new_v4().simple()),
        model: alias.to_string(),
        created_at: now_secs(),
        status: status.to_string(),
        output,
        usage: Some((prompt, completion, cached, 0)),
        incomplete_reason,
        instructions: None,
        tools: Vec::new(),
        tool_choice: json!("auto"),
        parallel_tool_calls: true,
        previous_response_id: None,
        metadata: None,
    }
    .to_json()
}

pub fn responses_to_chat_response(resp: &Value, alias: &str) -> Value {
    let text = responses_output_text(resp);
    let refusal = responses_refusal_text(resp);
    let tool_calls = responses_tool_calls(resp);
    let reasoning = responses_reasoning_text(resp);
    let (prompt, completion, cached) = responses_usage(resp);

    let mut message = json!({
        "role": "assistant",
        "content": if text.is_empty() && !tool_calls.is_empty() { Value::Null } else { json!(text) }
    });
    if !tool_calls.is_empty() {
        message["tool_calls"] = json!(tool_calls);
    }
    if !reasoning.is_empty() {
        message["reasoning_content"] = json!(reasoning);
    }
    if !refusal.is_empty() {
        message["refusal"] = json!(refusal);
    }

    // An incomplete response wins over tool_calls: partial arguments must not
    // look like a normal completed tool call.
    let finish_reason = if resp.get("status").and_then(|s| s.as_str()) == Some("incomplete") {
        "length"
    } else if !tool_calls.is_empty() {
        "tool_calls"
    } else {
        "stop"
    };

    let id = resp
        .get("id")
        .and_then(|x| x.as_str())
        .unwrap_or("resp_unknown")
        .trim_start_matches("resp_");

    json!({
        "id": format!("chatcmpl-{id}"),
        "object": "chat.completion",
        "created": now_secs(),
        "model": alias,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
            "logprobs": Value::Null
        }],
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": prompt + completion,
            "prompt_tokens_details": {"cached_tokens": cached}
        }
    })
}

pub fn responses_to_anthropic_request(req: &Value, upstream_model: &str) -> Value {
    let chat = responses_to_chat_request(req, upstream_model);
    openai_to_anthropic_request(&chat, upstream_model)
}

pub fn anthropic_to_responses_request(req: &Value, upstream_model: &str) -> Value {
    let chat = anthropic_to_openai_request(req, upstream_model);
    chat_to_responses_request(&chat, upstream_model)
}

pub fn anthropic_to_responses_response(resp: &Value, alias: &str) -> Value {
    let chat = anthropic_to_openai_response(resp, alias);
    let mut response = chat_to_responses_response(&chat, alias);
    response["usage"]["input_tokens_details"]["cache_write_tokens"] = json!(resp["usage"]
        ["cache_creation_input_tokens"]
        .as_u64()
        .unwrap_or(0));
    response
}

pub fn responses_to_anthropic_response(resp: &Value, alias: &str) -> Value {
    let chat = responses_to_chat_response(resp, alias);
    openai_to_anthropic_response(&chat, alias)
}

pub fn responses_estimate_tokens(req: &Value) -> u64 {
    let mut total = req
        .get("instructions")
        .map(estimate_value_tokens)
        .unwrap_or(0);
    total += req.get("input").map(estimate_value_tokens).unwrap_or(0);
    total += req.get("tools").map(estimate_value_tokens).unwrap_or(0);
    total.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_to_responses_preserves_cache_write_usage() {
        let response = anthropic_to_responses_response(
            &json!({
                "content": [{"type":"text","text":"hello"}],
                "usage": {"input_tokens":100,"cache_read_input_tokens":30,"cache_creation_input_tokens":40,"output_tokens":20}
            }),
            "m",
        );
        assert_eq!(response["usage"]["input_tokens"], 170);
        assert_eq!(
            response["usage"]["input_tokens_details"]["cached_tokens"],
            30
        );
        assert_eq!(
            response["usage"]["input_tokens_details"]["cache_write_tokens"],
            40
        );
        assert_eq!(response["usage"]["total_tokens"], 190);
    }

    #[test]
    fn tool_result_images_are_forwarded_as_user_content() {
        let request = json!({"messages":[
            {"role":"assistant","content":[{"type":"tool_use","id":"c1","name":"screenshot","input":{}}]},
            {"role":"user","content":[{"type":"tool_result","tool_use_id":"c1","content":[
                {"type":"text","text":"captured"},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"YWJj"}}
            ]}]}
        ]});
        let out = anthropic_to_openai_request(&request, "m");
        assert_eq!(out["messages"][1]["content"], "captured");
        assert_eq!(
            out["messages"][2]["content"][0]["image_url"]["url"],
            "data:image/png;base64,YWJj"
        );
    }

    fn refusal_response() -> Value {
        json!({
            "id": "resp_refusal",
            "object": "response",
            "status": "completed",
            "output": [{
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "content": [{"type": "refusal", "refusal": "I cannot help with that."}]
            }],
            "usage": {"input_tokens": 5, "output_tokens": 6}
        })
    }

    #[test]
    fn responses_refusal_maps_to_chat_and_anthropic() {
        let chat = responses_to_chat_response(&refusal_response(), "m");
        assert_eq!(
            chat["choices"][0]["message"]["refusal"],
            "I cannot help with that."
        );
        assert_eq!(chat["choices"][0]["message"]["content"], "");

        let anth = responses_to_anthropic_response(&refusal_response(), "m");
        assert_eq!(anth["content"][0]["type"], "text");
        assert_eq!(anth["content"][0]["text"], "I cannot help with that.");
    }

    #[test]
    fn chat_refusal_maps_to_responses_block() {
        let chat = json!({
            "id": "c1",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "", "refusal": "I cannot help."},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2}
        });
        let resp = chat_to_responses_response(&chat, "m");
        let output = resp["output"].as_array().unwrap();
        assert!(
            output.iter().any(|item| {
                item["type"] == "message"
                    && item["content"][0]["type"] == "refusal"
                    && item["content"][0]["refusal"] == "I cannot help."
            }),
            "{output:?}"
        );
    }

    #[test]
    fn chat_reasoning_maps_to_responses_item() {
        let chat = json!({
            "id": "c1",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "answer", "reasoning_content": "plan"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2}
        });
        let resp = chat_to_responses_response(&chat, "m");
        let output = resp["output"].as_array().unwrap();
        assert_eq!(output[0]["type"], "reasoning");
        assert_eq!(output[0]["summary"][0]["text"], "plan");
        assert_eq!(output[1]["type"], "message");
        assert_eq!(output[1]["content"][0]["text"], "answer");
    }

    #[test]
    fn translates_openai_request_with_tools() {
        let req = json!({
            "model": "claude",
            "messages": [
                {"role": "system", "content": "be nice"},
                {"role": "user", "content": "weather?"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "call_1", "type": "function",
                     "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}}
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "sunny"}
            ],
            "tools": [{"type": "function", "function": {"name": "get_weather", "parameters": {"type": "object"}}}],
            "max_tokens": 100
        });
        let out = openai_to_anthropic_request(&req, "claude-3");
        assert_eq!(out["model"], "claude-3");
        assert_eq!(out["system"], "be nice");
        assert_eq!(out["max_tokens"], 100);
        assert_eq!(out["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(out["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(out["tools"][0]["name"], "get_weather");
    }

    #[test]
    fn chat_reasoning_maps_to_anthropic_thinking() {
        let chat = json!({
            "id": "c1",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "answer",
                    "reasoning_content": "plan"
                },
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2}
        });
        let anth = openai_to_anthropic_response(&chat, "m");
        assert_eq!(anth["content"][0]["type"], "thinking");
        assert_eq!(anth["content"][0]["thinking"], "plan");
        assert_eq!(anth["content"][0]["signature"], "");
        assert_eq!(anth["content"][1]["type"], "text");
        assert_eq!(anth["content"][1]["text"], "answer");
    }

    #[test]
    fn responses_reasoning_maps_to_anthropic_thinking() {
        let responses = json!({
            "id": "resp_1",
            "status": "completed",
            "output": [
                {"type": "reasoning", "id": "rs_1", "summary": [{"type": "summary_text", "text": "plan"}]},
                {"type": "message", "id": "msg_1", "role": "assistant", "content": [{"type": "output_text", "text": "answer"}]}
            ],
            "usage": {"input_tokens": 10, "output_tokens": 2}
        });
        let anth = responses_to_anthropic_response(&responses, "m");
        assert_eq!(anth["content"][0]["type"], "thinking");
        assert_eq!(anth["content"][0]["thinking"], "plan");
        assert_eq!(anth["content"][1]["text"], "answer");
    }

    #[test]
    fn chat_to_anthropic_splits_cached_input() {
        let chat = json!({
            "id": "c1",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hi"},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 1000,
                "completion_tokens": 50,
                "prompt_tokens_details": {"cached_tokens": 800}
            }
        });
        let anth = openai_to_anthropic_response(&chat, "m");
        assert_eq!(anth["usage"]["input_tokens"], 200);
        assert_eq!(anth["usage"]["cache_read_input_tokens"], 800);
        assert_eq!(anth["usage"]["output_tokens"], 50);

        let chat = json!({
            "id": "c1",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 1, "prompt_tokens_details": {"cached_tokens": 20}}
        });
        let anth = openai_to_anthropic_response(&chat, "m");
        assert_eq!(anth["usage"]["input_tokens"], 0);
        assert_eq!(anth["usage"]["cache_read_input_tokens"], 10);
    }

    #[test]
    fn responses_to_anthropic_splits_cached_input() {
        let responses = json!({
            "id": "resp_1",
            "status": "completed",
            "output": [{
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "hi"}]
            }],
            "usage": {
                "input_tokens": 1000,
                "output_tokens": 50,
                "input_tokens_details": {"cached_tokens": 800}
            }
        });
        let anth = responses_to_anthropic_response(&responses, "m");
        assert_eq!(anth["usage"]["input_tokens"], 200);
        assert_eq!(anth["usage"]["cache_read_input_tokens"], 800);
    }

    #[test]
    fn parallel_tool_calls_disable_maps_both_ways() {
        fn tools() -> Value {
            json!([{"type": "function", "function": {"name": "f", "parameters": {"type": "object"}}}])
        }

        let chat = json!({
            "messages": [{"role": "user", "content": "x"}],
            "tools": tools(),
            "parallel_tool_calls": false
        });
        let anth = openai_to_anthropic_request(&chat, "m");
        assert_eq!(anth["tool_choice"]["type"], "auto");
        assert_eq!(anth["tool_choice"]["disable_parallel_tool_use"], true);

        let chat = json!({
            "messages": [{"role": "user", "content": "x"}],
            "tools": tools(),
            "parallel_tool_calls": false,
            "tool_choice": {"type": "function", "function": {"name": "f"}}
        });
        let anth = openai_to_anthropic_request(&chat, "m");
        assert_eq!(anth["tool_choice"]["type"], "tool");
        assert_eq!(anth["tool_choice"]["disable_parallel_tool_use"], true);

        let anthropic = json!({
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "x"}],
            "tools": [{"name": "f", "description": "", "input_schema": {"type": "object"}}],
            "tool_choice": {"type": "auto", "disable_parallel_tool_use": true}
        });
        let chat = anthropic_to_openai_request(&anthropic, "m");
        assert_eq!(chat["parallel_tool_calls"], false);

        let responses = json!({
            "input": "x",
            "tools": [{"type": "function", "name": "f", "parameters": {"type": "object"}}],
            "parallel_tool_calls": false
        });
        let anth = responses_to_anthropic_request(&responses, "m");
        assert_eq!(anth["tool_choice"]["disable_parallel_tool_use"], true);
        let back = anthropic_to_responses_request(&anth, "m");
        assert_eq!(back["parallel_tool_calls"], false);
    }

    #[test]
    fn incomplete_tool_call_maps_to_length() {
        let resp = json!({
            "id": "resp_1",
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [{
                "type": "function_call",
                "id": "fc_1",
                "call_id": "call_1",
                "name": "f",
                "arguments": "{\"x\":"
            }],
            "usage": {"input_tokens": 1, "output_tokens": 2}
        });
        let chat = responses_to_chat_response(&resp, "m");
        assert_eq!(chat["choices"][0]["finish_reason"], "length");
        let anth = responses_to_anthropic_response(&resp, "m");
        assert_eq!(anth["stop_reason"], "max_tokens");
    }

    #[test]
    fn chat_to_anthropic_does_not_inject_max_tokens() {
        let req = json!({"messages": [{"role": "user", "content": "hi"}]});
        let out = openai_to_anthropic_request(&req, "m");
        assert!(out.get("max_tokens").is_none());

        let req = json!({"messages": [{"role": "user", "content": "hi"}], "max_tokens": 77});
        let out = openai_to_anthropic_request(&req, "m");
        assert_eq!(out["max_tokens"], 77);
    }

    #[test]
    fn translates_anthropic_request() {
        let req = json!({
            "model": "gpt",
            "system": [{"type": "text", "text": "sys"}],
            "max_tokens": 50,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": "hi"}]},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "hello"},
                    {"type": "tool_use", "id": "t1", "name": "f", "input": {"a": 1}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": "result"}
                ]}
            ]
        });
        let out = anthropic_to_openai_request(&req, "gpt-4o");
        assert_eq!(out["messages"][0]["role"], "system");
        assert_eq!(out["messages"][2]["tool_calls"][0]["function"]["name"], "f");
        assert_eq!(out["messages"][3]["role"], "tool");
        assert_eq!(out["max_tokens"], 50);
    }

    #[test]
    fn response_roundtrip_shapes() {
        let anth = json!({
            "id": "msg_1", "type": "message", "role": "assistant",
            "content": [{"type": "text", "text": "hi"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 3, "output_tokens": 2}
        });
        let oai = anthropic_to_openai_response(&anth, "alias");
        assert_eq!(oai["choices"][0]["message"]["content"], "hi");
        assert_eq!(oai["usage"]["total_tokens"], 5);

        let back = openai_to_anthropic_response(&oai, "alias");
        assert_eq!(back["content"][0]["text"], "hi");
        assert_eq!(back["usage"]["input_tokens"], 3);
    }

    #[test]
    fn translates_responses_request() {
        let req = json!({
            "model": "resp-model",
            "instructions": "be brief",
            "input": [
                {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                {"type": "function_call", "call_id": "call_1", "name": "f", "arguments": "{\"a\":1}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "ok"}
            ],
            "max_output_tokens": 64,
            "tools": [{"type": "function", "name": "f", "description": "d",
                       "parameters": {"type": "object"}, "strict": false}],
            "text": {"format": {"type": "json_object"}}
        });
        let out = responses_to_chat_request(&req, "gpt-4o");
        assert_eq!(out["messages"][0]["role"], "system");
        assert_eq!(out["messages"][1]["content"], "hi");
        assert_eq!(out["messages"][2]["role"], "assistant");
        assert_eq!(out["messages"][2]["tool_calls"][0]["id"], "call_1");
        assert_eq!(out["messages"][3]["role"], "tool");
        assert_eq!(out["max_tokens"], 64);
        assert_eq!(out["tools"][0]["function"]["name"], "f");
        assert_eq!(out["response_format"]["type"], "json_object");
    }

    #[test]
    fn translates_chat_to_responses_request_and_response() {
        let chat = json!({
            "model": "gpt",
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "call_1", "type": "function",
                     "function": {"name": "f", "arguments": "{\"a\":1}"}}
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "ok"}
            ],
            "max_tokens": 32
        });
        let req = chat_to_responses_request(&chat, "gpt-5");
        assert_eq!(req["instructions"], "sys");
        assert_eq!(req["input"][1]["type"], "function_call");
        assert_eq!(req["input"][2]["type"], "function_call_output");
        assert_eq!(req["max_output_tokens"], 32);

        let chat_resp = json!({
            "id": "chatcmpl-1",
            "choices": [{
                "message": {"role": "assistant", "content": "hello",
                            "tool_calls": [{"id": "call_2", "type": "function",
                                            "function": {"name": "f", "arguments": "{\"b\":2}"}}]},
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 4,
                      "prompt_tokens_details": {"cached_tokens": 1}}
        });
        let resp = chat_to_responses_response(&chat_resp, "alias");
        assert_eq!(resp["object"], "response");
        assert_eq!(resp["status"], "completed");
        assert_eq!(resp["output"][0]["type"], "message");
        assert_eq!(resp["output"][1]["type"], "function_call");
        assert_eq!(resp["usage"]["total_tokens"], 9);
        assert_eq!(resp["usage"]["input_tokens_details"]["cached_tokens"], 1);

        let back = responses_to_chat_response(&resp, "alias");
        assert_eq!(back["choices"][0]["message"]["content"], "hello");
        assert_eq!(
            back["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "f"
        );
        assert_eq!(back["choices"][0]["finish_reason"], "tool_calls");
    }
}
