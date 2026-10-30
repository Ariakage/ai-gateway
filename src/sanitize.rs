//! Silent cleanup of upstream model artifacts.
//!
//! Some upstreams occasionally inject internal markers into otherwise normal
//! output, notably `##ERROR##` and `<ds_safety>…</ds_safety>` blocks emitted by
//! DeepSeek behind OpenCode Go. Those markers are not part of the answer the
//! client asked for, so the gateway removes them from both buffered and
//! streaming responses.
//!
//! Streaming cleanup has to be stateful: a marker (or a safety block) can be
//! split across two SSE chunks, so the cleaner holds back a possible partial
//! marker until the next chunk arrives.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::body::Bytes;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use serde_json::Value;

use crate::config::ApiFormat;
use crate::util::{SseEvent, SseParser};

const ERROR_MARKER: &str = "##ERROR##";
const SAFETY_OPEN: &str = "<ds_safety";
const SAFETY_CLOSE: &str = "</ds_safety>";
/// A real safety tag is short. If no `>` arrives within this many bytes the
/// tag is treated as a permanent artifact block instead of buffering forever.
const MAX_OPEN_TAG_BYTES: usize = 1024;

/// Remove artifacts from a complete (non-streaming) string.
pub fn clean(text: &str) -> String {
    if !text.contains(ERROR_MARKER) && !text.contains(SAFETY_OPEN) && !text.contains(SAFETY_CLOSE) {
        return text.to_string();
    }

    let mut out = String::with_capacity(text.len());
    let mut rest = text;

    loop {
        let err = rest.find(ERROR_MARKER);
        let open = rest.find(SAFETY_OPEN);

        match (err, open) {
            (None, None) => {
                out.push_str(rest);
                break;
            }
            (Some(e), Some(o)) if e < o => {
                out.push_str(&rest[..e]);
                rest = &rest[e + ERROR_MARKER.len()..];
            }
            (Some(e), None) => {
                out.push_str(&rest[..e]);
                rest = &rest[e + ERROR_MARKER.len()..];
            }
            (_, Some(o)) => {
                out.push_str(&rest[..o]);
                let after = &rest[o..];
                let Some(gt) = after.find('>') else {
                    // An opening tag without `>` means the block was cut off.
                    break;
                };
                let inner = &after[gt + 1..];
                match inner.find(SAFETY_CLOSE) {
                    Some(end) => rest = &inner[end + SAFETY_CLOSE.len()..],
                    None => break,
                }
            }
        }
    }

    // Drop a stray closing tag that lost its opening half.
    if out.contains(SAFETY_CLOSE) {
        out = out.replace(SAFETY_CLOSE, "");
    }
    out
}

/// Stateful cleaner for one text channel in a streaming response.
#[derive(Default)]
pub struct StreamCleaner {
    pending: String,
    in_block: bool,
}

impl StreamCleaner {
    pub fn push(&mut self, chunk: &str) -> String {
        self.pending.push_str(chunk);
        let mut out = String::new();

        loop {
            if self.in_block {
                if let Some(pos) = self.pending.find(SAFETY_CLOSE) {
                    self.pending.drain(..pos + SAFETY_CLOSE.len());
                    self.in_block = false;
                    continue;
                }
                // Keep only a possible partial closing tag.
                let keep = ends_with_prefix(&self.pending, SAFETY_CLOSE);
                let cut = self.pending.len() - keep;
                self.pending.drain(..cut);
                break;
            }

            let err = self.pending.find(ERROR_MARKER);
            let open = self.pending.find(SAFETY_OPEN);

            match (err, open) {
                (Some(e), Some(o)) if e < o => {
                    out.push_str(&self.pending[..e]);
                    self.pending.drain(..e + ERROR_MARKER.len());
                }
                (Some(e), None) => {
                    out.push_str(&self.pending[..e]);
                    self.pending.drain(..e + ERROR_MARKER.len());
                }
                (_, Some(o)) => {
                    out.push_str(&self.pending[..o]);
                    match self.pending[o..].find('>') {
                        Some(gt) => {
                            self.pending.drain(..o + gt + 1);
                            self.in_block = true;
                        }
                        None => {
                            // Hold the incomplete opening tag until more data,
                            // but never let a malformed tag grow without bound.
                            self.pending.drain(..o);
                            if self.pending.len() > MAX_OPEN_TAG_BYTES {
                                self.pending.clear();
                                self.in_block = true;
                            }
                            break;
                        }
                    }
                }
                (None, None) => {
                    let keep = partial_marker_suffix(&self.pending);
                    let cut = self.pending.len() - keep;
                    out.push_str(&self.pending[..cut]);
                    self.pending.drain(..cut);
                    break;
                }
            }
        }

        out
    }

    /// Flush held text at the end of a channel. An unfinished safety block is
    /// an upstream artifact and is dropped; ordinary partial text is returned.
    pub fn finish(&mut self) -> String {
        if self.in_block {
            self.pending.clear();
            return String::new();
        }
        let pending = std::mem::take(&mut self.pending);
        clean(&pending)
    }
}

/// Longest suffix of `s` that is a proper prefix of `marker`.
fn ends_with_prefix(s: &str, marker: &str) -> usize {
    let s = s.as_bytes();
    let m = marker.as_bytes();
    let max = s.len().min(m.len().saturating_sub(1));
    for len in (1..=max).rev() {
        if s[s.len() - len..] == m[..len] {
            return len;
        }
    }
    0
}

fn partial_marker_suffix(s: &str) -> usize {
    ends_with_prefix(s, ERROR_MARKER).max(ends_with_prefix(s, SAFETY_OPEN))
}

// ---------------------------------------------------------------------------
// Buffered responses
// ---------------------------------------------------------------------------

/// Remove artifacts from a complete response body in the given API shape.
pub fn clean_response(format: ApiFormat, value: &mut Value) {
    match format {
        ApiFormat::Openai => {
            let Some(choices) = value.get_mut("choices").and_then(|v| v.as_array_mut()) else {
                return;
            };
            for choice in choices {
                if let Some(message) = choice.get_mut("message") {
                    clean_openai_message(message);
                }
                if let Some(delta) = choice.get_mut("delta") {
                    clean_openai_message(delta);
                }
            }
        }
        ApiFormat::Anthropic => {
            if let Some(content) = value.get_mut("content").and_then(|v| v.as_array_mut()) {
                for block in content {
                    clean_anthropic_block(block);
                }
            }
        }
        ApiFormat::Responses => clean_responses_response(value),
    }
}

fn clean_openai_message(message: &mut Value) {
    if let Some(content) = message.get_mut("content") {
        clean_openai_content(content);
    }
    for field in ["reasoning_content", "reasoning", "refusal"] {
        if let Some(Value::String(text)) = message.get_mut(field) {
            *text = clean(text);
        }
    }
}

fn clean_openai_content(content: &mut Value) {
    match content {
        Value::String(text) => *text = clean(text),
        Value::Array(parts) => {
            for part in parts {
                if let Some(Value::String(text)) = part.get_mut("text") {
                    *text = clean(text);
                }
            }
        }
        _ => {}
    }
}

fn clean_anthropic_block(block: &mut Value) {
    for field in ["text", "thinking"] {
        if let Some(Value::String(text)) = block.get_mut(field) {
            *text = clean(text);
        }
    }
}

fn clean_responses_response(response: &mut Value) {
    if let Some(Value::String(text)) = response.get_mut("output_text") {
        *text = clean(text);
    }
    if let Some(output) = response.get_mut("output").and_then(|v| v.as_array_mut()) {
        for item in output {
            clean_responses_item(item);
        }
    }
}

fn clean_responses_item(item: &mut Value) {
    for field in ["content", "summary"] {
        if let Some(parts) = item.get_mut(field).and_then(|v| v.as_array_mut()) {
            for part in parts {
                for key in ["text", "refusal"] {
                    if let Some(Value::String(text)) = part.get_mut(key) {
                        *text = clean(text);
                    }
                }
            }
        }
    }
    if let Some(Value::String(text)) = item.get_mut("text") {
        *text = clean(text);
    }
}

/// Remove artifacts from a list of Responses output items.
pub fn clean_responses_items(items: &mut [Value]) {
    for item in items {
        clean_responses_item(item);
    }
}

// ---------------------------------------------------------------------------
// Streaming responses
// ---------------------------------------------------------------------------

/// Wrap a response SSE stream and remove artifacts as events flow through.
pub fn sanitize_stream(
    inner: BoxStream<'static, Result<Bytes, Infallible>>,
    inbound: ApiFormat,
    max_event_bytes: usize,
    delivery_failed: Arc<AtomicBool>,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = async_stream::stream! {
        let mut parser = SseParser::with_limit(max_event_bytes);
        let mut state = StreamSanitizer::new(inbound);
        let mut chunks = inner;
        while let Some(Ok(chunk)) = chunks.next().await {
            for event in parser.push(&chunk) {
                for bytes in state.rewrite(&event) {
                    yield Ok(bytes);
                }
            }
            if parser.overflowed() {
                delivery_failed.store(true, Ordering::Relaxed);
                yield Ok(sanitize_error_event(
                    inbound,
                    "gateway could not sanitize a stream event larger than the configured limit",
                ));
                return;
            }
        }
        for event in parser.finish() {
            for bytes in state.rewrite(&event) {
                yield Ok(bytes);
            }
        }
        if parser.overflowed() {
            delivery_failed.store(true, Ordering::Relaxed);
            yield Ok(sanitize_error_event(
                inbound,
                "gateway could not sanitize a stream event larger than the configured limit",
            ));
            return;
        }
        if !state.error_seen {
            for bytes in state.flush_all() {
                yield Ok(bytes);
            }
        }
    };
    s.boxed()
}

fn sanitize_error_event(format: ApiFormat, message: &str) -> Bytes {
    match format {
        ApiFormat::Openai => Bytes::from(format!(
            "data: {}\n\n",
            serde_json::json!({"error": {"message": message, "type": "gateway_error"}})
        )),
        ApiFormat::Anthropic => Bytes::from(format!(
            "event: error\ndata: {}\n\n",
            serde_json::json!({"type": "error", "error": {"type": "api_error", "message": message}})
        )),
        ApiFormat::Responses => Bytes::from(format!(
            "event: response.failed\ndata: {}\n\n",
            serde_json::json!({
                "type": "response.failed",
                "response": {
                    "id": "resp_gateway_error",
                    "object": "response",
                    "status": "failed",
                    "output": [],
                    "error": {"code": "gateway_error", "message": message}
                }
            })
        )),
    }
}

struct StreamSanitizer {
    format: ApiFormat,
    cleaners: HashMap<String, StreamCleaner>,
    response_items: HashMap<u64, String>,
    error_seen: bool,
    openai_id: Option<String>,
    openai_created: Option<i64>,
    openai_model: Option<String>,
    response_seq: u64,
}

impl StreamSanitizer {
    fn new(format: ApiFormat) -> Self {
        Self {
            format,
            cleaners: HashMap::new(),
            response_items: HashMap::new(),
            error_seen: false,
            openai_id: None,
            openai_created: None,
            openai_model: None,
            response_seq: 0,
        }
    }

    fn rewrite(&mut self, event: &SseEvent) -> Vec<Bytes> {
        let mut out = Vec::new();
        if let Some(index) = anthropic_block_stop(self.format, event) {
            let prefix = format!("anth:{index}:");
            out.extend(self.flush_prefix(&prefix));
        }
        if event.data.trim() == "[DONE]" {
            out.extend(self.flush_all());
            out.push(Bytes::from_static(b"data: [DONE]\n\n"));
            return out;
        }

        let Ok(mut value) = serde_json::from_str::<Value>(&event.data) else {
            out.push(encode_event(event.event.as_deref(), &event.data));
            return out;
        };
        if value.get("error").is_some()
            || matches!(
                value.get("type").and_then(|typ| typ.as_str()),
                Some("error" | "response.failed")
            )
        {
            self.error_seen = true;
        }

        if self.format == ApiFormat::Openai {
            if let Some(id) = value.get("id").and_then(|v| v.as_str()) {
                self.openai_id = Some(id.to_string());
            }
            if let Some(created) = value.get("created").and_then(|v| v.as_i64()) {
                self.openai_created = Some(created);
            }
            if let Some(model) = value.get("model").and_then(|v| v.as_str()) {
                self.openai_model = Some(model.to_string());
            }
        }

        for prefix in responses_flush_prefixes(self.format, &value) {
            out.extend(self.flush_prefix(&prefix));
        }

        match self.format {
            ApiFormat::Openai => self.rewrite_openai(&mut value),
            ApiFormat::Anthropic => self.rewrite_anthropic(&mut value),
            ApiFormat::Responses => self.rewrite_responses(&mut value),
        }

        if self.format == ApiFormat::Openai {
            // A finish_reason only ends one choice; other choices may still be
            // streaming split markers.
            if let Some(choices) = value.get("choices").and_then(|v| v.as_array()) {
                let mut finished = Vec::new();
                for choice in choices {
                    if choice
                        .get("finish_reason")
                        .map(|reason| !reason.is_null())
                        .unwrap_or(false)
                    {
                        let index = choice.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
                        finished.push(index);
                    }
                }
                for index in finished {
                    out.extend(self.flush_prefix(&format!("oai:{index}:")));
                }
            }
        } else if terminal_event(self.format, &value) {
            out.extend(self.flush_all());
        }

        if self.format == ApiFormat::Responses && value.is_object() {
            value["sequence_number"] = serde_json::json!(self.response_seq);
            self.response_seq += 1;
        }

        let data = serde_json::to_string(&value).unwrap_or_else(|_| event.data.clone());
        out.push(encode_event(event.event.as_deref(), &data));
        out
    }

    fn push_text(&mut self, key: String, text: &str) -> String {
        self.cleaners.entry(key).or_default().push(text)
    }

    fn flush_prefix(&mut self, prefix: &str) -> Vec<Bytes> {
        let keys: Vec<String> = self
            .cleaners
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        self.flush_keys(keys)
    }

    fn flush_all(&mut self) -> Vec<Bytes> {
        let keys: Vec<String> = self.cleaners.keys().cloned().collect();
        self.flush_keys(keys)
    }

    fn flush_keys(&mut self, keys: Vec<String>) -> Vec<Bytes> {
        let mut out = Vec::new();
        for key in keys {
            let Some(cleaner) = self.cleaners.get_mut(&key) else {
                continue;
            };
            let tail = cleaner.finish();
            if tail.is_empty() {
                continue;
            }
            if let Some(bytes) = self.flush_event(&key, &tail) {
                out.push(bytes);
            }
        }
        out
    }

    fn flush_event(&mut self, key: &str, text: &str) -> Option<Bytes> {
        let parts: Vec<&str> = key.split(':').collect();
        let index = parts.get(1)?.parse::<u64>().ok()?;
        match self.format {
            ApiFormat::Openai => {
                let channel = parts.get(2).copied().unwrap_or("text");
                let field = if channel == "reason" {
                    "reasoning_content"
                } else {
                    "content"
                };
                let id = self
                    .openai_id
                    .clone()
                    .unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                let created = self.openai_created.unwrap_or_else(crate::util::now_secs);
                let model = self.openai_model.clone().unwrap_or_default();
                let value = serde_json::json!({
                    "id": id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [{
                        "index": index,
                        "delta": {field: text},
                        "finish_reason": null
                    }]
                });
                Some(encode_event(None, &serde_json::to_string(&value).ok()?))
            }
            ApiFormat::Anthropic => {
                let channel = parts.get(2).copied().unwrap_or("text");
                let kind = if channel == "think" {
                    "thinking_delta"
                } else {
                    "text_delta"
                };
                let field = if channel == "think" {
                    "thinking"
                } else {
                    "text"
                };
                let value = serde_json::json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {"type": kind, field: text}
                });
                Some(encode_event(
                    Some("content_block_delta"),
                    &serde_json::to_string(&value).ok()?,
                ))
            }
            ApiFormat::Responses => {
                let content_index = parts.get(2).copied().unwrap_or("0");
                let channel = parts.get(3).copied().unwrap_or("text");
                let item_id = self
                    .response_items
                    .get(&index)
                    .cloned()
                    .unwrap_or_else(|| format!("msg_gateway_flush_{index}"));
                let (typ, index_name) = match channel {
                    "reason_summary" => ("response.reasoning_summary_text.delta", "summary_index"),
                    "reason_text" => ("response.reasoning_text.delta", "content_index"),
                    "refusal" => ("response.refusal.delta", "content_index"),
                    _ => ("response.output_text.delta", "content_index"),
                };
                let value = serde_json::json!({
                    "type": typ,
                    "item_id": item_id,
                    "output_index": index,
                    index_name: content_index.parse::<u64>().unwrap_or(0),
                    "delta": text,
                    "sequence_number": self.response_seq,
                });
                self.response_seq += 1;
                Some(encode_event(
                    Some(typ),
                    &serde_json::to_string(&value).ok()?,
                ))
            }
        }
    }

    fn rewrite_openai(&mut self, value: &mut Value) {
        let Some(choices) = value.get_mut("choices").and_then(|v| v.as_array_mut()) else {
            return;
        };
        for choice in choices {
            let index = choice.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
            let Some(delta) = choice.get_mut("delta").and_then(|v| v.as_object_mut()) else {
                continue;
            };
            if let Some(Value::String(text)) = delta.get_mut("content") {
                let cleaned = self.push_text(format!("oai:{index}:text"), text);
                *text = cleaned;
            }
            for field in ["reasoning_content", "reasoning"] {
                if let Some(Value::String(text)) = delta.get_mut(field) {
                    let cleaned = self.push_text(format!("oai:{index}:reason"), text);
                    *text = cleaned;
                }
            }
        }
    }

    fn rewrite_anthropic(&mut self, value: &mut Value) {
        let event = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        match event.as_str() {
            "content_block_delta" => {
                let index = value.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
                let Some(delta) = value.get_mut("delta").and_then(|v| v.as_object_mut()) else {
                    return;
                };
                let kind = delta
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let (field, channel) = match kind.as_str() {
                    "text_delta" => ("text", "text"),
                    "thinking_delta" => ("thinking", "think"),
                    _ => return,
                };
                if let Some(Value::String(text)) = delta.get_mut(field) {
                    let cleaned = self.push_text(format!("anth:{index}:{channel}"), text);
                    *text = cleaned;
                }
            }
            "content_block_start" => {
                if let Some(block) = value.get_mut("content_block") {
                    clean_anthropic_block(block);
                }
            }
            _ => {}
        }
    }

    fn rewrite_responses(&mut self, value: &mut Value) {
        let event = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if let (Some(index), Some(item_id)) = (
            value.get("output_index").and_then(|v| v.as_u64()),
            value.get("item_id").and_then(|v| v.as_str()),
        ) {
            self.response_items.insert(index, item_id.to_string());
        }
        if let Some(item) = value.get("item") {
            if let (Some(index), Some(item_id)) = (
                value.get("output_index").and_then(|v| v.as_u64()),
                item.get("id").and_then(|v| v.as_str()),
            ) {
                self.response_items.insert(index, item_id.to_string());
            }
        }

        match event.as_str() {
            "response.output_text.delta" => {
                let index = value
                    .get("output_index")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let content_index = value
                    .get("content_index")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                if let Some(Value::String(text)) = value.get_mut("delta") {
                    let cleaned =
                        self.push_text(format!("resp:{index}:{content_index}:text"), text);
                    *text = cleaned;
                }
            }
            "response.refusal.delta" => {
                let index = value
                    .get("output_index")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let content_index = value
                    .get("content_index")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                if let Some(Value::String(text)) = value.get_mut("delta") {
                    let cleaned =
                        self.push_text(format!("resp:{index}:{content_index}:refusal"), text);
                    *text = cleaned;
                }
            }
            "response.reasoning_summary_text.delta" => {
                let index = value
                    .get("output_index")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let summary_index = value
                    .get("summary_index")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                if let Some(Value::String(text)) = value.get_mut("delta") {
                    let cleaned = self
                        .push_text(format!("resp:{index}:{summary_index}:reason_summary"), text);
                    *text = cleaned;
                }
            }
            "response.reasoning_text.delta" => {
                let index = value
                    .get("output_index")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let content_index = value
                    .get("content_index")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                if let Some(Value::String(text)) = value.get_mut("delta") {
                    let cleaned =
                        self.push_text(format!("resp:{index}:{content_index}:reason_text"), text);
                    *text = cleaned;
                }
            }
            "response.output_text.done"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.done" => {
                if let Some(Value::String(text)) = value.get_mut("text") {
                    *text = clean(text);
                }
            }
            "response.refusal.done" => {
                if let Some(Value::String(text)) = value.get_mut("refusal") {
                    *text = clean(text);
                }
            }
            "response.content_part.added"
            | "response.content_part.done"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done" => {
                if let Some(part) = value.get_mut("part") {
                    for key in ["text", "refusal"] {
                        if let Some(Value::String(text)) = part.get_mut(key) {
                            *text = clean(text);
                        }
                    }
                }
            }
            "response.output_item.added" | "response.output_item.done" => {
                if let Some(item) = value.get_mut("item") {
                    clean_responses_item(item);
                }
            }
            "response.completed" | "response.incomplete" => {
                if let Some(response) = value.get_mut("response") {
                    clean_responses_response(response);
                }
            }
            _ => {}
        }
    }
}

fn anthropic_block_stop(format: ApiFormat, event: &SseEvent) -> Option<u64> {
    if format != ApiFormat::Anthropic {
        return None;
    }
    let value = serde_json::from_str::<Value>(&event.data).ok()?;
    if value.get("type").and_then(|v| v.as_str()) != Some("content_block_stop") {
        return None;
    }
    value.get("index").and_then(|v| v.as_u64())
}

fn responses_flush_prefixes(format: ApiFormat, value: &Value) -> Vec<String> {
    if format != ApiFormat::Responses {
        return Vec::new();
    }
    let typ = value
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let output_index = value.get("output_index").and_then(|v| v.as_u64());
    match typ {
        "response.output_text.done"
        | "response.content_part.done"
        | "response.output_item.done" => output_index
            .map(|index| vec![format!("resp:{index}:")])
            .unwrap_or_default(),
        "response.reasoning_summary_text.done" | "response.reasoning_summary_part.done" => {
            match (
                output_index,
                value.get("summary_index").and_then(|v| v.as_u64()),
            ) {
                (Some(index), Some(summary)) => {
                    vec![format!("resp:{index}:{summary}:reason_summary")]
                }
                _ => Vec::new(),
            }
        }
        "response.refusal.done" => {
            match (
                output_index,
                value.get("content_index").and_then(|v| v.as_u64()),
            ) {
                (Some(index), Some(content)) => vec![format!("resp:{index}:{content}:refusal")],
                _ => Vec::new(),
            }
        }
        "response.reasoning_text.done" => {
            match (
                output_index,
                value.get("content_index").and_then(|v| v.as_u64()),
            ) {
                (Some(index), Some(content)) => vec![format!("resp:{index}:{content}:reason_text")],
                _ => Vec::new(),
            }
        }
        _ => Vec::new(),
    }
}

fn terminal_event(format: ApiFormat, value: &Value) -> bool {
    let typ = value
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    match format {
        ApiFormat::Openai => value
            .get("choices")
            .and_then(|v| v.as_array())
            .map(|choices| {
                choices.iter().any(|choice| {
                    choice
                        .get("finish_reason")
                        .map(|reason| !reason.is_null())
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false),
        ApiFormat::Anthropic => {
            typ == "message_stop"
                || (typ == "message_delta"
                    && value
                        .get("delta")
                        .and_then(|delta| delta.get("stop_reason"))
                        .map(|reason| !reason.is_null())
                        .unwrap_or(false))
        }
        ApiFormat::Responses => {
            matches!(
                typ,
                "response.completed" | "response.incomplete" | "response.failed"
            )
        }
    }
}

fn encode_event(name: Option<&str>, data: &str) -> Bytes {
    match name {
        Some(name) => Bytes::from(format!("event: {name}\ndata: {data}\n\n")),
        None => Bytes::from(format!("data: {data}\n\n")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_error_marker() {
        assert_eq!(clean("before ##ERROR## after"), "before  after");
        assert_eq!(clean("plain text"), "plain text");
    }

    #[test]
    fn removes_safety_block() {
        assert_eq!(clean("A<ds_safety>secret</ds_safety>B"), "AB");
        assert_eq!(clean("A<ds_safety mode=\"x\">secret</ds_safety>B"), "AB");
        assert_eq!(clean("A<ds_safety>cut off"), "A");
        assert_eq!(clean("A</ds_safety>B"), "AB");
    }

    #[test]
    fn stream_handles_split_error_marker() {
        let mut cleaner = StreamCleaner::default();
        let mut out = cleaner.push("hello ##ER");
        out.push_str(&cleaner.push("ROR## world"));
        assert_eq!(out, "hello  world");
    }

    #[test]
    fn stream_handles_split_safety_block() {
        let mut cleaner = StreamCleaner::default();
        let mut out = cleaner.push("A<ds_sa");
        out.push_str(&cleaner.push("fety>secret</ds_sa"));
        out.push_str(&cleaner.push("fety>B"));
        assert_eq!(out, "AB");
    }

    #[test]
    fn stream_drops_unclosed_block() {
        let mut cleaner = StreamCleaner::default();
        let out = cleaner.push("A<ds_safety>garbage");
        assert_eq!(out, "A");
    }

    #[test]
    fn stream_keeps_normal_text() {
        let mut cleaner = StreamCleaner::default();
        let mut out = cleaner.push("hello ");
        out.push_str(&cleaner.push("world"));
        assert_eq!(out, "hello world");
    }

    #[test]
    fn finish_preserves_partial_text() {
        let mut cleaner = StreamCleaner::default();
        let mut out = cleaner.push("ends with #");
        out.push_str(&cleaner.finish());
        assert_eq!(out, "ends with #");

        let mut cleaner = StreamCleaner::default();
        let mut out = cleaner.push("ends with <ds_");
        out.push_str(&cleaner.finish());
        assert_eq!(out, "ends with <ds_");
    }

    #[test]
    fn finish_drops_unclosed_safety_block() {
        let mut cleaner = StreamCleaner::default();
        let mut out = cleaner.push("keep <ds_safety>drop");
        out.push_str(&cleaner.finish());
        assert_eq!(out, "keep ");
    }

    #[test]
    fn cleans_openai_response() {
        let mut value = serde_json::json!({
            "choices": [{
                "message": {
                    "content": "hello ##ERROR## world",
                    "reasoning_content": "thinking <ds_safety>x</ds_safety>done"
                }
            }]
        });
        clean_response(ApiFormat::Openai, &mut value);
        assert_eq!(value["choices"][0]["message"]["content"], "hello  world");
        assert_eq!(
            value["choices"][0]["message"]["reasoning_content"],
            "thinking done"
        );
    }

    #[test]
    fn cleans_responses_item_lists() {
        let mut items = vec![serde_json::json!({
            "type": "message",
            "content": [{"type": "output_text", "text": "A<ds_safety>x</ds_safety>B"}]
        })];
        clean_responses_items(&mut items);
        assert_eq!(items[0]["content"][0]["text"], "AB");
    }

    #[test]
    fn stream_flushes_only_finished_choice() {
        let mut state = StreamSanitizer::new(ApiFormat::Openai);
        let mut out = String::new();
        let events = [
            serde_json::json!({"id":"c","model":"m","choices":[{"index":1,"delta":{"content":"A<ds_sa"}}]}),
            serde_json::json!({"id":"c","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
            serde_json::json!({"id":"c","model":"m","choices":[{"index":1,"delta":{"content":"fety>secret</ds_safety>B"}}]}),
        ];
        for value in events {
            let event = SseEvent {
                event: None,
                data: value.to_string(),
            };
            for bytes in state.rewrite(&event) {
                out.push_str(&String::from_utf8_lossy(&bytes));
            }
        }
        let done = SseEvent {
            event: None,
            data: "[DONE]".to_string(),
        };
        for bytes in state.rewrite(&done) {
            out.push_str(&String::from_utf8_lossy(&bytes));
        }
        assert!(out.contains("\"content\":\"A\""), "{out}");
        assert!(out.contains("\"content\":\"B\""), "{out}");
        assert!(!out.contains("secret"), "{out}");
        assert!(!out.contains("<ds_sa"), "{out}");
    }

    #[tokio::test]
    async fn sanitize_stream_reports_overflow() {
        use futures_util::StreamExt;
        let flag = Arc::new(AtomicBool::new(false));
        let inner: BoxStream<'static, Result<Bytes, Infallible>> = async_stream::stream! {
            yield Ok(Bytes::from_static(b"data: {\"delta\":\""));
            yield Ok(Bytes::from(vec![b'x'; 200]));
            yield Ok(Bytes::from_static(b"\"}\n\n"));
        }
        .boxed();
        let mut stream = sanitize_stream(inner, ApiFormat::Responses, 64, Arc::clone(&flag));
        let mut out = String::new();
        while let Some(item) = stream.next().await {
            out.push_str(&String::from_utf8_lossy(&item.unwrap()));
        }
        assert!(flag.load(Ordering::Relaxed));
        assert!(out.contains("gateway_error"), "{out}");
    }

    #[test]
    fn refusal_stream_is_cleaned() {
        let mut state = StreamSanitizer::new(ApiFormat::Responses);
        let mut out = String::new();
        for value in [
            serde_json::json!({
                "type": "response.refusal.delta",
                "item_id": "msg_1",
                "output_index": 0,
                "content_index": 0,
                "delta": "A<ds_safety>x</ds_safety>B"
            }),
            serde_json::json!({
                "type": "response.refusal.done",
                "item_id": "msg_1",
                "output_index": 0,
                "content_index": 0,
                "refusal": "A<ds_safety>y</ds_safety>B"
            }),
        ] {
            let event = SseEvent {
                event: None,
                data: value.to_string(),
            };
            for bytes in state.rewrite(&event) {
                out.push_str(&String::from_utf8_lossy(&bytes));
            }
        }
        assert!(out.contains("AB"), "{out}");
        assert!(!out.contains("ds_safety"), "{out}");
    }

    #[test]
    fn reasoning_summary_part_done_is_cleaned() {
        let mut state = StreamSanitizer::new(ApiFormat::Responses);
        let event = SseEvent {
            event: None,
            data: serde_json::json!({
                "type": "response.reasoning_summary_part.done",
                "item_id": "rs_1",
                "output_index": 0,
                "summary_index": 0,
                "part": {"type": "summary_text", "text": "A<ds_safety>x</ds_safety>B"}
            })
            .to_string(),
        };
        let mut out = String::new();
        for bytes in state.rewrite(&event) {
            out.push_str(&String::from_utf8_lossy(&bytes));
        }
        assert!(out.contains("AB"), "{out}");
        assert!(!out.contains("x</ds_safety>"), "{out}");
    }

    #[test]
    fn reasoning_tail_flushes_before_done_event() {
        let mut state = StreamSanitizer::new(ApiFormat::Responses);
        let mut out = String::new();
        for value in [
            serde_json::json!({
                "type": "response.reasoning_summary_text.delta",
                "item_id": "rs_1",
                "output_index": 0,
                "summary_index": 0,
                "delta": "think #"
            }),
            serde_json::json!({
                "type": "response.reasoning_summary_text.done",
                "item_id": "rs_1",
                "output_index": 0,
                "summary_index": 0,
                "text": "think #"
            }),
        ] {
            let event = SseEvent {
                event: None,
                data: value.to_string(),
            };
            for bytes in state.rewrite(&event) {
                out.push_str(&String::from_utf8_lossy(&bytes));
            }
        }
        let flush_pos = out.find("\"delta\":\"#\"").expect("flushed tail");
        let done_pos = out
            .find("response.reasoning_summary_text.done")
            .expect("done event");
        assert!(flush_pos < done_pos, "{out}");
    }

    #[test]
    fn cleans_responses_output() {
        let mut value = serde_json::json!({
            "output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": "A<ds_safety>x</ds_safety>B"}]
            }]
        });
        clean_response(ApiFormat::Responses, &mut value);
        assert_eq!(value["output"][0]["content"][0]["text"], "AB");
    }
}

#[cfg(test)]
mod buffer_bound_tests {
    use super::*;

    #[test]
    fn unclosed_opening_tag_stops_buffering() {
        let mut cleaner = StreamCleaner::default();
        let mut out = cleaner.push("<ds_safety");
        let chunk = "A".repeat(64 * 1024);
        for _ in 0..128 {
            out.push_str(&cleaner.push(&chunk));
        }
        assert!(cleaner.pending.len() <= MAX_OPEN_TAG_BYTES);
        assert!(cleaner.in_block);
        out.push_str(&cleaner.finish());
        assert!(!out.contains('A'));
    }

    #[test]
    fn incomplete_opening_tag_below_cap_is_held() {
        let mut cleaner = StreamCleaner::default();
        assert_eq!(cleaner.push("before <ds_safety mode=\"x\""), "before ");
        assert!(cleaner.pending.starts_with("<ds_safety"));
        assert!(!cleaner.in_block);
    }
}
