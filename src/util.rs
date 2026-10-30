use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn now_secs() -> i64 {
    now_ms() / 1000
}

/// Character counts used by the chunk-independent token estimate.
pub fn text_token_counts(text: &str) -> (u64, u64) {
    let mut cjk = 0u64;
    let mut other = 0u64;
    for ch in text.chars() {
        if is_cjk(ch) {
            cjk += 1;
        } else {
            other += 1;
        }
    }
    (cjk, other)
}

/// Rough token estimate used only when a provider does not return usage data.
/// CJK characters are counted as ~1 token, everything else as ~3.5 chars/token.
pub fn estimate_tokens_from_counts(cjk: u64, other: u64) -> u64 {
    if cjk == 0 && other == 0 {
        return 0;
    }
    (cjk + ((other as f64) / 3.5).ceil() as u64).max(1)
}

pub fn estimate_tokens(text: &str) -> u64 {
    let (cjk, other) = text_token_counts(text);
    estimate_tokens_from_counts(cjk, other)
}

fn is_cjk(ch: char) -> bool {
    let c = ch as u32;
    matches!(c,
        0x1100..=0x11FF |   // Hangul Jamo
        0x2E80..=0x2EFF |   // CJK radicals
        0x3000..=0x303F |   // CJK punctuation
        0x3040..=0x30FF |   // Hiragana / Katakana
        0x3400..=0x4DBF |   // CJK ext A
        0x4E00..=0x9FFF |   // CJK unified
        0xAC00..=0xD7AF |   // Hangul syllables
        0xF900..=0xFAFF |   // CJK compat
        0x1F300..=0x1FAFF | // emoji
        0x20000..=0x2FA1F   // CJK ext B-F
    )
}

/// Estimate tokens for an arbitrary JSON payload by walking all strings.
pub fn estimate_value_tokens(value: &Value) -> u64 {
    match value {
        Value::String(s) => estimate_tokens(s),
        Value::Array(items) => items.iter().map(estimate_value_tokens).sum(),
        Value::Object(map) => map.values().map(estimate_value_tokens).sum(),
        Value::Null => 0,
        _ => 1,
    }
}

pub fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push('…');
    out
}

// ---------------------------------------------------------------------------
// Minimal SSE parser. Handles `\n\n`, `\r\n\r\n` and `\n\r\n` separators.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

const MAX_EVENT_BYTES: usize = 4 * 1024 * 1024;

pub struct SseParser {
    buf: Vec<u8>,
    overflowed: bool,
    scan_from: usize,
    max_event_bytes: usize,
}

impl SseParser {
    pub fn new() -> Self {
        Self::with_limit(MAX_EVENT_BYTES)
    }

    pub fn with_limit(max_event_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            overflowed: false,
            scan_from: 0,
            max_event_bytes: max_event_bytes.max(1),
        }
    }

    pub fn push(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        let mut start = self.scan_from.saturating_sub(3);
        while let Some((idx, sep_len)) = find_separator_from(&self.buf, start) {
            if idx > self.max_event_bytes {
                self.overflowed = true;
                self.buf.clear();
                self.scan_from = 0;
                break;
            }
            let block = String::from_utf8_lossy(&self.buf[..idx]).to_string();
            self.buf.drain(..idx + sep_len);
            if let Some(ev) = parse_block(&block) {
                events.push(ev);
            }
            start = 0;
        }
        if self.buf.len() > self.max_event_bytes {
            self.overflowed = true;
            self.buf.clear();
            self.scan_from = 0;
        } else {
            self.scan_from = self.buf.len().saturating_sub(3);
        }
        events
    }

    /// Flush any trailing event that was not terminated by a blank line.
    pub fn finish(&mut self) -> Vec<SseEvent> {
        if self.overflowed || self.buf.is_empty() {
            return Vec::new();
        }
        if self.buf.len() > self.max_event_bytes {
            self.overflowed = true;
            self.buf.clear();
            self.scan_from = 0;
            return Vec::new();
        }
        let block = String::from_utf8_lossy(&self.buf).to_string();
        self.buf.clear();
        parse_block(&block).into_iter().collect()
    }

    pub fn overflowed(&self) -> bool {
        self.overflowed
    }
}

fn find_separator_from(b: &[u8], start: usize) -> Option<(usize, usize)> {
    let mut i = start.min(b.len());
    while i + 1 < b.len() {
        if b[i] == b'\n' {
            if b[i + 1] == b'\n' {
                return Some((i, 2));
            }
            if b[i + 1] == b'\r' && i + 2 < b.len() && b[i + 2] == b'\n' {
                return Some((i, 3));
            }
        } else if b[i] == b'\r' && b[i + 1] == b'\n' {
            if i + 3 < b.len() && b[i + 2] == b'\r' && b[i + 3] == b'\n' {
                return Some((i, 4));
            }
            if i + 2 < b.len() && b[i + 2] == b'\n' {
                return Some((i, 3));
            }
        }
        i += 1;
    }
    None
}

fn parse_block(block: &str) -> Option<SseEvent> {
    let mut event = None;
    let mut data_lines: Vec<&str> = Vec::new();
    for line in block.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim_start().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if data_lines.is_empty() {
        return None;
    }
    Some(SseEvent {
        event,
        data: data_lines.join("\n"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_split_sse() {
        let mut p = SseParser::new();
        assert!(p.push(b"event: message_start\ndata: {\"a\":").is_empty());
        let evs = p.push(b"1}\n\n");
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].event.as_deref(), Some("message_start"));
        assert_eq!(evs[0].data, "{\"a\":1}");
    }

    #[test]
    fn parses_multiple_events_after_partial_scan() {
        let mut p = SseParser::new();
        assert!(p.push(b"data: first").is_empty());
        let events = p.push(b"\n\ndata: second\n\n");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data, "first");
        assert_eq!(events[1].data, "second");
    }

    #[test]
    fn parses_crlf() {
        let mut p = SseParser::new();
        let evs = p.push(b"data: hello\r\n\r\ndata: [DONE]\r\n\r\n");
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].data, "hello");
        assert_eq!(evs[1].data, "[DONE]");
    }

    #[test]
    fn rejects_oversized_events() {
        let mut p = SseParser::new();
        let oversized = vec![b'x'; MAX_EVENT_BYTES + 1];
        assert!(p.push(&oversized).is_empty());
        assert!(p.overflowed());
        assert!(p.finish().is_empty());
    }

    #[test]
    fn rejects_oversized_complete_events() {
        let mut p = SseParser::new();
        let mut oversized = vec![b'x'; MAX_EVENT_BYTES + 1];
        oversized.extend_from_slice(b"\n\n");
        assert!(p.push(&oversized).is_empty());
        assert!(p.overflowed());
    }

    #[test]
    fn chunked_estimates_match_one_shot() {
        let text = "abcdefghijklmnopqrstuvwxyz".repeat(4);
        let (mut cjk, mut other) = (0u64, 0u64);
        for ch in text.chars() {
            let chunk = ch.to_string();
            let (c, o) = text_token_counts(&chunk);
            cjk += c;
            other += o;
        }
        assert_eq!(
            estimate_tokens_from_counts(cjk, other),
            estimate_tokens(&text)
        );
    }

    #[test]
    fn estimates_cjk() {
        assert_eq!(estimate_tokens("你好世界"), 4);
        assert_eq!(estimate_tokens("hello world"), 4);
        assert_eq!(estimate_tokens(""), 0);
    }
}
