use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use serde_json::{json, Value};

use chrono::Local;

use crate::config::{ApiFormat, ModelPlan, PriceSnapshot, Provider, Resolved};
use crate::router::{ProbeLease, Router, SelectRequest};
use crate::stats::{RequestRecord, Stats};
use crate::store::ResponseStore;
use crate::translate;
use crate::util::{
    estimate_tokens_from_counts, estimate_value_tokens, now_ms, now_secs, text_token_counts,
    truncate, SseEvent, SseParser,
};
use crate::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Chat,
    Embeddings,
}

/// Embeddings only exist for OpenAI-format upstreams. Filter before routing so
/// cheapest/fastest/round-robin do not pick a route that cannot serve the call.
fn restrict_plan_for_kind(mut plan: ModelPlan, kind: Kind) -> ModelPlan {
    if kind == Kind::Embeddings {
        plan.routes
            .retain(|route| route.format == ApiFormat::Openai);
    }
    plan
}

impl Kind {
    fn endpoint(self, inbound: ApiFormat) -> &'static str {
        match (self, inbound) {
            (Kind::Embeddings, _) => "embeddings",
            (Kind::Chat, ApiFormat::Openai) => "chat.completions",
            (Kind::Chat, ApiFormat::Anthropic) => "messages",
            (Kind::Chat, ApiFormat::Responses) => "responses",
        }
    }
}

// ---------------------------------------------------------------------------
// usage accounting
// ---------------------------------------------------------------------------

pub struct UsageRec {
    stats: Arc<Stats>,
    router: Arc<Router>,
    endpoint: &'static str,
    inbound: &'static str,
    output_expected: bool,
    provider: String,
    model: String,
    upstream_model: String,
    route_id: String,
    stream: bool,
    started: Instant,
    first_token: Option<Instant>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub cache_write_tokens: u64,
    pub estimated: bool,
    pub status: u16,
    pub error: Option<String>,
    price: Option<PriceSnapshot>,
    reported_cost: Option<f64>,
    reported_currency: Option<String>,
    cooldown_secs: u64,
    error_details: bool,
    stream_idle_timeout: Duration,
    output_max_bytes: usize,
    estimated_cjk: u64,
    estimated_other: u64,
    reported_prompt: bool,
    reported_completion: bool,
    output_observed: bool,
    zero_completion: bool,
    finished: bool,
    recorded: bool,
    probe_lease: Option<ProbeLease>,
    delivery_failed: Arc<AtomicBool>,
}

impl UsageRec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stats: Arc<Stats>,
        router: Arc<Router>,
        endpoint: &'static str,
        inbound: &'static str,
        output_expected: bool,
        provider: String,
        model: String,
        upstream_model: String,
        route_id: String,
        stream: bool,
        prompt_estimate: u64,
        started: Instant,
        price: Option<PriceSnapshot>,
        cooldown_secs: u64,
        error_details: bool,
        stream_idle_timeout_secs: u64,
        output_max_bytes: usize,
    ) -> Self {
        Self {
            stats,
            router,
            endpoint,
            inbound,
            output_expected,
            provider,
            model,
            upstream_model,
            route_id,
            stream,
            started,
            first_token: None,
            prompt_tokens: prompt_estimate,
            completion_tokens: 0,
            cached_tokens: 0,
            cache_write_tokens: 0,
            estimated: true,
            status: 200,
            error: None,
            price,
            reported_cost: None,
            reported_currency: None,
            cooldown_secs,
            error_details,
            stream_idle_timeout: Duration::from_secs(stream_idle_timeout_secs),
            output_max_bytes,
            estimated_cjk: 0,
            estimated_other: 0,
            reported_prompt: false,
            reported_completion: false,
            output_observed: false,
            zero_completion: false,
            finished: false,
            recorded: false,
            probe_lease: None,
            delivery_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Keep the fastest-probe slot for the whole request lifetime.
    fn set_probe_lease(&mut self, lease: Option<ProbeLease>) {
        self.probe_lease = lease;
    }

    /// Share the delivery-failure flag with the final sanitizer stage.
    fn set_delivery_failed(&mut self, flag: Arc<AtomicBool>) {
        self.delivery_failed = flag;
    }

    pub fn mark_first(&mut self) {
        if self.first_token.is_none() {
            self.first_token = Some(Instant::now());
        }
    }

    pub fn mark_finished(&mut self) {
        self.finished = true;
    }

    /// Account for streamed output incrementally so client cancellation still
    /// leaves a partial token estimate in the request record.
    pub fn add_estimated_completion(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        self.output_observed = true;
        self.zero_completion = false;
        let (cjk, other) = text_token_counts(text);
        self.estimated_cjk = self.estimated_cjk.saturating_add(cjk);
        self.estimated_other = self.estimated_other.saturating_add(other);
        if !self.reported_completion {
            self.completion_tokens =
                estimate_tokens_from_counts(self.estimated_cjk, self.estimated_other);
            self.estimated = true;
        }
    }

    pub fn set_usage_full(
        &mut self,
        prompt: Option<u64>,
        completion: Option<u64>,
        cached: Option<u64>,
        cache_write: Option<u64>,
    ) {
        if let Some(p) = prompt {
            self.prompt_tokens = p;
            self.reported_prompt = true;
        }
        if let Some(c) = completion {
            if c > 0 {
                self.completion_tokens = c;
                self.reported_completion = true;
                self.output_observed = true;
                self.zero_completion = false;
            } else if !self.output_observed
                && !self.reported_completion
                && self.completion_tokens == 0
            {
                // A zero before any generated output is a genuinely empty
                // completion. Keep it distinguishable from a placeholder zero
                // that must not erase a running estimate.
                self.completion_tokens = 0;
                self.zero_completion = true;
            }
        }
        if let Some(c) = cached {
            self.cached_tokens = c;
        }
        if let Some(c) = cache_write {
            self.cache_write_tokens = c;
        }
        // Only clear the flag when both input and output came from upstream.
        // Embeddings never generate output, so a complete input usage is
        // sufficient for them.
        let completion_complete = if self.output_expected {
            self.reported_completion || self.zero_completion
        } else {
            true
        };
        self.estimated = !(self.reported_prompt && completion_complete);
    }

    pub fn set_error(&mut self, status: u16, message: String) {
        self.status = status;
        self.error = Some(if self.error_details {
            truncate(&message, 2000)
        } else {
            format!("HTTP {status}")
        });
    }

    pub fn set_reported_cost(&mut self, cost: Option<f64>, currency: Option<String>) {
        if let Some(cost) = cost {
            self.reported_cost = Some(cost);
        }
        if let Some(currency) = currency {
            self.reported_currency = Some(currency);
        }
    }

    pub fn record(&mut self) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        if self.delivery_failed.load(Ordering::Relaxed) && self.status < 400 {
            self.set_error(502, "gateway stream delivery failed".to_string());
        }
        if !self.finished && self.status < 400 {
            self.status = 499;
            self.estimated = true;
            self.error = Some(if self.error_details {
                "client disconnected before the response completed".to_string()
            } else {
                "HTTP 499".to_string()
            });
        }

        let duration_ms = self.started.elapsed().as_millis() as i64;
        let keep_partial_usage = self.status == 499
            || self.reported_prompt
            || self.reported_completion
            || self.output_observed
            || self.reported_cost.is_some();
        let (prompt, completion, cached, cache_write) = if self.status >= 400 && !keep_partial_usage
        {
            (0, 0, 0, 0)
        } else {
            (
                self.prompt_tokens,
                self.completion_tokens,
                self.cached_tokens.min(self.prompt_tokens),
                self.cache_write_tokens.min(self.prompt_tokens),
            )
        };
        let ttft_ms = if self.stream {
            self.first_token
                .map(|t| (t - self.started).as_millis() as i64)
        } else {
            None
        };
        let tokens_per_sec = if self.stream && completion > 0 {
            let secs = match self.first_token {
                Some(t) => (Instant::now() - t).as_secs_f64(),
                None => duration_ms as f64 / 1000.0,
            }
            .max(0.001);
            Some(completion as f64 / secs)
        } else {
            None
        };
        let (cost, currency) = if self.status < 400 || keep_partial_usage {
            let configured = self.price.as_ref().and_then(|price| {
                crate::config::comparable_cost(price, prompt, completion, cached, cache_write)
            });
            match configured {
                Some(cost) => (
                    Some(cost),
                    self.price.as_ref().map(|price| price.currency.clone()),
                ),
                None => (self.reported_cost, self.reported_currency.clone()),
            }
        } else {
            (None, None)
        };

        self.stats.insert(&RequestRecord {
            ts: now_ms(),
            endpoint: self.endpoint.to_string(),
            inbound: self.inbound.to_string(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            upstream_model: self.upstream_model.clone(),
            route_id: self.route_id.clone(),
            status: self.status,
            stream: self.stream,
            prompt_tokens: prompt,
            completion_tokens: completion,
            cached_tokens: cached,
            cache_write_tokens: cache_write,
            estimated: self.estimated,
            ttft_ms,
            duration_ms,
            tokens_per_sec,
            cost,
            currency,
            error: self.error.clone(),
        });

        self.router.record(
            &self.model,
            &self.route_id,
            self.status,
            duration_ms,
            ttft_ms,
            prompt,
            cached,
            self.cooldown_secs,
        );
    }
}

impl Drop for UsageRec {
    fn drop(&mut self) {
        self.record();
    }
}

// ---------------------------------------------------------------------------
// HTTP entry points
// ---------------------------------------------------------------------------

pub async fn openai_chat(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_inference(state, headers, body, ApiFormat::Openai, Kind::Chat).await
}

pub async fn anthropic_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_inference(state, headers, body, ApiFormat::Anthropic, Kind::Chat).await
}

pub async fn openai_responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_inference(state, headers, body, ApiFormat::Responses, Kind::Chat).await
}

pub async fn responses_input_tokens(State(state): State<AppState>, body: Bytes) -> Response {
    let v: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                ApiFormat::Responses,
                StatusCode::BAD_REQUEST,
                &format!("invalid JSON body: {e}"),
            )
        }
    };
    let tokens = match estimate_responses_input(&state, &v).await {
        Ok(tokens) => tokens,
        Err(error) => return error_response(ApiFormat::Responses, StatusCode::BAD_REQUEST, &error),
    };
    Json(json!({
        "input_tokens": tokens,
        "object": "response.input_tokens"
    }))
    .into_response()
}

/// Expand a cached `previous_response_id` the same way generation does, then
/// estimate the full prompt. Missing history is an error rather than a number
/// that silently counts only the new turn.
async fn estimate_responses_input(state: &AppState, body: &Value) -> Result<u64, String> {
    let Some(previous) = body.get("previous_response_id").and_then(|v| v.as_str()) else {
        return Ok(translate::responses_estimate_tokens(body));
    };
    let enabled = state.config.read().await.server.response_state;
    if !enabled {
        return Err(
            "previous_response_id is unavailable because server.response_state is disabled; \
             send the full input history instead"
                .to_string(),
        );
    }
    let stored = state.responses.get(previous).ok_or_else(|| {
        format!(
            "previous response '{previous}' was not found, expired, or was too large for the \
             local cache; send the full input history instead"
        )
    })?;
    let mut items = stored.items;
    items.extend(translate::responses_input_items(body));
    let mut expanded = body.clone();
    expanded["input"] = json!(items);
    if let Some(obj) = expanded.as_object_mut() {
        obj.remove("previous_response_id");
    }
    Ok(translate::responses_estimate_tokens(&expanded))
}

pub async fn responses_retrieve_unsupported(Path(response_id): Path<String>) -> Response {
    error_response(
        ApiFormat::Responses,
        StatusCode::NOT_IMPLEMENTED,
        &format!(
            "retrieving stored response '{response_id}' is not supported by this gateway; \
             use streaming or send the full input history instead"
        ),
    )
}

pub async fn openai_embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    handle_inference(state, headers, body, ApiFormat::Openai, Kind::Embeddings).await
}

pub async fn count_tokens(State(_state): State<AppState>, body: Bytes) -> Response {
    let v: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                ApiFormat::Anthropic,
                StatusCode::BAD_REQUEST,
                &format!("invalid JSON body: {e}"),
            )
        }
    };
    let tokens = estimate_request_tokens(&v);
    Json(json!({"input_tokens": tokens})).into_response()
}

async fn handle_inference(
    state: AppState,
    headers: HeaderMap,
    body: Bytes,
    inbound: ApiFormat,
    kind: Kind,
) -> Response {
    let started = Instant::now();
    let body: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                inbound,
                StatusCode::BAD_REQUEST,
                &format!("invalid JSON body: {e}"),
            )
        }
    };

    let requested = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if requested.is_empty() {
        return error_response(
            inbound,
            StatusCode::BAD_REQUEST,
            "missing required field: model",
        );
    }

    let prompt_estimate = estimate_request_tokens(&body);
    let mut routing_prompt_estimate = prompt_estimate;
    let mut prior_turn: Option<crate::store::StoredTurn> = None;
    if inbound == ApiFormat::Responses {
        if let Some(prev) = body.get("previous_response_id").and_then(|v| v.as_str()) {
            let state_enabled = state.config.read().await.server.response_state;
            if state_enabled {
                if let Some(stored) = state.responses.get(prev) {
                    routing_prompt_estimate = routing_prompt_estimate.saturating_add(
                        stored.items.iter().map(estimate_value_tokens).sum::<u64>(),
                    );
                    prior_turn = Some(stored);
                }
            }
        }
    }
    let max_output_hint = body
        .get("max_tokens")
        .or_else(|| body.get("max_completion_tokens"))
        .or_else(|| body.get("max_output_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(1024);
    let override_route = headers
        .get("x-gateway-route")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());

    let plan = {
        let cfg = state.config.read().await;
        cfg.resolve_plan(&requested)
    };
    let plan = match plan {
        Ok(plan) => restrict_plan_for_kind(plan, kind),
        Err(e) => return error_response(inbound, StatusCode::NOT_FOUND, &e),
    };
    if kind == Kind::Embeddings && plan.routes.is_empty() {
        return error_response(
            inbound,
            StatusCode::BAD_REQUEST,
            "embeddings are only supported for openai-format routes",
        );
    }

    let (selected, routing) = {
        let cfg = state.config.read().await;
        let request = SelectRequest {
            inbound,
            override_route: override_route.as_deref(),
            prompt_tokens: routing_prompt_estimate,
            max_output_tokens: max_output_hint,
            prefer_native: cfg.routing.prefer_native,
        };
        (state.router.select(&plan, &request), cfg.routing.clone())
    };
    let selection = match selected {
        Ok(selection) => selection,
        Err(e) => return error_response(inbound, StatusCode::BAD_REQUEST, &e),
    };
    let probe_lease = selection.lease;
    let route = selection.route;
    let provider = route.provider.clone();
    let format = route.format;
    let route_id = route.route_id.clone();
    let alias = plan.alias.clone();

    if kind == Kind::Embeddings && format != ApiFormat::Openai {
        return error_response(
            inbound,
            StatusCode::BAD_REQUEST,
            "embeddings are only supported for openai-format routes",
        );
    }

    // Responses API conversation state. When the upstream is not itself a
    // Responses API, `previous_response_id` is expanded from the bounded
    // in-memory cache. A Responses upstream owns its own state and is forwarded
    // as-is.
    let mut inbound_body = body.clone();
    let mut store_input: Vec<Value> = Vec::new();
    let mut should_store = false;
    let mut history_tokens: Option<u64> = None;

    if inbound == ApiFormat::Responses {
        let state_enabled = state.config.read().await.server.response_state;
        let wants_store = body.get("store").and_then(|s| s.as_bool()).unwrap_or(true);
        let mut store_complete = true;
        let mut full_history: Option<Vec<Value>> = None;

        if let Some(prev) = body.get("previous_response_id").and_then(|v| v.as_str()) {
            if state_enabled {
                match prior_turn.take() {
                    Some(stored) => {
                        let same_native_session = format == ApiFormat::Responses
                            && stored.meta.native
                            && stored.meta.route_id == route_id
                            && stored.meta.model.eq_ignore_ascii_case(&alias)
                            && stored.meta.provider == provider.id
                            && stored.meta.upstream_model == route.upstream_model
                            && stored.meta.provider_base_url == provider.base_url;
                        if same_native_session {
                            // Native Responses sessions stay on their upstream
                            // state. Keep previous_response_id in the request,
                            // but maintain a complete local copy for a later
                            // route switch.
                            let mut items = stored.items;
                            items.extend(translate::responses_input_items(&body));
                            // The upstream processes the whole session even
                            // though the request only carries the new turn.
                            history_tokens = Some(items.iter().map(estimate_value_tokens).sum());
                            full_history = Some(items);
                        } else {
                            let mut items = stored.items;
                            items.extend(translate::responses_input_items(&body));
                            inbound_body["input"] = json!(items);
                            if let Some(obj) = inbound_body.as_object_mut() {
                                obj.remove("previous_response_id");
                            }
                        }
                    }
                    None if format == ApiFormat::Responses => {
                        // Legacy/native state that predates this cache: let the
                        // Responses upstream resolve it. Local caching is
                        // skipped because we do not have the full history.
                        store_complete = false;
                    }
                    None => {
                        return error_response(
                            inbound,
                            StatusCode::BAD_REQUEST,
                            &format!(
                                "previous response '{prev}' was not found, expired, or was too \
                                 large for the local cache; send the full input history instead"
                            ),
                        );
                    }
                }
            } else if format != ApiFormat::Responses {
                return error_response(
                    inbound,
                    StatusCode::BAD_REQUEST,
                    "previous_response_id is unavailable because server.response_state is disabled; \
                     send the full input history instead",
                );
            }
        }

        if wants_store && state_enabled && store_complete {
            store_input =
                full_history.unwrap_or_else(|| translate::responses_input_items(&inbound_body));
            should_store = true;
        }
    }

    let stream = inbound_body
        .get("stream")
        .and_then(|s| s.as_bool())
        .unwrap_or(false);
    let prompt_estimate = estimate_prompt_with_history(&inbound_body, history_tokens);
    let price = route
        .pricing
        .as_ref()
        .map(|pricing| pricing.snapshot_at(Local::now()));

    let mut usage = UsageRec::new(
        Arc::clone(&state.stats),
        Arc::clone(&state.router),
        kind.endpoint(inbound),
        inbound.as_str(),
        kind == Kind::Chat,
        provider.id.clone(),
        alias.clone(),
        route.upstream_model.clone(),
        route_id.clone(),
        stream,
        prompt_estimate,
        started,
        price,
        routing.failure_cooldown_secs,
        state.config.read().await.server.store_error_details,
        state.config.read().await.server.stream_idle_timeout_secs,
        state.config.read().await.server.stream_output_max_bytes,
    );
    usage.set_probe_lease(probe_lease);

    let mut upstream_body = match (inbound, format) {
        (ApiFormat::Openai, ApiFormat::Anthropic) if kind == Kind::Chat => {
            translate::openai_to_anthropic_request(&inbound_body, &route.upstream_model)
        }
        (ApiFormat::Anthropic, ApiFormat::Openai) if kind == Kind::Chat => {
            translate::anthropic_to_openai_request(&inbound_body, &route.upstream_model)
        }
        (ApiFormat::Responses, ApiFormat::Openai) if kind == Kind::Chat => {
            translate::responses_to_chat_request(&inbound_body, &route.upstream_model)
        }
        (ApiFormat::Responses, ApiFormat::Anthropic) if kind == Kind::Chat => {
            translate::responses_to_anthropic_request(&inbound_body, &route.upstream_model)
        }
        (ApiFormat::Openai, ApiFormat::Responses) if kind == Kind::Chat => {
            translate::chat_to_responses_request(&inbound_body, &route.upstream_model)
        }
        (ApiFormat::Anthropic, ApiFormat::Responses) if kind == Kind::Chat => {
            translate::anthropic_to_responses_request(&inbound_body, &route.upstream_model)
        }
        _ => inbound_body.clone(),
    };

    if let Some(obj) = upstream_body.as_object_mut() {
        for (k, v) in &route.params {
            obj.entry(k.clone()).or_insert_with(|| v.clone());
        }
        obj.insert("model".into(), json!(route.upstream_model));

        if kind == Kind::Embeddings {
            obj.remove("stream");
            obj.remove("stream_options");
        } else if stream {
            obj.insert("stream".into(), json!(true));
            if format == ApiFormat::Openai && provider.stream_usage {
                obj.insert("stream_options".into(), json!({"include_usage": true}));
            }
        } else {
            obj.remove("stream");
            obj.remove("stream_options");
        }

        if format == ApiFormat::Anthropic && obj.get("max_tokens").is_none() {
            obj.insert("max_tokens".into(), json!(4096));
        }
    }

    let expected_choices = if format == ApiFormat::Openai && kind == Kind::Chat {
        upstream_body
            .get("n")
            .and_then(|value| value.as_u64())
            .filter(|n| *n > 1)
    } else {
        None
    };

    let url = endpoint_url(&provider.base_url, format, kind);
    let mut rb = state.client.post(&url).json(&upstream_body);
    if !stream {
        rb = rb.timeout(Duration::from_secs(provider.timeout_secs.max(1)));
    }
    if let Some(key) = provider.api_key.as_deref().filter(|k| !k.trim().is_empty()) {
        rb = match format {
            ApiFormat::Anthropic => rb.header("x-api-key", key),
            _ => rb.bearer_auth(key),
        };
    }
    if format == ApiFormat::Anthropic {
        let version = headers
            .get("anthropic-version")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("2023-06-01");
        rb = rb.header("anthropic-version", version);
        if let Some(beta) = headers.get("anthropic-beta").and_then(|v| v.to_str().ok()) {
            rb = rb.header("anthropic-beta", beta);
        }
    }
    for (k, v) in &provider.headers {
        rb = rb.header(k.as_str(), v.as_str());
    }
    rb = rb.header("content-type", "application/json");

    let header_timeout = if stream {
        state.config.read().await.server.stream_headers_timeout_secs
    } else {
        0
    };
    let send = rb.send();
    let sent = if header_timeout > 0 {
        match tokio::time::timeout(Duration::from_secs(header_timeout), send).await {
            Ok(result) => result,
            Err(_) => {
                let msg = format!(
                    "upstream {} did not return response headers within {}s",
                    provider.id, header_timeout
                );
                usage.set_error(StatusCode::GATEWAY_TIMEOUT.as_u16(), msg.clone());
                usage.mark_finished();
                usage.record();
                let mut response = error_response(inbound, StatusCode::GATEWAY_TIMEOUT, &msg);
                add_route_headers(&mut response, &route);
                return response;
            }
        }
    } else {
        send.await
    };
    let resp = match sent {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("upstream request to {} failed: {e}", provider.id);
            usage.set_error(502, msg.clone());
            usage.mark_finished();
            usage.record();
            let mut response = error_response(inbound, StatusCode::BAD_GATEWAY, &msg);
            add_route_headers(&mut response, &route);
            return response;
        }
    };

    let upstream_status = resp.status();
    if upstream_status.is_redirection() {
        let message = "upstream redirect refused; configure the final upstream URL";
        usage.set_error(502, message.to_string());
        usage.mark_finished();
        usage.record();
        let mut response = error_response(inbound, StatusCode::BAD_GATEWAY, message);
        add_route_headers(&mut response, &route);
        return response;
    }
    if !upstream_status.is_success() {
        let (error_body_timeout, error_body_max_bytes) = {
            let cfg = state.config.read().await;
            (
                cfg.server.error_body_timeout_secs,
                cfg.server.error_body_max_bytes,
            )
        };
        let text = read_upstream_error_body(resp, error_body_timeout, error_body_max_bytes).await;
        usage.set_error(upstream_status.as_u16(), extract_error_message(&text));
        usage.mark_finished();
        usage.record();
        let mut response = upstream_error_response(inbound, format, upstream_status, &text);
        add_route_headers(&mut response, &route);
        return response;
    }

    if stream {
        let store_ctx = if should_store {
            Some(ResponsesStoreCtx {
                store: Arc::clone(&state.responses),
                input_items: store_input.clone(),
                previous_response_id: body
                    .get("previous_response_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                native: format == ApiFormat::Responses,
                route_id: route_id.clone(),
                model: alias.clone(),
                provider: provider.id.clone(),
                upstream_model: route.upstream_model.clone(),
                provider_base_url: provider.base_url.clone(),
            })
        } else {
            None
        };
        let delivery_failed = Arc::new(AtomicBool::new(false));
        usage.set_delivery_failed(Arc::clone(&delivery_failed));
        let sanitize_max_bytes = {
            let cfg = state.config.read().await;
            cfg.server
                .stream_output_max_bytes
                .saturating_add(4 * 1024 * 1024)
                .max(8 * 1024 * 1024)
        };
        let body_stream: BoxStream<'static, Result<Bytes, Infallible>> = match (inbound, format) {
            (ApiFormat::Openai, ApiFormat::Openai) => {
                passthrough_openai(resp, usage, expected_choices)
            }
            (ApiFormat::Anthropic, ApiFormat::Anthropic) => passthrough_anthropic(resp, usage),
            (ApiFormat::Responses, ApiFormat::Responses) => {
                passthrough_responses(resp, usage, store_ctx)
            }
            (ApiFormat::Openai, ApiFormat::Anthropic) => {
                anthropic_to_openai_stream(resp, usage, alias.clone())
            }
            (ApiFormat::Anthropic, ApiFormat::Openai) => {
                openai_to_anthropic_stream(resp, usage, alias.clone(), expected_choices)
            }
            (ApiFormat::Responses, ApiFormat::Openai) => {
                chat_to_responses_stream(resp, usage, alias.clone(), store_ctx, expected_choices)
            }
            (ApiFormat::Responses, ApiFormat::Anthropic) => {
                anthropic_to_responses_stream(resp, usage, alias.clone(), store_ctx)
            }
            (ApiFormat::Openai, ApiFormat::Responses) => {
                responses_to_chat_stream(resp, usage, alias.clone())
            }
            (ApiFormat::Anthropic, ApiFormat::Responses) => {
                responses_to_anthropic_stream(resp, usage, alias.clone())
            }
        };
        let body_stream = crate::sanitize::sanitize_stream(
            body_stream,
            inbound,
            sanitize_max_bytes,
            delivery_failed,
        );
        let heartbeat_secs = state.config.read().await.server.stream_heartbeat_secs;
        let body_stream = with_heartbeat(body_stream, Duration::from_secs(heartbeat_secs));
        let mut response = sse_response(body_stream);
        add_route_headers(&mut response, &route);
        return response;
    }

    let status_code = StatusCode::from_u16(upstream_status.as_u16()).unwrap_or(StatusCode::OK);
    let body_limit = state.config.read().await.server.response_body_max_bytes;
    let bytes = match read_success_body(resp, body_limit).await {
        Ok(b) => b,
        Err(msg) => {
            usage.set_error(502, msg.clone());
            usage.mark_finished();
            usage.record();
            let mut response = error_response(inbound, StatusCode::BAD_GATEWAY, &msg);
            add_route_headers(&mut response, &route);
            return response;
        }
    };

    let upstream_json: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => {
            let message = "upstream returned a non-JSON response";
            usage.set_error(502, message.to_string());
            usage.mark_finished();
            usage.record();
            let mut response = error_response(inbound, StatusCode::BAD_GATEWAY, message);
            add_route_headers(&mut response, &route);
            return response;
        }
    };

    if let Err(message) = apply_and_validate_nonstream(&mut usage, kind, format, &upstream_json) {
        usage.set_error(502, message.clone());
        usage.mark_finished();
        usage.record();
        let mut response = error_response(inbound, StatusCode::BAD_GATEWAY, &message);
        add_route_headers(&mut response, &route);
        return response;
    }

    let mut upstream_json = upstream_json;
    if inbound != format {
        fill_missing_nonstream_usage(&mut upstream_json, format, &usage);
    }

    let mut out = match (inbound, format) {
        (ApiFormat::Openai, ApiFormat::Anthropic) => {
            translate::anthropic_to_openai_response(&upstream_json, &requested)
        }
        (ApiFormat::Anthropic, ApiFormat::Openai) => {
            translate::openai_to_anthropic_response(&upstream_json, &requested)
        }
        (ApiFormat::Responses, ApiFormat::Openai) => {
            translate::chat_to_responses_response(&upstream_json, &requested)
        }
        (ApiFormat::Responses, ApiFormat::Anthropic) => {
            translate::anthropic_to_responses_response(&upstream_json, &requested)
        }
        (ApiFormat::Openai, ApiFormat::Responses) => {
            translate::responses_to_chat_response(&upstream_json, &requested)
        }
        (ApiFormat::Anthropic, ApiFormat::Responses) => {
            translate::responses_to_anthropic_response(&upstream_json, &requested)
        }
        _ => {
            let mut v = upstream_json;
            if let Some(obj) = v.as_object_mut() {
                obj.insert("model".into(), json!(requested));
            }
            v
        }
    };
    if let Some(obj) = out.as_object_mut() {
        // Keep the gateway-facing model name for cross-format translations too.
        obj.insert("model".into(), json!(requested));
        if inbound == ApiFormat::Responses {
            if let Some(prev) = body.get("previous_response_id") {
                obj.insert("previous_response_id".into(), prev.clone());
            }
        }
    }

    crate::sanitize::clean_response(inbound, &mut out);

    if inbound == ApiFormat::Responses && should_store {
        if let Some(response_id) = out.get("id").and_then(|v| v.as_str()) {
            let mut items = store_input.clone();
            if let Some(output) = out.get("output").and_then(|v| v.as_array()) {
                items.extend(output.iter().cloned());
            }
            let stored = state.responses.put(
                response_id.to_string(),
                items,
                crate::store::TurnMeta {
                    native: format == ApiFormat::Responses,
                    route_id: route_id.clone(),
                    model: alias.clone(),
                    provider: provider.id.clone(),
                    upstream_model: route.upstream_model.clone(),
                    provider_base_url: provider.base_url.clone(),
                },
            );
            if !stored {
                if let Some(obj) = out.as_object_mut() {
                    let metadata = obj.entry("metadata").or_insert_with(|| json!({}));
                    if !metadata.is_object() {
                        *metadata = json!({});
                    }
                    if let Some(metadata) = metadata.as_object_mut() {
                        metadata.insert("gateway_cache".into(), json!("not_stored_too_large"));
                    }
                }
            }
        }
    }

    usage.mark_finished();
    usage.record();
    let mut response = (status_code, Json(out)).into_response();
    add_route_headers(&mut response, &route);
    response
}

// ---------------------------------------------------------------------------
// pass-through streams (still parsed for accounting)
// ---------------------------------------------------------------------------

fn upstream_stream(
    resp: reqwest::Response,
    idle: Duration,
) -> BoxStream<'static, Result<Bytes, String>> {
    let s = stream! {
        let mut chunks = resp.bytes_stream();
        loop {
            let next = if idle.is_zero() {
                chunks.next().await
            } else {
                match tokio::time::timeout(idle, chunks.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        yield Err(format!(
                            "upstream idle timeout after {}s",
                            idle.as_secs()
                        ));
                        break;
                    }
                }
            };
            match next {
                Some(Ok(chunk)) => yield Ok(chunk),
                Some(Err(error)) => {
                    yield Err(error.to_string());
                    break;
                }
                None => break,
            }
        }
    };
    s.boxed()
}

async fn read_upstream_error_body(
    resp: reqwest::Response,
    timeout_secs: u64,
    max_bytes: usize,
) -> String {
    let read = async move {
        let mut stream = resp.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    let remaining = max_bytes.saturating_sub(buf.len());
                    if chunk.len() > remaining {
                        buf.extend_from_slice(&chunk[..remaining]);
                        break;
                    }
                    buf.extend_from_slice(&chunk);
                    if buf.len() >= max_bytes {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        buf
    };
    let bytes = if timeout_secs == 0 {
        read.await
    } else {
        tokio::time::timeout(Duration::from_secs(timeout_secs), read)
            .await
            .unwrap_or_default()
    };
    String::from_utf8_lossy(&bytes).to_string()
}

/// Read a successful non-streaming upstream response with a hard byte cap.
async fn read_success_body(mut resp: reqwest::Response, max_bytes: usize) -> Result<Bytes, String> {
    if max_bytes > 0 {
        if let Some(len) = resp.content_length() {
            if len > max_bytes as u64 {
                return Err(format!(
                    "upstream response body exceeded {} bytes",
                    max_bytes
                ));
            }
        }
    }
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| format!("failed to read upstream response: {e}"))?
    {
        if max_bytes > 0 && buf.len().saturating_add(chunk.len()) > max_bytes {
            return Err(format!(
                "upstream response body exceeded {} bytes",
                max_bytes
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

/// Insert SSE comment heartbeats when the upstream stream is idle so that
/// downstream proxies do not close an otherwise healthy connection.
fn with_heartbeat(
    inner: BoxStream<'static, Result<Bytes, Infallible>>,
    interval: Duration,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    if interval.is_zero() {
        return inner;
    }
    let s = stream! {
        let mut inner = inner;
        loop {
            tokio::select! {
                item = inner.next() => match item {
                    Some(item) => yield item,
                    None => break,
                },
                _ = tokio::time::sleep(interval) => {
                    yield Ok(Bytes::from_static(b": keep-alive\n\n"));
                }
            }
        }
    };
    s.boxed()
}

/// Track per-choice completion for OpenAI Chat streams. `[DONE]` is the only
/// definitive end marker; EOF without it is only accepted when every choice
/// seen in the stream has reported a finish reason.
fn openai_stream_complete(
    saw_done: bool,
    seen: &HashSet<u64>,
    finished: &HashSet<u64>,
    expected_choices: Option<u64>,
) -> bool {
    if saw_done {
        return true;
    }
    if seen.is_empty() || seen.len() != finished.len() {
        return false;
    }
    expected_choices
        .map(|expected| seen.len() as u64 >= expected)
        .unwrap_or(true)
}

fn track_openai_choice(
    ev: &SseEvent,
    seen: &mut HashSet<u64>,
    finished: &mut HashSet<u64>,
) -> bool {
    if ev.data.trim() == "[DONE]" {
        return true;
    }
    let Ok(value) = serde_json::from_str::<Value>(&ev.data) else {
        return false;
    };
    if let Some(choices) = value.get("choices").and_then(|c| c.as_array()) {
        for choice in choices {
            let index = choice.get("index").and_then(|i| i.as_u64()).unwrap_or(0);
            seen.insert(index);
            if choice
                .get("finish_reason")
                .map(|reason| !reason.is_null())
                .unwrap_or(false)
            {
                finished.insert(index);
            }
        }
    }
    false
}

fn anthropic_stream_terminal(ev: &SseEvent) -> bool {
    serde_json::from_str::<Value>(&ev.data)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(|typ| typ.as_str())
                .map(|typ| typ == "message_stop")
        })
        .unwrap_or(false)
}

fn responses_stream_terminal(ev: &SseEvent) -> bool {
    serde_json::from_str::<Value>(&ev.data)
        .ok()
        .and_then(|value| {
            value.get("type").and_then(|typ| typ.as_str()).map(|typ| {
                matches!(
                    typ,
                    "response.completed" | "response.incomplete" | "response.failed"
                )
            })
        })
        .unwrap_or(false)
}

fn stream_event_failed(format: ApiFormat, ev: &SseEvent) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(&ev.data) else {
        return false;
    };
    match format {
        ApiFormat::Openai => value.get("error").is_some(),
        ApiFormat::Anthropic => value.get("type").and_then(|typ| typ.as_str()) == Some("error"),
        ApiFormat::Responses => matches!(
            value.get("type").and_then(|typ| typ.as_str()),
            Some("error" | "response.failed")
        ),
    }
}

fn stream_error_message(ev: &SseEvent) -> Option<String> {
    let value = serde_json::from_str::<Value>(&ev.data).ok()?;
    value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(|message| message.as_str())
        .or_else(|| value.get("message").and_then(|message| message.as_str()))
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("error"))
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
        })
        .map(str::to_string)
}

fn openai_stream_error_event(message: &str) -> Bytes {
    Bytes::from(format!(
        "data: {}\n\n",
        json!({"error": {"message": message, "type": "upstream_error"}})
    ))
}

fn anthropic_stream_error_event(message: &str) -> Bytes {
    Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        json!({"type": "error", "error": {"type": "api_error", "message": message}})
    ))
}

fn responses_stream_error_event(id: &str, model: &str, message: &str) -> Bytes {
    let value = json!({
        "type": "response.failed",
        "response": {
            "id": id,
            "object": "response",
            "model": model,
            "status": "failed",
            "output": [],
            "error": {"code": "upstream_error", "message": message}
        }
    });
    sse_event_bytes(&value)
}

fn passthrough_openai(
    resp: reqwest::Response,
    mut usage: UsageRec,
    expected_choices: Option<u64>,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut saw_done = false;
        let mut seen_choices = HashSet::new();
        let mut finished_choices = HashSet::new();
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    let events = parser.push(&chunk);
                    let ends_stream = events
                        .last()
                        .map(|ev| ev.data.trim() == "[DONE]")
                        .unwrap_or(false);
                    for ev in events {
                        if track_openai_choice(&ev, &mut seen_choices, &mut finished_choices) {
                            saw_done = true;
                        }
                        if stream_event_failed(ApiFormat::Openai, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                        }
                        tap_openai_event(&ev, &mut usage);
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        error_event = false;
                        break;
                    }
                    if ends_stream {
                        usage.mark_finished();
                        ended = true;
                    }
                    yield Ok(chunk);
                    if ended {
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            error_event = false;
        }
        for ev in parser.finish() {
            if track_openai_choice(&ev, &mut seen_choices, &mut finished_choices) {
                saw_done = true;
            }
            if stream_event_failed(ApiFormat::Openai, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
            }
            tap_openai_event(&ev, &mut usage);
        }
        let terminal = openai_stream_complete(
            saw_done,
            &seen_choices,
            &finished_choices,
            expected_choices,
        );
        if (!terminal || read_failed) && !error_event {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            yield Ok(openai_stream_error_event("upstream stream interrupted"));
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

fn passthrough_anthropic(
    resp: reqwest::Response,
    mut usage: UsageRec,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut terminal = false;
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                terminal = false;
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    let events = parser.push(&chunk);
                    let ends_stream = events
                        .last()
                        .map(anthropic_stream_terminal)
                        .unwrap_or(false);
                    for ev in events {
                        if anthropic_stream_terminal(&ev) {
                            terminal = true;
                        }
                        if stream_event_failed(ApiFormat::Anthropic, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                        }
                        tap_anthropic_event(&ev, &mut usage);
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        terminal = false;
                        error_event = false;
                        break;
                    }
                    if ends_stream {
                        usage.mark_finished();
                        ended = true;
                    }
                    yield Ok(chunk);
                    if ended {
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            terminal = false;
            error_event = false;
        }
        for ev in parser.finish() {
            if anthropic_stream_terminal(&ev) {
                terminal = true;
            }
            if stream_event_failed(ApiFormat::Anthropic, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
            }
            tap_anthropic_event(&ev, &mut usage);
        }
        if (!terminal || read_failed) && !error_event {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            yield Ok(anthropic_stream_error_event("upstream stream interrupted"));
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

fn tap_openai_event(ev: &SseEvent, usage: &mut UsageRec) {
    if ev.data.trim() == "[DONE]" {
        return;
    }
    let Ok(v) = serde_json::from_str::<Value>(&ev.data) else {
        return;
    };
    if let Some(u) = v.get("usage").filter(|u| !u.is_null()) {
        let p = u.get("prompt_tokens").and_then(|x| x.as_u64());
        let c = u.get("completion_tokens").and_then(|x| x.as_u64());
        let cached = u
            .get("prompt_tokens_details")
            .and_then(|d| d.get("cached_tokens"))
            .and_then(|x| x.as_u64());
        if p.is_some() || c.is_some() || cached.is_some() {
            usage.set_usage_full(p, c, cached, None);
        }
        let (cost, currency) = extract_reported_cost(u);
        usage.set_reported_cost(cost, currency);
    }
    if let Some(choices) = v.get("choices").and_then(|c| c.as_array()) {
        for choice in choices {
            let d = &choice["delta"];
            if let Some(t) = d.get("content").and_then(|c| c.as_str()) {
                if !t.is_empty() {
                    usage.add_estimated_completion(t);
                    usage.mark_first();
                }
            }
            if let Some(t) = d.get("reasoning_content").and_then(|c| c.as_str()) {
                usage.add_estimated_completion(t);
                if !t.is_empty() {
                    usage.mark_first();
                }
            }
            if let Some(t) = d.get("reasoning").and_then(|c| c.as_str()) {
                usage.add_estimated_completion(t);
                if !t.is_empty() {
                    usage.mark_first();
                }
            }
            if let Some(t) = d.get("refusal").and_then(|c| c.as_str()) {
                if !t.is_empty() {
                    usage.add_estimated_completion(t);
                    usage.mark_first();
                }
            }
            if let Some(tcs) = d.get("tool_calls").and_then(|t| t.as_array()) {
                for tc in tcs {
                    if let Some(a) = tc["function"]["arguments"].as_str() {
                        usage.add_estimated_completion(a);
                    }
                }
                if !tcs.is_empty() {
                    usage.mark_first();
                }
            }
        }
    }
}

// Anthropic reports uncached input separately and may update any cumulative
// counter in message_delta. Preserve prior counters when a field is omitted.
fn apply_anthropic_stream_usage(usage: &mut UsageRec, u: &Value, include_output: bool) {
    let input = u.get("input_tokens").and_then(Value::as_u64);
    let cached = u.get("cache_read_input_tokens").and_then(Value::as_u64);
    let cache_write = u.get("cache_creation_input_tokens").and_then(Value::as_u64);
    let prior_input = usage.reported_prompt.then(|| {
        usage
            .prompt_tokens
            .saturating_sub(usage.cached_tokens)
            .saturating_sub(usage.cache_write_tokens)
    });
    let prompt = input.or(prior_input).map(|value| {
        value
            .saturating_add(cached.unwrap_or(usage.cached_tokens))
            .saturating_add(cache_write.unwrap_or(usage.cache_write_tokens))
    });
    // The start counter is only a snapshot before generated content. Treating
    // it as final prevents later output from contributing to an estimate.
    let output = include_output
        .then(|| u.get("output_tokens").and_then(Value::as_u64))
        .flatten();
    usage.set_usage_full(prompt, output, cached, cache_write);
    let (cost, currency) = extract_reported_cost(u);
    usage.set_reported_cost(cost, currency);
}

fn tap_anthropic_event(ev: &SseEvent, usage: &mut UsageRec) {
    let Ok(v) = serde_json::from_str::<Value>(&ev.data) else {
        return;
    };
    let event_type = v
        .get("type")
        .and_then(|t| t.as_str())
        .or(ev.event.as_deref());
    match event_type {
        Some("message_start") => {
            apply_anthropic_stream_usage(usage, &v["message"]["usage"], false);
        }
        Some("content_block_delta") => {
            let delta = &v["delta"];
            match delta.get("type").and_then(|t| t.as_str()) {
                Some("text_delta") => {
                    if let Some(t) = delta.get("text").and_then(|t| t.as_str()) {
                        usage.add_estimated_completion(t);
                    }
                }
                Some("thinking_delta") => {
                    if let Some(t) = delta.get("thinking").and_then(|t| t.as_str()) {
                        usage.add_estimated_completion(t);
                    }
                }
                Some("input_json_delta") => {
                    if let Some(t) = delta.get("partial_json").and_then(|t| t.as_str()) {
                        usage.add_estimated_completion(t);
                    }
                }
                _ => {}
            }
            usage.mark_first();
        }
        Some("message_delta") => {
            apply_anthropic_stream_usage(usage, &v["usage"], true);
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Anthropic -> OpenAI stream translation
// ---------------------------------------------------------------------------

enum BlockKind {
    Text,
    Thinking,
    Tool(u64),
}

struct AnthToOai {
    alias: String,
    id: String,
    created: i64,
    role_sent: bool,
    blocks: HashMap<u64, BlockKind>,
    next_tool: u64,
    finish: Option<String>,
    finalized: bool,
}

impl AnthToOai {
    fn new(alias: &str) -> Self {
        Self {
            alias: alias.to_string(),
            id: "chatcmpl-gateway".to_string(),
            created: now_secs(),
            role_sent: false,
            blocks: HashMap::new(),
            next_tool: 0,
            finish: None,
            finalized: false,
        }
    }

    fn chunk(&self, delta: Value, finish: Option<&str>) -> String {
        format!(
            "data: {}\n\n",
            json!({
                "id": self.id,
                "object": "chat.completion.chunk",
                "created": self.created,
                "model": self.alias,
                "choices": [{
                    "index": 0,
                    "delta": delta,
                    "finish_reason": finish
                }]
            })
        )
    }

    fn handle(&mut self, ev: &SseEvent, usage: &mut UsageRec, out: &mut Vec<String>) {
        let Ok(v) = serde_json::from_str::<Value>(&ev.data) else {
            return;
        };
        match v.get("type").and_then(|t| t.as_str()) {
            Some("message_start") => {
                let msg = &v["message"];
                if let Some(id) = msg.get("id").and_then(|i| i.as_str()) {
                    self.id = id.to_string();
                }
                apply_anthropic_stream_usage(usage, &msg["usage"], false);
                if !self.role_sent {
                    self.role_sent = true;
                    out.push(self.chunk(json!({"role": "assistant", "content": ""}), None));
                }
            }
            Some("content_block_start") => {
                let index = v["index"].as_u64().unwrap_or(0);
                let block = &v["content_block"];
                match block.get("type").and_then(|t| t.as_str()) {
                    Some("text") => {
                        self.blocks.insert(index, BlockKind::Text);
                        if let Some(text) = block
                            .get("text")
                            .and_then(|t| t.as_str())
                            .filter(|t| !t.is_empty())
                        {
                            usage.add_estimated_completion(text);
                            usage.mark_first();
                            out.push(self.chunk(json!({"content": text}), None));
                        }
                    }
                    Some("thinking") | Some("redacted_thinking") => {
                        self.blocks.insert(index, BlockKind::Thinking);
                    }
                    Some("tool_use") => {
                        let tool_index = self.next_tool;
                        self.next_tool += 1;
                        self.blocks.insert(index, BlockKind::Tool(tool_index));
                        out.push(self.chunk(
                            json!({"tool_calls": [{
                                "index": tool_index,
                                "id": block.get("id").and_then(|i| i.as_str()).unwrap_or("call_0"),
                                "type": "function",
                                "function": {
                                    "name": block.get("name").and_then(|n| n.as_str()).unwrap_or("tool"),
                                    "arguments": ""
                                }
                            }]}),
                            None,
                        ));
                        if let Some(input) = block.get("input") {
                            if input.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
                                let args = input.to_string();
                                usage.add_estimated_completion(&args);
                                out.push(self.chunk(
                                    json!({"tool_calls": [{
                                        "index": tool_index,
                                        "function": {"arguments": args}
                                    }]}),
                                    None,
                                ));
                            }
                        }
                    }
                    _ => {}
                }
            }
            Some("content_block_delta") => {
                let index = v["index"].as_u64().unwrap_or(0);
                let delta = &v["delta"];
                match delta.get("type").and_then(|t| t.as_str()) {
                    Some("text_delta") => {
                        if let Some(text) = delta.get("text").and_then(|t| t.as_str()) {
                            if !text.is_empty() {
                                usage.add_estimated_completion(text);
                                usage.mark_first();
                                out.push(self.chunk(json!({"content": text}), None));
                            }
                        }
                    }
                    Some("thinking_delta") => {
                        if let Some(text) = delta.get("thinking").and_then(|t| t.as_str()) {
                            if !text.is_empty() {
                                usage.add_estimated_completion(text);
                                usage.mark_first();
                                out.push(self.chunk(json!({"reasoning_content": text}), None));
                            }
                        }
                    }
                    Some("input_json_delta") => {
                        if let Some(partial) = delta.get("partial_json").and_then(|p| p.as_str()) {
                            if let Some(BlockKind::Tool(tool_index)) = self.blocks.get(&index) {
                                let tool_index = *tool_index;
                                usage.add_estimated_completion(partial);
                                usage.mark_first();
                                out.push(self.chunk(
                                    json!({"tool_calls": [{
                                        "index": tool_index,
                                        "function": {"arguments": partial}
                                    }]}),
                                    None,
                                ));
                            }
                        }
                    }
                    _ => {}
                }
            }
            Some("message_delta") => {
                apply_anthropic_stream_usage(usage, &v["usage"], true);
                if let Some(reason) = v["delta"]["stop_reason"].as_str() {
                    self.finish =
                        Some(translate::anthropic_stop_to_openai(Some(reason)).to_string());
                }
            }
            Some("message_stop") => {}
            Some("error") => {
                let message = v["error"]["message"]
                    .as_str()
                    .unwrap_or("upstream stream error")
                    .to_string();
                usage.set_error(502, message.clone());
                out.push(format!(
                    "data: {}\n\n",
                    json!({"error": {"message": message, "type": "upstream_error"}})
                ));
                self.finalized = true;
                out.push("data: [DONE]\n\n".to_string());
            }
            _ => {}
        }
    }

    fn finalize(&mut self, usage: &mut UsageRec, out: &mut Vec<String>) {
        if self.finalized || !self.role_sent {
            self.finalized = true;
            return;
        }
        self.finalized = true;
        let finish = self.finish.clone().unwrap_or_else(|| "stop".to_string());
        out.push(self.chunk(json!({}), Some(&finish)));
        let prompt = usage.prompt_tokens;
        let completion = usage.completion_tokens;
        let cached = usage.cached_tokens.min(prompt);
        out.push(format!(
            "data: {}\n\n",
            json!({
                "id": self.id,
                "object": "chat.completion.chunk",
                "created": self.created,
                "model": self.alias,
                "choices": [],
                "usage": {
                    "prompt_tokens": prompt,
                    "completion_tokens": completion,
                    "total_tokens": prompt + completion,
                    "prompt_tokens_details": {"cached_tokens": cached}
                }
            })
        ));
        out.push("data: [DONE]\n\n".to_string());
    }
}

fn anthropic_to_openai_stream(
    resp: reqwest::Response,
    mut usage: UsageRec,
    alias: String,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut state = AnthToOai::new(&alias);
        let mut terminal = false;
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                terminal = false;
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    for ev in parser.push(&chunk) {
                        if anthropic_stream_terminal(&ev) {
                            terminal = true;
                            ended = true;
                        }
                        if stream_event_failed(ApiFormat::Anthropic, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                        }
                        let mut out = Vec::new();
                        state.handle(&ev, &mut usage, &mut out);
                        for payload in out {
                            yield Ok(Bytes::from(payload));
                        }
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        terminal = false;
                        error_event = false;
                        break;
                    }
                    if ended {
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            terminal = false;
            error_event = false;
        }
        for ev in parser.finish() {
            if anthropic_stream_terminal(&ev) {
                terminal = true;
            }
            if stream_event_failed(ApiFormat::Anthropic, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
            }
            let mut out = Vec::new();
            state.handle(&ev, &mut usage, &mut out);
            for payload in out {
                yield Ok(Bytes::from(payload));
            }
        }
        if error_event {
            // The upstream error was already translated into the OpenAI stream.
        } else if !terminal || read_failed {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            yield Ok(openai_stream_error_event("upstream stream interrupted"));
        } else {
            let mut out = Vec::new();
            state.finalize(&mut usage, &mut out);
            let last = out.len().saturating_sub(1);
            for (index, payload) in out.into_iter().enumerate() {
                if index == last {
                    usage.mark_finished();
                }
                yield Ok(Bytes::from(payload));
            }
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

// ---------------------------------------------------------------------------
// OpenAI -> Anthropic stream translation
// ---------------------------------------------------------------------------

struct PendingTool {
    index: Option<u64>,
    id: String,
    name: String,
    started: bool,
}

struct OaiToAnth {
    alias: String,
    count_estimates: bool,
    id: String,
    started: bool,
    next_index: u64,
    open_text: Option<u64>,
    open_thinking: Option<u64>,
    tools: HashMap<u64, PendingTool>,
    finish: Option<String>,
    finalized: bool,
    saw_usage: bool,
}

impl OaiToAnth {
    fn new(alias: &str) -> Self {
        Self {
            alias: alias.to_string(),
            count_estimates: true,
            id: format!("msg_{}", uuid::Uuid::new_v4().simple()),
            started: false,
            next_index: 0,
            open_text: None,
            open_thinking: None,
            tools: HashMap::new(),
            finish: None,
            finalized: false,
            saw_usage: false,
        }
    }

    /// The conversion may already have accounted for the same output in an
    /// earlier layer (Responses -> Chat). Avoid double counting in that case.
    fn skip_estimates(&mut self) {
        self.count_estimates = false;
    }

    fn estimate(&self, usage: &mut UsageRec, text: &str) {
        if self.count_estimates {
            usage.add_estimated_completion(text);
        }
    }

    fn event(&self, name: &str, value: Value) -> String {
        format!(
            "event: {name}\ndata: {}\n\n",
            serde_json::to_string(&value).unwrap_or_else(|_| "{}".into())
        )
    }

    fn start(&mut self, v: &Value, usage: &UsageRec, out: &mut Vec<String>) {
        if self.started {
            return;
        }
        self.started = true;
        if let Some(id) = v.get("id").and_then(|i| i.as_str()) {
            let clean = id
                .trim_start_matches("chatcmpl-")
                .trim_start_matches("msg_");
            self.id = format!("msg_{clean}");
        }
        out.push(self.event(
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": self.id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.alias,
                    "content": [],
                    "stop_reason": Value::Null,
                    "stop_sequence": Value::Null,
                    "usage": {"input_tokens": usage.prompt_tokens, "output_tokens": 0}
                }
            }),
        ));
    }

    fn close_text(&mut self, out: &mut Vec<String>) {
        if let Some(index) = self.open_text.take() {
            out.push(self.event(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": index}),
            ));
        }
    }

    fn close_thinking(&mut self, out: &mut Vec<String>) {
        if let Some(index) = self.open_thinking.take() {
            out.push(self.event(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": index}),
            ));
        }
    }

    fn close_tools(&mut self, out: &mut Vec<String>) {
        let mut indexes: Vec<u64> = self
            .tools
            .values()
            .filter(|tool| tool.started)
            .filter_map(|tool| tool.index)
            .collect();
        indexes.sort_unstable();
        for index in indexes {
            out.push(self.event(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": index}),
            ));
        }
        self.tools.clear();
    }

    fn start_tool(&mut self, key: u64, out: &mut Vec<String>) {
        let Some((id, name)) = self.tools.get(&key).and_then(|tool| {
            if tool.started {
                return None;
            }
            Some((
                if tool.id.is_empty() {
                    "toolu_gateway".to_string()
                } else {
                    tool.id.clone()
                },
                if tool.name.is_empty() {
                    "tool".to_string()
                } else {
                    tool.name.clone()
                },
            ))
        }) else {
            return;
        };
        self.close_text(out);
        self.close_thinking(out);
        // Allocate the downstream block index in actual emission order, not in
        // the order upstream tool names first appeared.
        let index = if let Some(index) = self.tools.get(&key).and_then(|tool| tool.index) {
            index
        } else {
            let index = self.next_index;
            self.next_index += 1;
            if let Some(tool) = self.tools.get_mut(&key) {
                tool.index = Some(index);
            }
            index
        };
        out.push(self.event(
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {
                    "type": "tool_use",
                    "id": id,
                    "name": name,
                    "input": {}
                }
            }),
        ));
        if let Some(tool) = self.tools.get_mut(&key) {
            tool.started = true;
        }
    }

    fn start_pending_tools(&mut self, out: &mut Vec<String>) {
        let keys: Vec<u64> = self
            .tools
            .iter()
            .filter(|(_, tool)| !tool.started)
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            self.start_tool(key, out);
        }
    }

    fn close_blocks(&mut self, out: &mut Vec<String>) {
        self.start_pending_tools(out);
        let mut indexes = Vec::new();
        if let Some(index) = self.open_text.take() {
            indexes.push(index);
        }
        if let Some(index) = self.open_thinking.take() {
            indexes.push(index);
        }
        indexes.extend(
            self.tools
                .values()
                .filter(|tool| tool.started)
                .filter_map(|tool| tool.index),
        );
        self.tools.clear();
        indexes.sort_unstable();
        for index in indexes {
            out.push(self.event(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": index}),
            ));
        }
    }

    fn ensure_thinking_block(&mut self, out: &mut Vec<String>) {
        if self.open_thinking.is_some() {
            return;
        }
        self.close_text(out);
        let index = self.next_index;
        self.next_index += 1;
        self.open_thinking = Some(index);
        out.push(self.event(
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {"type": "thinking", "thinking": ""}
            }),
        ));
    }

    fn ensure_text_block(&mut self, out: &mut Vec<String>) {
        if self.open_text.is_some() {
            return;
        }
        self.close_thinking(out);
        self.start_pending_tools(out);
        self.close_tools(out);
        let index = self.next_index;
        self.next_index += 1;
        self.open_text = Some(index);
        out.push(self.event(
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": index,
                "content_block": {"type": "text", "text": ""}
            }),
        ));
    }

    fn handle(&mut self, ev: &SseEvent, usage: &mut UsageRec, out: &mut Vec<String>) {
        let data = ev.data.trim();
        if data == "[DONE]" {
            return;
        }
        let Ok(v) = serde_json::from_str::<Value>(data) else {
            return;
        };
        if v.get("error").is_some() {
            let message = v["error"]["message"]
                .as_str()
                .unwrap_or("upstream stream error")
                .to_string();
            usage.set_error(502, message.clone());
            out.push(self.event(
                "error",
                json!({"type": "error", "error": {"type": "api_error", "message": message}}),
            ));
            self.finalized = true;
            return;
        }

        self.start(&v, usage, out);

        if let Some(choice) = v.get("choices").and_then(|c| c.get(0)) {
            if let Some(delta) = choice.get("delta") {
                // Thinking is emitted before the visible answer.
                if let Some(reasoning) = delta
                    .get("reasoning_content")
                    .or_else(|| delta.get("reasoning"))
                    .and_then(|r| r.as_str())
                {
                    if !reasoning.is_empty() {
                        self.ensure_thinking_block(out);
                        let index = self.open_thinking.unwrap();
                        self.estimate(usage, reasoning);
                        usage.mark_first();
                        out.push(self.event(
                            "content_block_delta",
                            json!({
                                "type": "content_block_delta",
                                "index": index,
                                "delta": {"type": "thinking_delta", "thinking": reasoning}
                            }),
                        ));
                    }
                }
                if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                    if !content.is_empty() {
                        self.ensure_text_block(out);
                        let index = self.open_text.unwrap();
                        self.estimate(usage, content);
                        usage.mark_first();
                        out.push(self.event(
                            "content_block_delta",
                            json!({
                                "type": "content_block_delta",
                                "index": index,
                                "delta": {"type": "text_delta", "text": content}
                            }),
                        ));
                    }
                }
                // Chat refusals are visible text for Anthropic clients.
                if let Some(refusal) = delta.get("refusal").and_then(|r| r.as_str()) {
                    if !refusal.is_empty() {
                        self.ensure_text_block(out);
                        let index = self.open_text.unwrap();
                        self.estimate(usage, refusal);
                        usage.mark_first();
                        out.push(self.event(
                            "content_block_delta",
                            json!({
                                "type": "content_block_delta",
                                "index": index,
                                "delta": {"type": "text_delta", "text": refusal}
                            }),
                        ));
                    }
                }

                if let Some(tool_calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
                    for tc in tool_calls {
                        let openai_index = tc.get("index").and_then(|i| i.as_u64()).unwrap_or(0);
                        self.tools
                            .entry(openai_index)
                            .or_insert_with(|| PendingTool {
                                index: None,
                                id: String::new(),
                                name: String::new(),
                                started: false,
                            });
                        if let Some(tool) = self.tools.get_mut(&openai_index) {
                            if let Some(id) = tc.get("id").and_then(|i| i.as_str()) {
                                if !id.is_empty() {
                                    tool.id = id.to_string();
                                }
                            }
                            if let Some(name) = tc
                                .get("function")
                                .and_then(|f| f.get("name"))
                                .and_then(|n| n.as_str())
                            {
                                if !name.is_empty() {
                                    tool.name.push_str(name);
                                }
                            }
                        }
                        let args = tc
                            .get("function")
                            .and_then(|f| f.get("arguments"))
                            .and_then(|a| a.as_str())
                            .filter(|a| !a.is_empty());
                        if args.is_some() {
                            self.start_tool(openai_index, out);
                        }
                        if let Some(args) = args {
                            let index = self
                                .tools
                                .get(&openai_index)
                                .and_then(|tool| tool.index)
                                .unwrap_or(0);
                            self.estimate(usage, args);
                            usage.mark_first();
                            out.push(self.event(
                                "content_block_delta",
                                json!({
                                    "type": "content_block_delta",
                                    "index": index,
                                    "delta": {"type": "input_json_delta", "partial_json": args}
                                }),
                            ));
                        }
                    }
                }
            }

            if let Some(reason) = choice.get("finish_reason").and_then(|r| r.as_str()) {
                self.finish = Some(translate::openai_finish_to_anthropic(Some(reason)).to_string());
            }
        }

        if let Some(u) = v.get("usage").filter(|u| !u.is_null()) {
            let prompt = u.get("prompt_tokens").and_then(|x| x.as_u64());
            let completion = u.get("completion_tokens").and_then(|x| x.as_u64());
            let cached = u
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|x| x.as_u64());
            if prompt.is_some() || completion.is_some() || cached.is_some() {
                usage.set_usage_full(prompt, completion, cached, None);
            }
            let (cost, currency) = extract_reported_cost(u);
            usage.set_reported_cost(cost, currency);
            self.saw_usage = true;
        }
    }

    fn finalize(&mut self, usage: &mut UsageRec, out: &mut Vec<String>) {
        if self.finalized || !self.started {
            self.finalized = true;
            return;
        }
        self.finalized = true;
        self.close_blocks(out);

        let stop = self
            .finish
            .clone()
            .unwrap_or_else(|| "end_turn".to_string());
        let prompt = usage.prompt_tokens;
        let cached = usage.cached_tokens.min(prompt);
        let cache_write = usage.cache_write_tokens.min(prompt.saturating_sub(cached));
        let input_tokens = prompt.saturating_sub(cached).saturating_sub(cache_write);
        out.push(self.event(
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop, "stop_sequence": Value::Null},
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": usage.completion_tokens,
                    "cache_read_input_tokens": cached,
                    "cache_creation_input_tokens": cache_write
                }
            }),
        ));
        out.push(self.event("message_stop", json!({"type": "message_stop"})));
    }
}

fn openai_to_anthropic_stream(
    resp: reqwest::Response,
    mut usage: UsageRec,
    alias: String,
    expected_choices: Option<u64>,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut state = OaiToAnth::new(&alias);
        let mut saw_done = false;
        let mut seen_choices = HashSet::new();
        let mut finished_choices = HashSet::new();
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    for ev in parser.push(&chunk) {
                        if track_openai_choice(&ev, &mut seen_choices, &mut finished_choices) {
                            saw_done = true;
                        }
                        if ev.data.trim() == "[DONE]" {
                            ended = true;
                        }
                        if stream_event_failed(ApiFormat::Openai, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                        }
                        let mut out = Vec::new();
                        state.handle(&ev, &mut usage, &mut out);
                        for payload in out {
                            yield Ok(Bytes::from(payload));
                        }
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        error_event = false;
                        break;
                    }
                    if ended {
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            error_event = false;
        }
        for ev in parser.finish() {
            if track_openai_choice(&ev, &mut seen_choices, &mut finished_choices) {
                saw_done = true;
            }
            if stream_event_failed(ApiFormat::Openai, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
            }
            let mut out = Vec::new();
            state.handle(&ev, &mut usage, &mut out);
            for payload in out {
                yield Ok(Bytes::from(payload));
            }
        }
        let terminal = openai_stream_complete(
            saw_done,
            &seen_choices,
            &finished_choices,
            expected_choices,
        );
        if error_event {
            // The upstream error was already translated into the Anthropic stream.
        } else if !terminal || read_failed {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            yield Ok(anthropic_stream_error_event("upstream stream interrupted"));
        } else {
            let mut out = Vec::new();
            state.finalize(&mut usage, &mut out);
            let last = out.len().saturating_sub(1);
            for (index, payload) in out.into_iter().enumerate() {
                if index == last {
                    usage.mark_finished();
                }
                yield Ok(Bytes::from(payload));
            }
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

// ---------------------------------------------------------------------------
// OpenAI Responses stream translation
// ---------------------------------------------------------------------------

struct ResponsesStoreCtx {
    store: Arc<ResponseStore>,
    input_items: Vec<Value>,
    previous_response_id: Option<String>,
    native: bool,
    route_id: String,
    model: String,
    provider: String,
    upstream_model: String,
    provider_base_url: String,
}

impl ResponsesStoreCtx {
    fn meta(&self) -> crate::store::TurnMeta {
        crate::store::TurnMeta {
            native: self.native,
            route_id: self.route_id.clone(),
            model: self.model.clone(),
            provider: self.provider.clone(),
            upstream_model: self.upstream_model.clone(),
            provider_base_url: self.provider_base_url.clone(),
        }
    }
}

struct ResponsesTextItem {
    output_index: u64,
    item_id: String,
    text: String,
}

struct ResponsesReasoningItem {
    output_index: u64,
    item_id: String,
    text: String,
}

struct ResponsesToolItem {
    output_index: u64,
    item_id: String,
    call_id: String,
    name: String,
    args: String,
    started: bool,
}

/// Usage fields actually reported by upstream. Missing fields stay `None` so
/// the final response can use the latest running estimate instead of freezing
/// a mid-stream snapshot.
#[derive(Default)]
struct ChatUsage {
    prompt: Option<u64>,
    completion: Option<u64>,
    cached: Option<u64>,
}

/// Upstream Chat Completions chunks -> OpenAI Responses API events.
/// Upper bound on retained tool-call metadata per converted stream.
const MAX_TOOL_ITEMS: usize = 128;

struct ChatToResponses {
    alias: String,
    count_estimates: bool,
    id: String,
    created_at: i64,
    started: bool,
    seq: u64,
    next_output: u64,
    text: Option<ResponsesTextItem>,
    reasoning: Option<ResponsesReasoningItem>,
    refusal: Option<ResponsesTextItem>,
    tools: HashMap<u64, ResponsesToolItem>,
    completed: Vec<(u64, Value)>,
    finish: Option<String>,
    usage: ChatUsage,
    finalized: bool,
    store: Option<ResponsesStoreCtx>,
    stored: bool,
    previous_response_id: Option<String>,
    output_bytes: usize,
    oversized: bool,
}

impl ChatToResponses {
    fn new(alias: &str, store: Option<ResponsesStoreCtx>) -> Self {
        let previous_response_id = store
            .as_ref()
            .and_then(|ctx| ctx.previous_response_id.clone());
        Self {
            alias: alias.to_string(),
            count_estimates: true,
            id: format!("resp_{}", uuid::Uuid::new_v4().simple()),
            created_at: now_secs(),
            started: false,
            seq: 0,
            next_output: 0,
            text: None,
            reasoning: None,
            refusal: None,
            tools: HashMap::new(),
            completed: Vec::new(),
            finish: None,
            usage: ChatUsage::default(),
            finalized: false,
            store,
            stored: false,
            previous_response_id,
            output_bytes: 0,
            oversized: false,
        }
    }

    /// The conversion may already have accounted for the same output in an
    /// earlier layer (Anthropic -> Chat). Avoid double counting in that case.
    fn skip_estimates(&mut self) {
        self.count_estimates = false;
    }

    fn estimate(&self, usage: &mut UsageRec, text: &str) {
        if self.count_estimates {
            usage.add_estimated_completion(text);
        }
    }

    fn oversized(&self) -> bool {
        self.oversized
    }

    fn start_tool(&mut self, key: u64, usage: &mut UsageRec, out: &mut Vec<Value>) {
        let Some((output_index, item_id, call_id, name)) = ({
            let Some(item) = self.tools.get_mut(&key) else {
                return;
            };
            if item.started {
                return;
            }
            if item.call_id.is_empty() {
                let generated = format!("call_{}", uuid::Uuid::new_v4().simple());
                let extra = generated.len();
                if self.output_bytes.saturating_add(extra) > usage.output_max_bytes {
                    self.oversized = true;
                    usage.set_error(502, "stream output exceeded configured limit".to_string());
                    return;
                }
                self.output_bytes += extra;
                item.call_id = generated;
            }
            if item.name.is_empty() {
                let extra = "tool".len();
                if self.output_bytes.saturating_add(extra) > usage.output_max_bytes {
                    self.oversized = true;
                    usage.set_error(502, "stream output exceeded configured limit".to_string());
                    return;
                }
                self.output_bytes += extra;
                item.name = "tool".to_string();
            }
            item.started = true;
            Some((
                item.output_index,
                item.item_id.clone(),
                item.call_id.clone(),
                item.name.clone(),
            ))
        }) else {
            return;
        };
        out.push(self.event(
            "response.output_item.added",
            json!({
                "output_index": output_index,
                "item": {
                    "type": "function_call",
                    "id": item_id,
                    "call_id": call_id,
                    "name": name,
                    "arguments": "",
                    "status": "in_progress"
                }
            }),
        ));
    }

    fn event(&mut self, typ: &str, mut data: Value) -> Value {
        if let Some(obj) = data.as_object_mut() {
            obj.insert("type".into(), json!(typ));
            obj.insert("sequence_number".into(), json!(self.seq));
        }
        self.seq += 1;
        data
    }

    fn envelope(
        &self,
        status: &str,
        output: Vec<Value>,
        usage: Option<(u64, u64, u64, u64)>,
        incomplete: Option<String>,
    ) -> Value {
        translate::ResponseEnvelope {
            id: self.id.clone(),
            model: self.alias.clone(),
            created_at: self.created_at,
            status: status.to_string(),
            output,
            usage,
            incomplete_reason: incomplete,
            instructions: None,
            tools: Vec::new(),
            tool_choice: json!("auto"),
            parallel_tool_calls: true,
            previous_response_id: self.previous_response_id.clone(),
            metadata: None,
        }
        .to_json()
    }

    fn start(&mut self, out: &mut Vec<Value>) {
        if self.started {
            return;
        }
        self.started = true;
        let created = self.envelope("in_progress", Vec::new(), None, None);
        out.push(self.event("response.created", json!({"response": created})));
        let in_progress = self.envelope("in_progress", Vec::new(), None, None);
        out.push(self.event("response.in_progress", json!({"response": in_progress})));
    }

    fn handle_chunk(&mut self, v: &Value, usage: &mut UsageRec, out: &mut Vec<Value>) {
        self.start(out);
        let choice = v.get("choices").and_then(|c| c.get(0));

        if let Some(delta) = choice.and_then(|c| c.get("delta")) {
            if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                if !content.is_empty() {
                    if self.text.is_none() {
                        let output_index = self.next_output;
                        self.next_output += 1;
                        let item_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
                        out.push(self.event(
                            "response.output_item.added",
                            json!({
                                "output_index": output_index,
                                "item": {
                                    "type": "message",
                                    "id": item_id,
                                    "status": "in_progress",
                                    "role": "assistant",
                                    "content": []
                                }
                            }),
                        ));
                        out.push(self.event(
                            "response.content_part.added",
                            json!({
                                "item_id": item_id,
                                "output_index": output_index,
                                "content_index": 0,
                                "part": {"type": "output_text", "text": "", "annotations": []}
                            }),
                        ));
                        self.text = Some(ResponsesTextItem {
                            output_index,
                            item_id,
                            text: String::new(),
                        });
                    }
                    if self.output_bytes.saturating_add(content.len()) > usage.output_max_bytes {
                        self.oversized = true;
                        usage.set_error(502, "stream output exceeded configured limit".to_string());
                    } else {
                        let item = self.text.as_mut().unwrap();
                        item.text.push_str(content);
                        self.output_bytes += content.len();
                    }
                    let (item_id, output_index) = {
                        let item = self.text.as_ref().unwrap();
                        (item.item_id.clone(), item.output_index)
                    };
                    self.estimate(usage, content);
                    usage.mark_first();
                    out.push(self.event(
                        "response.output_text.delta",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "content_index": 0,
                            "delta": content,
                            "logprobs": []
                        }),
                    ));
                }
            }

            if let Some(reasoning) = delta
                .get("reasoning_content")
                .or_else(|| delta.get("reasoning"))
                .and_then(|r| r.as_str())
            {
                if !reasoning.is_empty() {
                    if self.reasoning.is_none() {
                        let output_index = self.next_output;
                        self.next_output += 1;
                        let item_id = format!("rs_{}", uuid::Uuid::new_v4().simple());
                        out.push(self.event(
                            "response.output_item.added",
                            json!({
                                "output_index": output_index,
                                "item": {"type": "reasoning", "id": item_id, "summary": []}
                            }),
                        ));
                        out.push(self.event(
                            "response.reasoning_summary_part.added",
                            json!({
                                "item_id": item_id,
                                "output_index": output_index,
                                "summary_index": 0,
                                "part": {"type": "summary_text", "text": ""}
                            }),
                        ));
                        self.reasoning = Some(ResponsesReasoningItem {
                            output_index,
                            item_id,
                            text: String::new(),
                        });
                    }
                    if self.output_bytes.saturating_add(reasoning.len()) > usage.output_max_bytes {
                        self.oversized = true;
                        usage.set_error(502, "stream output exceeded configured limit".to_string());
                    } else {
                        let item = self.reasoning.as_mut().unwrap();
                        item.text.push_str(reasoning);
                        self.output_bytes += reasoning.len();
                    }
                    let (item_id, output_index) = {
                        let item = self.reasoning.as_ref().unwrap();
                        (item.item_id.clone(), item.output_index)
                    };
                    self.estimate(usage, reasoning);
                    usage.mark_first();
                    out.push(self.event(
                        "response.reasoning_summary_text.delta",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "summary_index": 0,
                            "delta": reasoning
                        }),
                    ));
                }
            }

            if let Some(refusal) = delta.get("refusal").and_then(|r| r.as_str()) {
                if !refusal.is_empty() {
                    if self.refusal.is_none() {
                        let output_index = self.next_output;
                        self.next_output += 1;
                        let item_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
                        out.push(self.event(
                            "response.output_item.added",
                            json!({
                                "output_index": output_index,
                                "item": {
                                    "type": "message",
                                    "id": item_id,
                                    "status": "in_progress",
                                    "role": "assistant",
                                    "content": []
                                }
                            }),
                        ));
                        out.push(self.event(
                            "response.content_part.added",
                            json!({
                                "item_id": item_id,
                                "output_index": output_index,
                                "content_index": 0,
                                "part": {"type": "refusal", "refusal": ""}
                            }),
                        ));
                        self.refusal = Some(ResponsesTextItem {
                            output_index,
                            item_id,
                            text: String::new(),
                        });
                    }
                    if self.output_bytes.saturating_add(refusal.len()) > usage.output_max_bytes {
                        self.oversized = true;
                        usage.set_error(502, "stream output exceeded configured limit".to_string());
                    } else {
                        let item = self.refusal.as_mut().unwrap();
                        item.text.push_str(refusal);
                        self.output_bytes += refusal.len();
                    }
                    let (item_id, output_index) = {
                        let item = self.refusal.as_ref().unwrap();
                        (item.item_id.clone(), item.output_index)
                    };
                    self.estimate(usage, refusal);
                    usage.mark_first();
                    out.push(self.event(
                        "response.refusal.delta",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "content_index": 0,
                            "delta": refusal
                        }),
                    ));
                }
            }

            if let Some(tool_calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
                for tc in tool_calls {
                    let index = tc.get("index").and_then(|i| i.as_u64()).unwrap_or(0);
                    if !self.tools.contains_key(&index) {
                        let output_index = self.next_output;
                        self.next_output += 1;
                        let item_id = format!("fc_{}", uuid::Uuid::new_v4().simple());
                        let call_id = tc
                            .get("id")
                            .and_then(|i| i.as_str())
                            .map(str::to_string)
                            .unwrap_or_default();
                        let metadata = item_id.len() + call_id.len();
                        if self.tools.len() >= MAX_TOOL_ITEMS
                            || self.output_bytes.saturating_add(metadata) > usage.output_max_bytes
                        {
                            self.oversized = true;
                            usage.set_error(
                                502,
                                "stream output exceeded configured limit".to_string(),
                            );
                            continue;
                        }
                        self.output_bytes += metadata;
                        self.tools.insert(
                            index,
                            ResponsesToolItem {
                                output_index,
                                item_id,
                                call_id,
                                name: String::new(),
                                args: String::new(),
                                started: false,
                            },
                        );
                    }

                    if let Some(item) = self.tools.get_mut(&index) {
                        if let Some(id) = tc
                            .get("id")
                            .and_then(|i| i.as_str())
                            .filter(|id| !id.is_empty())
                        {
                            if item.call_id != id {
                                let old = item.call_id.len();
                                let new = id.len();
                                if new > old
                                    && self.output_bytes.saturating_add(new - old)
                                        > usage.output_max_bytes
                                {
                                    self.oversized = true;
                                    usage.set_error(
                                        502,
                                        "stream output exceeded configured limit".to_string(),
                                    );
                                } else {
                                    item.call_id = id.to_string();
                                    if new > old {
                                        self.output_bytes += new - old;
                                    } else {
                                        self.output_bytes =
                                            self.output_bytes.saturating_sub(old - new);
                                    }
                                }
                            }
                        }
                        if let Some(name) = tc
                            .get("function")
                            .and_then(|f| f.get("name"))
                            .and_then(|n| n.as_str())
                        {
                            // OpenAI sends incremental name fragments; concatenate
                            // strictly so repeated identical fragments are kept.
                            if !name.is_empty() {
                                if self.output_bytes.saturating_add(name.len())
                                    > usage.output_max_bytes
                                {
                                    self.oversized = true;
                                    usage.set_error(
                                        502,
                                        "stream output exceeded configured limit".to_string(),
                                    );
                                } else {
                                    item.name.push_str(name);
                                    self.output_bytes += name.len();
                                }
                            }
                        }
                    }

                    let args = tc
                        .get("function")
                        .and_then(|f| f.get("arguments"))
                        .and_then(|a| a.as_str())
                        .filter(|a| !a.is_empty());
                    if args.is_some() {
                        self.start_tool(index, usage, out);
                    }
                    if let Some(args) = args {
                        if self.output_bytes.saturating_add(args.len()) > usage.output_max_bytes {
                            self.oversized = true;
                            usage.set_error(
                                502,
                                "stream output exceeded configured limit".to_string(),
                            );
                        } else if let Some(item) = self.tools.get_mut(&index) {
                            item.args.push_str(args);
                            self.output_bytes += args.len();
                        }
                        let (item_id, output_index) = {
                            let item = self.tools.get(&index).unwrap();
                            (item.item_id.clone(), item.output_index)
                        };
                        self.estimate(usage, args);
                        usage.mark_first();
                        out.push(self.event(
                            "response.function_call_arguments.delta",
                            json!({
                                "item_id": item_id,
                                "output_index": output_index,
                                "delta": args
                            }),
                        ));
                    }
                }
            }
        }

        if let Some(reason) = choice
            .and_then(|c| c.get("finish_reason"))
            .and_then(|r| r.as_str())
        {
            self.finish = Some(reason.to_string());
        }

        if let Some(u) = v.get("usage").filter(|u| !u.is_null()) {
            let p = u.get("prompt_tokens").and_then(|x| x.as_u64());
            let c = u.get("completion_tokens").and_then(|x| x.as_u64());
            let cached = u
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|x| x.as_u64());
            if p.is_some() || c.is_some() || cached.is_some() {
                if let Some(value) = p {
                    self.usage.prompt = Some(value);
                }
                // A reported zero output is treated like a missing field, in
                // line with `set_usage_full`, so later deltas can still update
                // the estimate.
                if let Some(value) = c.filter(|value| *value > 0) {
                    self.usage.completion = Some(value);
                }
                if let Some(value) = cached {
                    self.usage.cached = Some(value);
                }
                // In composite conversions this chunk is synthesized by the
                // previous layer; its values may include estimates and must not
                // be marked as exact upstream usage.
                if self.count_estimates {
                    usage.set_usage_full(p, c, cached, None);
                }
            }
            let (cost, currency) = extract_reported_cost(u);
            usage.set_reported_cost(cost, currency);
        }
    }

    fn finalize(&mut self, usage: &mut UsageRec, out: &mut Vec<Value>) {
        if self.finalized || !self.started {
            self.finalized = true;
            return;
        }
        self.finalized = true;

        if let Some(item) = self.text.take() {
            out.push(self.event(
                "response.output_text.done",
                json!({
                    "item_id": item.item_id,
                    "output_index": item.output_index,
                    "content_index": 0,
                    "text": item.text,
                    "logprobs": []
                }),
            ));
            let part = json!({"type": "output_text", "text": item.text, "annotations": []});
            out.push(self.event(
                "response.content_part.done",
                json!({
                    "item_id": item.item_id,
                    "output_index": item.output_index,
                    "content_index": 0,
                    "part": part
                }),
            ));
            let message = json!({
                "type": "message",
                "id": item.item_id,
                "status": "completed",
                "role": "assistant",
                "content": [part]
            });
            out.push(self.event(
                "response.output_item.done",
                json!({"output_index": item.output_index, "item": message}),
            ));
            self.completed.push((item.output_index, message));
        }

        let mut tool_indices: Vec<u64> = self.tools.keys().copied().collect();
        tool_indices.sort_unstable();
        for index in &tool_indices {
            self.start_tool(*index, usage, out);
        }
        for index in tool_indices {
            if let Some(item) = self.tools.remove(&index) {
                out.push(self.event(
                    "response.function_call_arguments.done",
                    json!({
                        "item_id": item.item_id,
                        "output_index": item.output_index,
                        "arguments": item.args
                    }),
                ));
                let fc = json!({
                    "type": "function_call",
                    "id": item.item_id,
                    "call_id": item.call_id,
                    "name": item.name,
                    "arguments": item.args,
                    "status": "completed"
                });
                out.push(self.event(
                    "response.output_item.done",
                    json!({"output_index": item.output_index, "item": fc}),
                ));
                self.completed.push((item.output_index, fc));
            }
        }

        if let Some(item) = self.refusal.take() {
            out.push(self.event(
                "response.refusal.done",
                json!({
                    "item_id": item.item_id,
                    "output_index": item.output_index,
                    "content_index": 0,
                    "refusal": item.text
                }),
            ));
            let part = json!({"type": "refusal", "refusal": item.text});
            out.push(self.event(
                "response.content_part.done",
                json!({
                    "item_id": item.item_id,
                    "output_index": item.output_index,
                    "content_index": 0,
                    "part": part
                }),
            ));
            let message = json!({
                "type": "message",
                "id": item.item_id,
                "status": "completed",
                "role": "assistant",
                "content": [part]
            });
            out.push(self.event(
                "response.output_item.done",
                json!({"output_index": item.output_index, "item": message}),
            ));
            self.completed.push((item.output_index, message));
        }

        if let Some(item) = self.reasoning.take() {
            out.push(self.event(
                "response.reasoning_summary_text.done",
                json!({
                    "item_id": item.item_id,
                    "output_index": item.output_index,
                    "summary_index": 0,
                    "text": item.text
                }),
            ));
            out.push(self.event(
                "response.reasoning_summary_part.done",
                json!({
                    "item_id": item.item_id,
                    "output_index": item.output_index,
                    "summary_index": 0,
                    "part": {"type": "summary_text", "text": item.text}
                }),
            ));
            let reasoning = json!({
                "type": "reasoning",
                "id": item.item_id,
                "summary": [{"type": "summary_text", "text": item.text}]
            });
            out.push(self.event(
                "response.output_item.done",
                json!({"output_index": item.output_index, "item": reasoning}),
            ));
            self.completed.push((item.output_index, reasoning));
        }

        let mut completed = std::mem::take(&mut self.completed);
        completed.sort_by_key(|(index, _)| *index);
        let mut output: Vec<Value> = completed.into_iter().map(|(_, item)| item).collect();
        crate::sanitize::clean_responses_items(&mut output);

        let prompt = self.usage.prompt.unwrap_or(usage.prompt_tokens);
        let completion = self.usage.completion.unwrap_or(usage.completion_tokens);
        let cached = self.usage.cached.unwrap_or(usage.cached_tokens).min(prompt);
        usage.prompt_tokens = prompt;
        usage.completion_tokens = completion;
        usage.cached_tokens = cached;

        let status = if self.finish.as_deref() == Some("length") {
            "incomplete"
        } else {
            "completed"
        };
        let incomplete = if status == "incomplete" {
            Some("max_output_tokens".to_string())
        } else {
            None
        };
        let mut cache_warning = false;
        if !self.stored {
            if let Some(ctx) = self.store.take() {
                let meta = ctx.meta();
                let mut items = ctx.input_items;
                items.extend(output.iter().cloned());
                let stored = ctx.store.put(self.id.clone(), items, meta);
                self.stored = stored;
                cache_warning = !stored;
            }
        }
        let mut response = self.envelope(
            status,
            output,
            Some((prompt, completion, cached, usage.cache_write_tokens)),
            incomplete,
        );
        if cache_warning {
            if let Some(metadata) = response.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                metadata.insert("gateway_cache".into(), json!("not_stored_too_large"));
            } else if let Some(obj) = response.as_object_mut() {
                obj.insert(
                    "metadata".into(),
                    json!({"gateway_cache": "not_stored_too_large"}),
                );
            }
        }
        let event_type = if status == "incomplete" {
            "response.incomplete"
        } else {
            "response.completed"
        };
        out.push(self.event(event_type, json!({"response": response})));
    }
}

/// Upstream Responses API events -> Chat Completions chunks.
struct ResponsesToChat {
    alias: String,
    id: String,
    created: i64,
    started: bool,
    next_tool: u64,
    tools: HashMap<String, (u64, String, String)>,
    status: Option<String>,
    incomplete_reason: Option<String>,
    finalized: bool,
    text_seen: HashSet<String>,
    refusal_seen: HashSet<String>,
    args_seen: HashSet<String>,
}

impl ResponsesToChat {
    fn new(alias: &str) -> Self {
        Self {
            alias: alias.to_string(),
            id: format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()),
            created: now_secs(),
            started: false,
            next_tool: 0,
            tools: HashMap::new(),
            status: None,
            incomplete_reason: None,
            finalized: false,
            text_seen: HashSet::new(),
            refusal_seen: HashSet::new(),
            args_seen: HashSet::new(),
        }
    }

    fn chunk(&self, delta: Value, finish: Option<&str>) -> Value {
        json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.alias,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]
        })
    }

    fn is_finalized(&self) -> bool {
        self.finalized
    }

    fn text_key(output_index: u64, content_index: u64) -> String {
        format!("{output_index}:{content_index}")
    }

    fn start(&mut self, v: &Value, out: &mut Vec<Value>) {
        if self.started {
            return;
        }
        self.started = true;
        if let Some(id) = v["response"]["id"].as_str() {
            self.id = format!("chatcmpl-{}", id.trim_start_matches("resp_"));
        }
        out.push(self.chunk(json!({"role": "assistant", "content": ""}), None));
    }

    fn handle_event(&mut self, ev: &SseEvent, usage: &mut UsageRec) -> Vec<Value> {
        let mut out: Vec<Value> = Vec::new();
        let Ok(v) = serde_json::from_str::<Value>(&ev.data) else {
            return out;
        };
        let typ = v
            .get("type")
            .and_then(|t| t.as_str())
            .or(ev.event.as_deref())
            .unwrap_or("");
        if !self.started && typ != "response.created" {
            self.start(&v, &mut out);
        }

        match typ {
            "response.created" => {
                self.start(&v, &mut out);
            }
            "response.output_item.added" => {
                let item = &v["item"];
                if item.get("type").and_then(|t| t.as_str()) == Some("function_call") {
                    let item_id = item
                        .get("id")
                        .and_then(|i| i.as_str())
                        .unwrap_or("")
                        .to_string();
                    let index = self.next_tool;
                    self.next_tool += 1;
                    let call_id = item
                        .get("call_id")
                        .and_then(|i| i.as_str())
                        .unwrap_or("call_0")
                        .to_string();
                    let name = item
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("tool")
                        .to_string();
                    self.tools
                        .insert(item_id, (index, call_id.clone(), name.clone()));
                    out.push(self.chunk(
                        json!({"tool_calls": [{
                            "index": index,
                            "id": call_id,
                            "type": "function",
                            "function": {"name": name, "arguments": ""}
                        }]}),
                        None,
                    ));
                }
            }
            "response.output_text.delta" => {
                if let Some(delta) = v["delta"].as_str() {
                    if !delta.is_empty() {
                        let output_index = v["output_index"].as_u64().unwrap_or(0);
                        let content_index = v["content_index"].as_u64().unwrap_or(0);
                        self.text_seen
                            .insert(Self::text_key(output_index, content_index));
                        usage.add_estimated_completion(delta);
                        usage.mark_first();
                        out.push(self.chunk(json!({"content": delta}), None));
                    }
                }
            }
            "response.output_text.done" => {
                if let Some(text) = v["text"].as_str().filter(|text| !text.is_empty()) {
                    let output_index = v["output_index"].as_u64().unwrap_or(0);
                    let content_index = v["content_index"].as_u64().unwrap_or(0);
                    let key = Self::text_key(output_index, content_index);
                    if !self.text_seen.contains(&key) {
                        self.text_seen.insert(key);
                        usage.add_estimated_completion(text);
                        usage.mark_first();
                        out.push(self.chunk(json!({"content": text}), None));
                    }
                }
            }
            "response.refusal.delta" => {
                if let Some(delta) = v["delta"].as_str() {
                    if !delta.is_empty() {
                        let output_index = v["output_index"].as_u64().unwrap_or(0);
                        let content_index = v["content_index"].as_u64().unwrap_or(0);
                        self.refusal_seen
                            .insert(Self::text_key(output_index, content_index));
                        usage.add_estimated_completion(delta);
                        usage.mark_first();
                        out.push(self.chunk(json!({"refusal": delta}), None));
                    }
                }
            }
            "response.refusal.done" => {
                if let Some(text) = v["refusal"]
                    .as_str()
                    .or_else(|| v["text"].as_str())
                    .filter(|text| !text.is_empty())
                {
                    let output_index = v["output_index"].as_u64().unwrap_or(0);
                    let content_index = v["content_index"].as_u64().unwrap_or(0);
                    let key = Self::text_key(output_index, content_index);
                    if !self.refusal_seen.contains(&key) {
                        self.refusal_seen.insert(key);
                        usage.add_estimated_completion(text);
                        usage.mark_first();
                        out.push(self.chunk(json!({"refusal": text}), None));
                    }
                }
            }
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                if let Some(delta) = v["delta"].as_str() {
                    if !delta.is_empty() {
                        usage.add_estimated_completion(delta);
                        usage.mark_first();
                        out.push(self.chunk(json!({"reasoning_content": delta}), None));
                    }
                }
            }
            "response.function_call_arguments.delta" => {
                let item_id = v["item_id"].as_str().unwrap_or("");
                let index = self.tools.get(item_id).map(|(index, _, _)| *index);
                if let (Some(index), Some(delta)) = (index, v["delta"].as_str()) {
                    if !delta.is_empty() {
                        self.args_seen.insert(item_id.to_string());
                        usage.add_estimated_completion(delta);
                        usage.mark_first();
                        out.push(self.chunk(
                            json!({"tool_calls": [{"index": index, "function": {"arguments": delta}}]}),
                            None,
                        ));
                    }
                }
            }
            "response.function_call_arguments.done" => {
                let item_id = v["item_id"].as_str().unwrap_or("").to_string();
                if !self.args_seen.contains(&item_id) {
                    let index = self.tools.get(&item_id).map(|(index, _, _)| *index);
                    if let (Some(index), Some(args)) = (index, v["arguments"].as_str()) {
                        if !args.is_empty() {
                            self.args_seen.insert(item_id);
                            usage.add_estimated_completion(args);
                            usage.mark_first();
                            out.push(self.chunk(
                                json!({"tool_calls": [{"index": index, "function": {"arguments": args}}]}),
                                None,
                            ));
                        }
                    }
                }
            }
            "response.output_item.done" => {
                // Be tolerant of upstreams that skip the delta events and only
                // send the completed output item.
                let item = &v["item"];
                match item.get("type").and_then(|t| t.as_str()) {
                    Some("function_call") => {
                        let item_id = item.get("id").and_then(|i| i.as_str()).unwrap_or("");
                        let index = if let Some((index, _, _)) = self.tools.get(item_id) {
                            *index
                        } else {
                            let index = self.next_tool;
                            self.next_tool += 1;
                            let call_id = item
                                .get("call_id")
                                .and_then(|i| i.as_str())
                                .unwrap_or("call_0")
                                .to_string();
                            let name = item
                                .get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("tool")
                                .to_string();
                            self.tools.insert(
                                item_id.to_string(),
                                (index, call_id.clone(), name.clone()),
                            );
                            out.push(self.chunk(
                                json!({"tool_calls": [{
                                    "index": index,
                                    "id": call_id,
                                    "type": "function",
                                    "function": {"name": name, "arguments": ""}
                                }]}),
                                None,
                            ));
                            index
                        };
                        if !self.args_seen.contains(item_id) {
                            if let Some(args) = item
                                .get("arguments")
                                .and_then(|a| a.as_str())
                                .filter(|a| !a.is_empty())
                            {
                                self.args_seen.insert(item_id.to_string());
                                usage.add_estimated_completion(args);
                                usage.mark_first();
                                out.push(self.chunk(
                                    json!({"tool_calls": [{"index": index, "function": {"arguments": args}}]}),
                                    None,
                                ));
                            }
                        }
                    }
                    Some("message") => {
                        let output_index = v["output_index"].as_u64().unwrap_or(0);
                        let mut unseen = String::new();
                        if let Some(parts) = item.get("content").and_then(|c| c.as_array()) {
                            for (index, part) in parts.iter().enumerate() {
                                if part.get("type").and_then(|t| t.as_str()) != Some("output_text")
                                {
                                    continue;
                                }
                                let key = Self::text_key(output_index, index as u64);
                                if self.text_seen.contains(&key) {
                                    continue;
                                }
                                if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                                    if !text.is_empty() {
                                        unseen.push_str(text);
                                        self.text_seen.insert(key);
                                    }
                                }
                            }
                        } else if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                            let key = Self::text_key(output_index, 0);
                            if !self.text_seen.contains(&key) && !text.is_empty() {
                                unseen.push_str(text);
                                self.text_seen.insert(key);
                            }
                        }
                        if !unseen.is_empty() {
                            usage.add_estimated_completion(&unseen);
                            usage.mark_first();
                            out.push(self.chunk(json!({"content": unseen}), None));
                        }
                        if let Some(parts) = item.get("content").and_then(|c| c.as_array()) {
                            for (index, part) in parts.iter().enumerate() {
                                if part.get("type").and_then(|t| t.as_str()) != Some("refusal") {
                                    continue;
                                }
                                let key = Self::text_key(output_index, index as u64);
                                if self.refusal_seen.contains(&key) {
                                    continue;
                                }
                                let Some(text) = part.get("refusal").and_then(|t| t.as_str())
                                else {
                                    continue;
                                };
                                if text.is_empty() {
                                    continue;
                                }
                                self.refusal_seen.insert(key);
                                usage.add_estimated_completion(text);
                                usage.mark_first();
                                out.push(self.chunk(json!({"refusal": text}), None));
                            }
                        }
                    }
                    _ => {}
                }
            }
            "response.completed" | "response.incomplete" | "response.failed" => {
                let resp = &v["response"];
                if let Some(u) = resp.get("usage").filter(|u| !u.is_null()) {
                    let p = u.get("input_tokens").and_then(|x| x.as_u64());
                    let c = u.get("output_tokens").and_then(|x| x.as_u64());
                    let cached = u
                        .get("input_tokens_details")
                        .and_then(|d| d.get("cached_tokens"))
                        .and_then(|x| x.as_u64());
                    let cache_write = u
                        .get("input_tokens_details")
                        .and_then(|d| d.get("cache_write_tokens"))
                        .and_then(|x| x.as_u64());
                    if p.is_some() || c.is_some() || cached.is_some() || cache_write.is_some() {
                        usage.set_usage_full(p, c, cached, cache_write);
                    }
                    let (cost, currency) = extract_reported_cost(u);
                    usage.set_reported_cost(cost, currency);
                }
                self.status = resp["status"].as_str().map(|s| s.to_string());
                self.incomplete_reason = resp["incomplete_details"]["reason"]
                    .as_str()
                    .map(|s| s.to_string());
                if typ == "response.failed" {
                    let message = resp["error"]["message"]
                        .as_str()
                        .unwrap_or("response failed")
                        .to_string();
                    usage.set_error(502, message.clone());
                    out.push(json!({"error": {"message": message, "type": "upstream_error"}}));
                    self.finalized = true;
                    return out;
                }
            }
            "error" => {
                let message = v["message"]
                    .as_str()
                    .or(v["error"]["message"].as_str())
                    .unwrap_or("upstream stream error")
                    .to_string();
                usage.set_error(502, message.clone());
                out.push(json!({"error": {"message": message, "type": "upstream_error"}}));
                self.finalized = true;
            }
            _ => {}
        }
        out
    }

    fn finalize(&mut self, usage: &mut UsageRec) -> Vec<Value> {
        let mut out = Vec::new();
        if self.finalized || !self.started {
            self.finalized = true;
            return out;
        }
        self.finalized = true;

        // Truncation takes priority over the presence of tool calls.
        let finish = if self.incomplete_reason.as_deref() == Some("max_output_tokens")
            || self.status.as_deref() == Some("incomplete")
        {
            "length"
        } else if !self.tools.is_empty() {
            "tool_calls"
        } else {
            "stop"
        };
        out.push(self.chunk(json!({}), Some(finish)));
        let prompt = usage.prompt_tokens;
        let completion = usage.completion_tokens;
        let cached = usage.cached_tokens.min(prompt);
        out.push(json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.alias,
            "choices": [],
            "usage": {
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "total_tokens": prompt + completion,
                "prompt_tokens_details": {"cached_tokens": cached}
            }
        }));
        out
    }
}

fn parse_sse_payload(payload: &str) -> Option<Value> {
    let data = payload.trim().strip_prefix("data:")?.trim();
    if data == "[DONE]" {
        return None;
    }
    serde_json::from_str(data).ok()
}

fn sse_event_bytes(event: &Value) -> Bytes {
    let typ = event["type"].as_str().unwrap_or("message").to_string();
    let data = serde_json::to_string(event).unwrap_or_default();
    Bytes::from(format!("event: {typ}\ndata: {data}\n\n"))
}

fn encode_sse_event(event: &SseEvent, data: &str) -> Bytes {
    match event.event.as_deref() {
        Some(name) => Bytes::from(format!("event: {name}\ndata: {data}\n\n")),
        None => Bytes::from(format!("data: {data}\n\n")),
    }
}

fn add_cache_warning(value: &mut Value) {
    let Some(response) = value
        .get_mut("response")
        .and_then(|response| response.as_object_mut())
    else {
        return;
    };
    let metadata = response.entry("metadata").or_insert_with(|| json!({}));
    if !metadata.is_object() {
        *metadata = json!({});
    }
    if let Some(metadata) = metadata.as_object_mut() {
        metadata.insert("gateway_cache".into(), json!("not_stored_too_large"));
    }
}

fn chat_to_responses_stream(
    resp: reqwest::Response,
    mut usage: UsageRec,
    alias: String,
    store: Option<ResponsesStoreCtx>,
    expected_choices: Option<u64>,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut state = ChatToResponses::new(&alias, store);
        let mut saw_done = false;
        let mut seen_choices = HashSet::new();
        let mut finished_choices = HashSet::new();
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        let mut error_message: Option<String> = None;
        'stream: while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    for ev in parser.push(&chunk) {
                        if track_openai_choice(&ev, &mut seen_choices, &mut finished_choices) {
                            saw_done = true;
                        }
                        if ev.data.trim() == "[DONE]" {
                            ended = true;
                        }
                        if stream_event_failed(ApiFormat::Openai, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                            error_message = stream_error_message(&ev);
                        }
                        if let Ok(v) = serde_json::from_str::<Value>(&ev.data) {
                            let mut out = Vec::new();
                            state.handle_chunk(&v, &mut usage, &mut out);
                            for event in out {
                                yield Ok(sse_event_bytes(&event));
                            }
                            if state.oversized() {
                                break 'stream;
                            }
                        }
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        error_event = false;
                        break;
                    }
                    if ended {
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            error_event = false;
        }
        for ev in parser.finish() {
            if track_openai_choice(&ev, &mut seen_choices, &mut finished_choices) {
                saw_done = true;
            }
            if stream_event_failed(ApiFormat::Openai, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                error_message = stream_error_message(&ev);
            }
            if let Ok(v) = serde_json::from_str::<Value>(&ev.data) {
                let mut out = Vec::new();
                state.handle_chunk(&v, &mut usage, &mut out);
                for event in out {
                    yield Ok(sse_event_bytes(&event));
                }
            }
        }
        let terminal = openai_stream_complete(
            saw_done,
            &seen_choices,
            &finished_choices,
            expected_choices,
        );
        if !terminal || error_event || read_failed || state.oversized() {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            yield Ok(responses_stream_error_event(
                &state.id,
                &alias,
                error_message
                    .as_deref()
                    .unwrap_or("upstream stream interrupted"),
            ));
        } else {
            let mut out = Vec::new();
            state.finalize(&mut usage, &mut out);
            let last = out.len().saturating_sub(1);
            for (index, event) in out.into_iter().enumerate() {
                if index == last {
                    usage.mark_finished();
                }
                yield Ok(sse_event_bytes(&event));
            }
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

fn anthropic_to_responses_stream(
    resp: reqwest::Response,
    mut usage: UsageRec,
    alias: String,
    store: Option<ResponsesStoreCtx>,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut a2c = AnthToOai::new(&alias);
        let mut c2r = ChatToResponses::new(&alias, store);
        c2r.skip_estimates();
        let mut terminal = false;
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        let mut error_message: Option<String> = None;
        'stream: while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                terminal = false;
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    for ev in parser.push(&chunk) {
                        if anthropic_stream_terminal(&ev) {
                            terminal = true;
                            ended = true;
                        }
                        if stream_event_failed(ApiFormat::Anthropic, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                            error_message = stream_error_message(&ev);
                        }
                        let mut payloads = Vec::new();
                        a2c.handle(&ev, &mut usage, &mut payloads);
                        for payload in payloads {
                            if let Some(chat_chunk) = parse_sse_payload(&payload) {
                                let mut out = Vec::new();
                                c2r.handle_chunk(&chat_chunk, &mut usage, &mut out);
                                for event in out {
                                    yield Ok(sse_event_bytes(&event));
                                }
                                if c2r.oversized() {
                                    break 'stream;
                                }
                            }
                        }
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        terminal = false;
                        error_event = false;
                        break;
                    }
                    if ended {
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            terminal = false;
            error_event = false;
        }
        for ev in parser.finish() {
            if anthropic_stream_terminal(&ev) {
                terminal = true;
            }
            if stream_event_failed(ApiFormat::Anthropic, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                error_message = stream_error_message(&ev);
            }
            let mut payloads = Vec::new();
            a2c.handle(&ev, &mut usage, &mut payloads);
            for payload in payloads {
                if let Some(chat_chunk) = parse_sse_payload(&payload) {
                    let mut out = Vec::new();
                    c2r.handle_chunk(&chat_chunk, &mut usage, &mut out);
                    for event in out {
                        yield Ok(sse_event_bytes(&event));
                    }
                }
            }
        }
        if !terminal || error_event || read_failed || c2r.oversized() {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            yield Ok(responses_stream_error_event(
                &c2r.id,
                &alias,
                error_message
                    .as_deref()
                    .unwrap_or("upstream stream interrupted"),
            ));
        } else {
            let mut payloads = Vec::new();
            a2c.finalize(&mut usage, &mut payloads);
            for payload in payloads {
                if let Some(chat_chunk) = parse_sse_payload(&payload) {
                    let mut out = Vec::new();
                    c2r.handle_chunk(&chat_chunk, &mut usage, &mut out);
                    for event in out {
                        yield Ok(sse_event_bytes(&event));
                    }
                }
            }
            let mut out = Vec::new();
            c2r.finalize(&mut usage, &mut out);
            let last = out.len().saturating_sub(1);
            for (index, event) in out.into_iter().enumerate() {
                if index == last {
                    usage.mark_finished();
                }
                yield Ok(sse_event_bytes(&event));
            }
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

fn responses_to_chat_stream(
    resp: reqwest::Response,
    mut usage: UsageRec,
    alias: String,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut state = ResponsesToChat::new(&alias);
        let mut done_sent = false;
        let mut terminal = false;
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                terminal = false;
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    for ev in parser.push(&chunk) {
                        if responses_stream_terminal(&ev) {
                            terminal = true;
                            ended = true;
                        }
                        if stream_event_failed(ApiFormat::Responses, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                        }
                        for payload in state.handle_event(&ev, &mut usage) {
                            yield Ok(Bytes::from(format!("data: {}\n\n", payload)));
                        }
                        if state.is_finalized() && !done_sent {
                            yield Ok(Bytes::from("data: [DONE]\n\n"));
                            done_sent = true;
                        }
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        terminal = false;
                        error_event = false;
                        break;
                    }
                    if ended {
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            terminal = false;
            error_event = false;
        }
        for ev in parser.finish() {
            if responses_stream_terminal(&ev) {
                terminal = true;
            }
            if stream_event_failed(ApiFormat::Responses, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
            }
            for payload in state.handle_event(&ev, &mut usage) {
                yield Ok(Bytes::from(format!("data: {}\n\n", payload)));
            }
            if state.is_finalized() && !done_sent {
                yield Ok(Bytes::from("data: [DONE]\n\n"));
                done_sent = true;
            }
        }
        if error_event {
            // ResponsesToChat already emitted the upstream error payload.
        } else if !terminal || read_failed {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            yield Ok(openai_stream_error_event("upstream stream interrupted"));
        } else {
            let payloads = state.finalize(&mut usage);
            let done_pending = state.is_finalized() && !done_sent;
            let total = payloads.len() + usize::from(done_pending);
            for (index, payload) in payloads.into_iter().enumerate() {
                if index + 1 == total {
                    usage.mark_finished();
                }
                yield Ok(Bytes::from(format!("data: {}\n\n", payload)));
            }
            if done_pending {
                usage.mark_finished();
                yield Ok(Bytes::from("data: [DONE]\n\n"));
            }
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

fn responses_to_anthropic_stream(
    resp: reqwest::Response,
    mut usage: UsageRec,
    alias: String,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut r2c = ResponsesToChat::new(&alias);
        let mut c2a = OaiToAnth::new(&alias);
        c2a.skip_estimates();
        let mut terminal = false;
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                terminal = false;
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    for ev in parser.push(&chunk) {
                        if responses_stream_terminal(&ev) {
                            terminal = true;
                            ended = true;
                        }
                        if stream_event_failed(ApiFormat::Responses, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                        }
                        for chat_chunk in r2c.handle_event(&ev, &mut usage) {
                            let chat_ev = SseEvent {
                                event: None,
                                data: serde_json::to_string(&chat_chunk).unwrap_or_default(),
                            };
                            let mut payloads = Vec::new();
                            c2a.handle(&chat_ev, &mut usage, &mut payloads);
                            for payload in payloads {
                                yield Ok(Bytes::from(payload));
                            }
                        }
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        terminal = false;
                        error_event = false;
                        break;
                    }
                    if ended {
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            terminal = false;
            error_event = false;
        }
        for ev in parser.finish() {
            if responses_stream_terminal(&ev) {
                terminal = true;
            }
            if stream_event_failed(ApiFormat::Responses, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
            }
            for chat_chunk in r2c.handle_event(&ev, &mut usage) {
                let chat_ev = SseEvent {
                    event: None,
                    data: serde_json::to_string(&chat_chunk).unwrap_or_default(),
                };
                let mut payloads = Vec::new();
                c2a.handle(&chat_ev, &mut usage, &mut payloads);
                for payload in payloads {
                    yield Ok(Bytes::from(payload));
                }
            }
        }
        if error_event {
            // The upstream error was already translated into the Anthropic stream.
        } else if !terminal || read_failed {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            yield Ok(anthropic_stream_error_event("upstream stream interrupted"));
        } else {
            for chat_chunk in r2c.finalize(&mut usage) {
                let chat_ev = SseEvent {
                    event: None,
                    data: serde_json::to_string(&chat_chunk).unwrap_or_default(),
                };
                let mut payloads = Vec::new();
                c2a.handle(&chat_ev, &mut usage, &mut payloads);
                for payload in payloads {
                    yield Ok(Bytes::from(payload));
                }
            }
            let mut payloads = Vec::new();
            c2a.finalize(&mut usage, &mut payloads);
            let last = payloads.len().saturating_sub(1);
            for (index, payload) in payloads.into_iter().enumerate() {
                if index == last {
                    usage.mark_finished();
                }
                yield Ok(Bytes::from(payload));
            }
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

fn passthrough_responses(
    resp: reqwest::Response,
    mut usage: UsageRec,
    mut store: Option<ResponsesStoreCtx>,
) -> BoxStream<'static, Result<Bytes, Infallible>> {
    let s = stream! {
        let mut parser = SseParser::new();
        let mut chunks = upstream_stream(resp, usage.stream_idle_timeout);
        let mut terminal = false;
        let mut error_event = false;
        let mut read_failed = false;
        let mut ended = false;
        let mut stored = false;
        while let Some(item) = chunks.next().await {
            if parser.overflowed() {
                usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                terminal = false;
                error_event = false;
                break;
            }
            match item {
                Ok(chunk) => {
                    for ev in parser.push(&chunk) {
                        if responses_stream_terminal(&ev) {
                            terminal = true;
                        }
                        if stream_event_failed(ApiFormat::Responses, &ev) {
                            error_event = true;
                            usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
                        }
                        tap_responses_event(&ev, &mut usage);
                        let mut value = serde_json::from_str::<Value>(&ev.data).ok();
                        if !stored {
                            if let Some(ctx) = store.as_mut() {
                                if let Some(v) = value.as_ref() {
                                    if let Some((id, items)) = completed_response(v, ctx) {
                                        let mut meta = ctx.meta();
                                        meta.native = true;
                                        let ok = ctx.store.put(id, items, meta);
                                        stored = true;
                                        if !ok {
                                            if let Some(v) = value.as_mut() {
                                                add_cache_warning(v);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        let data = value
                            .as_ref()
                            .map(|v| serde_json::to_string(v).unwrap_or_else(|_| ev.data.clone()))
                            .unwrap_or_else(|| ev.data.clone());
                        if responses_stream_terminal(&ev) {
                            usage.mark_finished();
                        }
                        yield Ok(encode_sse_event(&ev, &data));
                        if responses_stream_terminal(&ev) {
                            ended = true;
                            break;
                        }
                    }
                    if ended {
                        break;
                    }
                    if parser.overflowed() {
                        usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
                        terminal = false;
                        error_event = false;
                        break;
                    }
                }
                Err(e) => {
                    usage.set_error(502, format!("upstream stream error: {e}"));
                    read_failed = true;
                    break;
                }
            }
        }
        if parser.overflowed() {
            usage.set_error(502, "upstream SSE event exceeded 4 MiB".to_string());
            terminal = false;
            error_event = false;
        }
        for ev in parser.finish() {
            if responses_stream_terminal(&ev) {
                terminal = true;
            }
            if stream_event_failed(ApiFormat::Responses, &ev) {
                error_event = true;
                usage.set_error(502, stream_error_message(&ev).unwrap_or_else(|| "upstream stream error".to_string()));
            }
            tap_responses_event(&ev, &mut usage);
            let mut value = serde_json::from_str::<Value>(&ev.data).ok();
            if !stored {
                if let Some(ctx) = store.as_mut() {
                    if let Some(v) = value.as_ref() {
                        if let Some((id, items)) = completed_response(v, ctx) {
                            let mut meta = ctx.meta();
                            meta.native = true;
                            let ok = ctx.store.put(id, items, meta);
                            stored = true;
                            if !ok {
                                if let Some(v) = value.as_mut() {
                                    add_cache_warning(v);
                                }
                            }
                        }
                    }
                }
            }
            let data = value
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_else(|_| ev.data.clone()))
                .unwrap_or_else(|| ev.data.clone());
            if responses_stream_terminal(&ev) {
                usage.mark_finished();
                ended = true;
            }
            yield Ok(encode_sse_event(&ev, &data));
            if ended {
                break;
            }
        }
        if (!terminal || read_failed) && !error_event {
            if usage.status < 400 {
                usage.set_error(502, "upstream stream ended unexpectedly".to_string());
            }
            let id = format!("resp_{}", uuid::Uuid::new_v4().simple());
            yield Ok(responses_stream_error_event(
                &id,
                &usage.model,
                "upstream stream interrupted",
            ));
        }
        usage.mark_finished();
        usage.record();
    };
    s.boxed()
}

fn completed_response(value: &Value, ctx: &ResponsesStoreCtx) -> Option<(String, Vec<Value>)> {
    let typ = value.get("type").and_then(|typ| typ.as_str())?;
    if typ != "response.completed" && typ != "response.incomplete" {
        return None;
    }
    let mut response = value.get("response")?.clone();
    crate::sanitize::clean_response(ApiFormat::Responses, &mut response);
    let id = response.get("id").and_then(|id| id.as_str())?.to_string();
    let mut items = ctx.input_items.clone();
    if let Some(output) = response.get("output").and_then(|output| output.as_array()) {
        items.extend(output.iter().cloned());
    }
    Some((id, items))
}

fn tap_responses_event(ev: &SseEvent, usage: &mut UsageRec) {
    let Ok(v) = serde_json::from_str::<Value>(&ev.data) else {
        return;
    };
    let typ = v
        .get("type")
        .and_then(|t| t.as_str())
        .or(ev.event.as_deref())
        .unwrap_or("");
    match typ {
        "response.output_text.delta"
        | "response.function_call_arguments.delta"
        | "response.reasoning_summary_text.delta"
        | "response.reasoning_text.delta"
        | "response.refusal.delta" => {
            if let Some(delta) = v["delta"].as_str() {
                if !delta.is_empty() {
                    usage.add_estimated_completion(delta);
                    usage.mark_first();
                }
            }
        }
        "response.completed" | "response.incomplete" | "response.failed" => {
            let u = &v["response"]["usage"];
            let p = u.get("input_tokens").and_then(|x| x.as_u64());
            let c = u.get("output_tokens").and_then(|x| x.as_u64());
            let cached = u
                .get("input_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|x| x.as_u64());
            let cache_write = u
                .get("input_tokens_details")
                .and_then(|d| d.get("cache_write_tokens"))
                .and_then(|x| x.as_u64());
            if p.is_some() || c.is_some() || cached.is_some() || cache_write.is_some() {
                usage.set_usage_full(p, c, cached, cache_write);
            }
            let (cost, currency) = extract_reported_cost(u);
            usage.set_reported_cost(cost, currency);
            if typ == "response.failed" {
                let message = v["response"]["error"]["message"]
                    .as_str()
                    .unwrap_or("response failed")
                    .to_string();
                usage.set_error(502, message);
            }
        }
        "error" => {
            let message = v["message"]
                .as_str()
                .or(v["error"]["message"].as_str())
                .unwrap_or("upstream stream error")
                .to_string();
            usage.set_error(502, message);
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// utilities
// ---------------------------------------------------------------------------

fn sse_response(body: BoxStream<'static, Result<Bytes, Infallible>>) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("x-accel-buffering", "no")
        .body(Body::from_stream(body))
        .unwrap()
}

/// Estimate the prompt including locally cached session history. The new turn
/// is already part of the stored history, so only non-input request fields are
/// added separately.
fn estimate_prompt_with_history(inbound_body: &Value, history_tokens: Option<u64>) -> u64 {
    match history_tokens {
        Some(tokens) => {
            let mut body = inbound_body.clone();
            if let Some(obj) = body.as_object_mut() {
                obj.remove("input");
            }
            estimate_request_tokens(&body) + tokens
        }
        None => estimate_request_tokens(inbound_body),
    }
}

pub fn estimate_request_tokens(v: &Value) -> u64 {
    // Rough accounting of everything that is sent to the model.
    let mut total = 0;
    if let Some(messages) = v.get("messages") {
        total += estimate_value_tokens(messages);
    }
    if let Some(system) = v.get("system") {
        total += estimate_value_tokens(system);
    }
    if let Some(input) = v.get("input") {
        total += estimate_value_tokens(input);
    }
    if let Some(instructions) = v.get("instructions") {
        total += estimate_value_tokens(instructions);
    }
    if let Some(tools) = v.get("tools") {
        total += estimate_value_tokens(tools);
    }
    total + 3
}

fn validate_success_response(kind: Kind, format: ApiFormat, value: &Value) -> Result<(), String> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        let message = error
            .get("message")
            .and_then(|message| message.as_str())
            .or_else(|| error.as_str())
            .unwrap_or("upstream returned an error object");
        return Err(format!("upstream error: {message}"));
    }
    match (kind, format) {
        (Kind::Embeddings, ApiFormat::Openai) => value
            .get("data")
            .is_some_and(Value::is_array)
            .then_some(())
            .ok_or_else(|| "upstream response is missing the embeddings data array".to_string()),
        (Kind::Chat, ApiFormat::Openai) => value
            .get("choices")
            .is_some_and(Value::is_array)
            .then_some(())
            .ok_or_else(|| "upstream response is missing the choices array".to_string()),
        (Kind::Chat, ApiFormat::Anthropic) => value
            .get("content")
            .is_some_and(Value::is_array)
            .then_some(())
            .ok_or_else(|| "upstream response is missing the content array".to_string()),
        (Kind::Chat, ApiFormat::Responses) => {
            let has_output = value.get("output").is_some_and(Value::is_array)
                || value.get("output_text").is_some_and(Value::is_string);
            has_output
                .then_some(())
                .ok_or_else(|| "upstream response is missing the output array".to_string())
        }
        _ => Ok(()),
    }
}

fn extract_error_message(body: &str) -> String {
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        if let Some(m) = v["error"]["message"].as_str() {
            return m.to_string();
        }
        if let Some(m) = v["error"].as_str() {
            return m.to_string();
        }
        if let Some(m) = v["message"].as_str() {
            return m.to_string();
        }
    }
    truncate(body, 800)
}

pub fn error_response(inbound: ApiFormat, status: StatusCode, message: &str) -> Response {
    let body = match inbound {
        ApiFormat::Openai | ApiFormat::Responses => json!({
            "error": {
                "message": message,
                "type": openai_error_type(status),
                "code": status.as_u16()
            }
        }),
        ApiFormat::Anthropic => json!({
            "type": "error",
            "error": {"type": anthropic_error_type(status), "message": message}
        }),
    };
    (status, Json(body)).into_response()
}

fn openai_error_type(status: StatusCode) -> &'static str {
    match status.as_u16() {
        401 => "authentication_error",
        403 => "permission_error",
        429 => "rate_limit_error",
        500..=599 => "server_error",
        _ => "invalid_request_error",
    }
}

fn anthropic_error_type(status: StatusCode) -> &'static str {
    match status.as_u16() {
        400 | 404 | 422 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        429 => "rate_limit_error",
        503 | 529 => "overloaded_error",
        _ => "api_error",
    }
}

fn upstream_error_response(
    inbound: ApiFormat,
    upstream: ApiFormat,
    status: StatusCode,
    body: &str,
) -> Response {
    if inbound == upstream {
        if let Ok(v) = serde_json::from_str::<Value>(body) {
            return (status, Json(v)).into_response();
        }
    }
    error_response(inbound, status, &extract_error_message(body))
}

struct UsageInfo {
    prompt: Option<u64>,
    completion: Option<u64>,
    cached: Option<u64>,
    cache_write: Option<u64>,
    reported_cost: Option<f64>,
    currency: Option<String>,
}

fn extract_reported_cost(usage: &Value) -> (Option<f64>, Option<String>) {
    let list_cost = usage
        .get("opencode")
        .and_then(|opencode| opencode.get("list_cost"))
        .and_then(|value| value.as_f64());
    let actual_cost = usage
        .get("cost")
        .and_then(|value| value.as_f64())
        .filter(|cost| *cost > 0.0);
    let cost = list_cost.or(actual_cost);
    (cost, cost.map(|_| "USD".to_string()))
}

fn estimate_nonstream_output(usage: &mut UsageRec, format: ApiFormat, value: &Value) {
    let estimated = match format {
        ApiFormat::Openai => estimate_value_tokens(&value["choices"]),
        ApiFormat::Anthropic => estimate_value_tokens(&value["content"]),
        ApiFormat::Responses => estimate_value_tokens(&value["output"]),
    };
    if estimated > 0 {
        usage.completion_tokens = estimated;
        usage.estimated = true;
    }
}

fn apply_and_validate_nonstream(
    usage: &mut UsageRec,
    kind: Kind,
    format: ApiFormat,
    value: &Value,
) -> Result<(), String> {
    apply_nonstream_usage(usage, format, value);
    validate_success_response(kind, format, value)
}

fn apply_nonstream_usage(usage: &mut UsageRec, format: ApiFormat, value: &Value) {
    match extract_usage(format, value) {
        Some(info) => {
            let completion_missing = info.completion.is_none();
            usage.set_usage_full(info.prompt, info.completion, info.cached, info.cache_write);
            usage.set_reported_cost(info.reported_cost, info.currency);
            if completion_missing {
                estimate_nonstream_output(usage, format, value);
            }
        }
        None => estimate_nonstream_output(usage, format, value),
    }
}

// Fill only absent upstream counters before cross-format translation. Anthropic
// reports uncached input separately, while the gateway estimate is total input.
fn fill_missing_nonstream_usage(value: &mut Value, format: ApiFormat, usage: &UsageRec) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let entry = obj.entry("usage").or_insert_with(|| json!({}));
    if !entry.is_object() {
        *entry = json!({});
    }
    let Some(counters) = entry.as_object_mut() else {
        return;
    };
    let (input_key, output_key, input) = match format {
        ApiFormat::Openai => ("prompt_tokens", "completion_tokens", usage.prompt_tokens),
        ApiFormat::Responses => ("input_tokens", "output_tokens", usage.prompt_tokens),
        ApiFormat::Anthropic => (
            "input_tokens",
            "output_tokens",
            usage
                .prompt_tokens
                .saturating_sub(usage.cached_tokens)
                .saturating_sub(usage.cache_write_tokens),
        ),
    };
    if counters.get(input_key).and_then(Value::as_u64).is_none() {
        counters.insert(input_key.into(), json!(input));
    }
    if counters.get(output_key).and_then(Value::as_u64).is_none() {
        counters.insert(output_key.into(), json!(usage.completion_tokens));
    }
}

fn extract_usage(format: ApiFormat, v: &Value) -> Option<UsageInfo> {
    match format {
        ApiFormat::Openai => {
            let u = v.get("usage")?;
            let (reported_cost, currency) = extract_reported_cost(u);
            let prompt = u.get("prompt_tokens").and_then(|x| x.as_u64());
            let completion = u.get("completion_tokens").and_then(|x| x.as_u64());
            let cached = u
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|x| x.as_u64());
            if prompt.is_none() && completion.is_none() && cached.is_none() {
                None
            } else {
                Some(UsageInfo {
                    prompt,
                    completion,
                    cached,
                    cache_write: None,
                    reported_cost,
                    currency,
                })
            }
        }
        ApiFormat::Anthropic => {
            let u = v.get("usage")?;
            let (reported_cost, currency) = extract_reported_cost(u);
            let input = u.get("input_tokens").and_then(|x| x.as_u64());
            let cached = u.get("cache_read_input_tokens").and_then(|x| x.as_u64());
            let cache_write = u
                .get("cache_creation_input_tokens")
                .and_then(|x| x.as_u64());
            let completion = u.get("output_tokens").and_then(|x| x.as_u64());
            // Cache counters alone do not cover the uncached input, so the
            // total is only reported when input_tokens itself is present.
            let prompt = input.map(|value| value + cached.unwrap_or(0) + cache_write.unwrap_or(0));
            if prompt.is_none() && completion.is_none() && cached.is_none() && cache_write.is_none()
            {
                None
            } else {
                Some(UsageInfo {
                    prompt,
                    completion,
                    cached,
                    cache_write,
                    reported_cost,
                    currency,
                })
            }
        }
        ApiFormat::Responses => {
            let u = v.get("usage")?;
            let (reported_cost, currency) = extract_reported_cost(u);
            let prompt = u.get("input_tokens").and_then(|x| x.as_u64());
            let completion = u.get("output_tokens").and_then(|x| x.as_u64());
            let cached = u
                .get("input_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|x| x.as_u64());
            let cache_write = u
                .get("input_tokens_details")
                .and_then(|d| d.get("cache_write_tokens"))
                .and_then(|x| x.as_u64());
            if prompt.is_none() && completion.is_none() {
                None
            } else {
                Some(UsageInfo {
                    prompt,
                    completion,
                    cached,
                    cache_write,
                    reported_cost,
                    currency,
                })
            }
        }
    }
}

fn add_route_headers(response: &mut Response, route: &Resolved) {
    let headers = response.headers_mut();
    for (name, value) in [
        ("x-gateway-route", route.route_id.as_str()),
        ("x-gateway-provider", route.provider.id.as_str()),
        ("x-gateway-format", route.format.as_str()),
        ("x-gateway-upstream-model", route.upstream_model.as_str()),
    ] {
        if let Ok(value) = axum::http::HeaderValue::from_str(value) {
            headers.insert(axum::http::HeaderName::from_static(name), value);
        }
    }
}

pub fn normalize_base(base: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    if let Ok(url) = reqwest::Url::parse(trimmed) {
        if url.path().is_empty() || url.path() == "/" {
            return format!("{trimmed}/v1");
        }
    }
    trimmed.to_string()
}

fn endpoint_url(base: &str, format: ApiFormat, kind: Kind) -> String {
    let base = normalize_base(base);
    let path = match kind {
        Kind::Embeddings => "/embeddings",
        Kind::Chat => match format {
            ApiFormat::Openai => "/chat/completions",
            ApiFormat::Anthropic => "/messages",
            ApiFormat::Responses => "/responses",
        },
    };
    format!("{base}{path}")
}

/// Fetch the upstream model catalogue and return it in a normalized shape.
pub async fn discover_models(client: &reqwest::Client, provider: &Provider) -> Value {
    let url = provider.models_endpoint();
    let started = Instant::now();
    let mut rb = client
        .get(&url)
        .timeout(Duration::from_secs(15))
        .header("accept", "application/json");
    if let Some(key) = provider.api_key.as_deref().filter(|k| !k.trim().is_empty()) {
        rb = match provider.format {
            ApiFormat::Anthropic => rb
                .header("x-api-key", key)
                .header("anthropic-version", "2023-06-01"),
            _ => rb.bearer_auth(key),
        };
    }
    for (k, v) in &provider.headers {
        rb = rb.header(k.as_str(), v.as_str());
    }

    let base = json!({
        "id": provider.id,
        "format": provider.format.as_str(),
        "url": url,
        "latency_ms": started.elapsed().as_millis() as i64,
    });

    match rb.send().await {
        Ok(resp) => {
            let status = resp.status();
            // Model catalogues are small; bound a misbehaving upstream too.
            let text = read_upstream_error_body(resp, 15, 8 * 1024 * 1024).await;
            match serde_json::from_str::<Value>(&text) {
                Ok(value) if status.is_success() => {
                    let models = extract_model_ids(&value);
                    json!({
                        "id": provider.id,
                        "format": provider.format.as_str(),
                        "url": url,
                        "ok": true,
                        "status": status.as_u16(),
                        "latency_ms": started.elapsed().as_millis() as i64,
                        "count": models.len(),
                        "models": models,
                    })
                }
                Ok(_) => json!({
                    "id": provider.id,
                    "format": provider.format.as_str(),
                    "url": url,
                    "ok": false,
                    "status": status.as_u16(),
                    "latency_ms": started.elapsed().as_millis() as i64,
                    "error": truncate(&text, 300),
                }),
                Err(e) => json!({
                    "id": provider.id,
                    "format": provider.format.as_str(),
                    "url": url,
                    "ok": false,
                    "status": status.as_u16(),
                    "latency_ms": started.elapsed().as_millis() as i64,
                    "error": format!("invalid model list: {e}"),
                }),
            }
        }
        Err(e) => {
            let mut value = base;
            value["ok"] = json!(false);
            value["status"] = json!(0);
            value["error"] = json!(truncate(&e.to_string(), 300));
            value
        }
    }
}

fn extract_model_ids(value: &Value) -> Vec<String> {
    let items = value
        .get("data")
        .and_then(|v| v.as_array())
        .or_else(|| value.get("models").and_then(|v| v.as_array()))
        .or_else(|| value.as_array());
    let Some(items) = items else {
        return Vec::new();
    };
    let mut ids: Vec<String> = Vec::new();
    for item in items {
        let id = item
            .get("id")
            .and_then(|v| v.as_str())
            .or_else(|| item.as_str());
        if let Some(id) = id.filter(|id| !id.trim().is_empty()) {
            if !ids.iter().any(|existing| existing == id) {
                ids.push(id.to_string());
            }
        }
    }
    ids
}

pub async fn probe_provider(client: &reqwest::Client, provider: &Provider) -> Value {
    let url = provider.models_endpoint();
    let started = Instant::now();
    let mut rb = client
        .get(&url)
        .timeout(Duration::from_secs(8))
        .header("accept", "application/json");
    if let Some(key) = provider.api_key.as_deref().filter(|k| !k.trim().is_empty()) {
        rb = match provider.format {
            ApiFormat::Anthropic => rb
                .header("x-api-key", key)
                .header("anthropic-version", "2023-06-01"),
            _ => rb.bearer_auth(key),
        };
    }
    for (k, v) in &provider.headers {
        rb = rb.header(k.as_str(), v.as_str());
    }
    match rb.send().await {
        Ok(resp) => {
            let status = resp.status();
            json!({
                "id": provider.id,
                "format": provider.format.as_str(),
                "ok": status.is_success(),
                "status": status.as_u16(),
                "latency_ms": started.elapsed().as_millis() as i64
            })
        }
        Err(e) => json!({
            "id": provider.id,
            "format": provider.format.as_str(),
            "ok": false,
            "status": 0,
            "latency_ms": started.elapsed().as_millis() as i64,
            "error": truncate(&e.to_string(), 300)
        }),
    }
}

#[cfg(test)]
mod model_list_tests {
    use super::*;

    #[test]
    fn extracts_openai_model_list() {
        let value = json!({"object":"list","data":[{"id":"a"},{"id":"b"},{"id":"a"}]});
        assert_eq!(extract_model_ids(&value), vec!["a", "b"]);
    }

    #[test]
    fn extracts_bare_array_and_models_key() {
        let bare = json!([{"id":"x"},{"id":"y"}]);
        assert_eq!(extract_model_ids(&bare), vec!["x", "y"]);
        let wrapped = json!({"models":[{"id":"z"},{"id":""},{"name":"ignored"}]});
        assert_eq!(extract_model_ids(&wrapped), vec!["z"]);
    }
}

#[cfg(test)]
mod conversion_tests {
    use super::*;

    #[test]
    fn reasoning_marks_first_token() {
        let mut usage = test_usage();
        tap_openai_event(
            &SseEvent {
                event: None,
                data: json!({"choices":[{"delta":{"reasoning_content":"thinking"}}]}).to_string(),
            },
            &mut usage,
        );
        assert!(usage.first_token.is_some());
    }

    #[test]
    fn failed_stream_retains_reported_usage_and_cost() {
        let mut usage = test_usage();
        usage.set_usage_full(Some(100), Some(20), None, None);
        usage.set_reported_cost(Some(0.25), Some("USD".into()));
        usage.set_error(502, "interrupted".into());
        usage.mark_finished();
        usage.record();
        let records = usage.stats.recent(10, 0, None, None).unwrap();
        assert_eq!(
            (
                records[0].status,
                records[0].prompt_tokens,
                records[0].completion_tokens
            ),
            (502, 100, 20)
        );
        assert_eq!(records[0].cost, Some(0.25));
    }

    fn test_usage() -> UsageRec {
        test_usage_kind(true)
    }

    fn test_usage_kind(output_expected: bool) -> UsageRec {
        let stats = Arc::new(Stats::open(std::path::Path::new(":memory:")).unwrap());
        UsageRec::new(
            stats,
            Arc::new(Router::new()),
            "test",
            "openai",
            output_expected,
            "p".to_string(),
            "m".to_string(),
            "m".to_string(),
            "r".to_string(),
            true,
            0,
            Instant::now(),
            None,
            30,
            false,
            120,
            1024,
        )
    }

    #[test]
    fn responses_to_chat_keeps_later_items_without_deltas() {
        fn feed(conv: &mut ResponsesToChat, usage: &mut UsageRec, value: Value) -> Vec<Value> {
            conv.handle_event(
                &SseEvent {
                    event: None,
                    data: value.to_string(),
                },
                usage,
            )
        }

        let mut usage = test_usage();
        let mut conv = ResponsesToChat::new("m");
        let mut chunks = Vec::new();
        chunks.extend(feed(
            &mut conv,
            &mut usage,
            json!({"type":"response.created","response":{"id":"resp_1"}}),
        ));
        chunks.extend(feed(
            &mut conv,
            &mut usage,
            json!({"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"Hello "}),
        ));
        chunks.extend(feed(
            &mut conv,
            &mut usage,
            json!({"type":"response.output_text.done","item_id":"msg_1","output_index":0,"content_index":0,"text":"Hello "}),
        ));
        // Second item is only announced through its done events.
        chunks.extend(feed(
            &mut conv,
            &mut usage,
            json!({"type":"response.output_text.done","item_id":"msg_2","output_index":1,"content_index":0,"text":"World"}),
        ));
        chunks.extend(feed(
            &mut conv,
            &mut usage,
            json!({"type":"response.output_item.done","output_index":1,"item":{"type":"message","id":"msg_2","content":[{"type":"output_text","text":"World"}]}}),
        ));
        // Third item only has output_item.done.
        chunks.extend(feed(
            &mut conv,
            &mut usage,
            json!({"type":"response.output_item.done","output_index":2,"item":{"type":"message","id":"msg_3","content":[{"type":"output_text","text":"Again"}]}}),
        ));
        let text: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(text, "Hello WorldAgain");
        assert_eq!(usage.completion_tokens, 5);
    }

    #[test]
    fn composite_usage_chunk_is_not_marked_exact() {
        let value = json!({
            "id": "c1",
            "choices": [],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5}
        });

        let mut composite = test_usage();
        let mut conv = ChatToResponses::new("m", None);
        conv.skip_estimates();
        let mut out = Vec::new();
        conv.handle_chunk(&value, &mut composite, &mut out);
        assert!(
            composite.estimated,
            "synthetic combined usage must stay estimated"
        );

        let mut direct = test_usage();
        let mut conv = ChatToResponses::new("m", None);
        let mut out = Vec::new();
        conv.handle_chunk(&value, &mut direct, &mut out);
        assert!(!direct.estimated);
        assert_eq!(direct.prompt_tokens, 10);
        assert_eq!(direct.completion_tokens, 5);
    }

    #[test]
    fn failed_responses_event_records_usage_and_error() {
        let mut usage = test_usage();
        tap_responses_event(
            &SseEvent {
                event: None,
                data: json!({
                    "type": "response.failed", "response": {
                        "error": {"message": "failed"},
                        "usage": {"input_tokens": 120, "output_tokens": 9,
                            "input_tokens_details": {"cached_tokens": 30, "cache_write_tokens": 4},
                            "cost": 0.25}
                    }
                })
                .to_string(),
            },
            &mut usage,
        );
        assert_eq!(
            (
                usage.prompt_tokens,
                usage.completion_tokens,
                usage.cached_tokens,
                usage.cache_write_tokens
            ),
            (120, 9, 30, 4)
        );
        assert_eq!(usage.reported_cost, Some(0.25));
        assert_eq!(usage.status, 502);
        assert!(!usage.estimated);
    }

    #[test]
    fn cross_format_nonstream_fills_missing_usage_preserving_reported_values() {
        for format in [
            ApiFormat::Openai,
            ApiFormat::Anthropic,
            ApiFormat::Responses,
        ] {
            let (body, target) = match format {
                ApiFormat::Openai => (
                    json!({"choices":[{"message":{"content":"hello world"}}]}),
                    ApiFormat::Anthropic,
                ),
                ApiFormat::Anthropic => (
                    json!({"content":[{"type":"text","text":"hello world"}]}),
                    ApiFormat::Openai,
                ),
                ApiFormat::Responses => (
                    json!({"output":[{"type":"message","content":[{"type":"output_text","text":"hello world"}]}]}),
                    ApiFormat::Openai,
                ),
            };
            let mut usage = test_usage();
            apply_nonstream_usage(&mut usage, format, &body);
            let mut filled = body;
            fill_missing_nonstream_usage(&mut filled, format, &usage);
            let out = match (format, target) {
                (ApiFormat::Openai, _) => translate::openai_to_anthropic_response(&filled, "m"),
                (ApiFormat::Anthropic, _) => translate::anthropic_to_openai_response(&filled, "m"),
                _ => translate::responses_to_chat_response(&filled, "m"),
            };
            let result = extract_usage(target, &out).unwrap();
            assert_eq!(result.prompt, Some(usage.prompt_tokens));
            assert_eq!(result.completion, Some(usage.completion_tokens));
            assert!(usage.completion_tokens > 0);
        }
        let mut usage = test_usage();
        let mut body = json!({"content":[{"type":"text","text":"hello"}], "usage": {"input_tokens": 10, "cache_read_input_tokens": 3, "cache_creation_input_tokens": 2, "output_tokens": 0}});
        apply_nonstream_usage(&mut usage, ApiFormat::Anthropic, &body);
        fill_missing_nonstream_usage(&mut body, ApiFormat::Anthropic, &usage);
        assert_eq!(body["usage"]["input_tokens"], 10);
        assert_eq!(body["usage"]["output_tokens"], 0);
        assert_eq!(body["usage"]["cache_read_input_tokens"], 3);
        assert_eq!(body["usage"]["cache_creation_input_tokens"], 2);
    }

    #[test]
    fn nonstream_partial_usage_estimates_missing_output() {
        let mut usage = test_usage();
        apply_nonstream_usage(
            &mut usage,
            ApiFormat::Openai,
            &json!({
                "choices": [{"message": {"role": "assistant", "content": "hello world hello"}}],
                "usage": {"prompt_tokens": 20}
            }),
        );
        assert_eq!(usage.prompt_tokens, 20);
        assert!(usage.completion_tokens > 0);
        assert!(usage.estimated);

        let mut complete = test_usage();
        apply_nonstream_usage(
            &mut complete,
            ApiFormat::Openai,
            &json!({
                "choices": [{"message": {"role": "assistant", "content": "hello world hello"}}],
                "usage": {"prompt_tokens": 20, "completion_tokens": 5}
            }),
        );
        assert_eq!(complete.completion_tokens, 5);
        assert!(!complete.estimated);
    }

    #[test]
    fn failed_nonstream_responses_retains_reported_usage_and_cost() {
        let mut usage = test_usage();
        let response = json!({"object":"response", "status":"failed", "error":{"message":"failed"},
            "usage":{"input_tokens":120,"output_tokens":9,"input_tokens_details":{"cached_tokens":30,"cache_write_tokens":4},"cost":0.25}});
        assert!(apply_and_validate_nonstream(
            &mut usage,
            Kind::Chat,
            ApiFormat::Responses,
            &response
        )
        .is_err());
        usage.set_error(502, "failed".into());
        usage.mark_finished();
        usage.record();
        let record = usage.stats.recent(10, 0, None, None).unwrap().remove(0);
        assert_eq!(
            (
                record.status,
                record.prompt_tokens,
                record.completion_tokens,
                record.cached_tokens,
                record.cache_write_tokens
            ),
            (502, 120, 9, 30, 4)
        );
        assert_eq!(record.cost, Some(0.25));
        assert!(!record.estimated);
    }

    #[test]
    fn anthropic_start_output_does_not_suppress_estimates_before_final_usage() {
        for native in [true, false] {
            let mut usage = test_usage();
            let mut state = AnthToOai::new("m");
            let mut out = Vec::new();
            let mut send = |value: Value, usage: &mut UsageRec| {
                let event = SseEvent {
                    event: None,
                    data: value.to_string(),
                };
                if native {
                    tap_anthropic_event(&event, usage);
                } else {
                    state.handle(&event, usage, &mut out);
                }
            };
            send(
                json!({"type":"message_start","message":{"usage":{"input_tokens":100,"cache_read_input_tokens":30,"cache_creation_input_tokens":40,"output_tokens":1}}}),
                &mut usage,
            );
            send(
                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x".repeat(350)}}),
                &mut usage,
            );
            assert!(usage.completion_tokens > 1);
            assert!(usage.estimated);
            let estimate = usage.completion_tokens;
            send(
                json!({"type":"message_delta","usage":{"cache_read_input_tokens":35}}),
                &mut usage,
            );
            assert_eq!(usage.completion_tokens, estimate);
            assert!(usage.estimated);
            assert_eq!(
                (
                    usage.prompt_tokens,
                    usage.cached_tokens,
                    usage.cache_write_tokens
                ),
                (175, 35, 40)
            );
            send(
                json!({"type":"message_delta","usage":{"output_tokens":20}}),
                &mut usage,
            );
            assert_eq!(usage.completion_tokens, 20);
            assert!(!usage.estimated);
        }
    }

    #[test]
    fn anthropic_delta_updates_all_cumulative_input_counters() {
        for native in [true, false] {
            let mut usage = test_usage();
            let mut state = AnthToOai::new("m");
            let mut out = Vec::new();
            for value in [
                json!({"type":"message_start","message":{"usage":{"input_tokens":100,"cache_read_input_tokens":30,"cache_creation_input_tokens":40}}}),
                json!({"type":"message_delta","usage":{"input_tokens":1000,"cache_read_input_tokens":300,"cache_creation_input_tokens":400,"output_tokens":20}}),
                json!({"type":"message_delta","usage":{"cache_read_input_tokens":350}}),
            ] {
                let event = SseEvent {
                    event: None,
                    data: value.to_string(),
                };
                if native {
                    tap_anthropic_event(&event, &mut usage);
                } else {
                    state.handle(&event, &mut usage, &mut out);
                }
            }
            assert_eq!(
                (
                    usage.prompt_tokens,
                    usage.cached_tokens,
                    usage.cache_write_tokens,
                    usage.completion_tokens
                ),
                (1750, 350, 400, 20)
            );
            assert!(!usage.estimated);
        }
    }

    #[test]
    fn anthropic_stream_usage_provenance() {
        fn event(value: Value) -> SseEvent {
            SseEvent {
                event: None,
                data: value.to_string(),
            }
        }

        let cases = [
            (
                json!({"cache_read_input_tokens": 40}),
                json!({"output_tokens": 5}),
                100,
                40,
                5,
                true,
            ),
            (
                json!({"input_tokens": 10, "cache_read_input_tokens": 2}),
                json!({"output_tokens": 5}),
                12,
                2,
                5,
                false,
            ),
            (json!({}), json!({}), 100, 0, 0, true),
        ];

        for native in [true, false] {
            for (start_usage, delta_usage, prompt, cached, completion, estimated) in &cases {
                let mut usage = test_usage();
                usage.prompt_tokens = 100;
                let start = event(json!({
                    "type": "message_start",
                    "message": {"id": "msg_1", "usage": start_usage}
                }));
                let delta = event(json!({
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn"},
                    "usage": delta_usage
                }));
                if native {
                    tap_anthropic_event(&start, &mut usage);
                    tap_anthropic_event(&delta, &mut usage);
                } else {
                    let mut state = AnthToOai::new("m");
                    let mut out = Vec::new();
                    state.handle(&start, &mut usage, &mut out);
                    state.handle(&delta, &mut usage, &mut out);
                }
                assert_eq!(usage.prompt_tokens, *prompt);
                assert_eq!(usage.cached_tokens, *cached);
                assert_eq!(usage.completion_tokens, *completion);
                assert_eq!(usage.estimated, *estimated);
            }
        }
    }

    #[test]
    fn anthropic_cached_only_usage_keeps_input_estimate() {
        let info = extract_usage(
            ApiFormat::Anthropic,
            &json!({"usage": {"cache_read_input_tokens": 50, "output_tokens": 5}}),
        )
        .unwrap();
        assert_eq!(info.prompt, None);
        assert_eq!(info.cached, Some(50));
        assert_eq!(info.completion, Some(5));

        let mut usage = test_usage();
        usage.prompt_tokens = 100;
        apply_nonstream_usage(
            &mut usage,
            ApiFormat::Anthropic,
            &json!({
                "content": [{"type": "text", "text": "hi"}],
                "usage": {"cache_read_input_tokens": 50, "output_tokens": 5}
            }),
        );
        assert_eq!(usage.prompt_tokens, 100);
        assert_eq!(usage.cached_tokens, 50);
        assert_eq!(usage.completion_tokens, 5);
        assert!(usage.estimated);
    }

    #[test]
    fn extract_usage_preserves_missing_fields() {
        let info = extract_usage(
            ApiFormat::Openai,
            &json!({"usage": {"completion_tokens": 5}}),
        )
        .unwrap();
        assert_eq!(info.prompt, None);
        assert_eq!(info.completion, Some(5));
        assert_eq!(info.cached, None);

        let info =
            extract_usage(ApiFormat::Anthropic, &json!({"usage": {"input_tokens": 9}})).unwrap();
        assert_eq!(info.prompt, Some(9));
        assert_eq!(info.completion, None);

        let info = extract_usage(
            ApiFormat::Openai,
            &json!({"usage": {"prompt_tokens": 0, "completion_tokens": 0}}),
        )
        .unwrap();
        assert_eq!(info.prompt, Some(0));
        assert_eq!(info.completion, Some(0));
    }

    #[test]
    fn embeddings_input_usage_is_exact() {
        let mut embeddings = test_usage_kind(false);
        embeddings.set_usage_full(Some(10), None, None, None);
        assert!(!embeddings.estimated);
        assert_eq!(embeddings.completion_tokens, 0);

        // Chat still needs an exact output count.
        let mut chat = test_usage_kind(true);
        chat.set_usage_full(Some(10), None, None, None);
        assert!(chat.estimated);
    }

    #[test]
    fn continuation_estimate_includes_history() {
        let body = json!({"input": "new turn", "instructions": "be brief"});
        let plain = estimate_prompt_with_history(&body, None);
        let with_history = estimate_prompt_with_history(&body, Some(100));
        assert!(with_history > plain);
        assert!(with_history >= 100);
    }

    #[test]
    fn partial_usage_keeps_estimate_flag() {
        let mut usage = test_usage();
        usage.set_usage_full(Some(10), None, None, None);
        assert!(usage.estimated, "prompt-only usage is still partial");
        usage.set_usage_full(None, Some(5), None, None);
        assert_eq!(usage.prompt_tokens, 10);
        assert!(
            !usage.estimated,
            "complete prompt+completion usage should clear the flag"
        );

        let mut completion_only = test_usage();
        completion_only.set_usage_full(None, Some(5), None, None);
        assert!(
            completion_only.estimated,
            "completion-only usage is partial"
        );
    }

    #[test]
    fn embeddings_plan_only_keeps_openai_routes() {
        fn resolved(id: &str, format: ApiFormat) -> Resolved {
            Resolved {
                alias: "m".to_string(),
                upstream_model: "m".to_string(),
                provider: Provider {
                    id: format!("p-{id}"),
                    format,
                    base_url: "http://127.0.0.1:1/v1".to_string(),
                    api_key: None,
                    headers: HashMap::new(),
                    models: vec!["m".to_string()],
                    models_url: None,
                    timeout_secs: 60,
                    stream_usage: true,
                    pricing: None,
                },
                format,
                route_id: id.to_string(),
                priority: 1,
                weight: 1,
                params: HashMap::new(),
                pricing: None,
                label: None,
            }
        }

        let plan = ModelPlan {
            alias: "m".to_string(),
            requested: "m".to_string(),
            strategy: "cheapest".to_string(),
            manual_route: None,
            routes: vec![
                resolved("anthropic", ApiFormat::Anthropic),
                resolved("openai", ApiFormat::Openai),
            ],
        };
        let filtered = restrict_plan_for_kind(plan, Kind::Embeddings);
        assert_eq!(filtered.routes.len(), 1);
        assert_eq!(filtered.routes[0].route_id, "openai");

        let chat = ModelPlan {
            alias: "m".to_string(),
            requested: "m".to_string(),
            strategy: "cheapest".to_string(),
            manual_route: None,
            routes: vec![
                resolved("anthropic", ApiFormat::Anthropic),
                resolved("openai", ApiFormat::Openai),
            ],
        };
        assert_eq!(restrict_plan_for_kind(chat, Kind::Chat).routes.len(), 2);
    }

    #[test]
    fn responses_refusal_stream_is_forwarded_once() {
        let mut usage = test_usage();
        let mut conv = ResponsesToChat::new("m");
        let mut chunks = Vec::new();
        for value in [
            json!({"type":"response.created","response":{"id":"resp_1"}}),
            json!({"type":"response.refusal.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"I cannot"}),
            json!({"type":"response.refusal.done","item_id":"msg_1","output_index":0,"content_index":0,"refusal":"I cannot"}),
            json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_1","content":[{"type":"refusal","refusal":"I cannot"}]}}),
        ] {
            let ev = SseEvent {
                event: None,
                data: value.to_string(),
            };
            chunks.extend(conv.handle_event(&ev, &mut usage));
        }
        let refusals: String = chunks
            .iter()
            .filter_map(|chunk| chunk["choices"][0]["delta"]["refusal"].as_str())
            .collect();
        assert_eq!(refusals, "I cannot");
        let content: String = chunks
            .iter()
            .filter_map(|chunk| chunk["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert!(content.is_empty());
    }

    #[test]
    fn anthropic_to_responses_keeps_reasoning_once() {
        let mut usage = test_usage();
        let mut a2c = AnthToOai::new("m");
        let mut c2r = ChatToResponses::new("m", None);
        c2r.skip_estimates();
        let mut events = Vec::new();
        for value in [
            json!({"type":"message_start","message":{"id":"msg_1","usage":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"plan"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"answer"}}),
        ] {
            let ev = SseEvent {
                event: None,
                data: value.to_string(),
            };
            let mut payloads = Vec::new();
            a2c.handle(&ev, &mut usage, &mut payloads);
            for payload in payloads {
                if let Some(chat) = parse_sse_payload(&payload) {
                    let mut out = Vec::new();
                    c2r.handle_chunk(&chat, &mut usage, &mut out);
                    events.extend(out);
                }
            }
        }
        let mut final_out = Vec::new();
        c2r.finalize(&mut usage, &mut final_out);
        events.extend(final_out);

        assert!(events.iter().any(|event| {
            event["type"] == "response.reasoning_summary_text.delta" && event["delta"] == "plan"
        }));
        let completed = events
            .iter()
            .find(|event| event["type"] == "response.completed")
            .unwrap();
        let output = completed["response"]["output"].as_array().unwrap();
        assert!(output
            .iter()
            .any(|item| { item["type"] == "reasoning" && item["summary"][0]["text"] == "plan" }));
        // The first layer counts "plan" + "answer" once: ceil(10 / 3.5) = 3.
        assert_eq!(usage.completion_tokens, 3);
    }

    #[test]
    fn sanitizer_failure_marks_request_failed() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut usage = test_usage();
        usage.set_delivery_failed(Arc::clone(&flag));
        flag.store(true, Ordering::Relaxed);
        usage.record();
        assert_eq!(usage.status, 502);
    }

    #[test]
    fn openai_stream_requires_expected_choice_count() {
        let seen_one: HashSet<u64> = [0].into_iter().collect();
        let finished_one: HashSet<u64> = [0].into_iter().collect();
        assert!(!openai_stream_complete(
            false,
            &seen_one,
            &finished_one,
            Some(2)
        ));
        // [DONE] is always accepted.
        assert!(openai_stream_complete(
            true,
            &seen_one,
            &finished_one,
            Some(2)
        ));

        let seen_two: HashSet<u64> = [0, 1].into_iter().collect();
        let finished_two: HashSet<u64> = [0, 1].into_iter().collect();
        assert!(openai_stream_complete(
            false,
            &seen_two,
            &finished_two,
            Some(2)
        ));
        assert!(!openai_stream_complete(
            false,
            &seen_two,
            &finished_one,
            Some(2)
        ));
    }

    #[test]
    fn anthropic_tool_names_accumulate_fragments_and_delayed_ids() {
        let mut usage = test_usage();
        let mut state = OaiToAnth::new("m");
        let mut out = Vec::new();
        for value in [
            json!({"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"function":{"name":"get_","arguments":""}},
                {"index":1,"function":{"name":"list_","arguments":""}}
            ]},"finish_reason":null}]}),
            json!({"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"id":"call_1","function":{"name":"weather","arguments":""}},
                {"index":1,"id":"call_2","function":{"name":"files","arguments":""}}
            ]},"finish_reason":null}]}),
            json!({"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"function":{"arguments":"{}"}},
                {"index":1,"function":{"arguments":"{}"}}
            ]},"finish_reason":null}]}),
        ] {
            state.handle(
                &SseEvent {
                    event: None,
                    data: value.to_string(),
                },
                &mut usage,
                &mut out,
            );
        }
        state.finalize(&mut usage, &mut out);
        let joined = out.join(
            "
",
        );
        assert!(joined.contains("\"name\":\"get_weather\""), "{joined}");
        assert!(joined.contains("\"name\":\"list_files\""), "{joined}");
        assert!(joined.contains("\"id\":\"call_1\""), "{joined}");
        assert!(joined.contains("\"id\":\"call_2\""), "{joined}");
    }

    #[test]
    fn anthropic_to_chat_stream_reports_cached_usage() {
        fn data(value: &str) -> Option<Value> {
            let line = value.lines().find(|line| line.starts_with("data: "))?;
            serde_json::from_str(line.trim_start_matches("data: ")).ok()
        }

        let mut usage = test_usage();
        let mut state = AnthToOai::new("m");
        let mut out = Vec::new();
        for value in [
            json!({"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":200,"cache_read_input_tokens":800}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":20}}),
        ] {
            state.handle(
                &SseEvent {
                    event: None,
                    data: value.to_string(),
                },
                &mut usage,
                &mut out,
            );
        }
        state.finalize(&mut usage, &mut out);
        let usage_chunk = out
            .iter()
            .filter_map(|event| data(event))
            .find(|value| value["choices"].as_array().is_some_and(|c| c.is_empty()))
            .unwrap();
        assert_eq!(usage_chunk["usage"]["prompt_tokens"], 1000);
        assert_eq!(usage_chunk["usage"]["completion_tokens"], 20);
        assert_eq!(
            usage_chunk["usage"]["prompt_tokens_details"]["cached_tokens"],
            800
        );
    }

    #[test]
    fn responses_to_chat_stream_reports_cached_usage() {
        let mut usage = test_usage();
        let mut state = ResponsesToChat::new("m");
        for value in [
            json!({"type":"response.created","response":{"id":"resp_1"}}),
            json!({"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"hi"}),
            json!({"type":"response.completed","response":{"id":"resp_1","status":"completed","usage":{"input_tokens":1000,"output_tokens":20,"input_tokens_details":{"cached_tokens":800}}}}),
        ] {
            let ev = SseEvent {
                event: None,
                data: value.to_string(),
            };
            let _ = state.handle_event(&ev, &mut usage);
        }
        let out = state.finalize(&mut usage);
        let usage_chunk = out
            .iter()
            .find(|value| value["choices"].as_array().is_some_and(|c| c.is_empty()))
            .unwrap();
        assert_eq!(usage_chunk["usage"]["prompt_tokens"], 1000);
        assert_eq!(
            usage_chunk["usage"]["prompt_tokens_details"]["cached_tokens"],
            800
        );
    }

    #[test]
    fn chat_to_responses_keeps_latest_estimate_after_partial_usage() {
        let mut usage = test_usage();
        let mut conv = ChatToResponses::new("m", None);
        let mut events = Vec::new();

        let mut out = Vec::new();
        conv.handle_chunk(
            &json!({"id":"c1","choices":[{"index":0,"delta":{"content":"1234567"},"finish_reason":null}]}),
            &mut usage,
            &mut out,
        );
        events.extend(out);

        let mut out = Vec::new();
        conv.handle_chunk(
            &json!({"id":"c1","choices":[],"usage":{"prompt_tokens":10}}),
            &mut usage,
            &mut out,
        );
        events.extend(out);
        assert_eq!(usage.completion_tokens, 2);

        let mut out = Vec::new();
        conv.handle_chunk(
            &json!({"id":"c1","choices":[{"index":0,"delta":{"content":"x".repeat(343)},"finish_reason":null}]}),
            &mut usage,
            &mut out,
        );
        events.extend(out);

        let mut final_out = Vec::new();
        conv.finalize(&mut usage, &mut final_out);
        events.extend(final_out);

        assert_eq!(usage.completion_tokens, 100);
        let completed = events
            .iter()
            .find(|event| event["type"] == "response.completed")
            .unwrap();
        assert_eq!(completed["response"]["usage"]["output_tokens"], 100);
    }

    #[test]
    fn chat_to_responses_reported_completion_wins_over_estimate() {
        let mut usage = test_usage();
        let mut conv = ChatToResponses::new("m", None);
        let mut events = Vec::new();
        let mut out = Vec::new();
        conv.handle_chunk(
            &json!({"id":"c1","choices":[{"index":0,"delta":{"content":"x".repeat(100)},"finish_reason":null}]}),
            &mut usage,
            &mut out,
        );
        events.extend(out);
        let mut out = Vec::new();
        conv.handle_chunk(
            &json!({"id":"c1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}),
            &mut usage,
            &mut out,
        );
        events.extend(out);
        let mut final_out = Vec::new();
        conv.finalize(&mut usage, &mut final_out);
        events.extend(final_out);
        assert_eq!(usage.completion_tokens, 5);
        let completed = events
            .iter()
            .find(|event| event["type"] == "response.completed")
            .unwrap();
        assert_eq!(completed["response"]["usage"]["output_tokens"], 5);
    }

    #[test]
    fn anthropic_to_responses_keeps_cached_when_details_are_missing() {
        let mut usage = test_usage();
        usage.prompt_tokens = 1000;
        usage.completion_tokens = 20;
        usage.cached_tokens = 800;
        usage.cache_write_tokens = 100;
        let mut conv = ChatToResponses::new("m", None);
        conv.skip_estimates();
        let mut events = Vec::new();
        conv.handle_chunk(
            &json!({"id":"c1","choices":[],"usage":{"prompt_tokens":1000,"completion_tokens":20}}),
            &mut usage,
            &mut events,
        );
        conv.finalize(&mut usage, &mut events);
        let completed = events
            .iter()
            .find(|event| event["type"] == "response.completed")
            .unwrap();
        assert_eq!(
            completed["response"]["usage"]["input_tokens_details"]["cached_tokens"],
            800
        );
        assert_eq!(
            completed["response"]["usage"]["input_tokens_details"]["cache_write_tokens"],
            100
        );
    }

    #[test]
    fn anthropic_tool_block_indices_follow_emission_order() {
        fn data(value: &str) -> Option<Value> {
            let line = value.lines().find(|line| line.starts_with("data: "))?;
            serde_json::from_str(line.trim_start_matches("data: ")).ok()
        }

        let mut usage = test_usage();
        let mut state = OaiToAnth::new("m");
        let mut out = Vec::new();
        for value in [
            json!({"choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"function":{"name":"zero","arguments":""}},
                {"index":1,"function":{"name":"one","arguments":""}}
            ]},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{"tool_calls":[
                {"index":1,"function":{"arguments":"{}"}}
            ]},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"function":{"arguments":"{}"}}
            ]},"finish_reason":null}]}),
        ] {
            state.handle(
                &SseEvent {
                    event: None,
                    data: value.to_string(),
                },
                &mut usage,
                &mut out,
            );
        }
        state.finalize(&mut usage, &mut out);

        let mut starts = Vec::new();
        let mut deltas = Vec::new();
        for event in &out {
            let Some(value) = data(event) else {
                continue;
            };
            match value.get("type").and_then(|t| t.as_str()) {
                Some("content_block_start") => {
                    if let Some(block) = value.get("content_block") {
                        if block.get("type").and_then(|t| t.as_str()) == Some("tool_use") {
                            starts.push((
                                value["index"].as_u64().unwrap(),
                                block["name"].as_str().unwrap().to_string(),
                            ));
                        }
                    }
                }
                Some("content_block_delta") => {
                    deltas.push((
                        value["index"].as_u64().unwrap(),
                        value["delta"]["partial_json"].as_str().unwrap().to_string(),
                    ));
                }
                _ => {}
            }
        }
        assert_eq!(
            starts,
            vec![(0, "one".to_string()), (1, "zero".to_string())]
        );
        assert_eq!(deltas, vec![(0, "{}".to_string()), (1, "{}".to_string())]);
    }

    #[test]
    fn repeated_tool_name_fragments_are_concatenated() {
        let mut anth_usage = test_usage();
        let mut state = OaiToAnth::new("m");
        let mut out = Vec::new();
        for value in [
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"foo","arguments":""}}]},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"foo","arguments":"{}"}}]},"finish_reason":null}]}),
        ] {
            state.handle(
                &SseEvent {
                    event: None,
                    data: value.to_string(),
                },
                &mut anth_usage,
                &mut out,
            );
        }
        state.finalize(&mut anth_usage, &mut out);
        let joined = out.join("\n");
        assert!(joined.contains("\"name\":\"foofoo\""), "{joined}");

        let mut resp_usage = test_usage();
        let mut conv = ChatToResponses::new("m", None);
        let mut events = Vec::new();
        for value in [
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"foo","arguments":""}}]},"finish_reason":null}]}),
            json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"foo","arguments":"{}"}}]},"finish_reason":null}]}),
        ] {
            let mut chunk_out = Vec::new();
            conv.handle_chunk(&value, &mut resp_usage, &mut chunk_out);
            events.extend(chunk_out);
        }
        let mut final_out = Vec::new();
        conv.finalize(&mut resp_usage, &mut final_out);
        events.extend(final_out);
        let completed = events
            .iter()
            .find(|event| event["type"] == "response.completed")
            .unwrap();
        let fc = completed["response"]["output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "function_call")
            .unwrap();
        assert_eq!(fc["name"], "foofoo");
    }

    #[test]
    fn responses_tool_names_accumulate_fragments_and_delayed_ids() {
        let mut usage = test_usage();
        let mut conv = ChatToResponses::new("m", None);
        let mut events = Vec::new();
        for value in [
            json!({"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"function":{"name":"get_","arguments":""}}
            ]},"finish_reason":null}]}),
            json!({"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"id":"call_9","function":{"name":"weather","arguments":""}}
            ]},"finish_reason":null}]}),
            json!({"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"function":{"arguments":"{\"city\":\"Paris\"}"}}
            ]},"finish_reason":null}]}),
        ] {
            let mut out = Vec::new();
            conv.handle_chunk(&value, &mut usage, &mut out);
            events.extend(out);
        }
        let mut final_out = Vec::new();
        conv.finalize(&mut usage, &mut final_out);
        events.extend(final_out);

        let added = events
            .iter()
            .find(|event| {
                event["type"] == "response.output_item.added"
                    && event["item"]["type"] == "function_call"
            })
            .unwrap();
        assert_eq!(added["item"]["name"], "get_weather");
        assert_eq!(added["item"]["call_id"], "call_9");
        let completed = events
            .iter()
            .find(|event| event["type"] == "response.completed")
            .unwrap();
        let fc = completed["response"]["output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "function_call")
            .unwrap();
        assert_eq!(fc["name"], "get_weather");
        assert_eq!(fc["call_id"], "call_9");
    }

    #[test]
    fn chat_to_anthropic_streams_thinking_and_final_usage() {
        let mut usage = test_usage();
        let mut state = OaiToAnth::new("m");
        let mut out = Vec::new();
        for value in [
            json!({"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"plan"},"finish_reason":null}]}),
            json!({"id":"c1","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}),
            json!({"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],
                   "usage":{"prompt_tokens":1000,"completion_tokens":20,
                            "prompt_tokens_details":{"cached_tokens":800}}}),
        ] {
            state.handle(
                &SseEvent {
                    event: None,
                    data: value.to_string(),
                },
                &mut usage,
                &mut out,
            );
        }
        state.finalize(&mut usage, &mut out);
        let joined = out.join(
            "
",
        );
        assert!(joined.contains("\"type\":\"thinking\""), "{joined}");
        assert!(joined.contains("\"type\":\"thinking_delta\""), "{joined}");
        let delta = out
            .iter()
            .find(|event| event.contains("message_delta"))
            .unwrap();
        let data = delta
            .lines()
            .find(|line| line.starts_with("data: "))
            .unwrap()
            .trim_start_matches("data: ");
        let value: Value = serde_json::from_str(data).unwrap();
        assert_eq!(value["usage"]["input_tokens"], 200);
        assert_eq!(value["usage"]["cache_read_input_tokens"], 800);
        assert_eq!(value["usage"]["output_tokens"], 20);
    }

    #[test]
    fn responses_tool_truncation_stream_reports_length() {
        let mut usage = test_usage();
        let mut conv = ResponsesToChat::new("m");
        let mut chunks = Vec::new();
        for value in [
            json!({"type":"response.created","response":{"id":"resp_1"}}),
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"f","arguments":""}}),
            json!({"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":0,"delta":"{\"x\":"}),
            json!({"type":"response.incomplete","response":{"id":"resp_1","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[]}}),
        ] {
            chunks.extend(conv.handle_event(
                &SseEvent {
                    event: None,
                    data: value.to_string(),
                },
                &mut usage,
            ));
        }
        let final_chunks = conv.finalize(&mut usage);
        let all: Vec<Value> = chunks.into_iter().chain(final_chunks).collect();
        let finishes: Vec<&str> = all
            .iter()
            .filter_map(|chunk| chunk["choices"][0]["finish_reason"].as_str())
            .collect();
        assert!(finishes.contains(&"length"), "{finishes:?}");
        assert!(!finishes.contains(&"tool_calls"), "{finishes:?}");
    }

    #[test]
    fn openai_choice_tracker_requires_all_choices_to_finish() {
        fn event(value: Value) -> SseEvent {
            SseEvent {
                event: None,
                data: value.to_string(),
            }
        }
        let mut seen = HashSet::new();
        let mut finished = HashSet::new();
        assert!(!track_openai_choice(
            &event(json!({"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null}]})),
            &mut seen,
            &mut finished,
        ));
        assert!(!track_openai_choice(
            &event(json!({"choices":[{"index":1,"delta":{"content":"b"},"finish_reason":null}]})),
            &mut seen,
            &mut finished,
        ));
        assert!(!track_openai_choice(
            &event(json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})),
            &mut seen,
            &mut finished,
        ));
        assert!(seen.len() > finished.len());
        assert!(!track_openai_choice(
            &event(json!({"choices":[{"index":1,"delta":{},"finish_reason":"stop"}]})),
            &mut seen,
            &mut finished,
        ));
        assert_eq!(seen.len(), finished.len());
        assert!(track_openai_choice(
            &SseEvent {
                event: None,
                data: "[DONE]".to_string()
            },
            &mut seen,
            &mut finished,
        ));
    }

    #[test]
    fn native_refusal_streams_estimate_usage() {
        let mut chat = test_usage();
        tap_openai_event(
            &SseEvent {
                event: None,
                data: json!({"choices":[{"index":0,"delta":{"refusal":"I cannot"}}]}).to_string(),
            },
            &mut chat,
        );
        assert_eq!(chat.completion_tokens, 3);
        assert!(chat.first_token.is_some());

        let mut responses = test_usage();
        tap_responses_event(
            &SseEvent {
                event: None,
                data: json!({
                    "type": "response.refusal.delta",
                    "item_id": "msg_1",
                    "output_index": 0,
                    "content_index": 0,
                    "delta": "I cannot"
                })
                .to_string(),
            },
            &mut responses,
        );
        assert_eq!(responses.completion_tokens, 3);
        assert!(responses.first_token.is_some());
    }

    #[test]
    fn tool_metadata_counts_against_output_budget() {
        let mut usage = test_usage();
        usage.output_max_bytes = 300;
        let mut conv = ChatToResponses::new("m", None);
        let mut out = Vec::new();
        let name = "n".repeat(64);
        for index in 0..12u64 {
            conv.handle_chunk(
                &json!({
                    "id": "c1",
                    "choices": [{
                        "index": 0,
                        "delta": {"tool_calls": [{
                            "index": index,
                            "id": "call_1234567890",
                            "type": "function",
                            "function": {"name": name, "arguments": "{}"}
                        }]},
                        "finish_reason": null
                    }]
                }),
                &mut usage,
                &mut out,
            );
        }
        assert!(conv.oversized());
    }

    #[test]
    fn tool_item_count_is_bounded() {
        let mut usage = test_usage();
        usage.output_max_bytes = 10 * 1024 * 1024;
        let mut conv = ChatToResponses::new("m", None);
        let mut out = Vec::new();
        for index in 0..(MAX_TOOL_ITEMS as u64 + 1) {
            conv.handle_chunk(
                &json!({
                    "id": "c1",
                    "choices": [{
                        "index": 0,
                        "delta": {"tool_calls": [{
                            "index": index,
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "tool", "arguments": "{}"}
                        }]},
                        "finish_reason": null
                    }]
                }),
                &mut usage,
                &mut out,
            );
        }
        assert!(conv.oversized());
    }

    #[test]
    fn chat_to_responses_preserves_refusal() {
        let mut usage = test_usage();
        let mut conv = ChatToResponses::new("m", None);
        let mut events = Vec::new();
        let mut out = Vec::new();
        conv.handle_chunk(
            &json!({"id":"c1","choices":[{"index":0,"delta":{"refusal":"I cannot"},"finish_reason":null}]}),
            &mut usage,
            &mut out,
        );
        events.extend(out);
        let mut final_out = Vec::new();
        conv.finalize(&mut usage, &mut final_out);
        events.extend(final_out);

        assert!(events.iter().any(|event| {
            event["type"] == "response.refusal.delta" && event["delta"] == "I cannot"
        }));
        let done = events
            .iter()
            .find(|event| event["type"] == "response.refusal.done")
            .unwrap();
        assert_eq!(done["refusal"], "I cannot");
        let completed = events
            .iter()
            .find(|event| event["type"] == "response.completed")
            .unwrap();
        let output = completed["response"]["output"].as_array().unwrap();
        assert!(output.iter().any(|item| {
            item["content"][0]["type"] == "refusal" && item["content"][0]["refusal"] == "I cannot"
        }));
        // "I cannot" = 8 characters -> ceil(8 / 3.5) = 3.
        assert_eq!(usage.completion_tokens, 3);
    }

    #[test]
    fn chat_to_responses_preserves_reasoning() {
        let mut usage = test_usage();
        let mut conv = ChatToResponses::new("m", None);
        let mut events = Vec::new();
        for value in [
            json!({"id":"c1","choices":[{"index":0,"delta":{"reasoning_content":"plan"},"finish_reason":null}]}),
            json!({"id":"c1","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}),
        ] {
            let mut out = Vec::new();
            conv.handle_chunk(&value, &mut usage, &mut out);
            events.extend(out);
        }
        let mut final_out = Vec::new();
        conv.finalize(&mut usage, &mut final_out);
        events.extend(final_out);

        assert!(events.iter().any(|event| {
            event["type"] == "response.reasoning_summary_text.delta" && event["delta"] == "plan"
        }));
        let completed = events
            .iter()
            .find(|event| event["type"] == "response.completed")
            .unwrap();
        let output = completed["response"]["output"].as_array().unwrap();
        assert!(output
            .iter()
            .any(|item| { item["type"] == "reasoning" && item["summary"][0]["text"] == "plan" }));
        // "plan" + "answer" counted once: ceil(10 / 3.5) = 3.
        assert_eq!(usage.completion_tokens, 3);
    }

    #[test]
    fn chat_to_responses_reasoning_respects_output_limit() {
        let mut usage = test_usage();
        usage.output_max_bytes = 3;
        let mut conv = ChatToResponses::new("m", None);
        let mut out = Vec::new();
        conv.handle_chunk(
            &json!({"id":"c1","choices":[{"index":0,"delta":{"reasoning_content":"abcdef"},"finish_reason":null}]}),
            &mut usage,
            &mut out,
        );
        assert!(conv.oversized());
    }

    #[test]
    fn trailing_zero_completion_keeps_estimate() {
        let mut usage = test_usage();
        usage.set_usage_full(Some(10), None, None, None);
        usage.add_estimated_completion(&"x".repeat(350));
        assert_eq!(usage.completion_tokens, 100);
        usage.set_usage_full(None, Some(0), None, None);
        assert_eq!(usage.completion_tokens, 100);
        assert!(usage.estimated);
    }

    #[test]
    fn mid_stream_zero_does_not_block_later_estimates() {
        let mut usage = test_usage();
        usage.set_usage_full(Some(10), Some(0), None, None);
        assert_eq!(usage.completion_tokens, 0);
        assert!(!usage.estimated);
        usage.add_estimated_completion(&"x".repeat(350));
        assert_eq!(usage.completion_tokens, 100);
        assert!(usage.estimated);
        usage.set_usage_full(None, Some(0), None, None);
        assert_eq!(usage.completion_tokens, 100);
    }

    #[test]
    fn genuinely_empty_completion_is_exact_zero() {
        let mut usage = test_usage();
        usage.set_usage_full(Some(10), Some(0), None, None);
        assert_eq!(usage.completion_tokens, 0);
        assert!(!usage.estimated);
    }

    #[test]
    fn earlier_reported_completion_survives_zero() {
        let mut usage = test_usage();
        usage.set_usage_full(Some(10), Some(5), None, None);
        usage.set_usage_full(None, Some(0), None, None);
        assert_eq!(usage.completion_tokens, 5);
        assert!(!usage.estimated);
    }

    #[test]
    fn responses_reasoning_deltas_are_counted() {
        let mut usage = test_usage();
        let mut conv = ResponsesToChat::new("m");
        let out = conv.handle_event(
            &SseEvent {
                event: None,
                data: json!({
                    "type": "response.reasoning_text.delta",
                    "item_id": "rs_1",
                    "output_index": 0,
                    "content_index": 0,
                    "delta": "abcd"
                })
                .to_string(),
            },
            &mut usage,
        );
        assert!(out
            .iter()
            .any(|c| c["choices"][0]["delta"]["reasoning_content"] == "abcd"));
        assert_eq!(usage.completion_tokens, 2);
    }

    #[tokio::test]
    async fn heartbeat_fills_idle_stream_gaps() {
        let inner: BoxStream<'static, Result<Bytes, Infallible>> = stream! {
            tokio::time::sleep(Duration::from_millis(80)).await;
            yield Ok(Bytes::from_static(b"data: [DONE]\n\n"));
        }
        .boxed();
        let mut stream = with_heartbeat(inner, Duration::from_millis(20));
        let mut heartbeats = 0;
        while let Some(item) = stream.next().await {
            if item.unwrap().starts_with(b":") {
                heartbeats += 1;
            }
        }
        assert!(heartbeats >= 2, "heartbeats: {heartbeats}");
    }

    #[test]
    fn composite_conversion_counts_output_once() {
        let mut usage = test_usage();
        let chat = SseEvent {
            event: None,
            data: json!({
                "id": "c1",
                "choices": [{"index": 0, "delta": {"content": "hello"}, "finish_reason": null}]
            })
            .to_string(),
        };
        let value: Value = serde_json::from_str(&chat.data).unwrap();

        // Second layer is disabled in composite conversions.
        let mut second = ChatToResponses::new("m", None);
        second.skip_estimates();
        let mut out = Vec::new();
        second.handle_chunk(&value, &mut usage, &mut out);
        assert_eq!(usage.completion_tokens, 0);

        // First layer accounts for the text exactly once.
        let mut first = OaiToAnth::new("m");
        let mut out = Vec::new();
        first.handle(&chat, &mut usage, &mut out);
        assert_eq!(usage.completion_tokens, 2);

        // An unskipped second layer would count the same text again.
        let mut counted = ChatToResponses::new("m", None);
        let mut out = Vec::new();
        counted.handle_chunk(&value, &mut usage, &mut out);
        assert_eq!(usage.completion_tokens, 3);
    }
}
