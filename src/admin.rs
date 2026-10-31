use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Local;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::{self, ApiFormat};
use crate::proxy;
use crate::router::SelectRequest;
use crate::util::{now_ms, now_secs};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct RangeQuery {
    pub range: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RequestsQuery {
    pub model: Option<String>,
    pub provider: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ContextsQuery {
    pub limit: Option<usize>,
}

fn range_bounds(range: &str) -> (i64, i64) {
    let now = now_ms();
    match range {
        "1h" => (now - 3_600_000, 60_000),
        "6h" => (now - 6 * 3_600_000, 300_000),
        "7d" => (now - 7 * 86_400_000, 3 * 3_600_000),
        "30d" => (now - 30 * 86_400_000, 12 * 3_600_000),
        "all" => (0, 86_400_000),
        _ => (now - 24 * 3_600_000, 1_800_000),
    }
}

fn range_name(q: &RangeQuery) -> String {
    q.range
        .clone()
        .filter(|r| ["1h", "6h", "24h", "7d", "30d", "all"].contains(&r.as_str()))
        .unwrap_or_else(|| "24h".to_string())
}

pub async fn overview(State(state): State<AppState>, Query(q): Query<RangeQuery>) -> Response {
    let range = range_name(&q);
    let (from, _) = range_bounds(&range);
    let stats = match state.stats.overview(from) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };

    Json(json!({
        "range": range,
        "from_ms": from,
        "to_ms": now_ms(),
        "uptime_secs": state.started.elapsed().as_secs(),
        "stats": stats,
    }))
    .into_response()
}

pub async fn timeseries(State(state): State<AppState>, Query(q): Query<RangeQuery>) -> Response {
    let range = range_name(&q);
    let (mut from, bucket) = range_bounds(&range);
    if range == "all" {
        from = state
            .stats
            .first_ts()
            .ok()
            .flatten()
            .map(|t| t.min(now_ms() - 86_400_000))
            .unwrap_or(now_ms() - 86_400_000);
    }
    let to = now_ms();
    let buckets = state.stats.timeseries(from, to, bucket).unwrap_or_default();
    Json(json!({
        "range": range,
        "from_ms": from,
        "to_ms": to,
        "bucket_ms": bucket,
        "buckets": buckets,
    }))
    .into_response()
}

pub async fn model_stats(State(state): State<AppState>, Query(q): Query<RangeQuery>) -> Response {
    let range = range_name(&q);
    let (from, _) = range_bounds(&range);
    let models = state.stats.by_model(from).unwrap_or_default();
    Json(json!({"range": range, "models": models})).into_response()
}

pub async fn provider_stats(
    State(state): State<AppState>,
    Query(q): Query<RangeQuery>,
) -> Response {
    let range = range_name(&q);
    let (from, _) = range_bounds(&range);
    let providers = state.stats.by_provider(from).unwrap_or_default();
    Json(json!({"range": range, "providers": providers})).into_response()
}

#[derive(Debug, Serialize)]
struct RouteView {
    route_id: String,
    provider: String,
    format: String,
    upstream: String,
    priority: i32,
    weight: u32,
    label: Option<String>,
    pricing: Option<config::PriceSnapshot>,
    has_pricing: bool,
    estimated_cost: Option<f64>,
    effective_input: Option<f64>,
    cost_currency: Option<String>,
    stats: Value,
    runtime: crate::router::RouteRuntime,
    healthy: bool,
}

/// Pick the estimated cost and its currency as one unit. A historical average
/// must not inherit the currency of an incomplete current price card.
fn estimated_route_cost(
    pricing: Option<&config::PriceSnapshot>,
    stat: Option<&crate::stats::RouteStat>,
    avg_prompt: u64,
    avg_completion: u64,
    typical_cached: u64,
    cache_write_avg: u64,
) -> (Option<f64>, Option<String>) {
    if let Some(price) = pricing {
        if let Some(cost) = config::comparable_cost(
            price,
            avg_prompt,
            avg_completion,
            typical_cached,
            cache_write_avg,
        ) {
            return (Some(cost), Some(price.currency.clone()));
        }
    }
    if let Some(stat) = stat {
        if stat.requests > 0 && stat.cost > 0.0 {
            return (
                Some(stat.cost / stat.requests as f64),
                stat.currency.clone(),
            );
        }
    }
    (None, None)
}

pub async fn routing(State(state): State<AppState>, Query(q): Query<RangeQuery>) -> Response {
    let range = range_name(&q);
    let (from, _) = range_bounds(&range);
    let cfg = state.config.read().await.clone();
    let stats = state.stats.route_stats(from, None).unwrap_or_default();
    let stat_map: HashMap<(String, String), &crate::stats::RouteStat> = stats
        .iter()
        .map(|stat| ((stat.model.clone(), stat.route_id.clone()), stat))
        .collect();
    let now = Local::now();
    let mut models = Vec::new();

    for name in cfg.model_names() {
        let plan = match cfg.resolve_plan(&name) {
            Ok(plan) => plan,
            Err(_) => continue,
        };
        let mut total_requests = 0u64;
        let mut total_prompt = 0u64;
        let mut total_completion = 0u64;
        for route in &plan.routes {
            if let Some(stat) = stat_map.get(&(plan.alias.clone(), route.route_id.clone())) {
                total_requests += stat.requests;
                total_prompt += stat.prompt_tokens;
                total_completion += stat.completion_tokens;
            }
        }
        let avg_prompt = total_prompt.checked_div(total_requests).unwrap_or(1000);
        let avg_completion = total_completion.checked_div(total_requests).unwrap_or(500);

        let mut views: Vec<RouteView> = Vec::new();
        for route in &plan.routes {
            let stat = stat_map
                .get(&(plan.alias.clone(), route.route_id.clone()))
                .copied();
            let runtime = state.router.runtime(&plan.alias, &route.route_id);
            let cache_ratio = stat
                .filter(|stat| stat.prompt_tokens > 0)
                .map(|stat| stat.cached_tokens as f64 / stat.prompt_tokens as f64)
                .or(runtime.cache_ratio)
                .unwrap_or(0.0);
            let cache_write_avg = stat
                .and_then(|stat| stat.cache_write_tokens.checked_div(stat.requests))
                .unwrap_or(0);
            let typical_cached = (avg_prompt as f64 * cache_ratio) as u64;
            let pricing = route
                .pricing
                .as_ref()
                .map(|pricing| pricing.snapshot_at(now));
            let (estimated_cost, cost_currency) = estimated_route_cost(
                pricing.as_ref(),
                stat,
                avg_prompt,
                avg_completion,
                typical_cached,
                cache_write_avg,
            );
            let effective_input = pricing
                .as_ref()
                .and_then(|price| price.effective_input(cache_ratio));
            let has_pricing = pricing
                .as_ref()
                .map(|price| price.has_any())
                .unwrap_or(false);
            let stats_json = json!({
                "requests": stat.map(|s| s.requests).unwrap_or(0),
                "errors": stat.map(|s| s.errors).unwrap_or(0),
                "prompt_tokens": stat.map(|s| s.prompt_tokens).unwrap_or(0),
                "completion_tokens": stat.map(|s| s.completion_tokens).unwrap_or(0),
                "cached_tokens": stat.map(|s| s.cached_tokens).unwrap_or(0),
                "cache_write_tokens": stat.map(|s| s.cache_write_tokens).unwrap_or(0),
                "cache_hit": cache_ratio,
                "avg_ttft_ms": stat.map(|s| s.avg_ttft_ms).unwrap_or(0.0),
                "avg_duration_ms": stat.map(|s| s.avg_duration_ms).unwrap_or(0.0),
                "cost": stat.map(|s| s.cost).unwrap_or(0.0),
                "currency": stat.and_then(|s| s.currency.clone()),
                "costs": stat.map(|s| s.costs.clone()).unwrap_or_default(),
                "last_ts": stat.map(|s| s.last_ts).unwrap_or(0),
            });
            views.push(RouteView {
                route_id: route.route_id.clone(),
                provider: route.provider.id.clone(),
                format: route.format.as_str().to_string(),
                upstream: route.upstream_model.clone(),
                priority: route.priority,
                weight: route.weight,
                label: route.label.clone(),
                has_pricing,
                pricing,
                estimated_cost,
                effective_input,
                cost_currency,
                stats: stats_json,
                healthy: runtime.healthy,
                runtime,
            });
        }

        // Estimated costs are only comparable within one currency.
        let mut cost_currencies: Vec<String> = views
            .iter()
            .filter(|view| view.estimated_cost.is_some())
            .filter_map(|view| view.cost_currency.clone())
            .collect();
        cost_currencies.sort();
        cost_currencies.dedup();
        let mixed_currencies = cost_currencies.len() > 1;

        views.sort_by(|left, right| {
            right
                .healthy
                .cmp(&left.healthy)
                .then_with(|| {
                    if mixed_currencies {
                        std::cmp::Ordering::Equal
                    } else {
                        left.estimated_cost
                            .unwrap_or(f64::INFINITY)
                            .partial_cmp(&right.estimated_cost.unwrap_or(f64::INFINITY))
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }
                })
                .then_with(|| left.priority.cmp(&right.priority))
        });

        let mut choices = serde_json::Map::new();
        for (label, format) in [
            ("openai", ApiFormat::Openai),
            ("responses", ApiFormat::Responses),
            ("anthropic", ApiFormat::Anthropic),
        ] {
            let request = SelectRequest {
                inbound: format,
                override_route: None,
                prompt_tokens: avg_prompt,
                max_output_tokens: avg_completion,
                prefer_native: cfg.routing.prefer_native,
            };
            let chosen = state
                .router
                .preview(&plan, &request)
                .ok()
                .map(|route| route.route_id);
            choices.insert(label.to_string(), json!(chosen));
        }
        let choices = Value::Object(choices);
        let current_id = choices
            .get("openai")
            .and_then(|value| value.as_str())
            .map(|value| value.to_string());
        let current_view = current_id
            .as_ref()
            .and_then(|id| views.iter().find(|view| &view.route_id == id));
        let best_view = views.first();

        let mut suggested_route: Option<String> = None;
        let mut suggested_strategy: Option<String> = None;
        let suggested_reason: String;

        if let (Some(best), Some(current)) = (best_view, current_view) {
            if best.route_id == current.route_id {
                suggested_reason =
                    "current strategy already selects the best available route".to_string();
            } else if !current.healthy && best.healthy {
                suggested_route = Some(best.route_id.clone());
                suggested_strategy = Some(if plan.strategy == "manual" {
                    "manual".to_string()
                } else {
                    "auto".to_string()
                });
                suggested_reason =
                    "current route is in cooldown; switch to a healthy route".to_string();
            } else {
                let cost_ratio = match (best.estimated_cost, current.estimated_cost) {
                    (Some(best_cost), Some(current_cost))
                        if current_cost > 0.0
                            && best.cost_currency.is_some()
                            && best.cost_currency == current.cost_currency =>
                    {
                        Some(best_cost / current_cost)
                    }
                    _ => None,
                };
                let best_requests = best
                    .stats
                    .get("requests")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                let current_requests = current
                    .stats
                    .get("requests")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                let best_ttft = best
                    .stats
                    .get("avg_ttft_ms")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0);
                let current_ttft = current
                    .stats
                    .get("avg_ttft_ms")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0);
                let latency_better = best_requests > 0
                    && current_requests > 0
                    && best_ttft > 0.0
                    && current_ttft > 0.0
                    && best_ttft < current_ttft * 0.8;

                if let Some(ratio) = cost_ratio.filter(|ratio| *ratio < 0.9) {
                    suggested_route = Some(best.route_id.clone());
                    suggested_strategy = Some(if plan.strategy == "manual" {
                        "manual".to_string()
                    } else {
                        "cheapest".to_string()
                    });
                    suggested_reason = format!(
                        "~{}% lower estimated cost at the current price",
                        ((1.0 - ratio) * 100.0).round() as i64
                    );
                } else if latency_better {
                    suggested_route = Some(best.route_id.clone());
                    suggested_strategy = Some(if plan.strategy == "manual" {
                        "manual".to_string()
                    } else {
                        "fastest".to_string()
                    });
                    suggested_reason = format!(
                        "~{}% lower observed TTFT",
                        (((current_ttft - best_ttft) / current_ttft) * 100.0).round() as i64
                    );
                } else if cost_ratio.is_some() {
                    suggested_reason = "similar estimated cost; keep the current route".to_string();
                } else if mixed_currencies {
                    suggested_reason =
                        "routes are priced in different currencies; cost comparison is disabled"
                            .to_string();
                } else {
                    suggested_reason =
                        "no comparable pricing or latency; keep the current route".to_string();
                }
            }
        } else if best_view.is_some() {
            suggested_reason = "current route is not in the enabled route set".to_string();
        } else {
            suggested_reason = "no route available".to_string();
        }

        let explicit = cfg
            .models
            .iter()
            .any(|model| model.name.eq_ignore_ascii_case(&plan.alias));
        models.push(json!({
            "model": plan.alias,
            "explicit": explicit,
            "strategy": plan.strategy,
            "manual_route": plan.manual_route,
            "suggested_route": suggested_route,
            "suggested_strategy": suggested_strategy,
            "suggested_reason": suggested_reason,
            "choices": choices,
            "routes": views,
        }));
    }

    Json(json!({"range": range, "from_ms": from, "models": models})).into_response()
}

pub async fn requests(State(state): State<AppState>, Query(q): Query<RequestsQuery>) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let offset = q.offset.unwrap_or(0).max(0);
    let total = state
        .stats
        .count(q.model.clone(), q.provider.clone())
        .unwrap_or(0);
    let rows = state
        .stats
        .recent(limit, offset, q.model.clone(), q.provider.clone())
        .unwrap_or_default();
    Json(json!({
        "total": total,
        "limit": limit,
        "offset": offset,
        "requests": rows,
    }))
    .into_response()
}

/// Metadata of the retained request contexts, newest first. Bodies are only
/// returned by `context_detail` so this list stays small under polling.
pub async fn contexts(State(state): State<AppState>, Query(q): Query<ContextsQuery>) -> Response {
    let capacity = state.contexts.capacity();
    let limit = q.limit.unwrap_or(capacity).clamp(0, 500);
    Json(json!({
        "enabled": capacity > 0,
        "capacity": capacity,
        "retained": state.contexts.len(),
        "contexts": state.contexts.list(Some(limit)),
    }))
    .into_response()
}

pub async fn context_detail(State(state): State<AppState>, Path(id): Path<u64>) -> Response {
    match state.contexts.get(id) {
        Some(context) => Json(json!({"context": context})).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "request context not found; it may have aged out of the in-memory ring"
            })),
        )
            .into_response(),
    }
}

pub async fn config_view(State(state): State<AppState>) -> Response {
    let cfg = state.config.read().await;

    let providers: Vec<Value> = cfg
        .providers
        .iter()
        .map(|p| {
            json!({
                "id": p.id,
                "format": p.format.as_str(),
                "base_url": p.base_url,
                "has_key": p.api_key.as_deref().map(|k| !k.is_empty()).unwrap_or(false),
                "models": p.models,
                "stream_usage": p.stream_usage,
                "timeout_secs": p.timeout_secs,
                "pricing": p.pricing,
            })
        })
        .collect();

    let models: Vec<Value> = cfg
        .models
        .iter()
        .map(|m| {
            let plan = cfg.resolve_plan(&m.name).ok();
            let routes: Vec<Value> = plan
                .as_ref()
                .map(|p| {
                    p.routes
                        .iter()
                        .map(|r| {
                            json!({
                                "id": r.route_id,
                                "provider": r.provider.id,
                                "format": r.format.as_str(),
                                "upstream": r.upstream_model,
                                "priority": r.priority,
                                "weight": r.weight,
                                "label": r.label,
                                "pricing": r.pricing,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            json!({
                "name": m.name,
                "provider": m.provider,
                "upstream": m.upstream.clone().unwrap_or_else(|| m.name.clone()),
                "aliases": m.aliases,
                "params": m.params,
                "description": m.description,
                "strategy": plan.as_ref().map(|p| p.strategy.clone()),
                "manual_route": plan.as_ref().and_then(|p| p.manual_route.clone()),
                "routes": routes,
            })
        })
        .collect();

    Json(json!({
        "server": {
            "host": cfg.server.host,
            "port": cfg.server.port,
            "has_api_keys": !cfg.server.api_keys.is_empty(),
            "admin_token_set": cfg.server.admin_token.as_deref().map(|t| !t.is_empty()).unwrap_or(false),
            "database": cfg.server.database,
            "response_state": cfg.server.response_state,
            "response_state_max_entries": cfg.server.response_state_max_entries,
            "response_state_max_bytes": cfg.server.response_state_max_bytes,
            "response_state_max_entry_bytes": cfg.server.response_state_max_entry_bytes,
            "stream_idle_timeout_secs": cfg.server.stream_idle_timeout_secs,
            "stream_headers_timeout_secs": cfg.server.stream_headers_timeout_secs,
            "error_body_timeout_secs": cfg.server.error_body_timeout_secs,
            "error_body_max_bytes": cfg.server.error_body_max_bytes,
            "stream_output_max_bytes": cfg.server.stream_output_max_bytes,
            "response_body_max_bytes": cfg.server.response_body_max_bytes,
            "stream_heartbeat_secs": cfg.server.stream_heartbeat_secs,
            "store_error_details": cfg.server.store_error_details,
            "allowed_origins": &cfg.server.allowed_origins,
            "allowed_hosts": &cfg.server.allowed_hosts,
            "config_path": state.config_path.display().to_string(),
        },
        "routing": cfg.routing,
        "providers": providers,
        "models": models,
        "available_models": cfg.model_names(),
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
pub struct ConfigContent {
    pub content: String,
}

pub async fn config_raw(State(state): State<AppState>) -> Response {
    match std::fs::read_to_string(state.config_path.as_ref()) {
        Ok(content) => {
            let mut response = Json(json!({
                "path": state.config_path.display().to_string(),
                "content": content,
            }))
            .into_response();
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": format!("cannot read config: {e}")})),
        )
            .into_response(),
    }
}

pub async fn validate_config(Json(payload): Json<ConfigContent>) -> Response {
    match crate::config::parse(&payload.content) {
        Ok(cfg) => Json(json!({
            "ok": true,
            "providers": cfg.providers.len(),
            "models": cfg.model_names().len(),
        }))
        .into_response(),
        Err(e) => Json(json!({"ok": false, "error": e.to_string()})).into_response(),
    }
}

/// Every path that replaces the running configuration must pass the same
/// exposure checks as save/reload. Kept in one place so sync cannot miss it.
fn ensure_config_applicable(cfg: &crate::config::Config, bound_host: &str) -> Result<(), String> {
    crate::config::check_admin_exposure(&cfg.server, bound_host)
}

pub async fn save_config(
    State(state): State<AppState>,
    Json(payload): Json<ConfigContent>,
) -> Response {
    if payload.content.len() > 2_000_000 {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"ok": false, "error": "config file is too large"})),
        )
            .into_response();
    }

    let _guard = state.config_lock.lock().await;

    let new_cfg = match crate::config::parse(&payload.content) {
        Ok(cfg) => cfg,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"ok": false, "error": e.to_string()})),
            )
                .into_response()
        }
    };
    if let Err(error) = ensure_config_applicable(&new_cfg, &state.bound_host) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": error})),
        )
            .into_response();
    }

    let old = state.config.read().await.clone();
    let mut restart_fields = Vec::new();
    if old.server.host != new_cfg.server.host {
        restart_fields.push("server.host");
    }
    if old.server.port != new_cfg.server.port {
        restart_fields.push("server.port");
    }
    if old.server.database != new_cfg.server.database {
        restart_fields.push("server.database");
    }
    let admin_token_changed = old.server.admin_token != new_cfg.server.admin_token;

    let path = state.config_path.as_ref();
    let backup = path.with_extension("toml.bak");
    let backup_written = path.exists() && backup_config(path, &backup);

    let tmp = path.with_extension(format!("toml.tmp{}", uuid::Uuid::new_v4().simple()));
    if let Err(e) = write_private(&tmp, &payload.content) {
        let _ = std::fs::remove_file(&tmp);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": format!("cannot write config: {e}")})),
        )
            .into_response();
    }
    if let Ok(meta) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(&tmp, meta.permissions());
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": format!("cannot replace config: {e}")})),
        )
            .into_response();
    }

    let providers = new_cfg.providers.len();
    let models = new_cfg.model_names().len();
    *state.config.write().await = new_cfg;
    apply_response_limits(&state).await;
    tracing::info!("config saved: {providers} provider(s), {models} model(s)");

    Json(json!({
        "ok": true,
        "providers": providers,
        "models": models,
        "restart_required": !restart_fields.is_empty(),
        "restart_fields": restart_fields,
        "admin_token_changed": admin_token_changed,
        "backup": if backup_written { Some(backup.display().to_string()) } else { None },
    }))
    .into_response()
}

pub async fn reload(State(state): State<AppState>) -> Response {
    let _guard = state.config_lock.lock().await;
    match crate::config::load(&state.config_path) {
        Ok(new_cfg) => {
            if let Err(error) = ensure_config_applicable(&new_cfg, &state.bound_host) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"ok": false, "error": error})),
                )
                    .into_response();
            }
            let providers = new_cfg.providers.len();
            let models = new_cfg.model_names().len();
            *state.config.write().await = new_cfg;
            apply_response_limits(&state).await;
            Json(json!({"ok": true, "providers": providers, "models": models})).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn health(State(state): State<AppState>) -> Response {
    let providers = state.config.read().await.providers.clone();
    let client = state.client.clone();
    let results = join_all(providers.iter().map(|p| proxy::probe_provider(&client, p))).await;
    Json(json!({"providers": results})).into_response()
}

#[derive(Debug, Deserialize, Default)]
pub struct SyncModelsRequest {
    #[serde(default)]
    pub provider: Option<String>,
}

pub async fn upstream_models(State(state): State<AppState>) -> Response {
    let providers = state.config.read().await.providers.clone();
    let client = state.client.clone();
    let results = join_all(providers.iter().cloned().map(|provider| {
        let client = client.clone();
        async move {
            let mut value = proxy::discover_models(&client, &provider).await;
            let discovered = value["models"].as_array().cloned().unwrap_or_default();
            let missing: Vec<Value> = discovered
                .iter()
                .filter(|model| {
                    let id = model.as_str().unwrap_or_default();
                    !provider
                        .models
                        .iter()
                        .any(|existing| existing.eq_ignore_ascii_case(id))
                })
                .cloned()
                .collect();
            value["configured"] = json!(provider.models);
            value["missing_count"] = json!(missing.len());
            value["missing"] = json!(missing);
            value
        }
    }))
    .await;
    Json(json!({"providers": results})).into_response()
}

fn add_models_to_provider(
    doc: &mut toml_edit::DocumentMut,
    provider_id: &str,
    discovered: &[String],
) -> Result<Vec<String>, String> {
    let providers = doc
        .get_mut("providers")
        .and_then(|item| item.as_array_of_tables_mut())
        .ok_or_else(|| "config has no [[providers]] tables".to_string())?;
    for table in providers.iter_mut() {
        if table.get("id").and_then(|value| value.as_str()) != Some(provider_id) {
            continue;
        }
        if !table.contains_key("models") {
            table.insert(
                "models",
                toml_edit::Item::Value(toml_edit::Value::Array(toml_edit::Array::new())),
            );
        }
        let array = table
            .get_mut("models")
            .and_then(|item| item.as_array_mut())
            .ok_or_else(|| format!("provider '{provider_id}' has a non-array models field"))?;
        let mut added = Vec::new();
        for model in discovered {
            if array.iter().any(|value| {
                value
                    .as_str()
                    .map(|existing| existing.eq_ignore_ascii_case(model))
                    .unwrap_or(false)
            }) {
                continue;
            }
            array.push(model.as_str());
            added.push(model.clone());
        }
        return Ok(added);
    }
    Err(format!("provider '{provider_id}' not found in config"))
}

pub async fn sync_upstream_models(
    State(state): State<AppState>,
    Json(payload): Json<SyncModelsRequest>,
) -> Response {
    let _guard = state.config_lock.lock().await;
    let cfg = state.config.read().await.clone();
    let selected: Vec<_> = cfg
        .providers
        .iter()
        .filter(|provider| {
            payload
                .provider
                .as_deref()
                .map(|id| id == provider.id)
                .unwrap_or(true)
        })
        .cloned()
        .collect();
    if selected.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"ok": false, "error": "provider not found"})),
        )
            .into_response();
    }

    let path = state.config_path.as_ref();
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"ok": false, "error": format!("cannot read config: {e}")})),
            )
                .into_response()
        }
    };
    let mut doc: toml_edit::DocumentMut = match raw.parse() {
        Ok(doc) => doc,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"ok": false, "error": format!("cannot parse config: {e}")})),
            )
                .into_response()
        }
    };

    let mut summaries = Vec::new();
    let mut errors = Vec::new();
    let mut changed = false;

    for provider in selected {
        let result = proxy::discover_models(&state.client, &provider).await;
        if !result["ok"].as_bool().unwrap_or(false) {
            errors.push(json!({"id": provider.id, "error": result["error"].clone()}));
            continue;
        }
        let discovered: Vec<String> = result["models"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        match add_models_to_provider(&mut doc, &provider.id, &discovered) {
            Ok(added) => {
                if !added.is_empty() {
                    changed = true;
                }
                summaries.push(json!({
                    "id": provider.id,
                    "added": added,
                    "upstream": discovered.len(),
                }));
            }
            Err(error) => errors.push(json!({"id": provider.id, "error": error})),
        }
    }

    if !changed {
        return Json(json!({
            "ok": errors.is_empty(),
            "changed": false,
            "providers": summaries,
            "errors": errors,
        }))
        .into_response();
    }

    let updated = doc.to_string();
    let new_cfg = match config::parse(&updated) {
        Ok(cfg) => cfg,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"ok": false, "error": format!("synced config is invalid: {e}")})),
            )
                .into_response()
        }
    };
    if let Err(error) = ensure_config_applicable(&new_cfg, &state.bound_host) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"ok": false, "error": error})),
        )
            .into_response();
    }

    let backup = path.with_extension("toml.bak");
    let backup_written = backup_config(path, &backup);
    let tmp = path.with_extension(format!("toml.tmp{}", uuid::Uuid::new_v4().simple()));
    if let Err(e) = write_private(&tmp, &updated) {
        let _ = std::fs::remove_file(&tmp);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": format!("cannot write config: {e}")})),
        )
            .into_response();
    }
    if let Ok(meta) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(&tmp, meta.permissions());
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"ok": false, "error": format!("cannot replace config: {e}")})),
        )
            .into_response();
    }

    let providers = new_cfg.providers.len();
    let models = new_cfg.model_names().len();
    *state.config.write().await = new_cfg;
    apply_response_limits(&state).await;
    tracing::info!("upstream models synced: {providers} provider(s), {models} model(s)");

    Json(json!({
        "ok": errors.is_empty(),
        "changed": true,
        "providers": summaries,
        "errors": errors,
        "backup": if backup_written { Some(backup.display().to_string()) } else { None },
    }))
    .into_response()
}

pub async fn public_models(State(state): State<AppState>) -> Response {
    let models = state.config.read().await.public_models();
    let created = now_secs();
    let data: Vec<Value> = models.iter().map(|m| model_json(m, created)).collect();
    Json(json!({
        "object": "list",
        "data": data,
        "has_more": false,
        "first_id": models.first().map(|m| m.id.clone()),
        "last_id": models.last().map(|m| m.id.clone()),
    }))
    .into_response()
}

pub async fn public_model(State(state): State<AppState>, Path(model_id): Path<String>) -> Response {
    let models = state.config.read().await.public_models();
    match models.iter().find(|m| m.id.eq_ignore_ascii_case(&model_id)) {
        Some(m) => Json(model_json(m, now_secs())).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "message": format!("model '{model_id}' not found"),
                    "type": "invalid_request_error"
                }
            })),
        )
            .into_response(),
    }
}

fn model_json(m: &crate::config::PublicModel, created: i64) -> Value {
    json!({
        "id": m.id,
        "object": "model",
        "created": created,
        "owned_by": m.provider,
        "display_name": m.id,
        "created_at": "2025-01-01T00:00:00Z",
        "type": "model",
        "upstream": m.upstream,
        "formats": m.formats,
        "routes": m.routes,
    })
}

async fn apply_response_limits(state: &AppState) {
    let cfg = state.config.read().await;
    state.responses.set_limits(
        cfg.server.response_state_max_entries.max(1),
        cfg.server.response_state_max_bytes.max(1),
        cfg.server.response_state_max_entry_bytes.max(1),
    );
    state.contexts.set_limits(
        cfg.server.request_context_limit,
        cfg.server.request_context_max_entry_bytes.max(1),
    );
}

fn backup_config(path: &std::path::Path, backup: &std::path::Path) -> bool {
    if let Err(error) = std::fs::copy(path, backup) {
        tracing::warn!("could not create config backup: {error}");
        return false;
    }
    if let Ok(meta) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(backup, meta.permissions());
    }
    true
}

#[cfg(unix)]
fn write_private(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_private(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    std::fs::write(path, content)
}

#[cfg(test)]
mod config_apply_tests {
    use super::*;

    fn cfg(host: &str, token: Option<&str>) -> crate::config::Config {
        let token_line = token
            .map(|token| format!("admin_token = \"{token}\"\n"))
            .unwrap_or_default();
        crate::config::parse(&format!(
            r#"
[server]
host = "{host}"
{token_line}
[[providers]]
id = "p"
format = "openai"
base_url = "https://example.com/v1"
models = ["m"]
"#
        ))
        .unwrap()
    }

    #[test]
    fn public_exposure_without_token_is_rejected() {
        let config = cfg("0.0.0.0", None);
        assert!(ensure_config_applicable(&config, "0.0.0.0").is_err());

        // A public bound host is also unsafe even if the file claims loopback.
        let config = cfg("127.0.0.1", None);
        assert!(ensure_config_applicable(&config, "0.0.0.0").is_err());
    }

    #[test]
    fn token_or_loopback_is_accepted() {
        let config = cfg("0.0.0.0", Some("secret"));
        assert!(ensure_config_applicable(&config, "0.0.0.0").is_ok());

        let config = cfg("127.0.0.1", None);
        assert!(ensure_config_applicable(&config, "127.0.0.1").is_ok());
    }
}

#[cfg(test)]
mod model_sync_tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn private_temp_files_are_created_with_0600() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!(
            "ai-gateway-private-test-{}",
            uuid::Uuid::new_v4().simple()
        ));
        write_private(&path, "api_key = \"secret\"").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        let _ = std::fs::remove_file(&path);
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn adds_only_missing_models_to_the_matching_provider() {
        let mut doc: toml_edit::DocumentMut = r#"
[[providers]]
id = "p"
models = ["a", "b"]

[[providers]]
id = "q"
models = ["a"]
"#
        .parse()
        .unwrap();

        let added = add_models_to_provider(
            &mut doc,
            "p",
            &["b".to_string(), "c".to_string(), "d".to_string()],
        )
        .unwrap();
        assert_eq!(added, vec!["c", "d"]);

        let providers = doc["providers"].as_array_of_tables().unwrap();
        let first = providers.iter().next().unwrap();
        let models: Vec<&str> = first["models"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|value| value.as_str())
            .collect();
        assert_eq!(models, vec!["a", "b", "c", "d"]);

        assert!(add_models_to_provider(&mut doc, "p", &["c".to_string()])
            .unwrap()
            .is_empty());
        assert!(add_models_to_provider(&mut doc, "missing", &[]).is_err());
    }
}

#[cfg(test)]
mod cost_currency_tests {
    use super::*;
    use crate::config::PriceSnapshot;
    use crate::stats::RouteStat;

    fn stat(cost: f64, currency: &str) -> RouteStat {
        RouteStat {
            route_id: "r".to_string(),
            provider: "p".to_string(),
            model: "m".to_string(),
            requests: 1,
            errors: 0,
            prompt_tokens: 1000,
            completion_tokens: 100,
            cached_tokens: 0,
            cache_write_tokens: 0,
            avg_duration_ms: 10.0,
            avg_ttft_ms: 5.0,
            cost,
            currency: Some(currency.to_string()),
            costs: Vec::new(),
            last_ts: 0,
        }
    }

    fn price(currency: &str, input: Option<f64>, output: Option<f64>) -> PriceSnapshot {
        PriceSnapshot {
            currency: currency.to_string(),
            label: None,
            input,
            output,
            cache_read: None,
            cache_write: None,
        }
    }

    #[test]
    fn partial_price_card_uses_historical_currency() {
        let pricing = price("CNY", Some(1.0), None);
        let stat = stat(1.0, "USD");
        let (cost, currency) = estimated_route_cost(Some(&pricing), Some(&stat), 1000, 100, 0, 0);
        assert_eq!(cost, Some(1.0));
        assert_eq!(currency.as_deref(), Some("USD"));
    }

    #[test]
    fn complete_price_card_uses_its_own_currency() {
        let pricing = price("CNY", Some(1.0), Some(1.0));
        let stat = stat(1.0, "USD");
        let (cost, currency) = estimated_route_cost(Some(&pricing), Some(&stat), 1000, 100, 0, 0);
        assert!(cost.is_some());
        assert_eq!(currency.as_deref(), Some("CNY"));
    }

    #[test]
    fn no_source_has_no_currency() {
        let pricing = price("CNY", Some(1.0), None);
        let (cost, currency) = estimated_route_cost(Some(&pricing), None, 1000, 100, 0, 0);
        assert_eq!(cost, None);
        assert_eq!(currency, None);
    }
}
