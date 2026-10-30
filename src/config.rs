use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Local, Timelike, Weekday};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiFormat {
    Openai,
    Anthropic,
    #[serde(alias = "openai-responses", alias = "openai_responses")]
    Responses,
}

impl ApiFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            ApiFormat::Openai => "openai",
            ApiFormat::Anthropic => "anthropic",
            ApiFormat::Responses => "responses",
        }
    }
}

fn default_host() -> String {
    "127.0.0.1".into()
}

fn default_port() -> u16 {
    8787
}

fn default_db() -> String {
    "gateway.db".into()
}

fn default_true() -> bool {
    true
}

fn default_timeout() -> u64 {
    600
}

fn default_currency() -> String {
    "USD".into()
}

fn default_priority() -> i32 {
    100
}

fn default_weight() -> u32 {
    1
}

fn default_strategy() -> String {
    "auto".into()
}

fn default_cooldown() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub routing: RoutingConfig,
    #[serde(default)]
    pub providers: Vec<Provider>,
    #[serde(default)]
    pub models: Vec<ModelEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// Optional client keys for the inference endpoints. Empty = no auth.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// Optional token that protects /admin/* (and therefore the dashboard data).
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Extra browser origins allowed to call the gateway. Empty = same-origin
    /// only; cross-origin requests are rejected unless listed here.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    /// Host header names accepted by the gateway. Loopback and the bound host
    /// are always accepted; add reverse-proxy/public hostnames here.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// Store upstream error bodies in SQLite. Disabled by default so prompts
    /// echoed by an upstream are not persisted.
    #[serde(default)]
    pub store_error_details: bool,
    /// SQLite file used for usage accounting.
    #[serde(default = "default_db")]
    pub database: String,
    /// Keep a bounded in-memory cache of Responses API turns so that
    /// `previous_response_id` works when the upstream is not a Responses API.
    /// Nothing is written to disk.
    #[serde(default = "default_true")]
    pub response_state: bool,
    /// Maximum number of cached Responses turns.
    #[serde(default = "default_response_state_entries")]
    pub response_state_max_entries: usize,
    /// Global byte budget for the Responses turn cache.
    #[serde(default = "default_response_state_bytes")]
    pub response_state_max_bytes: usize,
    /// Maximum size of one cached Responses turn. Oversized turns are not cached.
    #[serde(default = "default_response_state_entry_bytes")]
    pub response_state_max_entry_bytes: usize,
    /// Abort a streaming upstream that stops sending bytes for this many seconds.
    /// 0 disables the idle timeout.
    #[serde(default = "default_stream_idle")]
    pub stream_idle_timeout_secs: u64,
    /// Abort a streaming request that has not received response headers within
    /// this many seconds. 0 disables the header timeout.
    #[serde(default = "default_stream_headers")]
    pub stream_headers_timeout_secs: u64,
    /// Timeout for reading an upstream error body.
    #[serde(default = "default_error_body_timeout")]
    pub error_body_timeout_secs: u64,
    /// Maximum bytes of upstream error body retained for diagnostics.
    #[serde(default = "default_error_body_bytes")]
    pub error_body_max_bytes: usize,
    /// Maximum bytes of generated text retained for a Responses conversion.
    /// Longer streams are failed instead of growing without bound.
    #[serde(default = "default_stream_output_bytes")]
    pub stream_output_max_bytes: usize,
    /// Maximum bytes of a successful non-streaming upstream response.
    /// 0 disables the limit.
    #[serde(default = "default_response_body_bytes")]
    pub response_body_max_bytes: usize,
    /// Send an SSE comment this often while an upstream stream is idle so
    /// downstream proxies keep the connection open. 0 disables heartbeats.
    #[serde(default = "default_stream_heartbeat")]
    pub stream_heartbeat_secs: u64,
}

fn default_stream_headers() -> u64 {
    30
}

fn default_error_body_timeout() -> u64 {
    10
}

fn default_error_body_bytes() -> usize {
    256 * 1024
}

fn default_stream_output_bytes() -> usize {
    8 * 1024 * 1024
}

fn default_response_body_bytes() -> usize {
    32 * 1024 * 1024
}

fn default_stream_heartbeat() -> u64 {
    15
}

fn default_stream_idle() -> u64 {
    120
}

fn default_response_state_entries() -> usize {
    128
}

fn default_response_state_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_response_state_entry_bytes() -> usize {
    2 * 1024 * 1024
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            api_keys: Vec::new(),
            admin_token: None,
            allowed_origins: Vec::new(),
            allowed_hosts: Vec::new(),
            store_error_details: false,
            database: default_db(),
            response_state: true,
            response_state_max_entries: default_response_state_entries(),
            response_state_max_bytes: default_response_state_bytes(),
            response_state_max_entry_bytes: default_response_state_entry_bytes(),
            stream_idle_timeout_secs: default_stream_idle(),
            stream_headers_timeout_secs: default_stream_headers(),
            error_body_timeout_secs: default_error_body_timeout(),
            error_body_max_bytes: default_error_body_bytes(),
            stream_output_max_bytes: default_stream_output_bytes(),
            response_body_max_bytes: default_response_body_bytes(),
            stream_heartbeat_secs: default_stream_heartbeat(),
        }
    }
}

pub fn is_loopback_host(host: &str) -> bool {
    let host = host.trim();
    host.eq_ignore_ascii_case("localhost")
        || host == "127.0.0.1"
        || host == "::1"
        || host == "[::1]"
}

/// Refuse to expose a public listener without an admin token. `bound_host` is
/// the address the process actually listens on (which may come from --host).
pub fn check_admin_exposure(server: &ServerConfig, bound_host: &str) -> Result<(), String> {
    let public = !is_loopback_host(&server.host) || !is_loopback_host(bound_host);
    let has_token = server
        .admin_token
        .as_deref()
        .map(str::trim)
        .map(|token| !token.is_empty())
        .unwrap_or(false);
    if public && !has_token {
        return Err(format!(
            "refusing to expose admin endpoints on {bound_host} without server.admin_token"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// auto | manual | priority | round_robin | cheapest | fastest
    #[serde(default = "default_strategy")]
    pub default_strategy: String,
    /// In `auto`, prefer routes that already speak the inbound protocol.
    #[serde(default = "default_true")]
    pub prefer_native: bool,
    /// A route that fails repeatedly is avoided for this many seconds.
    #[serde(default = "default_cooldown")]
    pub failure_cooldown_secs: u64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            default_strategy: default_strategy(),
            prefer_native: true,
            failure_cooldown_secs: default_cooldown(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pricing {
    #[serde(default = "default_currency")]
    pub currency: String,
    /// USD (or `currency`) per 1,000,000 input tokens.
    #[serde(default)]
    pub input: Option<f64>,
    /// Per 1,000,000 output tokens.
    #[serde(default)]
    pub output: Option<f64>,
    /// Per 1,000,000 cached input tokens.
    #[serde(default)]
    pub cache_read: Option<f64>,
    /// Per 1,000,000 cache-write input tokens.
    #[serde(default)]
    pub cache_write: Option<f64>,
    #[serde(default)]
    pub schedules: Vec<PriceSchedule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceSchedule {
    #[serde(default)]
    pub label: Option<String>,
    /// Empty = every day. Accepts mon/tue/... or full names.
    #[serde(default)]
    pub days: Vec<String>,
    /// HH:MM local time. Windows where start > end wrap midnight.
    pub start: String,
    pub end: String,
    #[serde(default)]
    pub input: Option<f64>,
    #[serde(default)]
    pub output: Option<f64>,
    #[serde(default)]
    pub cache_read: Option<f64>,
    #[serde(default)]
    pub cache_write: Option<f64>,
    /// Multiplies the resulting prices (base or overridden) by this factor.
    #[serde(default)]
    pub multiplier: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PriceSnapshot {
    pub currency: String,
    pub label: Option<String>,
    pub input: Option<f64>,
    pub output: Option<f64>,
    pub cache_read: Option<f64>,
    pub cache_write: Option<f64>,
}

impl Pricing {
    pub fn snapshot_at(&self, now: DateTime<Local>) -> PriceSnapshot {
        let mut snapshot = PriceSnapshot {
            currency: self.currency.clone(),
            label: None,
            input: self.input,
            output: self.output,
            cache_read: self.cache_read,
            cache_write: self.cache_write,
        };
        if let Some(schedule) = self.schedules.iter().find(|s| s.matches(now)) {
            if schedule.input.is_some() {
                snapshot.input = schedule.input;
            }
            if schedule.output.is_some() {
                snapshot.output = schedule.output;
            }
            if schedule.cache_read.is_some() {
                snapshot.cache_read = schedule.cache_read;
            }
            if schedule.cache_write.is_some() {
                snapshot.cache_write = schedule.cache_write;
            }
            if let Some(multiplier) = schedule.multiplier {
                snapshot.input = snapshot.input.map(|v| v * multiplier);
                snapshot.output = snapshot.output.map(|v| v * multiplier);
                snapshot.cache_read = snapshot.cache_read.map(|v| v * multiplier);
                snapshot.cache_write = snapshot.cache_write.map(|v| v * multiplier);
            }
            snapshot.label = schedule.label.clone();
        }
        snapshot
    }
}

impl PriceSnapshot {
    pub fn has_any(&self) -> bool {
        self.input.is_some()
            || self.output.is_some()
            || self.cache_read.is_some()
            || self.cache_write.is_some()
    }

    /// Blended input price per 1M tokens for a given cache-hit ratio.
    pub fn effective_input(&self, cache_ratio: f64) -> Option<f64> {
        let ratio = cache_ratio.clamp(0.0, 1.0);
        match (self.input, self.cache_read) {
            (Some(input), Some(cached)) => Some(input * (1.0 - ratio) + cached * ratio),
            (Some(input), None) => Some(input),
            (None, Some(cached)) => Some(cached),
            (None, None) => None,
        }
    }

    /// A single comparable score per 1M tokens (blended input + output).
    pub fn effective_score(&self, cache_ratio: f64) -> Option<f64> {
        match (self.effective_input(cache_ratio), self.output) {
            (Some(input), Some(output)) => Some(input + output),
            (Some(input), None) => Some(input),
            (None, Some(output)) => Some(output),
            (None, None) => None,
        }
    }
}

impl PriceSchedule {
    fn matches(&self, now: DateTime<Local>) -> bool {
        self.matches_at(now.weekday(), now.hour() * 60 + now.minute())
    }

    fn matches_at(&self, weekday: Weekday, minute: u32) -> bool {
        let start = parse_hhmm(&self.start).unwrap_or(0);
        let end = parse_hhmm(&self.end).unwrap_or(24 * 60);
        let wraps = start > end;
        let in_window = if start == end {
            true
        } else if start < end {
            minute >= start && minute < end
        } else {
            minute >= start || minute < end
        };
        if !in_window {
            return false;
        }
        if self.days.is_empty() {
            return true;
        }
        // A window such as Mon 22:00-06:00 covers early Tuesday too; the
        // after-midnight part belongs to the previous weekday.
        let weekday = if wraps && minute < end {
            weekday.pred()
        } else {
            weekday
        };
        self.days.iter().any(|day| day_matches(day, weekday))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provider {
    pub id: String,
    pub format: ApiFormat,
    pub base_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    /// Extra headers sent to the upstream, e.g. OpenRouter's HTTP-Referer.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Upstream model ids this provider serves. Used for name resolution.
    #[serde(default)]
    pub models: Vec<String>,
    /// Optional absolute URL for the model catalogue.
    /// Defaults to `<base_url>/models`.
    #[serde(default)]
    pub models_url: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Inject `stream_options.include_usage` on OpenAI streaming calls so the
    /// gateway can account tokens. Disable for backends that reject the field.
    #[serde(default = "default_true")]
    pub stream_usage: bool,
    /// Default price card inherited by routes that do not define their own.
    #[serde(default)]
    pub pricing: Option<Pricing>,
}

impl Provider {
    /// Absolute URL of the upstream model catalogue.
    pub fn models_endpoint(&self) -> String {
        if let Some(url) = self
            .models_url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
        {
            return url.to_string();
        }
        let base = self.base_url.trim_end_matches('/');
        let has_path = reqwest::Url::parse(base)
            .map(|url| !matches!(url.path(), "" | "/"))
            .unwrap_or(false);
        if has_path {
            format!("{base}/models")
        } else {
            format!("{base}/v1/models")
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRoute {
    /// Stable id used by the manual strategy and `x-gateway-route`.
    #[serde(default)]
    pub id: Option<String>,
    /// Provider id.
    pub provider: String,
    /// Upstream model id. Defaults to the model name.
    #[serde(default)]
    pub upstream: Option<String>,
    /// Override the provider's protocol for this route. This is how one model
    /// can expose openai / responses / anthropic endpoints at the same time.
    #[serde(default)]
    pub format: Option<ApiFormat>,
    /// Lower is preferred.
    #[serde(default = "default_priority")]
    pub priority: i32,
    /// Weight used by round_robin.
    #[serde(default = "default_weight")]
    pub weight: u32,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Route-level default request parameters (merged over model params).
    #[serde(default)]
    pub params: HashMap<String, Value>,
    #[serde(default)]
    pub pricing: Option<Pricing>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelEntry {
    /// Name exposed to clients (alias).
    pub name: String,
    /// Legacy single-provider route. Ignored when `routes` is non-empty.
    #[serde(default)]
    pub provider: Option<String>,
    /// Legacy upstream model id. Defaults to `name`.
    #[serde(default)]
    pub upstream: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Extra request parameters merged as defaults (client values win).
    #[serde(default)]
    pub params: HashMap<String, Value>,
    #[serde(default)]
    pub description: Option<String>,
    /// Multiple routes across providers and protocols.
    #[serde(default)]
    pub routes: Vec<ModelRoute>,
    /// auto | manual | priority | round_robin | cheapest | fastest
    #[serde(default)]
    pub strategy: Option<String>,
    /// Route id used when strategy = "manual".
    #[serde(default)]
    pub route: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Resolved {
    pub alias: String,
    pub upstream_model: String,
    pub provider: Provider,
    pub format: ApiFormat,
    pub route_id: String,
    pub priority: i32,
    pub weight: u32,
    pub params: HashMap<String, Value>,
    pub pricing: Option<Pricing>,
    pub label: Option<String>,
}

impl Resolved {
    pub fn provider_id(&self) -> &str {
        &self.provider.id
    }
}

#[derive(Debug, Clone)]
pub struct ModelPlan {
    pub alias: String,
    pub requested: String,
    pub strategy: String,
    pub manual_route: Option<String>,
    pub routes: Vec<Resolved>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicModel {
    pub id: String,
    pub provider: String,
    pub upstream: String,
    pub description: Option<String>,
    pub formats: Vec<String>,
    pub routes: usize,
}

pub fn parse(raw: &str) -> Result<Config> {
    let mut cfg: Config = toml::from_str(raw).context("invalid TOML")?;
    cfg.expand_env()?;
    cfg.validate()?;
    Ok(cfg)
}

pub fn load(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read config file {}", path.display()))?;
    parse(&raw).with_context(|| format!("invalid config in {}", path.display()))
}

impl Config {
    fn expand_env(&mut self) -> Result<()> {
        for p in &mut self.providers {
            p.base_url = expand_env(&p.base_url)?;
            if let Some(key) = &mut p.api_key {
                *key = expand_env(key)?;
            }
            for value in p.headers.values_mut() {
                *value = expand_env(value)?;
            }
        }
        if let Some(token) = &mut self.server.admin_token {
            *token = expand_env(token)?;
        }
        for key in &mut self.server.api_keys {
            *key = expand_env(key)?;
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.providers.is_empty() {
            bail!("config has no [[providers]]; add at least one provider");
        }
        if !valid_strategy(&self.routing.default_strategy) {
            bail!(
                "routing.default_strategy '{}' is not one of auto/manual/priority/round_robin/cheapest/fastest",
                self.routing.default_strategy
            );
        }

        let mut seen = HashSet::new();
        for p in &self.providers {
            if p.id.trim().is_empty() {
                bail!("provider id must not be empty");
            }
            if !seen.insert(p.id.to_lowercase()) {
                bail!("duplicate provider id '{}'", p.id);
            }
            if p.base_url.trim().is_empty() {
                bail!("provider '{}' has an empty base_url", p.id);
            }
            if p.base_url.contains("${") {
                bail!(
                    "provider '{}' base_url still contains an unexpanded env var: {}",
                    p.id,
                    p.base_url
                );
            }
            if let Some(pricing) = &p.pricing {
                validate_pricing(pricing, &format!("provider '{}'", p.id))?;
            }
        }

        let ids: HashSet<String> = self.providers.iter().map(|p| p.id.to_lowercase()).collect();
        let mut model_names = HashSet::new();
        for m in &self.models {
            if m.name.trim().is_empty() {
                bail!("model entry has an empty name");
            }
            if !model_names.insert(m.name.to_lowercase()) {
                bail!("duplicate model name '{}'", m.name);
            }
            for alias in &m.aliases {
                if alias.trim().is_empty() {
                    bail!("model '{}' has an empty alias", m.name);
                }
                if !model_names.insert(alias.to_lowercase()) {
                    bail!(
                        "model '{}' alias '{}' collides with another model name or alias",
                        m.name,
                        alias
                    );
                }
            }
            if m.routes.is_empty() {
                let provider = m
                    .provider
                    .as_deref()
                    .filter(|p| !p.trim().is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "model '{}' needs either a provider or [[models.routes]]",
                            m.name
                        )
                    })?;
                if !ids.contains(&provider.to_lowercase()) {
                    bail!(
                        "model '{}' references unknown provider '{}'",
                        m.name,
                        provider
                    );
                }
            } else {
                let mut route_ids = HashSet::new();
                for (index, route) in m.routes.iter().enumerate() {
                    if route.provider.trim().is_empty() {
                        bail!(
                            "model '{}' route #{} has an empty provider",
                            m.name,
                            index + 1
                        );
                    }
                    if let Some(id) = &route.id {
                        if id.trim().is_empty() {
                            bail!("model '{}' route #{} has an empty id", m.name, index + 1);
                        }
                    }
                    if let Some(upstream) = &route.upstream {
                        if upstream.trim().is_empty() {
                            bail!(
                                "model '{}' route #{} has an empty upstream",
                                m.name,
                                index + 1
                            );
                        }
                    }
                    if !ids.contains(&route.provider.to_lowercase()) {
                        bail!(
                            "model '{}' route #{} references unknown provider '{}'",
                            m.name,
                            index + 1,
                            route.provider
                        );
                    }
                    if route.weight == 0 {
                        bail!(
                            "model '{}' route '{}' has weight 0; use enabled = false to disable it",
                            m.name,
                            route.id.as_deref().unwrap_or("(unnamed)")
                        );
                    }
                    if let Some(pricing) = &route.pricing {
                        validate_pricing(
                            pricing,
                            &format!(
                                "model '{}' route '{}'",
                                m.name,
                                route.id.as_deref().unwrap_or("(unnamed)")
                            ),
                        )?;
                    }
                    let format = route.format.unwrap_or_else(|| {
                        self.provider(&route.provider)
                            .map(|p| p.format)
                            .unwrap_or(ApiFormat::Openai)
                    });
                    let base_id = route
                        .id
                        .clone()
                        .unwrap_or_else(|| format!("{}:{}", route.provider, format.as_str()));
                    if !route_ids.insert(base_id.clone()) {
                        bail!(
                            "model '{}' has duplicate route id '{}'; set a unique `id`",
                            m.name,
                            base_id
                        );
                    }
                }
            }

            let strategy = m
                .strategy
                .as_deref()
                .unwrap_or(&self.routing.default_strategy);
            if !valid_strategy(strategy) {
                bail!(
                    "model '{}' strategy '{}' is not one of auto/manual/priority/round_robin/cheapest/fastest",
                    m.name,
                    strategy
                );
            }
            if strategy == "manual" {
                let route = m
                    .route
                    .as_deref()
                    .filter(|r| !r.is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "model '{}' uses strategy = \"manual\" but no `route` is set",
                            m.name
                        )
                    })?;
                let routes = m.resolved_routes(self).map_err(|e| anyhow::anyhow!(e))?;
                if !routes.iter().any(|r| r.route_id == route) {
                    bail!(
                        "model '{}' manual route '{}' not found; known routes: {}",
                        m.name,
                        route,
                        routes
                            .iter()
                            .map(|r| r.route_id.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
        }
        Ok(())
    }

    pub fn provider(&self, id: &str) -> Option<&Provider> {
        self.providers
            .iter()
            .find(|p| p.id.eq_ignore_ascii_case(id))
    }

    /// Resolve a client-facing model name to every configured route.
    ///
    /// Resolution order:
    ///   1. `provider/model` explicit syntax (one route, manual)
    ///   2. `[[models]]` name or alias (all routes)
    ///   3. any model listed in a provider's `models` array (all providers)
    pub fn resolve_plan(&self, requested: &str) -> Result<ModelPlan, String> {
        let requested = requested.trim();
        if requested.is_empty() {
            return Err("missing 'model' field".into());
        }

        if let Some((provider_id, model)) = requested.split_once('/') {
            if let Some(provider) = self.provider(provider_id) {
                if model.is_empty() {
                    return Err(format!("invalid model reference '{}'", requested));
                }
                let route_id = format!("{}:{}", provider.id, provider.format.as_str());
                let route = self.route_for_provider(
                    provider,
                    requested,
                    model,
                    None,
                    route_id.clone(),
                    default_priority(),
                    default_weight(),
                    HashMap::new(),
                    None,
                    None,
                );
                return Ok(ModelPlan {
                    alias: requested.to_string(),
                    requested: requested.to_string(),
                    strategy: "manual".to_string(),
                    manual_route: Some(route_id),
                    routes: vec![route],
                });
            }
        }

        if let Some(entry) = self.models.iter().find(|m| {
            m.name.eq_ignore_ascii_case(requested)
                || m.aliases.iter().any(|a| a.eq_ignore_ascii_case(requested))
        }) {
            let routes = entry.resolved_routes(self)?;
            let strategy = entry
                .strategy
                .clone()
                .unwrap_or_else(|| self.routing.default_strategy.clone());
            let manual_route = if strategy == "manual" {
                let route = entry.route.clone().ok_or_else(|| {
                    format!(
                        "model '{}' uses strategy = \"manual\" but no `route` is set",
                        entry.name
                    )
                })?;
                if !routes.iter().any(|r| r.route_id == route) {
                    return Err(format!(
                        "model '{}' manual route '{}' not found; known routes: {}",
                        entry.name,
                        route,
                        routes
                            .iter()
                            .map(|r| r.route_id.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                Some(route)
            } else {
                entry.route.clone()
            };
            return Ok(ModelPlan {
                alias: entry.name.clone(),
                requested: requested.to_string(),
                strategy,
                manual_route,
                routes,
            });
        }

        let mut routes = Vec::new();
        let mut seen = HashSet::new();
        for provider in &self.providers {
            if !provider
                .models
                .iter()
                .any(|m| m.eq_ignore_ascii_case(requested))
            {
                continue;
            }
            let route_id = format!("{}:{}", provider.id, provider.format.as_str());
            if !seen.insert(route_id.clone()) {
                continue;
            }
            routes.push(self.route_for_provider(
                provider,
                requested,
                requested,
                None,
                route_id,
                default_priority(),
                default_weight(),
                HashMap::new(),
                None,
                None,
            ));
        }
        if routes.is_empty() {
            return Err(format!(
                "unknown model '{}'. available models: {}",
                requested,
                self.model_names().join(", ")
            ));
        }
        let strategy = self.routing.default_strategy.clone();
        let manual_route = if strategy == "manual" {
            routes
                .iter()
                .min_by(|left, right| {
                    left.priority
                        .cmp(&right.priority)
                        .then_with(|| left.route_id.cmp(&right.route_id))
                })
                .map(|route| route.route_id.clone())
        } else {
            None
        };
        Ok(ModelPlan {
            alias: requested.to_string(),
            requested: requested.to_string(),
            strategy,
            manual_route,
            routes,
        })
    }

    /// Backwards-compatible single-route resolution used by tests and admin views.
    pub fn resolve(&self, requested: &str, _inbound: ApiFormat) -> Result<Resolved, String> {
        let plan = self.resolve_plan(requested)?;
        if plan.strategy == "manual" {
            if let Some(route_id) = &plan.manual_route {
                if let Some(route) = plan.routes.iter().find(|r| &r.route_id == route_id) {
                    return Ok(route.clone());
                }
            }
        }
        plan.routes
            .into_iter()
            .min_by_key(|r| r.priority)
            .ok_or_else(|| format!("model '{}' has no enabled routes", requested))
    }

    /// Every name a client can use.
    pub fn model_names(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for m in &self.models {
            if !names.iter().any(|n| n.eq_ignore_ascii_case(&m.name)) {
                names.push(m.name.clone());
            }
        }
        for p in &self.providers {
            for m in &p.models {
                if !names.iter().any(|n| n.eq_ignore_ascii_case(m)) {
                    names.push(m.clone());
                }
            }
        }
        names
    }

    pub fn public_models(&self) -> Vec<PublicModel> {
        self.model_names()
            .into_iter()
            .filter_map(|name| {
                let plan = self.resolve_plan(&name).ok()?;
                let first = plan.routes.first()?;
                let mut formats: Vec<String> = plan
                    .routes
                    .iter()
                    .filter(|r| r.weight > 0)
                    .map(|r| r.format.as_str().to_string())
                    .collect();
                formats.sort();
                formats.dedup();
                Some(PublicModel {
                    id: name.clone(),
                    provider: first.provider.id.clone(),
                    upstream: first.upstream_model.clone(),
                    description: self
                        .models
                        .iter()
                        .find(|m| m.name.eq_ignore_ascii_case(&name))
                        .and_then(|m| m.description.clone()),
                    formats,
                    routes: plan.routes.len(),
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn route_for_provider(
        &self,
        provider: &Provider,
        alias: &str,
        upstream: &str,
        format: Option<ApiFormat>,
        route_id: String,
        priority: i32,
        weight: u32,
        params: HashMap<String, Value>,
        pricing: Option<Pricing>,
        label: Option<String>,
    ) -> Resolved {
        let format = format.unwrap_or(provider.format);
        Resolved {
            alias: alias.to_string(),
            upstream_model: upstream.to_string(),
            provider: provider.clone(),
            format,
            route_id,
            priority,
            weight,
            params,
            pricing: pricing.or_else(|| provider.pricing.clone()),
            label,
        }
    }
}

impl ModelEntry {
    pub fn resolved_routes(&self, cfg: &Config) -> Result<Vec<Resolved>, String> {
        if self.routes.is_empty() {
            let provider_id = self
                .provider
                .as_deref()
                .filter(|p| !p.trim().is_empty())
                .ok_or_else(|| {
                    format!(
                        "model '{}' needs either a provider or [[models.routes]]",
                        self.name
                    )
                })?;
            let provider = cfg.provider(provider_id).ok_or_else(|| {
                format!(
                    "model '{}' references unknown provider '{}'",
                    self.name, provider_id
                )
            })?;
            let upstream = self.upstream.clone().unwrap_or_else(|| self.name.clone());
            let route_id = format!("{}:{}", provider.id, provider.format.as_str());
            return Ok(vec![cfg.route_for_provider(
                provider,
                &self.name,
                &upstream,
                None,
                route_id,
                default_priority(),
                default_weight(),
                self.params.clone(),
                None,
                None,
            )]);
        }

        let mut routes = Vec::new();
        let mut seen = HashSet::new();
        for route in &self.routes {
            if !route.enabled {
                continue;
            }
            let provider = cfg.provider(&route.provider).ok_or_else(|| {
                format!(
                    "model '{}' references unknown provider '{}'",
                    self.name, route.provider
                )
            })?;
            let format = route.format.unwrap_or(provider.format);
            let upstream = route.upstream.clone().unwrap_or_else(|| self.name.clone());
            let mut params = self.params.clone();
            for (key, value) in &route.params {
                params.insert(key.clone(), value.clone());
            }
            let base_id = route
                .id
                .clone()
                .unwrap_or_else(|| format!("{}:{}", provider.id, format.as_str()));
            let mut route_id = base_id.clone();
            let mut suffix = 2;
            while !seen.insert(route_id.clone()) {
                route_id = format!("{base_id}-{suffix}");
                suffix += 1;
            }
            routes.push(cfg.route_for_provider(
                provider,
                &self.name,
                &upstream,
                Some(format),
                route_id,
                route.priority,
                route.weight,
                params,
                route.pricing.clone(),
                route.label.clone(),
            ));
        }
        Ok(routes)
    }
}

pub fn valid_strategy(strategy: &str) -> bool {
    matches!(
        strategy,
        "auto" | "manual" | "priority" | "round_robin" | "cheapest" | "fastest"
    )
}

/// Comparable cost: both the ordinary input price and the output price must be
/// present. Cache-only or output-only cards are not used for route comparison.
pub fn comparable_cost(
    price: &PriceSnapshot,
    prompt_tokens: u64,
    completion_tokens: u64,
    cached_tokens: u64,
    cache_write_tokens: u64,
) -> Option<f64> {
    let input = price.input?;
    price.output?;
    let cache_read = price.cache_read.unwrap_or(input);
    let cache_write = price.cache_write.unwrap_or(input);
    let cached = cached_tokens.min(prompt_tokens);
    let cache_write_count = cache_write_tokens.min(prompt_tokens.saturating_sub(cached));
    let uncached = prompt_tokens
        .saturating_sub(cached)
        .saturating_sub(cache_write_count);
    Some(
        (uncached as f64 * input
            + cached as f64 * cache_read
            + cache_write_count as f64 * cache_write
            + completion_tokens as f64 * price.output.unwrap_or(0.0))
            / 1_000_000.0,
    )
}

fn validate_pricing(pricing: &Pricing, context: &str) -> Result<()> {
    if pricing.currency.trim().is_empty() {
        bail!("{context}: pricing.currency must not be empty");
    }
    for (name, value) in [
        ("input", pricing.input),
        ("output", pricing.output),
        ("cache_read", pricing.cache_read),
        ("cache_write", pricing.cache_write),
    ] {
        if let Some(value) = value {
            if !value.is_finite() || value < 0.0 {
                bail!("{context}: pricing.{name} must be a non-negative number");
            }
        }
    }
    for (index, schedule) in pricing.schedules.iter().enumerate() {
        let label = schedule
            .label
            .as_deref()
            .map(|l| format!(" '{l}'"))
            .unwrap_or_default();
        if parse_hhmm(&schedule.start).is_none() {
            bail!(
                "{context}: schedule #{}{label} has invalid start '{}' (use HH:MM)",
                index + 1,
                schedule.start
            );
        }
        if parse_hhmm(&schedule.end).is_none() {
            bail!(
                "{context}: schedule #{}{label} has invalid end '{}' (use HH:MM)",
                index + 1,
                schedule.end
            );
        }
        for day in &schedule.days {
            if day_key(day).is_none() {
                bail!(
                    "{context}: schedule #{}{label} has unknown day '{}' (use mon/tue/... or full names)",
                    index + 1,
                    day
                );
            }
        }
        for (name, value) in [
            ("input", schedule.input),
            ("output", schedule.output),
            ("cache_read", schedule.cache_read),
            ("cache_write", schedule.cache_write),
            ("multiplier", schedule.multiplier),
        ] {
            if let Some(value) = value {
                if !value.is_finite() || value < 0.0 {
                    bail!(
                        "{context}: schedule #{}{label} {name} must be a non-negative number",
                        index + 1
                    );
                }
            }
        }
        if schedule.input.is_none()
            && schedule.output.is_none()
            && schedule.cache_read.is_none()
            && schedule.cache_write.is_none()
            && schedule.multiplier.is_none()
        {
            bail!(
                "{context}: schedule #{}{label} does not change any price",
                index + 1
            );
        }
    }
    Ok(())
}

fn day_matches(day: &str, weekday: Weekday) -> bool {
    day_key(day) == Some(weekday_key(weekday))
}

fn day_key(day: &str) -> Option<&'static str> {
    let lower = day.trim().to_ascii_lowercase();
    let prefix = lower.get(..3)?;
    match prefix {
        "mon" => Some("mon"),
        "tue" => Some("tue"),
        "wed" => Some("wed"),
        "thu" => Some("thu"),
        "fri" => Some("fri"),
        "sat" => Some("sat"),
        "sun" => Some("sun"),
        _ => None,
    }
}

fn weekday_key(weekday: Weekday) -> &'static str {
    match weekday {
        Weekday::Mon => "mon",
        Weekday::Tue => "tue",
        Weekday::Wed => "wed",
        Weekday::Thu => "thu",
        Weekday::Fri => "fri",
        Weekday::Sat => "sat",
        Weekday::Sun => "sun",
    }
}

fn parse_hhmm(value: &str) -> Option<u32> {
    let (hours, minutes) = value.trim().split_once(':')?;
    let hours: u32 = hours.trim().parse().ok()?;
    let minutes: u32 = minutes.trim().parse().ok()?;
    if hours > 24 || minutes > 59 || (hours == 24 && minutes != 0) {
        return None;
    }
    Some(hours * 60 + minutes)
}

/// Expand `${VAR}` references. Fails loudly when a variable is missing.
fn expand_env(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return Ok(out);
        };
        let name = &after[..end];
        let value = std::env::var(name)
            .with_context(|| format!("environment variable '{}' is not set", name))?;
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Resolve a possibly relative data path against the config file directory.
pub fn resolve_data_path(config_path: &Path, database: &str) -> PathBuf {
    let p = Path::new(database);
    if p.is_absolute() {
        p.to_path_buf()
    } else if let Some(parent) = config_path.parent() {
        parent.join(p)
    } else {
        p.to_path_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Config {
        let raw = r#"
[[providers]]
id = "oai"
format = "openai"
base_url = "https://api.openai.com/v1"
models = ["gpt-4o"]

[[providers]]
id = "anth"
format = "anthropic"
base_url = "https://api.anthropic.com/v1"
models = ["claude-x"]

[[models]]
name = "fast"
provider = "oai"
upstream = "gpt-4o"

[[models]]
name = "smart"
provider = "anth"
upstream = "claude-x"
[models.params]
max_tokens = 2048

[[models]]
name = "multi"
strategy = "auto"
[[models.routes]]
id = "multi-oai"
provider = "oai"
upstream = "gpt-4o"
priority = 10
[models.routes.pricing]
input = 2.0
output = 4.0
cache_read = 0.2
[[models.routes.pricing.schedules]]
label = "night"
start = "00:00"
end = "24:00"
input = 1.0
output = 2.0
[[models.routes]]
id = "multi-anth"
provider = "anth"
upstream = "claude-x"
priority = 20
"#;
        toml::from_str(raw).unwrap()
    }

    #[test]
    fn resolves_alias_and_params() {
        let cfg = sample();
        let r = cfg.resolve("fast", ApiFormat::Openai).unwrap();
        assert_eq!(r.upstream_model, "gpt-4o");
        assert_eq!(r.format, ApiFormat::Openai);
        let s = cfg.resolve("smart", ApiFormat::Openai).unwrap();
        assert_eq!(
            s.params.get("max_tokens").unwrap(),
            &serde_json::json!(2048)
        );
    }

    #[test]
    fn resolves_provider_model_cross_format() {
        let cfg = sample();
        let r = cfg.resolve("claude-x", ApiFormat::Openai).unwrap();
        assert_eq!(r.provider.id, "anth");
        assert_eq!(r.format, ApiFormat::Anthropic);
    }

    #[test]
    fn resolves_explicit_provider_prefix() {
        let cfg = sample();
        let r = cfg
            .resolve("oai/gpt-4o-mini", ApiFormat::Anthropic)
            .unwrap();
        assert_eq!(r.provider.id, "oai");
        assert_eq!(r.upstream_model, "gpt-4o-mini");
        assert_eq!(r.route_id, "oai:openai");
    }

    #[test]
    fn resolves_multiple_routes() {
        let cfg = sample();
        let plan = cfg.resolve_plan("multi").unwrap();
        assert_eq!(plan.routes.len(), 2);
        assert_eq!(plan.routes[0].route_id, "multi-oai");
        assert_eq!(plan.routes[0].priority, 10);
        assert_eq!(plan.routes[1].route_id, "multi-anth");
        assert_eq!(plan.routes[0].format, ApiFormat::Openai);
        assert_eq!(plan.routes[1].format, ApiFormat::Anthropic);
    }

    #[test]
    fn pricing_schedule_overrides_base() {
        let cfg = sample();
        let plan = cfg.resolve_plan("multi").unwrap();
        let pricing = plan.routes[0].pricing.as_ref().unwrap();
        let snapshot = pricing.snapshot_at(Local::now());
        assert_eq!(snapshot.input, Some(1.0));
        assert_eq!(snapshot.output, Some(2.0));
        assert_eq!(snapshot.label.as_deref(), Some("night"));
        assert_eq!(snapshot.effective_input(0.5), Some(0.6));
    }

    #[test]
    fn pricing_cross_midnight_uses_previous_weekday() {
        let schedule = PriceSchedule {
            label: None,
            days: vec!["mon".to_string()],
            start: "22:00".to_string(),
            end: "06:00".to_string(),
            input: None,
            output: None,
            cache_read: None,
            cache_write: None,
            multiplier: None,
        };
        // Monday 23:00 belongs to the Monday window.
        assert!(schedule.matches_at(Weekday::Mon, 23 * 60));
        // Tuesday 02:00 is the after-midnight half of the Monday window.
        assert!(schedule.matches_at(Weekday::Tue, 2 * 60));
        // Monday 02:00 would belong to Sunday, not Monday.
        assert!(!schedule.matches_at(Weekday::Mon, 2 * 60));
        // Tuesday 23:00 is outside the Monday window.
        assert!(!schedule.matches_at(Weekday::Tue, 23 * 60));
    }

    #[test]
    fn comparable_cost_requires_input_and_output_prices() {
        let partial = PriceSnapshot {
            currency: "USD".to_string(),
            label: None,
            input: None,
            output: Some(1.0),
            cache_read: Some(0.1),
            cache_write: None,
        };
        assert!(comparable_cost(&partial, 1000, 100, 0, 0).is_none());
        let complete = PriceSnapshot {
            input: Some(1.0),
            output: Some(2.0),
            ..partial
        };
        assert!(comparable_cost(&complete, 1000, 100, 0, 0).is_some());
    }

    #[test]
    fn implicit_model_gets_manual_fallback_route() {
        let raw = r#"
[routing]
default_strategy = "manual"
[[providers]]
id = "p"
format = "openai"
base_url = "https://example.com/v1"
models = ["implicit"]
"#;
        let cfg: Config = toml::from_str(raw).unwrap();
        cfg.validate().unwrap();
        let plan = cfg.resolve_plan("implicit").unwrap();
        assert_eq!(plan.strategy, "manual");
        assert_eq!(plan.manual_route.as_deref(), Some("p:openai"));
        assert_eq!(plan.routes.len(), 1);
    }

    #[test]
    fn route_format_override() {
        let raw = r#"
[[providers]]
id = "p"
format = "openai"
base_url = "https://example.com/v1"

[[models]]
name = "m"
[[models.routes]]
id = "chat"
provider = "p"
format = "openai"
[[models.routes]]
id = "responses"
provider = "p"
format = "responses"
[[models.routes]]
id = "anth"
provider = "p"
format = "anthropic"
"#;
        let cfg: Config = toml::from_str(raw).unwrap();
        let plan = cfg.resolve_plan("m").unwrap();
        let formats: Vec<&str> = plan.routes.iter().map(|r| r.format.as_str()).collect();
        assert_eq!(formats, vec!["openai", "responses", "anthropic"]);
    }

    #[test]
    fn manual_strategy_picks_route() {
        let raw = r#"
[[providers]]
id = "a"
format = "openai"
base_url = "https://a.example/v1"
[[providers]]
id = "b"
format = "openai"
base_url = "https://b.example/v1"

[[models]]
name = "m"
strategy = "manual"
route = "b-route"
[[models.routes]]
id = "a-route"
provider = "a"
[[models.routes]]
id = "b-route"
provider = "b"
"#;
        let cfg: Config = toml::from_str(raw).unwrap();
        let r = cfg.resolve("m", ApiFormat::Openai).unwrap();
        assert_eq!(r.route_id, "b-route");
        assert_eq!(r.provider.id, "b");
    }
}
