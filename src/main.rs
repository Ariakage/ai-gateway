mod admin;
mod config;
mod proxy;
mod router;
mod sanitize;
mod stats;
mod store;
mod translate;
mod util;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<config::Config>>,
    pub config_path: Arc<PathBuf>,
    pub stats: Arc<stats::Stats>,
    pub responses: Arc<store::ResponseStore>,
    pub router: Arc<router::Router>,
    pub client: reqwest::Client,
    pub started: std::time::Instant,
    /// Serializes read-modify-write updates to the config file.
    pub config_lock: Arc<tokio::sync::Mutex<()>>,
    /// Address the process actually listens on (after --host overrides).
    pub bound_host: String,
}

#[derive(Parser, Debug)]
#[command(
    name = "ai-gateway",
    version,
    about = "Local AI API gateway: OpenAI + Anthropic compatible, multi-provider, usage accounting"
)]
struct Cli {
    /// Path to the TOML config file.
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    /// Override the listen host.
    #[arg(long)]
    host: Option<String>,
    /// Override the listen port.
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("ai_gateway=info,info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let config_path = cli.config.clone();
    let mut cfg = config::load(&config_path)?;
    if let Some(host) = cli.host {
        cfg.server.host = host;
    }
    if let Some(port) = cli.port {
        cfg.server.port = port;
    }

    if let Err(error) = config::check_admin_exposure(&cfg.server, &cfg.server.host) {
        anyhow::bail!("{error}; set server.admin_token or bind 127.0.0.1");
    }
    if !config::is_loopback_host(&cfg.server.host) && cfg.server.api_keys.is_empty() {
        tracing::warn!(
            "inference auth is disabled while listening on {}; set server.api_keys",
            cfg.server.host
        );
    }

    let response_state_limits = (
        cfg.server.response_state_max_entries.max(1),
        cfg.server.response_state_max_bytes.max(1),
        cfg.server.response_state_max_entry_bytes.max(1),
    );

    let db_path = config::resolve_data_path(&config_path, &cfg.server.database);
    let stats = Arc::new(
        stats::Stats::open(&db_path)
            .with_context(|| format!("cannot open usage database {}", db_path.display()))?,
    );

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(20))
        .pool_idle_timeout(Duration::from_secs(90))
        .user_agent("ai-gateway/0.1")
        .build()?;

    let host = cfg.server.host.clone();
    let port = cfg.server.port;
    let model_names = cfg.model_names();
    let provider_count = cfg.providers.len();

    let bound_host = cfg.server.host.clone();
    let state = AppState {
        config: Arc::new(RwLock::new(cfg)),
        config_path: Arc::new(config_path.clone()),
        stats,
        responses: Arc::new(store::ResponseStore::new(
            response_state_limits.0,
            response_state_limits.1,
            response_state_limits.2,
        )),
        router: Arc::new(router::Router::new()),
        client,
        started: std::time::Instant::now(),
        config_lock: Arc::new(tokio::sync::Mutex::new(())),
        bound_host,
    };

    let app = build_router(state);
    let base = format!("http://{}:{port}", display_host(&host));
    let listener = TcpListener::bind((bind_host(&host), port))
        .await
        .with_context(|| format!("cannot bind {}:{port}", display_host(&host)))?;

    println!();
    println!(
        "  AI Gateway  ·  {} provider(s)  ·  {} model(s)",
        provider_count,
        model_names.len()
    );
    println!("  ─────────────────────────────────────────────────────────");
    println!("  OpenAI      POST {base}/v1/chat/completions");
    println!("  Anthropic   POST {base}/v1/messages");
    println!("  Responses   POST {base}/v1/responses");
    println!("  Embeddings  POST {base}/v1/embeddings");
    println!("  Models      GET  {base}/v1/models");
    println!("  Dashboard   {base}/");
    println!("  Config      {}", config_path.display());
    println!("  Usage DB    {}", db_path.display());
    if !model_names.is_empty() {
        println!("  Models      {}", model_names.join(", "));
    }
    println!();

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn build_router(state: AppState) -> Router {
    let inference = Router::new()
        .route("/v1/chat/completions", post(proxy::openai_chat))
        .route("/v1/messages", post(proxy::anthropic_messages))
        .route("/v1/messages/count_tokens", post(proxy::count_tokens))
        .route("/v1/embeddings", post(proxy::openai_embeddings))
        .route("/v1/responses", post(proxy::openai_responses))
        .route(
            "/v1/responses/input_tokens",
            post(proxy::responses_input_tokens),
        )
        .route(
            "/v1/responses/{response_id}",
            get(proxy::responses_retrieve_unsupported),
        )
        .route("/v1/models", get(admin::public_models))
        .route("/v1/models/{model_id}", get(admin::public_model))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            inference_auth,
        ));

    let admin = Router::new()
        .route("/admin/api/overview", get(admin::overview))
        .route("/admin/api/timeseries", get(admin::timeseries))
        .route("/admin/api/models", get(admin::model_stats))
        .route("/admin/api/providers", get(admin::provider_stats))
        .route("/admin/api/routing", get(admin::routing))
        .route("/admin/api/requests", get(admin::requests))
        .route(
            "/admin/api/config",
            get(admin::config_view).put(admin::save_config),
        )
        .route("/admin/api/config/raw", get(admin::config_raw))
        .route("/admin/api/config/validate", post(admin::validate_config))
        .route("/admin/api/reload", post(admin::reload))
        .route("/admin/api/health", get(admin::health))
        .route("/admin/api/upstream-models", get(admin::upstream_models))
        .route(
            "/admin/api/upstream-models/sync",
            post(admin::sync_upstream_models),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), admin_auth));

    Router::new()
        .route("/", get(dashboard))
        .route("/healthz", get(healthz))
        .merge(inference)
        .merge(admin)
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(state.clone(), origin_guard))
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}

async fn security_headers(req: Request, next: Next) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static(
            "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'",
        ),
    );
    response
}

async fn origin_guard(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let (allowed_origins, allowed_hosts) = {
        let cfg = state.config.read().await;
        (
            cfg.server.allowed_origins.clone(),
            cfg.server.allowed_hosts.clone(),
        )
    };
    let headers = req.headers();
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .map(str::to_string);
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let requested_headers = headers
        .get("access-control-request-headers")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    if !host_allowed(&host, &state.bound_host, &allowed_hosts, &allowed_origins) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "request Host is not allowed; add it to server.allowed_hosts"
            })),
        )
            .into_response();
    }

    let Some(origin) = origin else {
        return next.run(req).await;
    };
    if !origin_allowed(&origin, &host, &allowed_origins) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "cross-origin request rejected; add the origin to server.allowed_origins"
            })),
        )
            .into_response();
    }

    if req.method() == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        add_cors_headers(&mut response, &origin, requested_headers.as_deref(), true);
        return response;
    }

    let mut response = next.run(req).await;
    add_cors_headers(&mut response, &origin, None, false);
    response
}

/// Strip IPv6 brackets before passing a literal host to the resolver.
fn bind_host(host: &str) -> &str {
    let host = host.trim();
    host.strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host)
}

/// IPv6 literals need brackets when embedded in a URL or address string.
fn display_host(host: &str) -> String {
    let host = host.trim();
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn normalize_host(value: &str) -> Option<String> {
    let value = value.trim().trim_end_matches('.').to_ascii_lowercase();
    if value.is_empty() {
        return None;
    }
    if value.starts_with('[') {
        let end = value.find(']')?;
        return Some(value[..=end].to_string());
    }
    if value.matches(':').count() == 1 {
        return Some(value.split_once(':')?.0.to_string());
    }
    Some(value)
}

fn host_allowed(
    host: &str,
    bound_host: &str,
    allowed_hosts: &[String],
    allowed_origins: &[String],
) -> bool {
    let Some(host) = normalize_host(host) else {
        return false;
    };
    let mut allowed = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "[::1]".to_string(),
        "::1".to_string(),
    ];
    if let Some(bound) = normalize_host(bound_host) {
        allowed.push(bound);
    }
    for value in allowed_hosts {
        if let Some(value) = normalize_host(value) {
            allowed.push(value);
        }
    }
    for origin in allowed_origins {
        if let Ok(url) = reqwest::Url::parse(origin.trim()) {
            if let Some(value) = url.host_str().and_then(normalize_host) {
                allowed.push(value);
            }
        }
    }
    allowed.iter().any(|item| item == &host)
}

fn add_cors_headers(
    response: &mut Response,
    origin: &str,
    requested_headers: Option<&str>,
    preflight: bool,
) {
    let Ok(origin) = HeaderValue::from_str(origin) else {
        return;
    };
    let headers = response.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static(
            "x-gateway-route,x-gateway-provider,x-gateway-format,x-gateway-upstream-model",
        ),
    );
    if preflight {
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
        );
        let allowed = requested_headers.unwrap_or(
            "authorization,content-type,accept,x-api-key,x-admin-token,x-gateway-route,anthropic-version,anthropic-beta",
        );
        if let Ok(value) = HeaderValue::from_str(allowed) {
            headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, value);
        }
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from_static("600"),
        );
    }
}

fn origin_allowed(origin: &str, host: &str, allowed: &[String]) -> bool {
    let origin = origin.trim_end_matches('/');
    if allowed.iter().any(|item| {
        item.trim()
            .trim_end_matches('/')
            .eq_ignore_ascii_case(origin)
    }) {
        return true;
    }
    let Ok(url) = reqwest::Url::parse(origin) else {
        return false;
    };
    let Some(host) = host
        .split(',')
        .next()
        .map(str::trim)
        .filter(|h| !h.is_empty())
    else {
        return false;
    };
    format!("{}://{}", url.scheme(), host) == origin
}

async fn inference_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let keys = state.config.read().await.server.api_keys.clone();
    if keys.is_empty() {
        return next.run(req).await;
    }
    let provided = client_key(req.headers());
    if provided
        .as_deref()
        .map(|k| keys.iter().any(|x| x == k))
        .unwrap_or(false)
    {
        return next.run(req).await;
    }
    let is_anthropic = req.uri().path().starts_with("/v1/messages");
    if is_anthropic {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "type": "error",
                "error": {"type": "authentication_error", "message": "invalid or missing api key"}
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": {
                    "message": "invalid or missing api key",
                    "type": "authentication_error",
                    "code": 401
                }
            })),
        )
            .into_response()
    }
}

async fn admin_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let token = state
        .config
        .read()
        .await
        .server
        .admin_token
        .clone()
        .filter(|t| !t.is_empty());
    let provided = req
        .headers()
        .get("x-admin-token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| {
            req.headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.strip_prefix("Bearer "))
                .map(|s| s.to_string())
        });
    let authorized = token
        .as_deref()
        .map(|expected| provided.as_deref() == Some(expected))
        .unwrap_or(true);
    let mut response = if authorized {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid or missing admin token"})),
        )
            .into_response()
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn client_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.trim().to_string())
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim().to_string())
        })
}

async fn dashboard() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn healthz(State(state): State<AppState>) -> Response {
    Json(json!({
        "ok": true,
        "uptime_secs": state.started.elapsed().as_secs(),
        "version": env!("CARGO_PKG_VERSION"),
    }))
    .into_response()
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::{bind_host, display_host};

    #[test]
    fn ipv6_hosts_are_formatted_for_binding_and_display() {
        assert_eq!(bind_host("::1"), "::1");
        assert_eq!(bind_host("[::1]"), "::1");
        assert_eq!(display_host("::1"), "[::1]");
        assert_eq!(display_host("[::1]"), "[::1]");
        assert_eq!(display_host("127.0.0.1"), "127.0.0.1");
        assert_eq!(display_host("localhost"), "localhost");
    }
}
