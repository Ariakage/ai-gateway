use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use serde::Serialize;

use crate::config::{ApiFormat, ModelPlan, Resolved};

pub struct SelectRequest<'a> {
    pub inbound: ApiFormat,
    pub override_route: Option<&'a str>,
    pub prompt_tokens: u64,
    pub max_output_tokens: u64,
    pub prefer_native: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RouteRuntime {
    pub healthy: bool,
    pub failures: u32,
    pub open_for_ms: Option<u64>,
    pub latency_ms: Option<f64>,
    pub cache_ratio: Option<f64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct Health {
    failures: u32,
    open_until: Option<Instant>,
    last_error: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    round_robin: HashMap<String, u64>,
    health: HashMap<String, Health>,
    latency: HashMap<String, f64>,
    cache: HashMap<String, f64>,
    probe_backoff: HashMap<String, Instant>,
    probe_inflight: HashMap<String, usize>,
}

/// A selected route plus an optional lease for a fastest-mode probe. The lease
/// must stay alive for the duration of the request; dropping it releases the
/// probe slot. Requests that did not reserve a slot get `None` and therefore
/// cannot release another request's probe.
#[derive(Debug)]
pub struct Selection {
    pub route: Resolved,
    pub lease: Option<ProbeLease>,
}

impl Selection {
    fn plain(route: Resolved) -> Self {
        Self { route, lease: None }
    }
}

#[derive(Debug)]
pub struct ProbeLease {
    inner: Arc<Mutex<Inner>>,
    key: String,
}

impl Drop for ProbeLease {
    fn drop(&mut self) {
        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(count) = inner.probe_inflight.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                inner.probe_inflight.remove(&self.key);
            }
        }
    }
}

/// Runtime routing state: circuit health, latency EWMA, cache-hit EWMA and the
/// round-robin cursor. This is intentionally in memory; historical metrics live
/// in SQLite.
pub struct Router {
    inner: Arc<Mutex<Inner>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    pub fn select(
        &self,
        plan: &ModelPlan,
        request: &SelectRequest<'_>,
    ) -> Result<Selection, String> {
        self.choose(plan, request, true)
    }

    /// Same selection logic without advancing the round-robin cursor.
    pub fn preview(
        &self,
        plan: &ModelPlan,
        request: &SelectRequest<'_>,
    ) -> Result<Resolved, String> {
        self.choose(plan, request, false)
            .map(|selection| selection.route)
    }

    fn choose(
        &self,
        plan: &ModelPlan,
        request: &SelectRequest<'_>,
        advance: bool,
    ) -> Result<Selection, String> {
        let routes = &plan.routes;
        if routes.is_empty() {
            return Err(format!("model '{}' has no enabled routes", plan.alias));
        }

        if let Some(forced) = request.override_route {
            return routes
                .iter()
                .find(|route| route.route_id == forced)
                .cloned()
                .map(Selection::plain)
                .ok_or_else(|| {
                    format!(
                        "route '{}' is not configured for model '{}'. routes: {}",
                        forced,
                        plan.alias,
                        route_ids(routes)
                    )
                });
        }

        if plan.strategy == "manual" {
            let route_id = plan.manual_route.as_deref().ok_or_else(|| {
                format!(
                    "model '{}' uses manual routing but no route is configured",
                    plan.alias
                )
            })?;
            return routes
                .iter()
                .find(|route| route.route_id == route_id)
                .cloned()
                .map(Selection::plain)
                .ok_or_else(|| {
                    format!(
                        "manual route '{}' not found for model '{}'",
                        route_id, plan.alias
                    )
                });
        }

        let now = Local::now();
        let mut inner = self.lock();
        let health: HashMap<String, bool> = routes
            .iter()
            .map(|route| {
                (
                    route.route_id.clone(),
                    is_healthy(inner.health.get(&runtime_key(&plan.alias, &route.route_id))),
                )
            })
            .collect();
        let cache_ratio: HashMap<String, f64> = routes
            .iter()
            .map(|route| {
                (
                    route.route_id.clone(),
                    inner
                        .cache
                        .get(&runtime_key(&plan.alias, &route.route_id))
                        .copied()
                        .unwrap_or(0.0),
                )
            })
            .collect();
        let now_instant = Instant::now();
        let probe_blocked: HashMap<String, bool> = routes
            .iter()
            .map(|route| {
                (
                    route.route_id.clone(),
                    inner
                        .probe_backoff
                        .get(&runtime_key(&plan.alias, &route.route_id))
                        .map(|until| *until > now_instant)
                        .unwrap_or(false),
                )
            })
            .collect();
        let probe_counts: HashMap<String, usize> = routes
            .iter()
            .map(|route| {
                (
                    route.route_id.clone(),
                    inner
                        .probe_inflight
                        .get(&runtime_key(&plan.alias, &route.route_id))
                        .copied()
                        .unwrap_or(0),
                )
            })
            .collect();
        let latency = |route: &Resolved| {
            // Backoff applies before sampling state: a route that had earlier
            // success samples but now returns 401/403/404 must not keep winning.
            if probe_blocked.get(&route.route_id).copied().unwrap_or(false) {
                f64::INFINITY
            } else {
                inner
                    .latency
                    .get(&runtime_key(&plan.alias, &route.route_id))
                    .copied()
                    // An unsampled route is optimistic (0 ms) so fastest can
                    // probe it, but concurrent requests avoid it while a probe
                    // is in flight.
                    .unwrap_or_else(|| {
                        if probe_counts.get(&route.route_id).copied().unwrap_or(0) > 0 {
                            f64::INFINITY
                        } else {
                            0.0
                        }
                    })
            }
        };
        // Among routes that all rank as infinite (everything is probing, or a
        // probe is backed off), spread excess requests over the least-loaded
        // routes instead of always choosing the highest-priority one. Backed
        // off routes rank last.
        let probe_rank = |route: &Resolved| -> usize {
            if probe_blocked.get(&route.route_id).copied().unwrap_or(false) {
                usize::MAX
            } else {
                probe_counts.get(&route.route_id).copied().unwrap_or(0)
            }
        };

        if plan.strategy == "round_robin" {
            let mut pool: Vec<&Resolved> = routes
                .iter()
                .filter(|route| health.get(&route.route_id).copied().unwrap_or(true))
                .collect();
            if pool.is_empty() {
                pool = routes.iter().collect();
            }
            pool.sort_by(|a, b| a.route_id.cmp(&b.route_id));
            let key = format!(
                "{}|{}",
                plan.alias,
                pool.iter()
                    .map(|route| route.route_id.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let total: u64 = pool.iter().map(|route| route.weight.max(1) as u64).sum();
            let slot = if total == 0 {
                0
            } else {
                let cursor = inner.round_robin.entry(key).or_insert(0);
                let slot = *cursor % total;
                if advance {
                    *cursor = cursor.wrapping_add(1);
                }
                slot
            };
            let mut accumulated = 0u64;
            for route in &pool {
                accumulated += route.weight.max(1) as u64;
                if slot < accumulated {
                    return Ok(Selection::plain((*route).clone()));
                }
            }
            return Ok(Selection::plain((*pool[0]).clone()));
        }

        let mut candidates: Vec<&Resolved> = routes
            .iter()
            .filter(|route| health.get(&route.route_id).copied().unwrap_or(true))
            .collect();
        if candidates.is_empty() {
            candidates = routes.iter().collect();
        }

        if plan.strategy == "auto" && request.prefer_native {
            let native: Vec<&Resolved> = candidates
                .iter()
                .copied()
                .filter(|route| route.format == request.inbound)
                .collect();
            if !native.is_empty() {
                candidates = native;
            }
        }

        let cost = |route: &Resolved| {
            route_cost(
                route,
                now,
                cache_ratio.get(&route.route_id).copied().unwrap_or(0.0),
                request.prompt_tokens,
                request.max_output_tokens,
            )
        };

        // Costs in different currencies are not comparable without explicit
        // exchange rates. Detect that before any cost-based ordering.
        let mut currencies: Vec<String> = candidates
            .iter()
            .filter(|route| cost(route).is_some())
            .filter_map(|route| {
                route
                    .pricing
                    .as_ref()
                    .map(|pricing| pricing.snapshot_at(now).currency)
            })
            .collect();
        currencies.sort();
        currencies.dedup();
        let mixed_currencies = currencies.len() > 1;
        if plan.strategy == "cheapest" && mixed_currencies {
            return Err(format!(
                "model '{}' has comparable routes priced in multiple currencies ({}); \
                 cheapest requires a single currency or an explicit conversion policy",
                plan.alias,
                currencies.join(", ")
            ));
        }

        let cost_cmp = |left: &Resolved, right: &Resolved| {
            cost(left)
                .unwrap_or(f64::INFINITY)
                .partial_cmp(&cost(right).unwrap_or(f64::INFINITY))
                .unwrap_or(std::cmp::Ordering::Equal)
        };
        candidates.sort_by(|left, right| {
            match plan.strategy.as_str() {
                "cheapest" => {
                    cost_cmp(left, right).then_with(|| left.priority.cmp(&right.priority))
                }
                "fastest" => latency(left)
                    .partial_cmp(&latency(right))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| probe_rank(left).cmp(&probe_rank(right)))
                    .then_with(|| left.priority.cmp(&right.priority)),
                "priority" => left.priority.cmp(&right.priority).then_with(|| {
                    if mixed_currencies {
                        std::cmp::Ordering::Equal
                    } else {
                        cost_cmp(left, right)
                    }
                }),
                _ => {
                    // auto: healthy routes are already filtered. With mixed
                    // currencies fall back to explicit priority; otherwise
                    // prefer the cheapest effective price.
                    if mixed_currencies {
                        left.priority.cmp(&right.priority)
                    } else {
                        cost_cmp(left, right).then_with(|| left.priority.cmp(&right.priority))
                    }
                }
            }
        });

        let chosen = (*candidates[0]).clone();
        let mut lease = None;
        if advance && plan.strategy == "fastest" {
            let key = runtime_key(&plan.alias, &chosen.route_id);
            let blocked = probe_blocked
                .get(&chosen.route_id)
                .copied()
                .unwrap_or(false);
            if !blocked && !inner.latency.contains_key(&key) {
                // Each request that uses an unsampled route holds one slot
                // until it finishes; only the lease owner releases it.
                *inner.probe_inflight.entry(key.clone()).or_insert(0) += 1;
                lease = Some(ProbeLease {
                    inner: Arc::clone(&self.inner),
                    key,
                });
            }
        }
        Ok(Selection {
            route: chosen,
            lease,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        model: &str,
        route_id: &str,
        status: u16,
        duration_ms: i64,
        ttft_ms: Option<i64>,
        prompt_tokens: u64,
        cached_tokens: u64,
        cooldown_secs: u64,
    ) {
        let key = runtime_key(model, route_id);
        let route_failed = status >= 500 || status == 429 || status == 408;
        let mut inner = self.lock();
        // 499 is synthesized by the gateway when the client disconnects; it
        // says nothing about the upstream route.
        if (400..500).contains(&status) && status != 408 && status != 429 && status != 499 {
            inner.probe_backoff.insert(
                key.clone(),
                Instant::now() + Duration::from_secs(cooldown_secs.clamp(5, 300)),
            );
            return;
        }
        if route_failed {
            let health = inner.health.entry(key.clone()).or_default();
            health.failures += 1;
            health.last_error = Some(format!("HTTP {status}"));
            if health.failures >= 3 {
                health.open_until =
                    Some(Instant::now() + Duration::from_secs(cooldown_secs.max(1)));
            }
            return;
        }
        if status >= 400 {
            return;
        }

        inner.probe_backoff.remove(&key);
        let health = inner.health.entry(key.clone()).or_default();
        health.failures = 0;
        health.open_until = None;
        health.last_error = None;

        let sample = ttft_ms.filter(|value| *value > 0).unwrap_or(duration_ms) as f64;
        if sample > 0.0 {
            let ewma = inner.latency.entry(key.clone()).or_insert(sample);
            *ewma = *ewma * 0.75 + sample * 0.25;
        }
        if prompt_tokens > 0 {
            let ratio = cached_tokens.min(prompt_tokens) as f64 / prompt_tokens as f64;
            let ewma = inner.cache.entry(key.clone()).or_insert(ratio);
            *ewma = *ewma * 0.75 + ratio * 0.25;
        }
    }

    pub fn runtime(&self, model: &str, route_id: &str) -> RouteRuntime {
        let key = runtime_key(model, route_id);
        let inner = self.lock();
        let now = Instant::now();
        let health = inner.health.get(&key);
        let open_for = health
            .and_then(|h| h.open_until)
            .filter(|until| *until > now)
            .map(|until| until.duration_since(now).as_millis() as u64);
        RouteRuntime {
            healthy: open_for.is_none(),
            failures: health.map(|h| h.failures).unwrap_or(0),
            open_for_ms: open_for,
            latency_ms: inner.latency.get(&key).copied(),
            cache_ratio: inner.cache.get(&key).copied(),
            last_error: health.and_then(|h| h.last_error.clone()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn runtime_key(model: &str, route_id: &str) -> String {
    format!("{model}\u{1f}{route_id}")
}

fn is_healthy(health: Option<&Health>) -> bool {
    health
        .and_then(|h| h.open_until)
        .map(|until| until <= Instant::now())
        .unwrap_or(true)
}

fn route_ids(routes: &[Resolved]) -> String {
    routes
        .iter()
        .map(|route| route.route_id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn route_cost(
    route: &Resolved,
    now: DateTime<Local>,
    cache_ratio: f64,
    prompt_tokens: u64,
    max_output_tokens: u64,
) -> Option<f64> {
    let pricing = route.pricing.as_ref()?;
    let price = pricing.snapshot_at(now);
    if !price.has_any() {
        return None;
    }
    // A missing input or output price makes the route incomparable: treating
    // the absent side as free would make a partial price card look cheapest.
    // A cache-only card is also rejected because the uncached input price is
    // required for a meaningful comparison.
    let base_input = price.input?;
    let input = match price.cache_read {
        Some(cached) => {
            let ratio = cache_ratio.clamp(0.0, 1.0);
            base_input * (1.0 - ratio) + cached * ratio
        }
        None => base_input,
    };
    let output_price = price.output?;
    let prompt = if prompt_tokens == 0 {
        1_000.0
    } else {
        prompt_tokens as f64
    };
    let output = if max_output_tokens == 0 {
        512.0
    } else {
        max_output_tokens as f64
    };
    let cost = prompt / 1_000_000.0 * input + output / 1_000_000.0 * output_price;
    Some(cost)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Provider;
    use std::collections::HashMap;

    fn plan(name: &str, route_id: &str) -> ModelPlan {
        let provider = Provider {
            id: "p".to_string(),
            format: ApiFormat::Openai,
            base_url: "http://127.0.0.1:1/v1".to_string(),
            api_key: None,
            headers: HashMap::new(),
            models: vec![name.to_string()],
            models_url: None,
            timeout_secs: 60,
            stream_usage: true,
            pricing: None,
        };
        ModelPlan {
            alias: name.to_string(),
            requested: name.to_string(),
            strategy: "auto".to_string(),
            manual_route: None,
            routes: vec![Resolved {
                alias: name.to_string(),
                upstream_model: name.to_string(),
                provider,
                format: ApiFormat::Openai,
                route_id: route_id.to_string(),
                priority: 1,
                weight: 1,
                params: HashMap::new(),
                pricing: None,
                label: None,
            }],
        }
    }

    #[test]
    fn partial_price_cards_do_not_win_cheapest() {
        let mut plan = plan("m", "partial");
        plan.strategy = "cheapest".to_string();
        let mut partial = plan.routes[0].clone();
        partial.route_id = "partial".to_string();
        partial.pricing = Some(crate::config::Pricing {
            currency: "USD".to_string(),
            input: Some(0.01),
            output: None,
            cache_read: None,
            cache_write: None,
            schedules: Vec::new(),
        });
        let mut complete = partial.clone();
        complete.route_id = "complete".to_string();
        complete.pricing = Some(crate::config::Pricing {
            input: Some(1.0),
            output: Some(1.0),
            ..partial.pricing.clone().unwrap()
        });
        plan.routes = vec![partial, complete];
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1000,
            max_output_tokens: 1000,
            prefer_native: true,
        };
        let selected = Router::new().select(&plan, &request).unwrap();
        assert_eq!(selected.route.route_id, "complete");
    }

    #[test]
    fn cache_only_price_card_is_not_comparable() {
        let mut plan = plan("m", "cache-only");
        plan.strategy = "cheapest".to_string();
        let mut cache_only = plan.routes[0].clone();
        cache_only.route_id = "cache-only".to_string();
        cache_only.pricing = Some(crate::config::Pricing {
            currency: "USD".to_string(),
            input: None,
            output: Some(0.0),
            cache_read: Some(0.0),
            cache_write: None,
            schedules: Vec::new(),
        });
        let mut complete = cache_only.clone();
        complete.route_id = "complete".to_string();
        complete.pricing = Some(crate::config::Pricing {
            input: Some(1.0),
            output: Some(1.0),
            cache_read: Some(0.1),
            ..cache_only.pricing.clone().unwrap()
        });
        plan.routes = vec![cache_only, complete];
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1000,
            max_output_tokens: 1000,
            prefer_native: true,
        };
        let selected = Router::new().select(&plan, &request).unwrap();
        assert_eq!(selected.route.route_id, "complete");
    }

    #[test]
    fn fastest_probes_unsampled_routes() {
        let mut plan = plan("m", "a");
        plan.strategy = "fastest".to_string();
        let mut b = plan.routes[0].clone();
        b.route_id = "b".to_string();
        plan.routes.push(b);
        let router = Router::new();
        router.record("m", "a", 200, 500, Some(500), 1, 0, 30);
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1,
            max_output_tokens: 1,
            prefer_native: true,
        };
        assert_eq!(router.select(&plan, &request).unwrap().route.route_id, "b");
        router.record("m", "b", 200, 50, Some(50), 1, 0, 30);
        assert_eq!(router.select(&plan, &request).unwrap().route.route_id, "b");
    }

    #[test]
    fn fastest_backs_off_failed_probes() {
        let mut plan = plan("m", "a");
        plan.strategy = "fastest".to_string();
        let mut b = plan.routes[0].clone();
        b.route_id = "b".to_string();
        plan.routes.push(b);
        let router = Router::new();
        router.record("m", "a", 200, 100, Some(100), 1, 0, 30);
        router.record("m", "b", 401, 10, None, 0, 0, 30);
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1,
            max_output_tokens: 1,
            prefer_native: true,
        };
        assert_eq!(router.select(&plan, &request).unwrap().route.route_id, "a");
    }

    #[test]
    fn fastest_backs_off_sampled_route_on_4xx() {
        let mut plan = plan("m", "a");
        plan.strategy = "fastest".to_string();
        let mut b = plan.routes[0].clone();
        b.route_id = "b".to_string();
        plan.routes.push(b);
        let router = Router::new();
        router.record("m", "a", 200, 50, Some(50), 1, 0, 30);
        router.record("m", "b", 200, 200, Some(200), 1, 0, 30);
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1,
            max_output_tokens: 1,
            prefer_native: true,
        };
        assert_eq!(router.select(&plan, &request).unwrap().route.route_id, "a");
        router.record("m", "a", 401, 5, None, 0, 0, 30);
        assert_eq!(router.select(&plan, &request).unwrap().route.route_id, "b");
    }

    #[test]
    fn fastest_reserves_unsampled_routes_for_one_probe() {
        let mut plan = plan("m", "a");
        plan.strategy = "fastest".to_string();
        let mut b = plan.routes[0].clone();
        b.route_id = "b".to_string();
        plan.routes.push(b);
        let router = Router::new();
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1,
            max_output_tokens: 1,
            prefer_native: true,
        };
        // Preview does not reserve the route.
        assert_eq!(router.preview(&plan, &request).unwrap().route_id, "a");
        assert_eq!(router.preview(&plan, &request).unwrap().route_id, "a");

        // The first request reserves a; the next goes to b.
        let first = router.select(&plan, &request).unwrap();
        assert_eq!(first.route.route_id, "a");
        assert!(first.lease.is_some());
        let second = router.select(&plan, &request).unwrap();
        assert_eq!(second.route.route_id, "b");
        assert!(second.lease.is_some());

        // When every route is already probing, excess requests are spread to
        // the least-loaded route rather than piling onto the highest priority.
        let third = router.select(&plan, &request).unwrap();
        assert_eq!(third.route.route_id, "a");
        let fourth = router.select(&plan, &request).unwrap();
        assert_eq!(fourth.route.route_id, "b");

        drop(first);
        drop(second);
        drop(third);
        drop(fourth);
        let after = router.select(&plan, &request).unwrap();
        assert_eq!(after.route.route_id, "a");
    }

    #[test]
    fn forced_route_does_not_release_another_probe() {
        let mut plan = plan("m", "a");
        plan.strategy = "fastest".to_string();
        let mut b = plan.routes[0].clone();
        b.route_id = "b".to_string();
        plan.routes.push(b);
        let router = Router::new();
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1,
            max_output_tokens: 1,
            prefer_native: true,
        };
        let probe = router.select(&plan, &request).unwrap();
        assert_eq!(probe.route.route_id, "a");
        assert!(probe.lease.is_some());

        // A forced request for the same route owns no lease.
        let forced = router
            .select(
                &plan,
                &SelectRequest {
                    inbound: ApiFormat::Openai,
                    override_route: Some("a"),
                    prompt_tokens: 1,
                    max_output_tokens: 1,
                    prefer_native: true,
                },
            )
            .unwrap();
        assert_eq!(forced.route.route_id, "a");
        assert!(forced.lease.is_none());
        drop(forced);

        // The probe must still be reserved.
        let next = router.select(&plan, &request).unwrap();
        assert_eq!(next.route.route_id, "b");
        drop(probe);
        let freed = router.select(&plan, &request).unwrap();
        assert_eq!(freed.route.route_id, "a");
    }

    #[test]
    fn client_cancellation_does_not_back_off_probe() {
        let mut plan = plan("m", "a");
        plan.strategy = "fastest".to_string();
        let mut b = plan.routes[0].clone();
        b.route_id = "b".to_string();
        plan.routes.push(b);
        let router = Router::new();
        router.record("m", "a", 200, 50, Some(50), 1, 0, 30);
        router.record("m", "b", 200, 200, Some(200), 1, 0, 30);
        router.record("m", "a", 499, 10, None, 1, 0, 30);
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1,
            max_output_tokens: 1,
            prefer_native: true,
        };
        assert_eq!(router.select(&plan, &request).unwrap().route.route_id, "a");
    }

    #[test]
    fn cheapest_rejects_mixed_currencies() {
        fn price(currency: &str, input: f64) -> crate::config::Pricing {
            crate::config::Pricing {
                currency: currency.to_string(),
                input: Some(input),
                output: Some(input),
                cache_read: None,
                cache_write: None,
                schedules: Vec::new(),
            }
        }

        let mut plan = plan("m", "usd");
        plan.strategy = "cheapest".to_string();
        plan.routes[0].pricing = Some(price("USD", 1.0));
        let mut cny = plan.routes[0].clone();
        cny.route_id = "cny".to_string();
        cny.pricing = Some(price("CNY", 1.0));
        plan.routes.push(cny);
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1000,
            max_output_tokens: 1000,
            prefer_native: true,
        };
        let error = Router::new().select(&plan, &request).unwrap_err();
        assert!(error.contains("multiple currencies"), "{error}");
    }

    #[test]
    fn auto_mixed_currencies_fall_back_to_priority() {
        fn price(currency: &str, input: f64) -> crate::config::Pricing {
            crate::config::Pricing {
                currency: currency.to_string(),
                input: Some(input),
                output: Some(input),
                cache_read: None,
                cache_write: None,
                schedules: Vec::new(),
            }
        }

        let mut plan = plan("m", "a");
        plan.strategy = "auto".to_string();
        plan.routes[0].priority = 5;
        plan.routes[0].pricing = Some(price("USD", 100.0));
        let mut b = plan.routes[0].clone();
        b.route_id = "b".to_string();
        b.priority = 1;
        b.pricing = Some(price("CNY", 0.01));
        plan.routes.push(b);
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1000,
            max_output_tokens: 1000,
            prefer_native: true,
        };
        let selected = Router::new().select(&plan, &request).unwrap();
        assert_eq!(selected.route.route_id, "b");
    }

    #[test]
    fn cheapest_same_currency_still_orders_by_cost() {
        fn price(currency: &str, input: f64) -> crate::config::Pricing {
            crate::config::Pricing {
                currency: currency.to_string(),
                input: Some(input),
                output: Some(input),
                cache_read: None,
                cache_write: None,
                schedules: Vec::new(),
            }
        }

        let mut plan = plan("m", "expensive");
        plan.strategy = "cheapest".to_string();
        plan.routes[0].pricing = Some(price("USD", 10.0));
        let mut cheap = plan.routes[0].clone();
        cheap.route_id = "cheap".to_string();
        cheap.pricing = Some(price("USD", 1.0));
        plan.routes.push(cheap);
        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1000,
            max_output_tokens: 1000,
            prefer_native: true,
        };
        let selected = Router::new().select(&plan, &request).unwrap();
        assert_eq!(selected.route.route_id, "cheap");
    }

    #[test]
    fn runtime_health_is_scoped_by_model() {
        let router = Router::new();
        for _ in 0..3 {
            router.record("model-a", "shared-route", 500, 10, None, 0, 0, 30);
        }
        assert!(!router.runtime("model-a", "shared-route").healthy);
        assert!(router.runtime("model-b", "shared-route").healthy);

        let request = SelectRequest {
            inbound: ApiFormat::Openai,
            override_route: None,
            prompt_tokens: 1,
            max_output_tokens: 1,
            prefer_native: true,
        };
        assert!(router
            .select(&plan("model-b", "shared-route"), &request)
            .is_ok());
    }
}
