use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct RequestRecord {
    pub ts: i64,
    pub endpoint: String,
    pub inbound: String,
    pub provider: String,
    pub model: String,
    pub upstream_model: String,
    pub route_id: String,
    pub status: u16,
    pub stream: bool,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub cache_write_tokens: u64,
    pub estimated: bool,
    pub ttft_ms: Option<i64>,
    pub duration_ms: i64,
    pub tokens_per_sec: Option<f64>,
    pub cost: Option<f64>,
    pub currency: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Overview {
    pub requests: u64,
    pub success: u64,
    pub errors: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub total_tokens: u64,
    pub estimated_requests: u64,
    pub avg_tps: f64,
    pub avg_ttft_ms: f64,
    pub avg_duration_ms: f64,
    pub cost: f64,
    pub cost_currency: Option<String>,
    /// Per-currency totals. `cost`/`cost_currency` are only populated when
    /// every costed request in the range shares one currency.
    pub costs: Vec<CostTotal>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CostTotal {
    pub currency: String,
    pub cost: f64,
}

#[derive(Debug, Serialize)]
pub struct Bucket {
    pub ts: i64,
    pub requests: u64,
    pub errors: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

#[derive(Debug, Serialize)]
pub struct GroupStat {
    pub name: String,
    pub requests: u64,
    pub errors: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub avg_tps: f64,
    pub avg_duration_ms: f64,
    pub cost: f64,
    pub currency: Option<String>,
    pub costs: Vec<CostTotal>,
}

#[derive(Debug, Serialize)]
pub struct RouteStat {
    pub route_id: String,
    pub provider: String,
    pub model: String,
    pub requests: u64,
    pub errors: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub cache_write_tokens: u64,
    pub avg_duration_ms: f64,
    pub avg_ttft_ms: f64,
    pub cost: f64,
    pub currency: Option<String>,
    pub costs: Vec<CostTotal>,
    pub last_ts: i64,
}

#[derive(Debug, Serialize)]
pub struct RecordView {
    pub id: i64,
    pub ts: i64,
    pub endpoint: String,
    pub inbound: String,
    pub provider: String,
    pub model: String,
    pub upstream_model: String,
    pub route_id: String,
    pub status: u16,
    pub stream: bool,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub cache_write_tokens: u64,
    pub total_tokens: u64,
    pub estimated: bool,
    pub ttft_ms: Option<i64>,
    pub duration_ms: i64,
    pub tokens_per_sec: Option<f64>,
    pub cost: Option<f64>,
    pub currency: Option<String>,
    pub error: Option<String>,
}

pub struct Stats {
    conn: Mutex<Connection>,
}

fn single_cost(costs: &[CostTotal]) -> (f64, Option<String>) {
    if costs.len() == 1 {
        (costs[0].cost, Some(costs[0].currency.clone()))
    } else {
        (0.0, None)
    }
}

fn cost_totals(conn: &Connection, from_ms: i64) -> Result<Vec<CostTotal>> {
    let mut stmt = conn.prepare(
        r#"SELECT currency, SUM(cost)
           FROM requests
           WHERE ts >= ?1 AND cost IS NOT NULL AND currency IS NOT NULL AND currency <> ''
           GROUP BY currency
           ORDER BY SUM(cost) DESC"#,
    )?;
    let rows = stmt.query_map([from_ms], |row| {
        Ok(CostTotal {
            currency: row.get(0)?,
            cost: row.get(1)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn grouped_costs(
    conn: &Connection,
    column: &str,
    from_ms: i64,
) -> Result<HashMap<String, Vec<CostTotal>>> {
    let sql = format!(
        r#"SELECT {column}, currency, SUM(cost)
           FROM requests
           WHERE ts >= ?1 AND cost IS NOT NULL AND currency IS NOT NULL AND currency <> ''
           GROUP BY {column}, currency
           ORDER BY SUM(cost) DESC"#
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([from_ms], |row| {
        Ok((
            row.get::<_, String>(0)?,
            CostTotal {
                currency: row.get(1)?,
                cost: row.get(2)?,
            },
        ))
    })?;
    let mut out: HashMap<String, Vec<CostTotal>> = HashMap::new();
    for row in rows {
        let (name, total) = row?;
        out.entry(name).or_default().push(total);
    }
    Ok(out)
}

fn route_costs(
    conn: &Connection,
    from_ms: i64,
    model: Option<&str>,
) -> Result<HashMap<(String, String, String), Vec<CostTotal>>> {
    let mut stmt = conn.prepare(
        r#"SELECT route_id, provider, model, currency, SUM(cost)
           FROM requests
           WHERE ts >= ?1 AND (?2 IS NULL OR model = ?2)
             AND cost IS NOT NULL AND currency IS NOT NULL AND currency <> ''
           GROUP BY route_id, provider, model, currency
           ORDER BY SUM(cost) DESC"#,
    )?;
    let rows = stmt.query_map(params![from_ms, model], |row| {
        Ok((
            (
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ),
            CostTotal {
                currency: row.get(3)?,
                cost: row.get(4)?,
            },
        ))
    })?;
    let mut out: HashMap<(String, String, String), Vec<CostTotal>> = HashMap::new();
    for row in rows {
        let (key, total) = row?;
        out.entry(key).or_default().push(total);
    }
    Ok(out)
}

impl Stats {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).ok();
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("cannot open sqlite database {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL").ok();
        conn.pragma_update(None, "synchronous", "NORMAL").ok();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                endpoint TEXT NOT NULL,
                inbound TEXT NOT NULL,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                upstream_model TEXT NOT NULL,
                route_id TEXT NOT NULL DEFAULT '',
                status INTEGER NOT NULL,
                stream INTEGER NOT NULL,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                cached_tokens INTEGER NOT NULL DEFAULT 0,
                cache_write_tokens INTEGER NOT NULL DEFAULT 0,
                estimated INTEGER NOT NULL DEFAULT 0,
                ttft_ms INTEGER,
                duration_ms INTEGER NOT NULL,
                tokens_per_sec REAL,
                cost REAL,
                currency TEXT,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_requests_ts ON requests(ts);
            CREATE INDEX IF NOT EXISTS idx_requests_model ON requests(model);
            CREATE INDEX IF NOT EXISTS idx_requests_provider ON requests(provider);
            "#,
        )?;
        migrate(&conn)?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_requests_route ON requests(route_id)",
            [],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn insert(&self, r: &RequestRecord) {
        let conn = self.lock();
        let result = conn.execute(
            r#"INSERT INTO requests
               (ts, endpoint, inbound, provider, model, upstream_model, route_id, status, stream,
                prompt_tokens, completion_tokens, cached_tokens, cache_write_tokens, estimated,
                ttft_ms, duration_ms, tokens_per_sec, cost, currency, error)
               VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)"#,
            params![
                r.ts,
                r.endpoint,
                r.inbound,
                r.provider,
                r.model,
                r.upstream_model,
                r.route_id,
                r.status as i64,
                r.stream as i64,
                r.prompt_tokens as i64,
                r.completion_tokens as i64,
                r.cached_tokens as i64,
                r.cache_write_tokens as i64,
                r.estimated as i64,
                r.ttft_ms,
                r.duration_ms,
                r.tokens_per_sec,
                r.cost,
                r.currency,
                r.error,
            ],
        );
        if let Err(e) = result {
            tracing::warn!("failed to write usage record: {e}");
        }
    }

    pub fn overview(&self, from_ms: i64) -> Result<Overview> {
        let conn = self.lock();
        let mut overview = conn.query_row(
            r#"SELECT COUNT(*),
                      COALESCE(SUM(CASE WHEN status < 400 THEN 1 ELSE 0 END), 0),
                      COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0),
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(completion_tokens), 0),
                      COALESCE(SUM(cached_tokens), 0),
                      COALESCE(AVG(CASE WHEN tokens_per_sec IS NOT NULL THEN tokens_per_sec END), 0),
                      COALESCE(AVG(ttft_ms), 0),
                      COALESCE(AVG(duration_ms), 0),
                      COALESCE(SUM(CASE WHEN estimated = 1 THEN 1 ELSE 0 END), 0)
               FROM requests WHERE ts >= ?1"#,
            [from_ms],
            |r| {
                let prompt: i64 = r.get(3)?;
                let completion: i64 = r.get(4)?;
                Ok(Overview {
                    requests: r.get::<_, i64>(0)? as u64,
                    success: r.get::<_, i64>(1)? as u64,
                    errors: r.get::<_, i64>(2)? as u64,
                    prompt_tokens: prompt as u64,
                    completion_tokens: completion as u64,
                    cached_tokens: r.get::<_, i64>(5)? as u64,
                    total_tokens: (prompt + completion) as u64,
                    estimated_requests: r.get::<_, i64>(9)? as u64,
                    avg_tps: r.get(6)?,
                    avg_ttft_ms: r.get(7)?,
                    avg_duration_ms: r.get(8)?,
                    cost: 0.0,
                    cost_currency: None,
                    costs: Vec::new(),
                })
            },
        )?;
        overview.costs = cost_totals(&conn, from_ms)?;
        let (cost, currency) = single_cost(&overview.costs);
        overview.cost = cost;
        overview.cost_currency = currency;
        Ok(overview)
    }

    pub fn timeseries(&self, from_ms: i64, to_ms: i64, bucket_ms: i64) -> Result<Vec<Bucket>> {
        let bucket_ms = bucket_ms.max(1000);
        let conn = self.lock();
        let mut stmt = conn.prepare(
            r#"SELECT (ts / ?1) * ?1 AS bucket,
                      COUNT(*),
                      COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0),
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(completion_tokens), 0)
               FROM requests WHERE ts >= ?2 GROUP BY bucket ORDER BY bucket"#,
        )?;
        let rows = stmt.query_map(params![bucket_ms, from_ms], |r| {
            Ok(Bucket {
                ts: r.get(0)?,
                requests: r.get::<_, i64>(1)? as u64,
                errors: r.get::<_, i64>(2)? as u64,
                prompt_tokens: r.get::<_, i64>(3)? as u64,
                completion_tokens: r.get::<_, i64>(4)? as u64,
            })
        })?;

        let mut buckets: Vec<Bucket> = Vec::new();
        for row in rows {
            buckets.push(row?);
        }

        let mut filled = Vec::new();
        let mut cursor = (from_ms / bucket_ms) * bucket_ms;
        let end = to_ms.max(cursor);
        let mut i = 0;
        while cursor <= end {
            if i < buckets.len() && buckets[i].ts == cursor {
                filled.push(Bucket {
                    ts: cursor,
                    requests: buckets[i].requests,
                    errors: buckets[i].errors,
                    prompt_tokens: buckets[i].prompt_tokens,
                    completion_tokens: buckets[i].completion_tokens,
                });
                i += 1;
            } else {
                filled.push(Bucket {
                    ts: cursor,
                    requests: 0,
                    errors: 0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
                });
            }
            cursor += bucket_ms;
        }
        Ok(filled)
    }

    pub fn by_model(&self, from_ms: i64) -> Result<Vec<GroupStat>> {
        self.group_by("model", from_ms)
    }

    pub fn by_provider(&self, from_ms: i64) -> Result<Vec<GroupStat>> {
        self.group_by("provider", from_ms)
    }

    fn group_by(&self, column: &str, from_ms: i64) -> Result<Vec<GroupStat>> {
        let sql = format!(
            r#"SELECT {column}, COUNT(*),
                      COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0),
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(completion_tokens), 0),
                      COALESCE(SUM(cached_tokens), 0),
                      COALESCE(AVG(CASE WHEN tokens_per_sec IS NOT NULL THEN tokens_per_sec END), 0),
                      COALESCE(AVG(duration_ms), 0)
               FROM requests WHERE ts >= ?1
               GROUP BY {column}
               ORDER BY SUM(prompt_tokens + completion_tokens) DESC
               LIMIT 100"#
        );
        let conn = self.lock();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([from_ms], |r| {
            Ok(GroupStat {
                name: r.get(0)?,
                requests: r.get::<_, i64>(1)? as u64,
                errors: r.get::<_, i64>(2)? as u64,
                prompt_tokens: r.get::<_, i64>(3)? as u64,
                completion_tokens: r.get::<_, i64>(4)? as u64,
                cached_tokens: r.get::<_, i64>(5)? as u64,
                avg_tps: r.get(6)?,
                avg_duration_ms: r.get(7)?,
                cost: 0.0,
                currency: None,
                costs: Vec::new(),
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        drop(stmt);
        let mut by_name = grouped_costs(&conn, column, from_ms)?;
        for stat in &mut out {
            if let Some(list) = by_name.remove(&stat.name) {
                let (cost, currency) = single_cost(&list);
                stat.cost = cost;
                stat.currency = currency;
                stat.costs = list;
            }
        }
        Ok(out)
    }

    pub fn route_stats(&self, from_ms: i64, model: Option<String>) -> Result<Vec<RouteStat>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            r#"SELECT route_id, provider, model, COUNT(*),
                      COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END), 0),
                      COALESCE(SUM(prompt_tokens), 0),
                      COALESCE(SUM(completion_tokens), 0),
                      COALESCE(SUM(cached_tokens), 0),
                      COALESCE(SUM(cache_write_tokens), 0),
                      COALESCE(AVG(duration_ms), 0),
                      COALESCE(AVG(ttft_ms), 0),
                      MAX(ts)
               FROM requests
               WHERE ts >= ?1 AND (?2 IS NULL OR model = ?2)
               GROUP BY route_id, provider, model
               ORDER BY SUM(prompt_tokens + completion_tokens) DESC"#,
        )?;
        let rows = stmt.query_map(params![from_ms, model], |r| {
            Ok(RouteStat {
                route_id: r.get(0)?,
                provider: r.get(1)?,
                model: r.get(2)?,
                requests: r.get::<_, i64>(3)? as u64,
                errors: r.get::<_, i64>(4)? as u64,
                prompt_tokens: r.get::<_, i64>(5)? as u64,
                completion_tokens: r.get::<_, i64>(6)? as u64,
                cached_tokens: r.get::<_, i64>(7)? as u64,
                cache_write_tokens: r.get::<_, i64>(8)? as u64,
                avg_duration_ms: r.get(9)?,
                avg_ttft_ms: r.get(10)?,
                cost: 0.0,
                currency: None,
                costs: Vec::new(),
                last_ts: r.get(11)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        drop(stmt);
        let mut by_route = route_costs(&conn, from_ms, model.as_deref())?;
        for stat in &mut out {
            let key = (
                stat.route_id.clone(),
                stat.provider.clone(),
                stat.model.clone(),
            );
            if let Some(list) = by_route.remove(&key) {
                let (cost, currency) = single_cost(&list);
                stat.cost = cost;
                stat.currency = currency;
                stat.costs = list;
            }
        }
        Ok(out)
    }

    pub fn recent(
        &self,
        limit: i64,
        offset: i64,
        model: Option<String>,
        provider: Option<String>,
    ) -> Result<Vec<RecordView>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            r#"SELECT id, ts, endpoint, inbound, provider, model, upstream_model, route_id,
                      status, stream, prompt_tokens, completion_tokens, cached_tokens,
                      cache_write_tokens, estimated, ttft_ms, duration_ms, tokens_per_sec,
                      cost, currency, error
               FROM requests
               WHERE (?1 IS NULL OR model = ?1) AND (?2 IS NULL OR provider = ?2)
               ORDER BY id DESC LIMIT ?3 OFFSET ?4"#,
        )?;
        let rows = stmt.query_map(params![model, provider, limit, offset], |r| {
            let prompt: i64 = r.get(10)?;
            let completion: i64 = r.get(11)?;
            Ok(RecordView {
                id: r.get(0)?,
                ts: r.get(1)?,
                endpoint: r.get(2)?,
                inbound: r.get(3)?,
                provider: r.get(4)?,
                model: r.get(5)?,
                upstream_model: r.get(6)?,
                route_id: r.get(7)?,
                status: r.get::<_, i64>(8)? as u16,
                stream: r.get::<_, i64>(9)? != 0,
                prompt_tokens: prompt as u64,
                completion_tokens: completion as u64,
                cached_tokens: r.get::<_, i64>(12)? as u64,
                cache_write_tokens: r.get::<_, i64>(13)? as u64,
                total_tokens: (prompt + completion) as u64,
                estimated: r.get::<_, i64>(14)? != 0,
                ttft_ms: r.get(15)?,
                duration_ms: r.get(16)?,
                tokens_per_sec: r.get(17)?,
                cost: r.get(18)?,
                currency: r.get(19)?,
                error: r.get(20)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn count(&self, model: Option<String>, provider: Option<String>) -> Result<u64> {
        let conn = self.lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM requests WHERE (?1 IS NULL OR model = ?1) AND (?2 IS NULL OR provider = ?2)",
            params![model, provider],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    pub fn first_ts(&self) -> Result<Option<i64>> {
        let conn = self.lock();
        let v: Option<i64> = conn.query_row("SELECT MIN(ts) FROM requests", [], |r| r.get(0))?;
        Ok(v)
    }

    pub fn clear(&self) -> Result<()> {
        let conn = self.lock();
        conn.execute("DELETE FROM requests", [])?;
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        match self.conn.lock() {
            Ok(conn) => conn,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn migrate(conn: &Connection) -> Result<()> {
    let mut columns = std::collections::HashSet::new();
    {
        let mut stmt = conn.prepare("PRAGMA table_info(requests)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for row in rows {
            columns.insert(row?);
        }
    }
    let additions = [
        ("route_id", "TEXT NOT NULL DEFAULT ''"),
        ("cached_tokens", "INTEGER NOT NULL DEFAULT 0"),
        ("cache_write_tokens", "INTEGER NOT NULL DEFAULT 0"),
        ("cost", "REAL"),
        ("currency", "TEXT"),
    ];
    for (name, definition) in additions {
        if !columns.contains(name) {
            conn.execute(
                &format!("ALTER TABLE requests ADD COLUMN {name} {definition}"),
                [],
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn record(
        ts: i64,
        model: &str,
        provider: &str,
        route: &str,
        cost: f64,
        currency: &str,
    ) -> RequestRecord {
        RequestRecord {
            ts,
            endpoint: "/v1/chat/completions".to_string(),
            inbound: "openai".to_string(),
            provider: provider.to_string(),
            model: model.to_string(),
            upstream_model: model.to_string(),
            route_id: route.to_string(),
            status: 200,
            stream: false,
            prompt_tokens: 10,
            completion_tokens: 2,
            cached_tokens: 0,
            cache_write_tokens: 0,
            estimated: false,
            ttft_ms: None,
            duration_ms: 10,
            tokens_per_sec: None,
            cost: Some(cost),
            currency: Some(currency.to_string()),
            error: None,
        }
    }

    #[test]
    fn overview_splits_costs_by_currency() {
        let stats = Stats::open(Path::new(":memory:")).unwrap();
        stats.insert(&record(10, "m1", "p1", "r1", 1.0, "USD"));
        stats.insert(&record(11, "m1", "p1", "r1", 2.0, "CNY"));
        let overview = stats.overview(0).unwrap();
        assert_eq!(overview.costs.len(), 2);
        assert_eq!(overview.cost_currency, None);
        assert_eq!(overview.cost, 0.0);
        let usd = overview
            .costs
            .iter()
            .find(|item| item.currency == "USD")
            .unwrap();
        assert!((usd.cost - 1.0).abs() < 1e-9);

        let stats = Stats::open(Path::new(":memory:")).unwrap();
        stats.insert(&record(10, "m1", "p1", "r1", 1.5, "USD"));
        stats.insert(&record(11, "m1", "p1", "r1", 0.5, "USD"));
        let overview = stats.overview(0).unwrap();
        assert_eq!(overview.costs.len(), 1);
        assert_eq!(overview.cost_currency.as_deref(), Some("USD"));
        assert!((overview.cost - 2.0).abs() < 1e-9);
    }

    #[test]
    fn grouped_and_route_costs_split_by_currency() {
        let stats = Stats::open(Path::new(":memory:")).unwrap();
        stats.insert(&record(10, "m1", "p1", "r1", 1.0, "USD"));
        stats.insert(&record(11, "m1", "p1", "r1", 2.0, "CNY"));
        stats.insert(&record(12, "m2", "p1", "r2", 0.5, "USD"));

        let models = stats.by_model(0).unwrap();
        let m1 = models.iter().find(|item| item.name == "m1").unwrap();
        assert_eq!(m1.costs.len(), 2);
        assert_eq!(m1.cost, 0.0);
        let m2 = models.iter().find(|item| item.name == "m2").unwrap();
        assert_eq!(m2.costs.len(), 1);
        assert_eq!(m2.currency.as_deref(), Some("USD"));
        assert!((m2.cost - 0.5).abs() < 1e-9);

        let routes = stats.route_stats(0, None).unwrap();
        let r1 = routes.iter().find(|item| item.route_id == "r1").unwrap();
        assert_eq!(r1.costs.len(), 2);
        assert_eq!(r1.cost, 0.0);
        let r2 = routes.iter().find(|item| item.route_id == "r2").unwrap();
        assert_eq!(r2.currency.as_deref(), Some("USD"));
        assert!((r2.cost - 0.5).abs() < 1e-9);
    }
}
