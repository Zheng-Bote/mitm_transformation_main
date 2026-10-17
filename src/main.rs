/*
 * SPDX-FileComment: Transformation Batch Job
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file main.rs
 * @brief Claims raw fragments, aggregates them per correlation_id and writes target fragments
 * @version 1.1.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use mitm_transformer::{
    envelope_decrypt, merge_payloads, process_sources, MappingRule, MappingSource,
    MappingTargetField, PipelineError, RuleSet, RuleStep,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    PgPool, Row,
};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream as StdUnixStream,
    sync::Arc,
};
use tokio::{
    io::AsyncWriteExt,
    net::UnixStream,
    signal::unix::{signal, SignalKind},
    sync::{watch, Mutex},
    task::JoinSet,
};

const APP_NAME: &str = "Transformation Engine (main)";
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Exit code if records failed validation or could not be processed.
const EXIT_RECORDS_FAILED: i32 = 2;

#[derive(Debug, Deserialize, Default)]
struct DBConfigEnvelope {
    #[serde(default)]
    db: DBConnectionConfig,
}

#[derive(Debug, Deserialize, Default)]
struct DBConnectionConfig {
    #[serde(default)] host: String,
    #[serde(default)] port: u16,
    #[serde(default)] user: String,
    #[serde(default)] password: String,
    #[serde(default)] database: String,
    #[serde(default)] sslmode: bool,
}

#[derive(Debug, Deserialize)]
struct JobArgs {
    #[serde(default = "default_batch_size")] batch_size: usize,
    #[serde(default = "default_workers")] workers: usize,
    #[serde(default)] retry_failed: bool,
    #[serde(default = "default_topic")] topic: String,
    /// Overrides `topic_dependencies.required_sources` if set.
    #[serde(default)] required_sources: Option<Vec<String>>,
    #[serde(default = "default_source_name")] source_name: String,
}

fn default_batch_size() -> usize { 500 }
fn default_workers() -> usize { 5 }
fn default_topic() -> String { "employee.data".into() }
fn default_source_name() -> String { "TRANSFORMATION".into() }

impl Default for JobArgs {
    fn default() -> Self {
        Self { batch_size: default_batch_size(), workers: default_workers(), retry_failed: false, topic: default_topic(), required_sources: None, source_name: default_source_name() }
    }
}

/// Status and audit events over one persistent scheduler socket connection.
#[derive(Clone)]
struct IpcClient {
    socket_path: String,
    run_id: i64,
    topic: String,
    source_name: String,
    component: String,
    conn: Arc<Mutex<Option<UnixStream>>>,
}

impl IpcClient {
    async fn send(&self, event_type: &str, status: Option<&str>, message: &str, progress: Option<u8>) {
        let event = json!({
            "run_id": self.run_id,
            "type": event_type,
            "message": self.message(message),
            "component": self.component,
            "status": status.unwrap_or("unknown"),
            "progress": progress.unwrap_or(0),
        });
        let mut line = event.to_string();
        line.push('\n');

        let mut conn = self.conn.lock().await;
        // Re-dial once if the connection broke.
        for _ in 0..2 {
            if conn.is_none() {
                match UnixStream::connect(&self.socket_path).await {
                    Ok(stream) => *conn = Some(stream),
                    Err(error) => { eprintln!("[IPC ERROR] Failed to connect to scheduler: {error}"); return; }
                }
            }
            if let Some(stream) = conn.as_mut()
                && stream.write_all(line.as_bytes()).await.is_ok()
            {
                return;
            }
            *conn = None;
        }
        eprintln!("[IPC ERROR] Failed to send scheduler event");
    }

    fn message(&self, message: &str) -> String {
        format!("{}: {}: {message}", self.topic, self.source_name)
    }
}

#[derive(Clone)]
struct RawFragment {
    id: String,
    source_system: String,
    created_us: i64,
    payload: Vec<u8>,
    nonce: Vec<u8>,
    wrapped_key: Vec<u8>,
}

#[derive(Clone)]
struct AggregatedFragment {
    correlation_id: String,
    topic: String,
    /// Claimed fragments (status 'processing'); only these change status.
    fragments: Vec<RawFragment>,
}

#[derive(Clone)]
struct TransformationError {
    failed_field: String,
    rule_name: String,
    error_message: String,
}

/// Static context shared by all aggregate tasks.
struct JobContext {
    pool: PgPool,
    rules: RuleSet,
    master_key: Vec<u8>,
    wrapped_key: Vec<u8>,
    /// Canonical spelling of the topic as configured in delivery_targets.
    target_topic: String,
    required_sources: Vec<String>,
}

/// Decrypted payloads per source system with their creation time (µs).
type PayloadsBySource = BTreeMap<String, Vec<(i64, Map<String, Value>)>>;

enum Outcome {
    Processed,
    FailedValidation,
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("Transformation Engine failed: {:?}", e);
            std::process::exit(1);
        }
    }
}

async fn run() -> Result<i32> {
    // Secrets are kept in variables only (never written to the process environment).
    let (scheduler_db_config, scheduler_master_key) = fetch_scheduler_credentials().unwrap_or_else(|error| {
        if env::var_os("RUN_ID").is_some() && env::var_os("SCHEDULER_SOCKET_PATH").is_some() {
            eprintln!("[IPC WARNING] Failed to get credentials from scheduler: {error}");
        }
        (None, None)
    });
    let db_config_json = scheduler_db_config.or_else(|| env::var("MITM_DB_CONFIG_JSON").ok());
    let master_key_input = scheduler_master_key.or_else(|| env::var("MASTER_KEY").ok()).unwrap_or_default();

    let mut args = parse_job_args()?;
    args.batch_size = args.batch_size.max(1);
    args.workers = args.workers.max(1);

    let ipc = ipc_client(&args);
    if let Some(ipc) = &ipc {
        ipc.send("status", Some("started"), &format!("{APP_NAME} ({VERSION}) started"), Some(0)).await;
        ipc.send("audit", None, &format!("{APP_NAME} ({VERSION}) started"), None).await;
    }

    let result = run_job(&args, db_config_json.as_deref(), &master_key_input, ipc.as_ref()).await;
    if let Err(error) = &result {
        let message = format!("Transformation Batch Job failed: {error:#}");
        eprintln!("{message}");
        if let Some(ipc) = &ipc {
            ipc.send("status", Some("failed"), &message, None).await;
            ipc.send("audit", None, &message, None).await;
        }
    }
    result
}

async fn run_job(args: &JobArgs, db_config_json: Option<&str>, master_key_input: &str, ipc: Option<&IpcClient>) -> Result<i32> {
    let master_key = master_key(master_key_input)?;
    let database = database_config(db_config_json)?;
    let pool = connect(&database, args.workers).await?;

    let outcome = run_with_pool(args, &pool, master_key, ipc).await;
    if let Err(error) = &outcome {
        log_system(&pool, "ERROR", &format!("Transformation Batch Job failed: {error:#}")).await;
    }
    outcome
}

async fn run_with_pool(args: &JobArgs, pool: &PgPool, master_key: Vec<u8>, ipc: Option<&IpcClient>) -> Result<i32> {
    // Singleton per topic; the session lock is held by this connection until the process exits.
    let mut lock_conn = pool.acquire().await.context("failed to acquire lock connection")?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
        .bind(format!("transformation_{}", args.topic)).fetch_one(&mut *lock_conn).await.context("failed to acquire advisory lock")?;
    if !locked {
        let message = format!("Another transformation instance for topic '{}' is already running. Exiting gracefully.", args.topic);
        eprintln!("{message}");
        if let Some(ipc) = ipc { ipc.send("status", Some("exited"), &message, None).await; }
        return Ok(0);
    }

    let rules = load_rules(pool).await?;
    eprintln!("Loaded {} sources, {} targets, {} rules", rules.sources.len(), rules.target_fields.len(), rules.rules.len());
    log_system(pool, "INFO", &format!("{} ({}) started for topic {}", APP_NAME, VERSION, args.topic)).await;

    let required_sources = resolve_required_sources(pool, &args.topic, args.required_sources.as_deref()).await?;
    // Never encrypt with a key that is not persisted: a missing delivery key is fatal.
    let (target_topic, wrapped_key) = delivery_key(pool, &args.topic).await?;

    let source_status = if args.retry_failed { "failed_validation" } else { "pending" };

    // Rows left in 'processing' by a crashed run are orphaned (we hold the topic lock).
    let orphaned = reset_processing(pool, &args.topic, "pending").await?;
    if orphaned > 0 {
        audit(ipc, &format!("Reset {orphaned} orphaned fragments from 'processing' to 'pending'")).await;
    }

    let context = Arc::new(JobContext { pool: pool.clone(), rules, master_key, wrapped_key, target_topic, required_sources });

    // Graceful shutdown on SIGTERM (scheduler stop) and SIGINT
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let mut sigterm = signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        eprintln!("Shutting down gracefully...");
        let _ = shutdown_tx.send(true);
    });

    let mut tasks: JoinSet<Result<Outcome>> = JoinSet::new();
    let mut claimed_total = 0_usize;
    let mut counts = (0_usize, 0_usize, 0_usize); // processed, failed validation, errors

    let loop_result: Result<()> = async {
        'claim: loop {
            if *shutdown_rx.borrow() { break; }
            let fragments = tokio::select! {
                _ = shutdown_rx.changed() => break,
                claimed = claim_aggregated_fragments(pool, &args.topic, &context.required_sources, args.batch_size, source_status) => claimed?,
            };
            if fragments.is_empty() { break; }
            for fragment in fragments {
                if *shutdown_rx.borrow() { break 'claim; }
                while tasks.len() >= args.workers {
                    if let Some(result) = tasks.join_next().await { tally(result, &mut counts); }
                }
                claimed_total += 1;
                if claimed_total % args.batch_size == 0 {
                    let msg = format!("Progress: {} records claimed", claimed_total);
                    if let Some(ipc) = ipc {
                        ipc.send("status", Some("processing"), &msg, Some(0)).await;
                        ipc.send("audit", None, &msg, None).await;
                    }
                }
                tasks.spawn(process_aggregate(context.clone(), fragment));
            }
        }
        Ok(())
    }.await;

    // In-flight aggregates always finish, also on shutdown or claim errors.
    while let Some(result) = tasks.join_next().await { tally(result, &mut counts); }

    // Claimed but unprocessed aggregates (shutdown, task errors) return to the source status.
    match reset_processing(pool, &args.topic, source_status).await {
        Ok(released) if released > 0 => audit(ipc, &format!("Reset {released} unprocessed fragments from 'processing' to '{source_status}'")).await,
        Ok(_) => {}
        Err(error) => eprintln!("Failed to reset unprocessed fragments: {error:#}"),
    }
    loop_result?;

    let (processed, failed, errors) = counts;
    let stopped = *shutdown_rx.borrow();
    let message = format!("Topic {}: {processed} records processed, {failed} failed validation, {errors} errors{}",
        args.topic, if stopped { " (stopped by signal)" } else { "" });
    let (status, code, level) = if failed > 0 || errors > 0 { ("failed", EXIT_RECORDS_FAILED, "ERROR") }
        else if stopped { ("cancelled", 0, "INFO") } else { ("finished", 0, "INFO") };
    eprintln!("Transformation Batch Job {status}. {message}");
    if let Some(ipc) = ipc {
        ipc.send("status", Some(status), &message, Some(100)).await;
        ipc.send("audit", None, &message, None).await;
    }
    log_system(pool, level, &format!("Transformation Batch Job {status}. {message}")).await;
    drop(lock_conn);
    Ok(code)
}

fn tally(result: std::result::Result<Result<Outcome>, tokio::task::JoinError>, counts: &mut (usize, usize, usize)) {
    match result {
        Ok(Ok(Outcome::Processed)) => counts.0 += 1,
        Ok(Ok(Outcome::FailedValidation)) => counts.1 += 1,
        Ok(Err(error)) => { eprintln!("Aggregate failed: {error:#}"); counts.2 += 1; }
        Err(error) => { eprintln!("Aggregate task panicked: {error}"); counts.2 += 1; }
    }
}

async fn audit(ipc: Option<&IpcClient>, message: &str) {
    eprintln!("AUDIT: {message}");
    if let Some(ipc) = ipc { ipc.send("audit", None, message, None).await; }
}

async fn log_system(pool: &PgPool, level: &str, message: &str) {
    let _ = sqlx::query("INSERT INTO system_logs (level, component, message) VALUES ($1, 'transformation-engine', $2)")
        .bind(level).bind(message).execute(pool).await;
}

fn parse_job_args() -> Result<JobArgs> {
    match env::args().nth(1) {
        Some(raw) => serde_json::from_str(&raw).context("failed to parse job arguments JSON"),
        None => Ok(JobArgs::default()),
    }
}

fn database_config(db_config_json: Option<&str>) -> Result<DBConnectionConfig> {
    if let Some(raw) = db_config_json {
        let config: DBConfigEnvelope = serde_json::from_str(raw).context("failed to parse MITM_DB_CONFIG_JSON")?;
        if config.db.host.is_empty() { return Err(anyhow!("MITM_DB_CONFIG_JSON does not contain db.host")); }
        return Ok(config.db);
    }
    let host = env::var("MITM_DB_HOST").unwrap_or_default();
    if host.is_empty() { return Err(anyhow!("MitM database credentials not found in MITM_DB_HOST or MITM_DB_CONFIG_JSON")); }
    Ok(DBConnectionConfig {
        host,
        port: env::var("MITM_DB_PORT").ok().and_then(|value| value.parse().ok()).unwrap_or(5432),
        user: env::var("MITM_DB_USER").unwrap_or_default(),
        password: env::var("MITM_DB_PASSWORD").unwrap_or_default(),
        database: env::var("MITM_DB_NAME").unwrap_or_default(),
        sslmode: matches!(env::var("MITM_DB_SSLMODE").as_deref(), Ok("true") | Ok("require")),
    })
}

async fn connect(config: &DBConnectionConfig, workers: usize) -> Result<PgPool> {
    let options = PgConnectOptions::new().host(&config.host).port(if config.port == 0 { 5432 } else { config.port })
        .username(&config.user).password(&config.password).database(&config.database)
        .ssl_mode(if config.sslmode { PgSslMode::Require } else { PgSslMode::Disable });
    PgPoolOptions::new().max_connections((workers + 5) as u32).connect_with(options).await.context("unable to connect to database")
}

async fn load_rules(pool: &PgPool) -> Result<RuleSet> {
    let mut rules = RuleSet::default();
    for row in sqlx::query("SELECT id::text, name, topic FROM mapping_source").fetch_all(pool).await? {
        let id: String = row.try_get("id")?;
        rules.sources.insert(id.clone(), MappingSource { id, name: row.try_get("name")?, topic: row.try_get("topic")? });
    }
    for row in sqlx::query("SELECT id::text, field_name, data_type, is_required, encrypted FROM mapping_target_field").fetch_all(pool).await? {
        let id: String = row.try_get("id")?;
        rules.target_fields.insert(id.clone(), MappingTargetField { id, field_name: row.try_get("field_name")?, data_type: row.try_get("data_type")?, is_required: row.try_get("is_required")?, encrypted: row.try_get("encrypted")? });
    }
    for row in sqlx::query("SELECT source_id::text, target_field_id::text, source_field, priority, transformation_chain, validation_chain FROM mapping_rule ORDER BY priority, id").fetch_all(pool).await? {
        rules.rules.push(MappingRule {
            source_id: row.try_get("source_id")?,
            target_field_id: row.try_get("target_field_id")?,
            source_field: row.try_get("source_field")?,
            priority: row.try_get("priority")?,
            transformations: parse_chain(row.try_get("transformation_chain")?)?,
            validations: parse_chain(row.try_get("validation_chain")?)?,
        });
    }
    Ok(rules)
}

fn parse_chain(value: Option<Value>) -> Result<Vec<RuleStep>> {
    match value { Some(value @ Value::Array(_)) => serde_json::from_value(value).context("invalid rule chain"), Some(Value::Null) | None => Ok(Vec::new()), Some(_) => Err(anyhow!("rule chain must be a JSON array")) }
}

/// Required sources from JobArgs (override) or topic_dependencies; deduplicated and sorted.
async fn resolve_required_sources(pool: &PgPool, topic: &str, override_sources: Option<&[String]>) -> Result<Vec<String>> {
    let sources = match override_sources {
        Some(sources) if !sources.is_empty() => sources.to_vec(),
        _ => sqlx::query_scalar::<_, Vec<String>>("SELECT required_sources FROM topic_dependencies WHERE LOWER(topic) = LOWER($1) LIMIT 1")
            .bind(topic).fetch_optional(pool).await.context("failed to read topic_dependencies")?
            .unwrap_or_default(),
    };
    normalize_sources(sources).ok_or_else(|| anyhow!("no required sources for topic '{topic}' (neither in job arguments nor in topic_dependencies)"))
}

fn normalize_sources(mut sources: Vec<String>) -> Option<Vec<String>> {
    sources.retain(|source| !source.trim().is_empty());
    sources.sort();
    sources.dedup();
    (!sources.is_empty()).then_some(sources)
}

/// Returns the canonical delivery topic and its wrapped DEK.
async fn delivery_key(pool: &PgPool, topic: &str) -> Result<(String, Vec<u8>)> {
    let row = sqlx::query("SELECT dt.topic, sk.wrapped_key FROM delivery_targets dt JOIN storage_keys sk ON dt.dek_id = sk.id WHERE LOWER(dt.topic) = LOWER($1) AND dt.is_active = true LIMIT 1")
        .bind(topic).fetch_optional(pool).await.context("failed to read delivery key")?
        .ok_or_else(|| anyhow!("no active delivery target (and key) configured for topic '{topic}'"))?;
    Ok((row.try_get("topic")?, row.try_get("wrapped_key")?))
}

async fn reset_processing(pool: &PgPool, topic: &str, status: &str) -> Result<u64> {
    let result = sqlx::query("UPDATE raw_ingestion SET status = $2 WHERE topic = $1 AND status = 'processing'")
        .bind(topic).bind(status).execute(pool).await.context("failed to reset processing fragments")?;
    Ok(result.rows_affected())
}

/// Claims, in one statement, the fragments in `status` of correlation_ids whose required sources are
/// complete, counting fragments that are already 'processed' (re-aggregation of single-source updates).
async fn claim_aggregated_fragments(pool: &PgPool, topic: &str, required_sources: &[String], limit: usize, status: &str) -> Result<Vec<AggregatedFragment>> {
    let rows = sqlx::query(
        "WITH candidates AS (
            SELECT r.correlation_id
            FROM raw_ingestion r
            WHERE r.topic = $1 AND r.status = $2 AND r.correlation_id IS NOT NULL
            GROUP BY r.correlation_id
            HAVING (
                SELECT COUNT(DISTINCT a.source_system) FROM raw_ingestion a
                WHERE a.topic = $1 AND a.correlation_id = r.correlation_id
                  AND a.source_system = ANY($3::text[])
                  AND a.status IN ($2, 'processed')
            ) = cardinality($3::text[])
            LIMIT $4
        )
        UPDATE raw_ingestion SET status = 'processing', processed_at = NOW()
        FROM storage_keys
        WHERE raw_ingestion.correlation_id IN (SELECT correlation_id FROM candidates)
          AND raw_ingestion.topic = $1 AND raw_ingestion.status = $2 AND raw_ingestion.dek_id = storage_keys.id
        RETURNING raw_ingestion.id::text, raw_ingestion.topic, raw_ingestion.correlation_id::text, raw_ingestion.source_system,
                  (EXTRACT(EPOCH FROM raw_ingestion.created_at) * 1000000)::bigint AS created_us,
                  raw_ingestion.payload, raw_ingestion.nonce, storage_keys.wrapped_key")
        .bind(topic).bind(status).bind(required_sources.to_vec()).bind(limit as i64).fetch_all(pool).await?;
    let mut grouped: HashMap<String, AggregatedFragment> = HashMap::new();
    for row in rows {
        let correlation_id: String = row.try_get("correlation_id")?;
        let topic: String = row.try_get("topic")?;
        grouped.entry(correlation_id.clone()).or_insert_with(|| AggregatedFragment { correlation_id: correlation_id.clone(), topic, fragments: Vec::new() })
            .fragments.push(raw_fragment(&row)?);
    }
    Ok(grouped.into_values().collect())
}

/// Latest 'processed' fragment of every source system that is not part of the claimed fragments.
async fn load_counterparts(pool: &PgPool, aggregate: &AggregatedFragment) -> Result<Vec<RawFragment>> {
    let claimed_sources: Vec<String> = aggregate.fragments.iter().map(|f| f.source_system.clone()).collect();
    let rows = sqlx::query(
        "SELECT DISTINCT ON (r.source_system) r.id::text, r.source_system,
                (EXTRACT(EPOCH FROM r.created_at) * 1000000)::bigint AS created_us, r.payload, r.nonce, sk.wrapped_key
         FROM raw_ingestion r JOIN storage_keys sk ON r.dek_id = sk.id
         WHERE r.topic = $1 AND r.correlation_id = $2::uuid AND r.status = 'processed'
           AND NOT (r.source_system = ANY($3::text[]))
         ORDER BY r.source_system, r.created_at DESC")
        .bind(&aggregate.topic).bind(&aggregate.correlation_id).bind(claimed_sources).fetch_all(pool).await
        .context("failed to load processed fragments for re-aggregation")?;
    rows.iter().map(raw_fragment).collect()
}

fn raw_fragment(row: &sqlx::postgres::PgRow) -> Result<RawFragment> {
    Ok(RawFragment {
        id: row.try_get("id")?,
        source_system: row.try_get("source_system")?,
        created_us: row.try_get::<Option<i64>, _>("created_us")?.unwrap_or(0),
        payload: row.try_get("payload")?,
        nonce: row.try_get("nonce")?,
        wrapped_key: row.try_get("wrapped_key")?,
    })
}

async fn process_aggregate(context: Arc<JobContext>, aggregate: AggregatedFragment) -> Result<Outcome> {
    let counterparts = load_counterparts(&context.pool, &aggregate).await?;

    // Decrypt claimed fragments and processed counterparts, grouped per source (oldest → newest)
    let mut errors = Vec::new();
    let mut per_source: PayloadsBySource = BTreeMap::new();
    for fragment in aggregate.fragments.iter().chain(counterparts.iter()) {
        let decrypted = match envelope_decrypt(&context.master_key, &fragment.wrapped_key, &fragment.nonce, &fragment.payload) {
            Ok(payload) => payload,
            Err(error) => {
                errors.push(TransformationError { failed_field: "payload".into(), rule_name: "decryption".into(), error_message: format!("Failed to decrypt envelope: {error:#}") });
                continue;
            }
        };
        match serde_json::from_slice::<Map<String, Value>>(&decrypted) {
            Ok(payload) => per_source.entry(fragment.source_system.clone()).or_default().push((fragment.created_us, payload)),
            Err(error) => errors.push(TransformationError { failed_field: "payload".into(), rule_name: "json_parse".into(), error_message: json_error_message(&error) }),
        }
    }

    for required in &context.required_sources {
        if !errors.is_empty() { break; }
        if !per_source.contains_key(required) {
            errors.push(TransformationError { failed_field: "source_system".into(), rule_name: "required_source".into(), error_message: format!("required source '{required}' is missing") });
        }
    }

    let mut data = Map::new();
    if errors.is_empty() {
        let sources: BTreeMap<String, Map<String, Value>> = per_source.into_iter().map(|(source, mut payloads)| {
            payloads.sort_by_key(|(created_us, _)| *created_us);
            (source, merge_payloads(payloads.into_iter().map(|(_, payload)| payload)))
        }).collect();
        let (mapped, pipeline_errors) = process_sources(&sources, &aggregate.topic, &context.rules, &context.master_key, &context.wrapped_key);
        errors.extend(pipeline_errors.into_iter().map(convert_pipeline_error));
        data = mapped;
    }
    write_target_and_complete(&context, &aggregate, data, &errors).await
}

/// serde messages may quote the offending value (PII); keep only category and position.
fn json_error_message(error: &serde_json::Error) -> String {
    format!("payload is not a valid JSON object ({:?} error at line {}, column {})", error.classify(), error.line(), error.column())
}

fn convert_pipeline_error(error: PipelineError) -> TransformationError {
    TransformationError { failed_field: error.failed_field, rule_name: error.rule_name, error_message: error.error_message }
}

async fn write_target_and_complete(context: &JobContext, aggregate: &AggregatedFragment, data: Map<String, Value>, errors: &[TransformationError]) -> Result<Outcome> {
    let first_id = aggregate.fragments.first().ok_or_else(|| anyhow!("claimed aggregate contains no fragments"))?.id.clone();
    // Only the claimed fragments change status, never other topics or newly arrived fragments.
    let claimed_ids: Vec<String> = aggregate.fragments.iter().map(|f| f.id.clone()).collect();
    let mut transaction = context.pool.begin().await?;
    let outcome = if errors.is_empty() {
        if !data.is_empty() {
            sqlx::query("INSERT INTO target_fragments (raw_ingestion_id, topic, payload_jsonb, delivery_status) VALUES ($1::uuid, $2, $3, 'PENDING')")
                .bind(&first_id).bind(&context.target_topic).bind(Value::Object(data)).execute(&mut *transaction).await?;
        }
        sqlx::query("UPDATE raw_ingestion SET status = 'processed', processed_at = NOW() WHERE id = ANY($1::uuid[])")
            .bind(&claimed_ids).execute(&mut *transaction).await?;
        Outcome::Processed
    } else {
        for error in errors {
            sqlx::query("INSERT INTO transformation_errors (raw_ingestion_id, failed_field, rule_name, error_message) VALUES ($1::uuid, $2, $3, $4)")
                .bind(&first_id).bind(&error.failed_field).bind(&error.rule_name).bind(&error.error_message).execute(&mut *transaction).await?;
        }
        sqlx::query("UPDATE raw_ingestion SET status = 'failed_validation', processed_at = NOW() WHERE id = ANY($1::uuid[])")
            .bind(&claimed_ids).execute(&mut *transaction).await?;
        Outcome::FailedValidation
    };
    transaction.commit().await?;
    Ok(outcome)
}

/// Decodes the KEK (base64 or raw) and requires exactly 32 bytes (AES-256).
fn master_key(input: &str) -> Result<Vec<u8>> {
    let decoded = STANDARD.decode(input).unwrap_or_else(|_| input.as_bytes().to_vec());
    if decoded.len() != 32 {
        return Err(anyhow!("MASTER_KEY must be 32 bytes (AES-256), got {} bytes", decoded.len()));
    }
    Ok(decoded)
}

fn ipc_client(args: &JobArgs) -> Option<IpcClient> {
    Some(IpcClient {
        socket_path: env::var("SCHEDULER_SOCKET_PATH").ok()?,
        run_id: env::var("RUN_ID").ok()?.parse().ok()?,
        topic: args.topic.clone(),
        source_name: args.source_name.clone(),
        component: "mitm_transformation".to_string(),
        conn: Arc::new(Mutex::new(None)),
    })
}

fn fetch_scheduler_credentials() -> Result<(Option<String>, Option<String>)> {
    let socket_path = env::var("SCHEDULER_SOCKET_PATH").context("not running under scheduler")?;
    let run_id: i64 = env::var("RUN_ID").context("not running under scheduler")?.parse().context("invalid RUN_ID")?;
    let response = request_socket_json(&socket_path, &json!({"type": "get_credentials", "run_id": run_id}))?;
    Ok((response.get("db_config_json").and_then(Value::as_str).map(str::to_owned), response.get("master_key").and_then(Value::as_str).map(str::to_owned)))
}

fn request_socket_json(socket_path: &str, message: &Value) -> Result<Value> {
    let mut stream = StdUnixStream::connect(socket_path).with_context(|| format!("failed to connect to {socket_path}"))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
    writeln!(stream, "{}", serde_json::to_string(message)?).context("failed to send scheduler request")?;
    let mut response = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut response).context("failed to read scheduler response")?;
    serde_json::from_str(&response).context("scheduler returned invalid JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, BufReader as AsyncBufReader};
    use tokio::net::UnixListener;

    #[test]
    fn master_key_requires_32_bytes() {
        assert!(master_key("").is_err());
        assert!(master_key("short").is_err());
        assert_eq!(master_key(&STANDARD.encode([7_u8; 32])).unwrap(), vec![7_u8; 32]);
        // Valid base64 is always decoded first (24 bytes here), raw keys are used only if not base64.
        assert!(master_key("0123456789abcdef0123456789abcdef").is_err());
        assert_eq!(master_key("0123456789abcdef0123456789abcde!").unwrap().len(), 32);
    }

    #[test]
    fn json_error_message_hides_value() {
        let error = serde_json::from_str::<Map<String, Value>>(r#""secret-ssn-123""#).unwrap_err();
        assert!(error.to_string().contains("secret-ssn-123"), "precondition: serde quotes the value");
        let message = json_error_message(&error);
        assert!(!message.contains("secret-ssn-123"), "{message}");
    }

    #[test]
    fn required_sources_are_normalized() {
        assert_eq!(normalize_sources(vec!["PG".into(), "ORA".into(), "PG".into(), " ".into()]), Some(vec!["ORA".to_string(), "PG".to_string()]));
        assert_eq!(normalize_sources(vec![]), None);
    }

    #[test]
    fn job_args_without_required_sources_use_topic_dependencies() {
        let args: JobArgs = serde_json::from_str(r#"{"topic": "Employee"}"#).unwrap();
        assert!(args.required_sources.is_none());
    }

    #[tokio::test]
    async fn ipc_reuses_one_connection() {
        let dir = std::env::temp_dir().join(format!("mitm-ipc-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scheduler.sock");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();

        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let lines = Arc::new(Mutex::new(Vec::<String>::new()));
        let (acc, collected) = (accepted.clone(), lines.clone());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                acc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let collected = collected.clone();
                tokio::spawn(async move {
                    let mut reader = AsyncBufReader::new(stream).lines();
                    while let Ok(Some(line)) = reader.next_line().await { collected.lock().await.push(line); }
                });
            }
        });

        let client = IpcClient { socket_path: path.to_string_lossy().into(), run_id: 1, topic: "Employee".into(), source_name: "TRANSFORMATION".into(), component: "mitm_transformation".into(), conn: Arc::new(Mutex::new(None)) };
        for i in 0..5 { client.send("audit", None, &format!("msg {i}"), None).await; }

        for _ in 0..100 {
            if lines.lock().await.len() == 5 { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(lines.lock().await.len(), 5);
        assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(lines.lock().await[0].contains("Employee: TRANSFORMATION: msg 0"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
