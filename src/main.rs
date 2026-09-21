use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use mitm_transformer::{
    envelope_decrypt, generate_wrapped_dek, merge_payloads, process_payload, MappingRule,
    MappingTargetField, PipelineError, RuleSet, RuleStep,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    PgPool, Row,
};
use std::{
    collections::HashMap,
    env,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    sync::Arc,
};
use tokio::{signal, task::JoinSet};

const APP_NAME: &str = "Transformation Engine";
const VERSION: &str = env!("CARGO_PKG_VERSION");

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
    #[serde(default = "default_sources")] required_sources: Vec<String>,
    #[serde(default = "default_source_name")] source_name: String,
}

fn default_batch_size() -> usize { 500 }
fn default_workers() -> usize { 5 }
fn default_topic() -> String { "employee.data".into() }
fn default_sources() -> Vec<String> { vec!["ORA_EMPLOYEE".into()] }
fn default_source_name() -> String { "TRANSFORMATION".into() }

impl Default for JobArgs {
    fn default() -> Self {
        Self { batch_size: default_batch_size(), workers: default_workers(), retry_failed: false, topic: default_topic(), required_sources: default_sources(), source_name: default_source_name() }
    }
}

#[derive(Clone)]
struct IpcClient {
    socket_path: String,
    run_id: i64,
    topic: String,
    source_name: String,
    component: String,
}

impl IpcClient {
    fn send(&self, event_type: &str, status: Option<&str>, message: &str, progress: Option<u8>) {
        let mut event = json!({"run_id": self.run_id, "type": event_type, "message": self.message(message)});
        event["component"] = Value::String(self.component.clone());
        event["status"] = Value::String(status.unwrap_or("unknown").into());
        event["progress"] = json!(progress.unwrap_or(0));
        if let Err(error) = send_socket_json(&self.socket_path, &event) {
            eprintln!("[IPC ERROR] Failed to send scheduler event: {error}");
        }
    }

    fn message(&self, message: &str) -> String {
        format!("{}: {}: {message}", self.topic, self.source_name)
    }
}

#[derive(Clone)]
struct RawFragment {
    id: String,
    payload: Vec<u8>,
    nonce: Vec<u8>,
    wrapped_key: Vec<u8>,
}

#[derive(Clone)]
struct AggregatedFragment {
    correlation_id: String,
    topic: String,
    fragments: Vec<RawFragment>,
}

#[derive(Clone)]
struct TransformationError {
    failed_field: String,
    rule_name: String,
    error_message: String,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Transformation Engine failed: {:?}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let (scheduler_db_config, scheduler_master_key) = fetch_scheduler_credentials().unwrap_or_else(|error| {
        if env::var_os("RUN_ID").is_some() && env::var_os("SCHEDULER_SOCKET_PATH").is_some() {
            eprintln!("[IPC WARNING] Failed to get credentials from scheduler: {error}");
        }
        (None, None)
    });
    if let Some(config) = scheduler_db_config { unsafe { env::set_var("MITM_DB_CONFIG_JSON", config); } }
    if let Some(key) = scheduler_master_key { unsafe { env::set_var("MASTER_KEY", key); } }

    let mut args = parse_job_args()?;
    args.batch_size = args.batch_size.max(1);
    args.workers = args.workers.max(1);
    if args.required_sources.is_empty() { args.required_sources = default_sources(); }

    let ipc = ipc_client(&args);
    if let Some(ipc) = &ipc {
        ipc.send("status", Some("started"), &format!("{APP_NAME} ({VERSION}) started"), Some(0));
        ipc.send("audit", None, &format!("{APP_NAME} ({VERSION}) started"), None);
    }

    let database = database_config()?;
    let pool = Arc::new(connect(&database, args.workers).await?);
    let rules = Arc::new(load_rules(&pool).await?);
    eprintln!("Loaded {} sources, {} targets, {} rules", rules.sources.len(), rules.target_fields.len(), rules.rules.len());

    let master_key = master_key();
    let wrapped_key = match topic_wrapped_key(&pool, &args.topic).await {
        Ok(key) => key,
        Err(error) => {
            let msg = format!("Warning: Failed to fetch wrapped key for topic {}: {}", args.topic, error);
            eprintln!("{}", msg);
            if let Some(ipc) = &ipc { ipc.send("audit", None, &msg, None); }
            generate_wrapped_dek(&master_key)?
        }
    };

    let mut tasks = JoinSet::new();
    let mut processed = 0_usize;
    let shutdown = signal::ctrl_c();
    tokio::pin!(shutdown);
    
    let res = async {
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    eprintln!("Shutting down gracefully...");
                    break;
                }
                claimed = claim_aggregated_fragments(&pool, &args.topic, &args.required_sources, args.batch_size, args.retry_failed) => {
                    let fragments = claimed?;
                    if fragments.is_empty() { break; }
                    for fragment in fragments {
                        while tasks.len() >= args.workers {
                            if let Some(result) = tasks.join_next().await { result??; }
                        }
                        processed += 1;
                        if processed % args.batch_size == 0 {
                            let msg = format!("Progress: {} records processed", processed);
                            if let Some(ipc) = &ipc {
                                ipc.send("status", Some("processing"), &msg, Some(0));
                                ipc.send("audit", None, &msg, None);
                            }
                        }
                        tasks.spawn(process_aggregate(pool.clone(), rules.clone(), master_key.clone(), wrapped_key.clone(), fragment));
                    }
                }
            }
        }
        while let Some(result) = tasks.join_next().await { result??; }
        Ok(())
    }.await;

    match res {
        Ok(_) => {
            let message = format!("Transformation Batch Job finished successfully. {processed} records processed");
            eprintln!("{message}");
            if let Some(ipc) = &ipc {
                ipc.send("status", Some("finished"), &message, Some(100));
                ipc.send("audit", None, &message, None);
            }
            Ok(())
        }
        Err(e) => {
            let message = format!("Transformation Batch Job failed: {}", e);
            eprintln!("{}", message);
            if let Some(ipc) = &ipc {
                ipc.send("status", Some("failed"), &message, None);
                ipc.send("audit", None, &message, None);
            }
            let _ = sqlx::query("INSERT INTO system_logs (level, component, message) VALUES ('ERROR', 'transformation-engine', $1)")
                .bind(&message).execute(&*pool).await;
            Err(e)
        }
    }
}

fn parse_job_args() -> Result<JobArgs> {
    match env::args().nth(1) {
        Some(raw) => serde_json::from_str(&raw).context("failed to parse job arguments JSON"),
        None => Ok(JobArgs::default()),
    }
}

fn database_config() -> Result<DBConnectionConfig> {
    if let Ok(raw) = env::var("MITM_DB_CONFIG_JSON") {
        let config: DBConfigEnvelope = serde_json::from_str(&raw).context("failed to parse MITM_DB_CONFIG_JSON")?;
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
    for row in sqlx::query("SELECT id::text, topic FROM mapping_source").fetch_all(pool).await? {
        rules.sources.insert(row.try_get("id")?, row.try_get("topic")?);
    }
    for row in sqlx::query("SELECT id::text, field_name, data_type, encrypted FROM mapping_target_field").fetch_all(pool).await? {
        let id: String = row.try_get("id")?;
        rules.target_fields.insert(id.clone(), MappingTargetField { id, field_name: row.try_get("field_name")?, data_type: row.try_get("data_type")?, encrypted: row.try_get("encrypted")? });
    }
    for row in sqlx::query("SELECT source_id::text, target_field_id::text, source_field, transformation_chain, validation_chain FROM mapping_rule ORDER BY priority").fetch_all(pool).await? {
        rules.rules.push(MappingRule {
            source_id: row.try_get("source_id")?,
            target_field_id: row.try_get("target_field_id")?,
            source_field: row.try_get("source_field")?,
            transformations: parse_chain(row.try_get("transformation_chain")?)?,
            validations: parse_chain(row.try_get("validation_chain")?)?,
        });
    }
    Ok(rules)
}

fn parse_chain(value: Option<Value>) -> Result<Vec<RuleStep>> {
    match value { Some(value @ Value::Array(_)) => serde_json::from_value(value).context("invalid rule chain"), Some(Value::Null) | None => Ok(Vec::new()), Some(_) => Err(anyhow!("rule chain must be a JSON array")) }
}

async fn topic_wrapped_key(pool: &PgPool, topic: &str) -> Result<Vec<u8>> {
    sqlx::query_scalar("SELECT sk.wrapped_key FROM delivery_targets dt JOIN storage_keys sk ON dt.dek_id = sk.id WHERE LOWER(dt.topic) = LOWER($1) AND dt.is_active = true LIMIT 1")
        .bind(topic).fetch_one(pool).await.context("no active delivery key configured")
}

async fn claim_aggregated_fragments(pool: &PgPool, topic: &str, required_sources: &[String], limit: usize, retry_failed: bool) -> Result<Vec<AggregatedFragment>> {
    let status = if retry_failed { "failed_validation" } else { "pending" };
    let correlation_ids: Vec<String> = sqlx::query_scalar(
        "SELECT correlation_id::text FROM raw_ingestion WHERE topic = $1 AND status = $2 AND correlation_id IS NOT NULL GROUP BY correlation_id HAVING COUNT(DISTINCT source_system) FILTER (WHERE source_system = ANY($3::text[])) = cardinality($3::text[]) LIMIT $4")
        .bind(topic).bind(status).bind(required_sources.to_vec()).bind(limit as i64).fetch_all(pool).await?;
    if correlation_ids.is_empty() { return Ok(Vec::new()); }
    let rows = sqlx::query(
        "UPDATE raw_ingestion SET status = 'processing', processed_at = NOW() FROM storage_keys WHERE raw_ingestion.correlation_id::text = ANY($1::text[]) AND raw_ingestion.topic = $2 AND raw_ingestion.status = $3 AND raw_ingestion.dek_id = storage_keys.id RETURNING raw_ingestion.id::text, raw_ingestion.topic, raw_ingestion.correlation_id::text, raw_ingestion.payload, raw_ingestion.nonce, storage_keys.wrapped_key")
        .bind(correlation_ids.clone()).bind(topic).bind(status).fetch_all(pool).await?;
    let mut grouped: HashMap<String, AggregatedFragment> = HashMap::new();
    for row in rows {
        let correlation_id: String = row.try_get("correlation_id")?;
        let topic: String = row.try_get("topic")?;
        grouped.entry(correlation_id.clone()).or_insert_with(|| AggregatedFragment { correlation_id: correlation_id.clone(), topic, fragments: Vec::new() })
            .fragments.push(RawFragment { id: row.try_get("id")?, payload: row.try_get("payload")?, nonce: row.try_get("nonce")?, wrapped_key: row.try_get("wrapped_key")? });
    }
    Ok(grouped.into_values().collect())
}

async fn process_aggregate(pool: Arc<PgPool>, rules: Arc<RuleSet>, master_key: Vec<u8>, wrapped_key: Vec<u8>, aggregate: AggregatedFragment) -> Result<()> {
    let mut payloads = Vec::new();
    let mut errors = Vec::new();
    for fragment in &aggregate.fragments {
        let decrypted = match envelope_decrypt(&master_key, &fragment.wrapped_key, &fragment.nonce, &fragment.payload) {
            Ok(payload) => payload,
            Err(error) => {
                errors.push(TransformationError { failed_field: "payload".into(), rule_name: "decryption".into(), error_message: format!("Failed to decrypt envelope: {error}") });
                continue;
            }
        };
        match serde_json::from_slice::<Map<String, Value>>(&decrypted) {
            Ok(payload) => payloads.push(payload),
            Err(error) => errors.push(TransformationError { failed_field: "payload".into(), rule_name: "json_parse".into(), error_message: error.to_string() }),
        }
    }
    if errors.is_empty() {
        let source_id = rules.sources.iter().find(|(_, topic)| topic.eq_ignore_ascii_case(&aggregate.topic)).map(|(id, _)| id.as_str());
        match source_id {
            Some(source_id) => {
                let (data, pipeline_errors) = process_payload(&merge_payloads(payloads), source_id, &rules, &master_key, &wrapped_key);
                errors.extend(pipeline_errors.into_iter().map(convert_pipeline_error));
                write_target_and_complete(&pool, &aggregate, data, &errors).await?;
            }
            None => {
                errors.push(TransformationError { failed_field: "topic".into(), rule_name: "source_lookup".into(), error_message: "No mapping source found for topic".into() });
                write_target_and_complete(&pool, &aggregate, Map::new(), &errors).await?;
            }
        }
    } else {
        write_target_and_complete(&pool, &aggregate, Map::new(), &errors).await?;
    }
    Ok(())
}

fn convert_pipeline_error(error: PipelineError) -> TransformationError {
    TransformationError { failed_field: error.failed_field, rule_name: error.rule_name, error_message: error.error_message }
}

async fn write_target_and_complete(pool: &PgPool, aggregate: &AggregatedFragment, data: Map<String, Value>, errors: &[TransformationError]) -> Result<()> {
    let first_id = aggregate.fragments.first().ok_or_else(|| anyhow!("claimed aggregate contains no fragments"))?.id.clone();
    let mut transaction = pool.begin().await?;
    if errors.is_empty() {
        if !data.is_empty() {
            sqlx::query("INSERT INTO target_fragments (raw_ingestion_id, topic, payload_jsonb, delivery_status) VALUES ($1::uuid, $2, $3, 'PENDING')")
                .bind(&first_id).bind(&aggregate.topic).bind(Value::Object(data)).execute(&mut *transaction).await?;
        }
        sqlx::query("UPDATE raw_ingestion SET status = 'processed', processed_at = NOW() WHERE correlation_id = $1::uuid")
            .bind(&aggregate.correlation_id).execute(&mut *transaction).await?;
    } else {
        for error in errors {
            sqlx::query("INSERT INTO transformation_errors (raw_ingestion_id, failed_field, rule_name, error_message) VALUES ($1::uuid, $2, $3, $4)")
                .bind(&first_id).bind(&error.failed_field).bind(&error.rule_name).bind(&error.error_message).execute(&mut *transaction).await?;
        }
        sqlx::query("UPDATE raw_ingestion SET status = 'failed_validation', processed_at = NOW() WHERE correlation_id = $1::uuid")
            .bind(&aggregate.correlation_id).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    Ok(())
}

fn master_key() -> Vec<u8> {
    let input = env::var("MASTER_KEY").unwrap_or_default();
    let decoded = STANDARD.decode(&input).unwrap_or_else(|_| input.into_bytes());
    decoded.into_iter().chain(std::iter::repeat(0)).take(32).collect()
}

fn ipc_client(args: &JobArgs) -> Option<IpcClient> {
    Some(IpcClient { socket_path: env::var("SCHEDULER_SOCKET_PATH").ok()?, run_id: env::var("RUN_ID").ok()?.parse().ok()?, topic: args.topic.clone(), source_name: args.source_name.clone(), component: "mitm_transformation".to_string() })
}

fn fetch_scheduler_credentials() -> Result<(Option<String>, Option<String>)> {
    let socket_path = env::var("SCHEDULER_SOCKET_PATH").context("not running under scheduler")?;
    let run_id: i64 = env::var("RUN_ID").context("not running under scheduler")?.parse().context("invalid RUN_ID")?;
    let response = request_socket_json(&socket_path, &json!({"type": "get_credentials", "run_id": run_id}))?;
    Ok((response.get("db_config_json").and_then(Value::as_str).map(str::to_owned), response.get("master_key").and_then(Value::as_str).map(str::to_owned)))
}

fn request_socket_json(socket_path: &str, message: &Value) -> Result<Value> {
    let mut stream = UnixStream::connect(socket_path).with_context(|| format!("failed to connect to {socket_path}"))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
    writeln!(stream, "{}", serde_json::to_string(message)?).context("failed to send scheduler request")?;
    let mut response = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut response).context("failed to read scheduler response")?;
    serde_json::from_str(&response).context("scheduler returned invalid JSON")
}

fn send_socket_json(socket_path: &str, message: &Value) -> Result<()> {
    let mut stream = UnixStream::connect(socket_path).with_context(|| format!("failed to connect to {socket_path}"))?;
    writeln!(stream, "{}", serde_json::to_string(message)?).context("failed to send scheduler event")
}
