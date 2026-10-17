# Implementation Plan: Issue #7

## 1. Spec Chain & Authority

`mitm-2/mitm-2.sdd` → `transformation-layer/transformation-layer.sdd` (Owns `./**`) → this Spec Kit feature.
Schema: `mitm-2/migrations/setup.sql` (read-only). No `.sdd` change.

## 2. Architecture Check

| Constraint | Result |
|---|---|
| Envelope Encryption / PII | Stronger: no unpersisted DEK, KEK length check, no values in error messages. |
| KEK never persisted, IPC credentials | No `env::set_var`; credentials passed as values. |
| IPC nil-safe status/audit | `Option<IpcClient>`, async persistent connection. |
| Progress & cursors in PostgreSQL | Statuses in `raw_ingestion`; advisory lock is session-scoped. |
| Logging separation | `system_logs` start/finish/failure; audit counts via IPC. |

## 3. Design

### A. `src/lib.rs` (pure, unit-tested)
- `MappingSource { id, name, topic }`; `RuleSet.sources: HashMap<id, MappingSource>`.
- `MappingRule.priority: i32`, `MappingTargetField.is_required: bool`.
- `process_sources(sources: &BTreeMap<String, Map>, topic, rules, kek, wrapped_key) -> (Map, Vec<PipelineError>)`:
  1. For each source system: resolve its `mapping_source` (name + topic, case-insensitive); missing → `source_lookup` error.
  2. Run each rule of that source (transform → validate → cast) and collect a candidate `(priority, source, value)` per target field.
  3. Per target field, pick the first non-empty candidate by `(priority, source)`, else the first one; check `is_required`; encrypt if `encrypted`.
- `cast_value`: `date` → ISO via `autodetect_date`; `jsonb`; `bool` accepts `1/0` and any case.
- `validate_length`: `chars().count()`; `range_check`: numeric strings.
- `autodetect_date`: Go format `… -0700 MST`; error without value.
- Regex cache (`LazyLock<Mutex<HashMap<String, Regex>>>`), static e-mail regex.
- `PipelineError.error_message` uses `{:#}` (cause chain).
- `process_payload` is replaced by `process_sources` (only caller is `main.rs`).

### B. `src/main.rs`
- Credentials: `fetch_scheduler_credentials` → variables; `database_config(Option<String>)`, `master_key(&str) -> Result<Vec<u8>>` (32 bytes).
- Lock connection: `pool.acquire()` + `pg_try_advisory_lock(hashtextextended(...))`.
- `resolve_required_sources`: JobArgs `Option<Vec<String>>` → `topic_dependencies` (case-insensitive) → error; deduplicated.
- `delivery_key(topic) -> (canonical_topic, wrapped_key)`; error → exit 1 (R1, R13).
- Startup reset: `processing` → `pending` for the topic.
- Claim (single statement, CTE): candidates = `correlation_id`s with a fragment in source status, where the required sources are covered by fragments in source status **or** `processed`; UPDATE of the source-status fragments → `processing`, RETURNING `id, correlation_id, source_system, created_at, payload, nonce, wrapped_key`.
- Per aggregate: load the latest `processed` fragment of every other source system (`DISTINCT ON (source_system)`), decrypt all fragments, group by source (sorted by `created_at`), `merge_payloads` per source, then `process_sources`.
- `write_target_and_complete`: status update `WHERE id = ANY($1::uuid[])` with the claimed IDs; `target_fragments.topic` = canonical topic.
- Shutdown: `watch` channel set by SIGTERM/SIGINT; the loop stops claiming and spawning; in-flight tasks finish; end reset `processing` → source status.
- Results: `Outcome::{Processed, FailedValidation}`; DB errors are counted, the job continues. Exit code 0 / 2; fatal errors → 1.
- IPC: `tokio::net::UnixStream` behind `Arc<tokio::sync::Mutex<Option<…>>>`, `send().await`, reconnect once.

### C. Cargo
- `tokio` features `net`, `io-util`; version 1.1.0; fix homepage/repository URLs.

## 4. Edge Cases
- A counterpart deleted by the cleanup between claim and load → required source missing → `failed_validation` (`required_source` error, no value).
- Fragments of a non-required source without a mapping fail the record (configuration error, loud).
- Duplicate entries in `required_sources` are deduplicated (`cardinality` must match).
- `retry_failed`: source status `failed_validation`; the end reset returns to that status.
