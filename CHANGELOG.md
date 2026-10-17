# Changelog

All notable changes to the Transformation Layer will be documented in this file.

## [v1.1.0] - 2026-10-17

Hardening ([#7](https://github.com/Zheng-Bote/mitm_transformation_main/issues/7)).

### Added

- **Mapping per source:** each source system is mapped with its own `mapping_source` rules; `mapping_rule.priority` resolves fields mapped by several sources (lowest first, first non-empty value).
- **Re-aggregation:** single-source updates of an already processed `correlation_id` are merged with the latest processed fragments of the other sources.
- **`topic_dependencies`** is read for the required sources (JobArgs `required_sources` overrides).
- Target types `date` (ISO `YYYY-MM-DD`) and `jsonb`; `is_required` is enforced.
- Per-topic advisory lock; SIGTERM handling; start/end reset of `processing` rows.
- Final counts (processed / failed validation / errors) via IPC and `system_logs`; exit code `2` if records failed.

### Changed

- **Breaking:** `required_sources` no longer defaults to `["ORA_EMPLOYEE"]`; it comes from `topic_dependencies` or the job arguments.
- `target_fragments.topic` uses the canonical `delivery_targets.topic` spelling.
- IPC events use one persistent async socket connection.
- `bool` cast accepts `1/0` and any case; `min_length`/`max_length` count characters; `range_check` accepts numeric strings; `parse_date` accepts Go `… -0700 MST`; regexes are compiled once.
- Error messages contain the full cause chain.
- Cargo metadata (repository URLs) and SPDX headers.

### Fixed

- **Data loss:** status updates only touch the claimed fragments (previously all fragments of the `correlation_id`, including newly arrived ones and other topics).
- **Security:** no fallback to an unpersisted DEK when the delivery key is missing (fields became undecryptable); `MASTER_KEY` must be exactly 32 bytes; no `env::set_var` for secrets.
- **Security:** field values are no longer included in `transformation_errors` (date format and JSON parse errors).
- Nondeterministic Golden Records (random mapping source, arbitrary merge order).
- Rows stuck in `processing` after SIGTERM or task errors.

## [v0.20.0] - 2026-09-16

### Changed
- **Architecture:** Completely rewritten in Rust using `tokio` async workers. `go build` replaced with `cargo build`.

## [v0.19.0] - 2026-09-09

### Changed
- **Performance:** Dynamic Database Connection Pool Scaling (Issue #4). `MaxConns` is now dynamically calculated based on the configured worker count (`jobArgs.Workers + 5`) instead of being statically limited to 20, preventing bottlenecks in high-throughput topics.

## [v0.18.4] - 2026-09-01

### Fixed

- **IPC SSLMode DSN Fix**: Fixed an issue where the constructed database connection string (DSN) would incorrectly overwrite `MITM_DB_SSLMODE=require` with `disable`, which caused `FATAL: no encryption` errors in AWS RDS.

## [v0.18.3] - 2026-09-01

### Fixed

- **IPC SSLMode Type Fix**: Changed `SSLMode` field in JSON parsing struct from `string` to `bool` to correctly unmarshal boolean values (`true`/`false`) sent by the scheduler.

## [v0.18.2] - 2026-09-01

### Fixed

- **IPC SSLMode Fix**: Fixed an issue where `SSLMode` was not correctly parsed from the scheduler's JSON configuration and improved the `MITM_DB_SSLMODE` fallback logic to support proper PostgreSQL sslmode strings (e.g., `require`, `verify-full`).

## [v0.18.1] - 2026-08-31

### Changed

- **Job Audit Logging**: Reduced the granularity of Job Audit logging for progress reports. Progress is now reported exactly every `batch_size` interval (default: 500) and once for the remaining smaller batch at the end of the job, significantly reducing the volume of log messages.

## [v0.18.0] - 2026-08-31

### Added

- **IPC Socket as Credential Broker**: The transformation engine now fetches database credentials and the master key at runtime from the Scheduler via a Unix Domain Socket request (`get_credentials` with `RUN_ID` and `SCHEDULER_SOCKET_PATH`), instead of holding them locally.

## [v0.17.0] - 2026-08-29

### Added

- **Admin Interface: Transformation Rules Management**
  - **`/admin/transformation`**: Added a comprehensive page to view, add, edit, and delete transformation rules.
  - **Rule Preview**: Real-time preview of transformations using sample data directly from the UI.
  - **Validation Feedback**: Immediate visual feedback on rule validity (e.g., valid regex, type compatibility).
- **Admin Interface: Transformation Errors**
  - **`/admin/transformation/errors_bin`**: Fixed the 500 error and added a "Resolve" button to retry failed records.
- **API: Transformation Rules Management**
  - **`POST /admin/transformation/rule`**: Create new transformation rule.
  - **`PUT /admin/transformation/rule/{id}`**: Update existing rule.
  - **`DELETE /admin/transformation/rule/{id}`**: Delete rule.
  - **`GET /admin/transformation/rule/{id}/preview`**: Get preview of a transformation rule.

### Changed

- **Performance Optimization**: Removed the expensive `COUNT(*)` query on `raw_ingestion` during job initialization. The job now starts immediately instead of performing a full table scan.
- **Performance Optimization**: Fixed an N+1 query issue in `ClaimAggregatedFragments`. The `wrapped_key` is now fetched directly via a `JOIN` on `storage_keys` within the `UPDATE ... RETURNING` batch statement, saving thousands of sequential round trips.
- **Performance Optimization**: Pre-parsed `TransformationChain` and `ValidationChain` JSON structures directly into the cached `MappingRule` struct. This eliminates thousands of redundant `json.Unmarshal` calls for every payload field during processing, significantly reducing CPU usage and GC pressure.

## [v0.16.0] - 2026-08-26

### Fixed

- **Logging**: Added `ipc.SendAudit` to the Transformation Layer to send detailed completion statistics to the Scheduler.

## [v0.15.0] - 2026-08-23

### Added

- **Detailed Reporting**: Introduced atomic metrics (`sync/atomic`) across the worker pool to accurately track processed, transformed, validated, and failed records per batch job.
- **Audit Logging**: Enhanced IPC reporting to the Scheduler. Batch jobs now send detailed completion statistics (e.g., `<Topic>: <SourceName>: 10 records transformed`) directly to the Job Audit Log.
- **Documentation**: Added examples for `regex_replace`, `string_split`, and `cast_type` in `transformation-layer.md`.
- **Auto-Casting**: Implemented automatic type casting in `PipelineEngine.ProcessPayload` based on `mapping_target_field.data_type`. Implicit cast failures generate a `PipelineError` for graceful DLQ handling.

### Fixed

- **Data Parsing**: Replaced `json.Unmarshal` with `json.Decoder.UseNumber()` to prevent large integer values (like IDs) from being implicitly cast to `float64` and converted to scientific notation during processing in the Transformation and Delivery layers.

## [v0.14.0] - 2026-07-29

### Added

- **Delivery Layer**: Implemented configurable `slowdown` and `timeout` parameters for the `CORITY_SAAS` delivery adapter.

### Changed

- **Database**: Synced PostgreSQL database schema IST-Zustand across all layer `.sql` migrations (`setup.sql`, `transformation-layer`, `delivery-layer`, `scheduler`).
- **Components Logging**: Refactored component version logging mechanism across all layers (Collectors, Transformation, Delivery, Scheduler) to consistently output a clean `Major.Minor.Patch` version format.

### Fixed

- **Scheduler**: Resolved an HTTP 500 error on the `/admin/transformation/errors_bin` API endpoint by updating the query to correctly reference the `raw_ingestion_id` column and gracefully handle null values.

## [v0.13.0] - 2026-07-24

### Changed

- **Version Bump**: Updated engine version to `v0.13.0` to align with the overarching MitM Aggregator system updates, documentation enhancements, and database configuration fixes.

## [v0.12.0] - 2026-07-19

### Changed

- **Logging for Transformation errors**: Added total counts of validation errors per topic and correlation ID to the audit log message.

## [v0.11.0] - 2026-07-15

### Added

- **IPC Logging Enhancements**: Added `Topic` and `SourceName` fields to `IPCClient` to consistently prefix all IPC messages with `<Topic>: <SourceName>: `.
- **Job Arguments**: Added `source_name` to `JobArgs` to explicitly pass the identity from the Scheduler to the Transformer (defaults to `"TRANSFORMATION"`).
- **Audit Logging for Validation**: Validation errors are now broadcasted directly to the Scheduler console using the IPC audit log (`ipc.SendAudit`).
- **Date Formatting Resilience**: Extended the fallback list of supported date string formats in `parse_date` to include `time.RFC3339` and `time.RFC3339Nano` to securely process standard dates mapped from serialized payloads of collector databases.

### Fixed

- **Validation Errors (DLQ)**: Fixed a bug where validation errors were skipped and only printed to the console. They are now correctly inserted into the `transformation_errors` table (DLQ).

## [v0.10.1] - 2026-07-08

### Fixed

- **Target Insertion Error**: Fixed an issue in `target_repo.go` where the `INSERT INTO target_fragments` statement incorrectly referenced the outdated `raw_ingestion_id` column instead of the correct `correlation_id` column.

## [v0.10.0] - 2026-07-07

### Added

- **SSL Support**: Added support for the `MITM_DB_SSLMODE` environment variable. The transformation engine now respects this setting and applies it to the MitM PostgreSQL connection string.
- **Date Conversion**: Enhanced the `parse_date` transformation function to support automatic date format detection (e.g., `dd.mm.yyyy`, `mm/dd/yyyy`). If `input_format` is not provided, it falls back to a predefined list of common formats and uses `yyyy-mm-dd` (`2006-01-02`) as the default `output_format`.

## [v0.9.0] - 2026-06-30

### Changed

- **Config Restructuring**: Updated database configuration parsing to fully support the nested `"db"` object format provided via the `MITM_DB_CONFIG_JSON` environment variable.
- **Database Connection**: The transformer now prioritizes the JSON configuration (`MITM_DB_CONFIG_JSON`) and only falls back to direct environment variables (`MITM_DB_HOST`, etc.) if the JSON is missing.
- **Audit Logging**: Added comprehensive IPC audit logging (`ipc.SendAudit`) during initialization to explicitly log whether the database configuration was sourced from JSON or direct environment variables.

## [v0.8.0] - 2026-06-24

### Added

- **True Envelope Encryption for Targets**: Replaced the static mock target key (`0123456789abcdef0123456789abcdef`) with a fully realized Envelope Encryption mechanism. The Transformer now securely queries `storage_keys` via `delivery_targets` to fetch the specific `wrapped_key` (DEK) for the current target topic, wrapping the encrypted JSON objects dynamically using the `MASTER_KEY` (KEK).
- **Expanded Crypto Primitives**: Implemented `EnvelopeEncrypt` and `GenerateWrappedDEK` within `internal/crypto/aes.go`. These new functions securely bundle `ciphertext` and `nonce` into JSON structures matching the expected format of the Delivery Layer.

### Fixed

- **Testing Architecture**: Fixed unit and end-to-end testing payloads (`engine_test.go`) to generate cryptographically valid mock DEKs on-the-fly, ensuring `ProcessPayload` successfully evaluates the new Envelope Encryption pipeline without failing authentication checks.

## [v0.7.0] - 2026-06-21

- **Stateful Aggregation**: Transformed the mapping layer from a 1:1 forwarder into an N:1 stateful aggregator.
- **Topic Dependencies**: The Transformer now utilizes the `topic_dependencies` table to verify if all required source fragments for a given `correlation_id` are present in `raw_ingestion` before generating a "Golden Record".

### Changed

- **Correlation ID Engine**: Replaced legacy processing logic based on `raw_ingestion_id` with a `GROUP BY correlation_id` architecture.
- **Transformation Errors**: Updated the `transformation_errors` table to reference `correlation_id` instead of `raw_ingestion_id` for accurate Dead Letter Queue tracking.

## [v0.6.0] - 2026-06-15

### Added

- **Validators**: Added `min_length` and `max_length` validators to the transformation engine's validation library.
- **Centralized App Info & IPC**: Added `appName` and `version` globally. The component now broadcasts its name and version via IPC when starting.

## [v0.5.0] - 2026-06-10

### Added

- **Target Fragments Table**: Replaced dynamic target tables with a unified `target_fragments` table to store transformed payload JSON.

### Fixed

- **Case Sensitivity**: Fixed an issue where payload JSON key mapping failed because source systems provided differently cased fields than expected by the mapping rules. Mapping lookups are now case-insensitive.

## [v0.4.0] - 2026-06-09

### Fixed

- **Envelope Encryption**: Fixed a critical bug where `raw_ingestion.payload` was not decrypted prior to `json.Unmarshal`. The Transformer worker now retrieves the `wrapped_key` via a SQL JOIN and uses `EnvelopeDecrypt` to securely parse incoming fragments.

## [v0.2.0] - 2026-06-06

### Changed

- Refactored database initialization to read from `MITM_DB_*` environment variables instead of `os.Args[1]` to match the updated scheduler convention.
- Moved Job Arguments JSON from `os.Args[2]` to `os.Args[1]`.

## [v0.1.0] - 2026-06-06

### Added

- **Repositories**: `MappingRepo` for rule caching, `IngestionRepo` for fetching/claiming raw records via `FOR UPDATE SKIP LOCKED`, and `TargetRepo` for transacting target writes and tracking errors.
- **Pipeline Engine**: Core `PipelineEngine` to orchestrate mapping, transformation, validation, and encryption rules per source configuration.
- **Transformation Library**: Added functions `trim_whitespace`, `to_upper`, `to_lower`, `default_value`, `regex_replace`, `parse_date`, `string_split`, `cast_type`.
- **Validation Library**: Added functions `not_null`, `regex_match`, `range_check`, `email`, `in_list`.
- **Envelope Encryption**: `crypto/aes.go` for AES-256-GCM encryption of sensitive target fields.
- **CLI Batch-Job**: Go-based entrypoint `cmd/transformer/main.go` that acts as a worker-pool to consume the queue and exits gracefully.
- **DLQ Reprocessing**: Added `--retry-failed` flag to the CLI tool for re-processing failed validations.
- **E2E Tests**: Comprehensive end-to-end testing against the PostgreSQL instance confirming table inserts and DLQ handling.
