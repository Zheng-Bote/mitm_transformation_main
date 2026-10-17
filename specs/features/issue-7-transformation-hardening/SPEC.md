# Feature: Harden Transformation Job (Issue #7)

**Source:** [Zheng-Bote/mitm_transformation_main#7](https://github.com/Zheng-Bote/mitm_transformation_main/issues/7)
**Status:** Done (pending review)
**Base Version:** v1.0.0 → **Target Version:** v1.1.0

## 1. Feature Intent

Make the transformation job safe and deterministic:

- it only changes the status of fragments it actually claimed;
- it never encrypts with a key that is not persisted;
- it builds Golden Records deterministically from all sources of a topic;
- it re-aggregates single-source updates;
- it survives SIGTERM without leaving rows in `processing`;
- it reports truthful results.

## 2. Operator Decisions

| ID | Decision |
|---|---|
| D1 | **Re-aggregation:** a new fragment of an already processed `correlation_id` is merged with the latest `processed` fragment of every other source system of that `correlation_id`, so a complete Golden Record is produced (Cority upsert overwrites). |
| D2/D3 | **Mapping per source:** each source system's payload is mapped with the rules of its own `mapping_source` (`name = source_system`, same topic). If several sources map the same target field, the lowest `mapping_rule.priority` with a non-empty value wins (tie: source name). |
| D4 | **`date`** target type: if the rule's transformation chain contains `parse_date`, the chain's format is kept and the cast only validates the date; otherwise it is normalized to ISO `YYYY-MM-DD` (auto-detect of the known formats). **`jsonb`**: objects/arrays unchanged, JSON strings parsed. *(Refined by the Operator: the transformation/validation chains keep precedence.)* |
| D5 | **Required sources:** from `topic_dependencies`; `required_sources` in the JobArgs overrides; if neither is set, the job aborts. |

## 3. Scope

- **Component:** `transformation-layer/mitm_transformation_main` (`src/main.rs`, `src/lib.rs`, `Cargo.toml`, docs).
- **Schema:** `mitm-2/migrations/setup.sql`, **no change**. It uses `raw_ingestion.source_system/created_at`, `mapping_source.name`, `mapping_rule.priority`, `mapping_target_field.is_required`, `topic_dependencies` and `delivery_targets.topic`.
- **Cross-layer contract (unchanged format):** encrypted target values stay `{"ciphertext","nonce"}` (Delivery #4 decrypts envelope objects); `target_fragments.topic` becomes the canonical `delivery_targets.topic`.

### Non-Goals

- Progress percentage (existing issue #2).
- Renaming the statuses `processed` / `failed_validation` (used by `mitm_maintenance_cleanup`); the outdated schema comment is reported only.
- No `.sdd` change: the aggregation contract in `transformation-layer.sdd` stays valid.

### Constraints

- Re-aggregation (D1) can only use fragments within the `raw_ingestion` retention of `mitm_maintenance_cleanup` (`raw_ingestion_orphan_days`, default 14).

## 4. Requirements (EARS)

| ID | Requirement |
|---|---|
| R1 | **If** no active delivery key exists for the topic, **then** the system **shall** exit with code 1 before claiming any fragment. |
| R2 | **When** an aggregate is completed, the system **shall** update exactly the claimed `raw_ingestion` IDs. |
| R3 | The system **shall** map each source system's payload with its own `mapping_source` rules. **If** a fragment's source system has no `mapping_source` for the topic, **then** the record **shall** fail (`source_lookup`). |
| R4 | Fragments of one source **shall** be merged oldest → newest (`created_at`). Target fields **shall** be resolved by `priority` (lowest first, first non-empty value, tie by source name). |
| R5 | **When** SIGTERM or SIGINT is received, the system **shall** stop claiming and spawning, finish in-flight aggregates, and reset unprocessed `processing` rows of the topic to the run's source status. **When** the job starts, it **shall** reset orphaned `processing` rows of its topic to `pending`. **If** an aggregate fails with a DB error, **then** the job **shall** continue and count it as an error. |
| R6 | **When** pending fragments exist for a `correlation_id` whose required sources are complete counting `processed` fragments, the system **shall** claim the pending fragments and re-aggregate them with the latest `processed` fragment of every other source system (D1). |
| R7 | The system **shall** support target types `date` and `jsonb` (D4). **Where** the transformation chain contains `parse_date`, the `date` cast **shall** only validate and keep the chain's format; otherwise it **shall** normalize to ISO `YYYY-MM-DD`. |
| R8 | Stored error messages **shall** contain the full cause chain and **shall not** contain field values. |
| R9 | **If** the decoded `MASTER_KEY` is not exactly 32 bytes, **then** the system **shall** exit with code 1. |
| R10 | IPC credentials **shall** be kept in variables; `env::set_var` **shall not** be used. |
| R11 | The system **shall** hold `pg_try_advisory_lock(hashtextextended('transformation_' \|\| topic, 0))` for the run (exit 0 if it is held elsewhere) and **shall** claim in one statement. |
| R12 | **When** the job finishes, the system **shall** report processed / failed-validation / error counts via IPC and `system_logs`, and **shall** exit with code 2 if any record failed validation or with an error. |
| R13 | `target_fragments.topic` **shall** be the canonical `delivery_targets.topic`. |
| R14 | Status and audit events **shall** use one persistent Tokio Unix-socket connection per run (reconnect once). |
| R15 | `min_length` / `max_length` **shall** count characters; `range_check` **shall** accept numeric strings; `parse_date` **shall** also accept `2006-01-02 15:04:05.999999999 -0700 MST`; `cast bool` **shall** accept `true/false/1/0` case-insensitively; regexes **shall** be compiled once per pattern; `is_required` target fields **shall** be enforced. |
| R16 | **Required sources** **shall** come from `topic_dependencies` unless overridden by JobArgs (D5). |
| R17 | README, docs, Cargo metadata and SPDX headers **shall** match the code. |

## 5. Acceptance Criteria

- [x] AC1 (R1): Without an active delivery key, the job exits 1 before claiming (review).
- [x] AC2 (R2): Only claimed IDs change status (review: `WHERE id = ANY`).
- [x] AC3 (R3, R4): Unit tests: per-source mapping, priority resolution, deterministic merge, missing mapping source.
- [x] AC4 (R5): SIGTERM handled; start/end reset of `processing` (review).
- [x] AC5 (R6): Re-aggregation query includes `processed` counterparts (review); merge with counterpart (unit test).
- [x] AC6 (R7): Unit tests for `date` and `jsonb`.
- [x] AC7 (R8): Unit test: error messages contain the cause but not the value.
- [x] AC8 (R9, R10): Key length check; no `set_var` (grep).
- [x] AC9 (R11, R12, R13, R14): Advisory lock, counts, exit codes, canonical topic, persistent IPC (review + IPC unit test).
- [x] AC10 (R15): Unit tests for each validation/transformation fix.
- [x] AC11 (R16, R17): `topic_dependencies` lookup; CHANGELOG.md and README.md are up to date.

## 6. SpecDD Architecture Alignment

- **Architecture:** Transformation only; layering unchanged.
- **Security:** No unpersisted DEK; KEK length enforced; KEK not in the environment; no field values in `transformation_errors`.
- **Data Model:** No DDL.
- **IPC / Scheduler:** Same event format; new exit code 2; SIGTERM handled (scheduler stop path).
