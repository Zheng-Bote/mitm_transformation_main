# MitM Transformation Layer (CLI Batch Job)

This directory contains the Rust implementation of the MitM Transformation Layer, built as a robust CLI application.

## Overview

The Transformation Layer acts as a **Stateful Aggregator**. It pulls data from the `raw_ingestion` table, waits until all required source fragments for a given `correlation_id` have arrived, and merges them into a single Golden Record. Each source system's payload is mapped with the rules of its own `mapping_source` (`mapping_source.name` = `raw_ingestion.source_system`); if several sources map the same target field, the rule with the lowest `mapping_rule.priority` and a non-empty value wins. It then encrypts sensitive target fields using AES-256 GCM with the topic's delivery key, and writes the results to the `target_fragments` table (topic spelled as in `delivery_targets`).

**Re-aggregation:** if a source delivers a new fragment for an already processed `correlation_id`, the job merges it with the latest processed fragment of every other source and writes a complete Golden Record again. This only works within the `raw_ingestion` retention of `mitm_maintenance_cleanup` (`raw_ingestion_orphan_days`). If validation fails, the payload is pushed to a Dead Letter Queue (DLQ) in the `transformation_errors` table.

This module is designed as a **Scheduled Batch Job** rather than a continuous daemon. It processes all aggregated groups concurrently via Tokio async workers and terminates successfully when the queue is empty.

## Building

To build the executable statically, run:
```bash
./build.sh   # cargo build --release --target x86_64-unknown-linux-musl, copies mitm-transformer to ../../app/bin/mitm-transformer-main
```

## Usage

The batch job expects database credentials via environment variables and optional job configuration as a JSON argument (`os.Args[1]`), exactly as passed by the `mitm_scheduler`.

```bash
# DB Config via IPC (Preferred) or JSON/Env (Fallback)
export MITM_DB_CONFIG_JSON='{"db":{"host":"192.168.0.31","port":6543,"user":"mitm_user","password":"...","database":"mitm"}}'

# Or via Direct Environment Variables (Fallback)
export MITM_DB_HOST="192.168.0.31"
export MITM_DB_PORT="6543"
export MITM_DB_USER="mitm_user"
export MITM_DB_PASSWORD="..."
export MITM_DB_NAME="mitm"
export MITM_DB_SSLMODE="true"

# 2. Optional Job Args JSON
ARGS_JSON='{"workers": 5, "batch_size": 500, "retry_failed": false, "topic": "Employee", "required_sources": ["ORA_EMPLOYEE", "PG_EMPLOYEE"], "source_name": "TRANSFORMATION"}'

./bin/mitm-transformer "$ARGS_JSON"
```

### Parameters
- `MITM_DB_CONFIG_JSON` (**Preferred**): A JSON string containing the database configuration nested under a `"db"` object.
- `MITM_DB_*` (**Fallback**): Database connection parameters provided directly via Environment variables (`MITM_DB_HOST`, `MITM_DB_PORT`, etc.). Used if JSON is missing.
- `MITM_DB_SSLMODE` (**Optional**): Set to `"true"` to enforce SSL connections (`sslmode=require`) to the MitM database.
- `os.Args[1]` (**Optional**): Job configuration JSON object. Supported properties:
  - `workers`: Number of concurrent Tokio tasks (default: 5).
  - `batch_size`: Number of aggregated records to claim atomically per cycle (default: 500).
  - `retry_failed`: Re-process records currently in `failed_validation` state instead of processing `pending` records.
  - `topic`: The topic to process (e.g. `employee.data`).
  - `required_sources`: Optional override of `topic_dependencies.required_sources`: source system names that are mandatory before aggregation can occur. If neither is set, the job aborts.
  - `source_name`: The component identity for audit logging (default: `"TRANSFORMATION"`).

### Requirements & Exit Codes

- An active `delivery_targets` row (with key) must exist for the topic; otherwise the job exits before claiming anything (it never encrypts with an unpersisted key).
- `MASTER_KEY` (via IPC or ENV) must decode to exactly 32 bytes. Credentials from IPC are kept in memory only.
- One instance per topic (PostgreSQL advisory lock); a second instance exits with code `0`.
- SIGTERM/SIGINT: claiming stops, running aggregates finish, unprocessed fragments return from `processing` to their previous status. Orphaned `processing` rows are reset to `pending` on start.

| Exit Code | Meaning |
|---|---|
| `0` | Finished (status `finished`), stopped by signal (`cancelled`) or another instance is running. |
| `1` | Fatal error (configuration, `MASTER_KEY`, missing delivery key or required sources, DB failure). |
| `2` | Finished, but records failed validation or could not be processed (status `failed`). |

### Target Data Types

`text`/`string`/`varchar`, `int`, `float`/`numeric`, `bool` (`true/false/1/0`, any case), `date` and `jsonb` (objects/arrays unchanged, JSON strings parsed). Target fields with `is_required = true` must not be empty.

The schema cast always runs after the transformation and validation chains. For `date`, the chain has precedence: if the rule's transformation chain contains `parse_date`, its output format is kept (e.g. `02.01.2006` for Cority `dateFormat: dd.mm.yyyy`) and the cast only checks that the value is a valid date. Without `parse_date` in the chain, the value is normalized to ISO `YYYY-MM-DD`.

## Dead Letter Queue (DLQ) & Reprocessing

If a transformation or validation fails:
1. An entry is created in `transformation_errors` (error messages never contain field values).
2. The `status` of the `raw_ingestion` record is changed to `failed_validation`.

After correcting the root cause (e.g., updating a regex rule in `mapping_rule`), you can reprocess the failed records by setting `"retry_failed": true` in the job configuration JSON:

```bash
RETRY_ARGS='{"workers": 5, "batch_size": 500, "retry_failed": true}'
./bin/mitm-transformer "$RETRY_ARGS"
```
