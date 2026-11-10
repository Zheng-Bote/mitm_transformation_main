# Feature: ERROR: Audit Events for Failed Runs (Issue #10)

**Source:** [Zheng-Bote/mitm_transformation_main#10](https://github.com/Zheng-Bote/mitm_transformation_main/issues/10)
**Status:** Done (pending review)
**Base Version:** v1.1.0 → **Target Version:** v1.1.1

## 1. Feature Intent

Failed transformation runs follow the error-reporting convention of the collectors and the scheduler: every failure is visible as `ERROR: <message>` in `job_audit_logs`, so `mitm_maintenance_monitor` can put the reason into its failure mails.

## 2. Requirements

| ID | Requirement (EARS) |
|---|---|
| R1 | When a run finishes with failed validations or errors, the engine shall send its summary as audit event prefixed with `ERROR: ` and exit with code `2`. |
| R2 | When a run fails with an error, the engine shall send `ERROR: Transformation Batch Job failed: <error>` as audit event in addition to the `failed` status event and exit with code `1`. |
| R3 | If the job arguments cannot be parsed, then the engine shall send a `failed` status event and an `ERROR:` audit event (IPC client with default arguments) and exit with code `1`. |
| R4 | The engine shall keep successful and cancelled runs unchanged (no prefix, exit code `0`). |
| R5 | The engine shall not include decrypted payloads or secrets in these messages. |

## 3. Scope

- **Component:** `transformation-layer/mitm_transformation_main` (`src/main.rs`, `Cargo.toml`, `README.md`, `CHANGELOG.md`).
- **Schema:** no change.

## 4. Acceptance Criteria

| ID | Criterion |
|---|---|
| AC1 | `audit_message("failed", …)` returns the `ERROR: ` prefix; `finished` / `cancelled` are unchanged (unit test). |
| AC2 | Unparsable arguments: `failed` status + `ERROR:` audit over IPC, exit code `1` (fake scheduler socket). |
| AC3 | Job error (DB unreachable): `failed` status + `ERROR: Transformation Batch Job failed: …` audit, exit code `1` (fake scheduler socket). |
| AC4 | `cargo test` passes. |
