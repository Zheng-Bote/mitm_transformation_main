# Work Breakdown: Issue #10

All tasks: **Component** `transformation-layer/mitm_transformation_main`.

- [x] **T1 – Prefix** (`src/main.rs`): `audit_message`, used for the failure audit in `run()` and the summary audit in `run_with_pool()`. (R1, R2, R4)
- [x] **T2 – Arguments** (`src/main.rs`): report `parse_job_args()` errors via IPC with default arguments. (R3)
- [x] **T3 – Tests**: unit test for `audit_message`; fake scheduler socket for unparsable arguments and DB failure. (AC1–AC4)
- [x] **T4 – Docs & metadata**: README (exit codes), CHANGELOG, `Cargo.toml` 1.1.1.

## Converge Report

| Requirement | Evidence |
|---|---|
| R1 | `failed_results_are_error_audits` |
| R2 | Fake scheduler: `ERROR: Transformation Batch Job failed: unable to connect to database …`, exit 1 |
| R3 | Fake scheduler: `failed` status + `ERROR: … failed to parse job arguments JSON …`, exit 1 |
| R4 | `failed_results_are_error_audits` (`finished`, `cancelled`) |
| R5 | Messages unchanged apart from the prefix |
