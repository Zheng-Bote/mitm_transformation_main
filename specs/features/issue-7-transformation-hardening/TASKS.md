# Work Breakdown: Issue #7

All tasks: **Component** `transformation-layer/mitm_transformation_main`. Contracts first.

- [x] **T1 – Library contracts** (`src/lib.rs`): `MappingSource`, `priority`, `is_required`, `process_sources`. (R3, R4)
- [x] **T2 – Library rules** (`src/lib.rs`): `date`/`jsonb`/`bool` cast, char length, numeric-string range, MST date format, regex cache, `is_required`, error chain without values. (R7, R8, R15)
- [x] **T3 – Library tests** (`src/lib.rs` `mod tests`). (AC3, AC5–AC7, AC10)
- [x] **T4 – Main: credentials & key** (`src/main.rs`): no `set_var`, `master_key` length, `delivery_key` with canonical topic, fatal on missing key. (R1, R9, R10, R13)
- [x] **T5 – Main: lock, sources, claim** (`src/main.rs`): advisory lock, `topic_dependencies`, single-statement claim with re-aggregation, counterpart loading, claimed-ID status update. (R2, R6, R11, R16)
- [x] **T6 – Main: shutdown, results, IPC** (`src/main.rs`, `Cargo.toml`): SIGTERM, start/end reset, outcome counts, exit codes, persistent async IPC. (R5, R12, R14)
- [x] **T7 – Docs & metadata** (`README.md`, `docs/`, `CHANGELOG.md`, `Cargo.toml`, SPDX headers). (R17)
- [x] **T8 – Converge**: verify R/AC; `cargo test`, `cargo clippy`.

## Converge Report

- R1–R17 are implemented. `cargo test` (21 tests) passes; `cargo clippy` shows only 3 warnings in code this feature did not touch (`range_check` nested ifs, `processed % batch_size`).
- **Verified by unit tests:** R3/R4 (per-source mapping, priority, fallback, determinism, unknown source), R7 (`date`, `jsonb`), R8 (no values in pipeline and JSON errors), R9 (key length), R14 (one IPC connection), R15 (bool, char length, numeric range, MST date, regex cache, `is_required`), R16 (normalization, override parsing).
- **Verified by review only (DB-dependent, no local PostgreSQL server):** R1 `delivery_key`, R2 `WHERE id = ANY`, R5 resets and SIGTERM, R6 claim CTE and `load_counterparts`, R11 advisory lock, R12 counts and exit codes, R13 canonical topic, R16 `topic_dependencies` query.
- **Converge findings (fixed):** (1) the SPDX header `/** … */` is a Rust *doc* comment (clippy: empty line after doc comment), so it is now `/* … */`; (2) JSON parse errors of payloads quoted the value (PII; this existed before), so they now report only category and position (`json_error_message`).
- **Correction to issue finding 8:** pipeline errors did not leak values in v1.0.0, because only the outer context ("step X failed") was stored. The messages were uninformative instead. They now carry the full cause chain without values.
- **Operator refinement (D4):** for `date`, a `parse_date` step in the transformation chain keeps its output format (e.g. `dd.mm.yyyy` for Cority) and the cast only validates; without `parse_date`, the value is normalized to ISO. Test: `date_schema_keeps_chain_format_and_normalizes_otherwise`.
- **Breaking:** `required_sources` has no `ORA_EMPLOYEE` default any more (D5).
