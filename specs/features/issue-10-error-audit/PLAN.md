# Implementation Plan: Issue #10

## 1. Spec Chain & Authority

`mitm-2/mitm-2.sdd` → `transformation-layer/transformation-layer.sdd` (Owns `./**`) → this Spec Kit feature.
Related: mitm-2 feature specs `job-failure-logging`, `collector-failure-logging`, `maintenance-monitor`. No `.sdd` change.

## 2. Architecture Check

| Constraint | Result |
|---|---|
| Logging separation | Processing results stay in `job_audit_logs` (IPC audit); `system_logs` unchanged. |
| IPC nil-safe | Unchanged `Option<IpcClient>`; for unparsable arguments the client is built from `JobArgs::default()`. |
| PII | Messages unchanged apart from the prefix; error chains already hide values (#7). |

## 3. Design

- `audit_message(status, message)`: `ERROR: ` prefix for status `failed`.
- `run()`: summary/failure audits use `audit_message`; `parse_job_args()` errors are reported before returning.
