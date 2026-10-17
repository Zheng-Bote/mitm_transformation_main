/*
 * SPDX-FileComment: Transformation Pipeline Library
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file lib.rs
 * @brief Envelope encryption, per-source mapping, transformation and validation rules
 * @version 1.1.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Number, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{LazyLock, Mutex};

#[derive(Debug, Clone, Deserialize)]
pub struct RuleStep {
    pub name: String,
    #[serde(default)]
    pub parameters: Map<String, Value>,
}

#[derive(Debug, Clone)]
pub struct MappingSource {
    pub id: String,
    /// Source system name, matches `raw_ingestion.source_system` (e.g. `ORA_EMPLOYEE`).
    pub name: String,
    pub topic: String,
}

#[derive(Debug, Clone)]
pub struct MappingTargetField {
    pub id: String,
    pub field_name: String,
    pub data_type: String,
    pub is_required: bool,
    pub encrypted: bool,
}

#[derive(Debug, Clone)]
pub struct MappingRule {
    pub source_id: String,
    pub target_field_id: String,
    pub source_field: String,
    /// Lower value wins if several sources map the same target field.
    pub priority: i32,
    pub transformations: Vec<RuleStep>,
    pub validations: Vec<RuleStep>,
}

#[derive(Debug, Clone, Default)]
pub struct RuleSet {
    pub sources: HashMap<String, MappingSource>,
    pub target_fields: HashMap<String, MappingTargetField>,
    pub rules: Vec<MappingRule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineError {
    pub failed_field: String,
    pub rule_name: String,
    pub error_message: String,
}

/// Merges payloads in the given order; later payloads overwrite earlier keys.
/// Callers pass the fragments of one source sorted oldest → newest.
pub fn merge_payloads(payloads: impl IntoIterator<Item = Map<String, Value>>) -> Map<String, Value> {
    let mut record = Map::new();
    for payload in payloads {
        record.extend(payload);
    }
    record
}

pub fn encrypt(key: &[u8], plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let cipher = cipher(key)?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher.encrypt(&nonce, plaintext).map_err(|_| anyhow!("AES-GCM encryption failed"))?;
    Ok((ciphertext, nonce.to_vec()))
}

pub fn decrypt(key: &[u8], nonce: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
    let cipher = cipher(key)?;
    if nonce.len() != 12 { bail!("invalid nonce size: expected 12, got {}", nonce.len()) }
    cipher.decrypt(Nonce::from_slice(nonce), ciphertext).map_err(|_| anyhow!("AES-GCM decryption failed"))
}

pub fn envelope_decrypt(kek: &[u8], wrapped_key: &[u8], payload_nonce: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    if wrapped_key.len() < 12 { bail!("wrapped DEK too short") }
    let (dek_nonce, encrypted_dek) = wrapped_key.split_at(12);
    let dek = decrypt(&normalized_key(kek), dek_nonce, encrypted_dek).context("failed to decrypt DEK")?;
    decrypt(&dek, payload_nonce, payload)
}

pub fn envelope_encrypt(kek: &[u8], wrapped_key: &[u8], plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    if wrapped_key.len() < 12 { bail!("wrapped DEK too short") }
    let (dek_nonce, encrypted_dek) = wrapped_key.split_at(12);
    let dek = decrypt(&normalized_key(kek), dek_nonce, encrypted_dek).context("failed to decrypt DEK")?;
    encrypt(&dek, plaintext)
}

pub fn generate_wrapped_dek(kek: &[u8]) -> Result<Vec<u8>> {
    let mut dek = [0_u8; 32];
    use aes_gcm::aead::rand_core::RngCore;
    OsRng.fill_bytes(&mut dek);
    let (encrypted_dek, nonce) = encrypt(&normalized_key(kek), &dek)?;
    let mut wrapped = nonce;
    wrapped.extend(encrypted_dek);
    Ok(wrapped)
}

/// A mapped value of one source for one target field.
struct Candidate {
    priority: i32,
    source: String,
    value: Value,
}

/// Maps the payloads of one aggregate (merged per source system) into one target record.
/// Each source is mapped with the rules of its own `mapping_source`; if several sources map
/// the same target field, the lowest priority with a non-empty value wins (tie: source name).
pub fn process_sources(
    sources: &BTreeMap<String, Map<String, Value>>,
    topic: &str,
    rules: &RuleSet,
    master_key: &[u8],
    wrapped_key: &[u8],
) -> (Map<String, Value>, Vec<PipelineError>) {
    let mut errors = Vec::new();
    let mut candidates: BTreeMap<String, Vec<Candidate>> = BTreeMap::new();

    for (source_system, payload) in sources {
        let Some(source) = find_mapping_source(rules, source_system, topic) else {
            errors.push(PipelineError {
                failed_field: "source_system".into(),
                rule_name: "source_lookup".into(),
                error_message: format!("no mapping source '{source_system}' for topic '{topic}'"),
            });
            continue;
        };
        for rule in rules.rules.iter().filter(|rule| rule.source_id == source.id) {
            let Some(target) = rules.target_fields.get(&rule.target_field_id) else { continue };
            if let Some(value) = map_rule(payload, rule, target, &mut errors) {
                candidates.entry(target.id.clone()).or_default().push(Candidate { priority: rule.priority, source: source.name.clone(), value });
            }
        }
    }

    let mut result = Map::new();
    for (target_id, mut field_candidates) in candidates {
        let target = &rules.target_fields[&target_id];
        field_candidates.sort_by(|a, b| a.priority.cmp(&b.priority).then_with(|| a.source.cmp(&b.source)));
        let index = field_candidates.iter().position(|c| !is_blank(&c.value)).unwrap_or(0);
        let value = field_candidates.swap_remove(index).value;

        if target.is_required && is_blank(&value) {
            errors.push(PipelineError { failed_field: target.field_name.clone(), rule_name: "required".into(), error_message: "required field is missing".into() });
            continue;
        }

        let value = if target.encrypted && !is_blank(&value) {
            match envelope_encrypt(master_key, wrapped_key, value_to_string(&value).as_bytes()) {
                Ok((ciphertext, nonce)) => {
                    use base64::prelude::BASE64_STANDARD;
                    use base64::Engine;
                    serde_json::json!({"ciphertext": BASE64_STANDARD.encode(ciphertext), "nonce": BASE64_STANDARD.encode(nonce)})
                }
                Err(error) => {
                    errors.push(PipelineError { failed_field: target.field_name.clone(), rule_name: "encryption".into(), error_message: format!("{error:#}") });
                    continue;
                }
            }
        } else { value };
        result.insert(target.field_name.clone(), value);
    }
    (result, errors)
}

fn find_mapping_source<'a>(rules: &'a RuleSet, source_system: &str, topic: &str) -> Option<&'a MappingSource> {
    // Several matches are a configuration error; pick the smallest id for determinism.
    rules.sources.values()
        .filter(|source| source.name.eq_ignore_ascii_case(source_system) && source.topic.eq_ignore_ascii_case(topic))
        .min_by(|a, b| a.id.cmp(&b.id))
}

/// Runs transformation, validation and schema cast for one rule; errors are collected.
fn map_rule(payload: &Map<String, Value>, rule: &MappingRule, target: &MappingTargetField, errors: &mut Vec<PipelineError>) -> Option<Value> {
    let input = payload.iter().find(|(name, _)| name.eq_ignore_ascii_case(&rule.source_field))
        .map(|(_, value)| value.clone()).unwrap_or(Value::Null);
    let value = match apply_transformations(input, &rule.transformations) {
        Ok(value) => value,
        Err(error) => { errors.push(pipeline_error(rule, "transformation", error)); return None; }
    };
    if let Err(error) = apply_validations(&value, &rule.validations) {
        errors.push(pipeline_error(rule, "validation", error));
        return None;
    }
    match cast_to_schema(value, &target.data_type) {
        Ok(value) => Some(value),
        Err(error) => { errors.push(pipeline_error(rule, "auto_cast", error)); None }
    }
}

/// Error messages carry the full cause chain but never the field value (PII).
fn pipeline_error(rule: &MappingRule, rule_name: &str, error: anyhow::Error) -> PipelineError {
    PipelineError { failed_field: rule.source_field.clone(), rule_name: rule_name.into(), error_message: format!("{error:#}") }
}

fn apply_transformations(mut value: Value, chain: &[RuleStep]) -> Result<Value> {
    for step in chain.iter().filter(|step| !step.name.is_empty()) {
        value = transform(&step.name, value, &step.parameters)
            .with_context(|| format!("step {} failed", step.name))?;
    }
    Ok(value)
}

fn apply_validations(value: &Value, chain: &[RuleStep]) -> Result<()> {
    for step in chain.iter().filter(|step| !step.name.is_empty()) {
        validate(&step.name, value, &step.parameters)
            .with_context(|| format!("step {} failed", step.name))?;
    }
    Ok(())
}

fn transform(name: &str, value: Value, params: &Map<String, Value>) -> Result<Value> {
    match name {
        "trim_whitespace" => Ok(string_value(&value).map(|s| Value::String(s.trim().into())).unwrap_or(value)),
        "to_upper" => Ok(string_value(&value).map(|s| Value::String(s.to_uppercase())).unwrap_or(value)),
        "to_lower" => Ok(string_value(&value).map(|s| Value::String(s.to_lowercase())).unwrap_or(value)),
        "default_value" => if value.is_null() || is_empty(&value) {
            params.get("value").cloned().ok_or_else(|| anyhow!("missing 'value' parameter for default_value"))
        } else { Ok(value) },
        "regex_replace" => {
            let Some(text) = string_value(&value) else { return Ok(value) };
            if text.is_empty() { return Ok(Value::String(text)) }
            let pattern = parameter_string(params, "pattern")?;
            let replacement = parameter_string(params, "replace")?;
            Ok(Value::String(cached_regex(pattern)?.replace_all(&text, replacement).into_owned()))
        }
        "parse_date" => parse_date(value, params),
        "string_split" => {
            let Some(text) = string_value(&value) else { return Ok(value) };
            if text.is_empty() { return Ok(Value::String(text)) }
            let separator = parameter_string(params, "separator")?;
            let index = parameter_usize(params, "index")?;
            text.split(separator).nth(index).map(|part| Value::String(part.into()))
                .ok_or_else(|| anyhow!("index {index} out of bounds for split result"))
        }
        "cast_type" => cast_value(value, parameter_string(params, "target_type")?),
        _ => bail!("transform function '{name}' not found in registry"),
    }
}

fn validate(name: &str, value: &Value, params: &Map<String, Value>) -> Result<()> {
    match name {
        "not_null" => {
            if value.is_null() { bail!("value is null") }
            if is_empty(value) { bail!("value is an empty string") }
            Ok(())
        }
        "regex_match" => {
            if value.is_null() || is_empty(value) { return Ok(()) }
            let re = cached_regex(parameter_string(params, "pattern")?)?;
            if re.is_match(&value_to_string(value)) { Ok(()) } else { bail!("value does not match pattern") }
        }
        "email" => {
            if value.is_null() || is_empty(value) { return Ok(()) }
            if EMAIL_REGEX.is_match(&value_to_string(value)) { Ok(()) } else { bail!("value is not a valid email address") }
        }
        "in_list" => {
            if value.is_null() || is_empty(value) { return Ok(()) }
            let allowed = params.get("allowed").and_then(Value::as_array).ok_or_else(|| anyhow!("'allowed' parameter must be a list"))?;
            if allowed.iter().any(|item| value_to_string(item) == value_to_string(value)) { Ok(()) } else { bail!("value is not in the allowed list") }
        }
        "range_check" => {
            if value.is_null() || is_empty(value) { return Ok(()) }
            let number = value.as_f64()
                .or_else(|| value.as_str().and_then(|text| text.trim().parse::<f64>().ok()))
                .ok_or_else(|| anyhow!("value is not numeric"))?;
            if let Some(min) = params.get("min").and_then(Value::as_f64) { if number < min { bail!("value is less than minimum {min}") } }
            if let Some(max) = params.get("max").and_then(Value::as_f64) { if number > max { bail!("value is greater than maximum {max}") } }
            Ok(())
        }
        "min_length" => validate_length(value, params, true),
        "max_length" => validate_length(value, params, false),
        _ => bail!("validate function '{name}' not found in registry"),
    }
}

fn validate_length(value: &Value, params: &Map<String, Value>, minimum: bool) -> Result<()> {
    if value.is_null() || is_empty(value) { return Ok(()) }
    let length = parameter_usize(params, "length")?;
    let actual = value_to_string(value).chars().count();
    if (minimum && actual < length) || (!minimum && actual > length) { bail!("value length ({actual}) is outside permitted bound ({length})") }
    Ok(())
}

fn parse_date(value: Value, params: &Map<String, Value>) -> Result<Value> {
    let Some(text) = string_value(&value) else { return Ok(value) };
    if text.is_empty() { return Ok(Value::String(text)) }
    let input = params.get("input_format").and_then(Value::as_str);
    let date = if let Some(format) = input { parse_with_format(&text, format)? } else { autodetect_date(&text)? };
    let output = params.get("output_format").and_then(Value::as_str).unwrap_or("2006-01-02");
    Ok(Value::String(format_date(date, output)?))
}

fn autodetect_date(text: &str) -> Result<NaiveDate> {
    for format in ["%Y-%m-%d", "%Y/%m/%d", "%d.%m.%Y", "%d.%m.%y", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y", "%m-%d-%Y"] {
        if let Ok(date) = NaiveDate::parse_from_str(text, format) { return Ok(date) }
    }
    if let Ok(datetime) = DateTime::parse_from_rfc3339(text) { return Ok(datetime.date_naive()) }
    for format in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(datetime) = NaiveDateTime::parse_from_str(text, format) { return Ok(datetime.date()) }
    }
    // Go "2006-01-02 15:04:05.999999999 -0700 MST": chrono cannot parse zone names, so drop it.
    if let Some((rest, zone)) = text.rsplit_once(' ')
        && zone.chars().all(|c| c.is_ascii_alphabetic())
        && let Ok(datetime) = DateTime::parse_from_str(rest, "%Y-%m-%d %H:%M:%S%.f %z")
    {
        return Ok(datetime.date_naive());
    }
    // The value itself is not included (may be PII).
    bail!("unsupported date format")
}

fn parse_with_format(text: &str, layout: &str) -> Result<NaiveDate> {
    let format = go_layout(layout)?;
    NaiveDate::parse_from_str(text, format).or_else(|_| NaiveDateTime::parse_from_str(text, format).map(|date| date.date()))
        .with_context(|| format!("failed to parse date using {layout}"))
}

fn format_date(date: NaiveDate, format: &str) -> Result<String> {
    if format == "2006-01-02T15:04:05Z07:00" || format == time_rfc3339() {
        return Ok(date.and_hms_opt(0, 0, 0).unwrap().and_utc().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    }
    Ok(date.format(go_layout(format)?).to_string())
}

fn time_rfc3339() -> &'static str { "2006-01-02T15:04:05Z07:00" }

fn go_layout(format: &str) -> Result<&str> {
    match format {
        "2006-01-02" => Ok("%Y-%m-%d"), "02.01.2006" => Ok("%d.%m.%Y"),
        "01/02/2006" => Ok("%m/%d/%Y"), "02/01/2006" => Ok("%d/%m/%Y"),
        "02-01-2006" => Ok("%d-%m-%Y"), "01-02-2006" => Ok("%m-%d-%Y"),
        "2006/01/02" => Ok("%Y/%m/%d"), "02.01.06" => Ok("%d.%m.%y"),
        _ => bail!("unsupported Go date layout: {format}"),
    }
}

fn cast_to_schema(value: Value, target_type: &str) -> Result<Value> {
    if value.is_null() || target_type.is_empty() { return Ok(value) }
    cast_value(value, target_type)
}

fn cast_value(value: Value, target_type: &str) -> Result<Value> {
    let text = value_to_string(&value);
    match target_type.to_ascii_lowercase().as_str() {
        "int" | "integer" | "int64" => Ok(Value::Number(Number::from(text.parse::<i64>().context("failed to cast to integer")?))),
        "float" | "float64" | "numeric" | "decimal" => Number::from_f64(text.parse::<f64>().context("failed to cast to float")?)
            .map(Value::Number).ok_or_else(|| anyhow!("float is not a finite JSON number")),
        "bool" | "boolean" => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(Value::Bool(true)),
            "false" | "0" => Ok(Value::Bool(false)),
            _ => bail!("failed to cast to bool"),
        },
        "string" | "text" | "varchar" => Ok(Value::String(text)),
        "date" => {
            if is_empty(&value) { return Ok(value) }
            let date = autodetect_date(text.trim()).context("failed to cast to date")?;
            Ok(Value::String(date.format("%Y-%m-%d").to_string()))
        }
        "jsonb" | "json" => match value {
            Value::String(raw) => {
                if raw.is_empty() { return Ok(Value::String(raw)) }
                serde_json::from_str(&raw).map_err(|_| anyhow!("failed to cast to jsonb: invalid JSON"))
            }
            other => Ok(other),
        },
        _ => bail!("unsupported target_type: {target_type}"),
    }
}

static EMAIL_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").expect("valid static email regex"));
static REGEX_CACHE: LazyLock<Mutex<HashMap<String, Regex>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Compiles each rule pattern once per process.
fn cached_regex(pattern: &str) -> Result<Regex> {
    let mut cache = REGEX_CACHE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(re) = cache.get(pattern) { return Ok(re.clone()) }
    let re = Regex::new(pattern).context("invalid regex pattern")?;
    cache.insert(pattern.to_owned(), re.clone());
    Ok(re)
}

fn normalized_key(key: &[u8]) -> Vec<u8> { key.iter().copied().chain(std::iter::repeat(0)).take(32).collect() }
fn cipher(key: &[u8]) -> Result<Aes256Gcm> { Aes256Gcm::new_from_slice(key).map_err(|_| anyhow!("AES-256 key must be exactly 32 bytes")) }
fn string_value(value: &Value) -> Option<String> { value.as_str().map(str::to_owned) }
fn is_empty(value: &Value) -> bool { value.as_str().is_some_and(str::is_empty) }
fn is_blank(value: &Value) -> bool { value.is_null() || is_empty(value) }
fn value_to_string(value: &Value) -> String { match value { Value::String(value) => value.clone(), Value::Null => "null".into(), _ => value.to_string() } }
fn parameter_string<'a>(params: &'a Map<String, Value>, name: &str) -> Result<&'a str> { params.get(name).and_then(Value::as_str).ok_or_else(|| anyhow!("missing or invalid '{name}' parameter")) }
fn parameter_usize(params: &Map<String, Value>, name: &str) -> Result<usize> { params.get(name).and_then(Value::as_u64).map(|value| value as usize).ok_or_else(|| anyhow!("missing or invalid '{name}' parameter")) }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(value: Value) -> Map<String, Value> { value.as_object().unwrap().clone() }

    fn target(id: &str, name: &str, data_type: &str) -> MappingTargetField {
        MappingTargetField { id: id.into(), field_name: name.into(), data_type: data_type.into(), is_required: false, encrypted: false }
    }

    fn rule(source: &str, target: &str, field: &str, priority: i32) -> MappingRule {
        MappingRule { source_id: source.into(), target_field_id: target.into(), source_field: field.into(), priority, transformations: vec![], validations: vec![] }
    }

    /// Two sources (ORA, PG) of topic Employee mapping the same target field "email".
    fn two_source_rules() -> RuleSet {
        let mut rules = RuleSet::default();
        for (id, name) in [("s-ora", "ORA_EMPLOYEE"), ("s-pg", "PG_EMPLOYEE")] {
            rules.sources.insert(id.into(), MappingSource { id: id.into(), name: name.into(), topic: "Employee".into() });
        }
        rules.target_fields.insert("t-mail".into(), target("t-mail", "email", "text"));
        rules.target_fields.insert("t-name".into(), target("t-name", "last_name", "text"));
        rules.rules.push(rule("s-ora", "t-mail", "MAIL", 2));
        rules.rules.push(rule("s-pg", "t-mail", "email", 1));
        rules.rules.push(rule("s-ora", "t-name", "LASTNAME", 1));
        rules
    }

    fn sources(entries: &[(&str, Value)]) -> BTreeMap<String, Map<String, Value>> {
        entries.iter().map(|(name, value)| (name.to_string(), obj(value.clone()))).collect()
    }

    #[test]
    fn envelope_round_trip() {
        let kek = [7; 32];
        let wrapped = generate_wrapped_dek(&kek).unwrap();
        let (ciphertext, nonce) = envelope_encrypt(&kek, &wrapped, b"secret").unwrap();
        assert_eq!(envelope_decrypt(&kek, &wrapped, &nonce, &ciphertext).unwrap(), b"secret");
    }

    #[test]
    fn transforms_and_validations_match_contract() {
        let params = obj(json!({"pattern": "[^0-9]", "replace": ""}));
        assert_eq!(transform("regex_replace", Value::String("ab12".into()), &params).unwrap(), Value::String("12".into()));
        let params = obj(json!({"allowed": ["active"]}));
        assert!(validate("in_list", &Value::String("active".into()), &params).is_ok());
    }

    #[test]
    fn pipeline_encrypts_transformed_values() {
        let mut rules = RuleSet::default();
        rules.sources.insert("source".into(), MappingSource { id: "source".into(), name: "ORA_EMPLOYEE".into(), topic: "Employee".into() });
        let mut email = target("target", "email", "text");
        email.encrypted = true;
        rules.target_fields.insert("target".into(), email);
        let mut mail_rule = rule("source", "target", "Mail", 1);
        mail_rule.transformations = vec![RuleStep { name: "to_lower".into(), parameters: Map::new() }];
        mail_rule.validations = vec![RuleStep { name: "email".into(), parameters: Map::new() }];
        rules.rules.push(mail_rule);
        let kek = [9; 32];
        let wrapped = generate_wrapped_dek(&kek).unwrap();
        let (result, errors) = process_sources(&sources(&[("ORA_EMPLOYEE", json!({"mail": "TEST@EXAMPLE.COM"}))]), "Employee", &rules, &kek, &wrapped);
        assert!(errors.is_empty(), "{errors:?}");
        assert!(result["email"]["ciphertext"].is_string());
    }

    #[test]
    fn each_source_uses_its_own_rules_and_priority_wins() {
        let rules = two_source_rules();
        let input = sources(&[
            ("ORA_EMPLOYEE", json!({"MAIL": "ora@example.com", "LASTNAME": "Doe", "email": "ignored@example.com"})),
            ("PG_EMPLOYEE", json!({"email": "pg@example.com", "LASTNAME": "ignored"})),
        ]);
        let (result, errors) = process_sources(&input, "employee", &rules, &[1; 32], &[]);
        assert!(errors.is_empty(), "{errors:?}");
        // PG has priority 1 for email; last_name is only mapped from ORA.
        assert_eq!(result["email"], json!("pg@example.com"));
        assert_eq!(result["last_name"], json!("Doe"));
    }

    #[test]
    fn empty_high_priority_value_falls_back_to_next_source() {
        let rules = two_source_rules();
        let input = sources(&[
            ("ORA_EMPLOYEE", json!({"MAIL": "ora@example.com", "LASTNAME": "Doe"})),
            ("PG_EMPLOYEE", json!({"email": ""})),
        ]);
        let (result, _) = process_sources(&input, "Employee", &rules, &[1; 32], &[]);
        assert_eq!(result["email"], json!("ora@example.com"));
    }

    #[test]
    fn result_is_deterministic() {
        let rules = two_source_rules();
        let input = sources(&[
            ("PG_EMPLOYEE", json!({"email": "pg@example.com"})),
            ("ORA_EMPLOYEE", json!({"MAIL": "ora@example.com", "LASTNAME": "Doe"})),
        ]);
        let first = process_sources(&input, "Employee", &rules, &[1; 32], &[]).0;
        for _ in 0..20 {
            assert_eq!(process_sources(&input, "Employee", &rules, &[1; 32], &[]).0, first);
        }
    }

    #[test]
    fn unknown_source_fails_record() {
        let rules = two_source_rules();
        let (_, errors) = process_sources(&sources(&[("CSV_EMPLOYEE", json!({"x": "1"}))]), "Employee", &rules, &[1; 32], &[]);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].rule_name, "source_lookup");
    }

    #[test]
    fn merge_keeps_newest_value_per_source() {
        let merged = merge_payloads([obj(json!({"a": "old", "b": "kept"})), obj(json!({"a": "new"}))]);
        assert_eq!(merged["a"], json!("new"));
        assert_eq!(merged["b"], json!("kept"));
    }

    #[test]
    fn required_field_is_enforced() {
        let mut rules = two_source_rules();
        rules.target_fields.get_mut("t-name").unwrap().is_required = true;
        let (_, errors) = process_sources(&sources(&[("ORA_EMPLOYEE", json!({"MAIL": "a@b.de"}))]), "Employee", &rules, &[1; 32], &[]);
        assert!(errors.iter().any(|e| e.rule_name == "required" && e.failed_field == "last_name"), "{errors:?}");
    }

    #[test]
    fn date_and_jsonb_casts() {
        assert_eq!(cast_value(json!("31.12.1990"), "date").unwrap(), json!("1990-12-31"));
        assert_eq!(cast_value(json!("1990-12-31T08:00:00Z"), "date").unwrap(), json!("1990-12-31"));
        assert_eq!(cast_value(json!(""), "date").unwrap(), json!(""));
        assert!(cast_value(json!("not a date"), "date").is_err());
        assert_eq!(cast_value(json!("{\"a\":1}"), "jsonb").unwrap(), json!({"a": 1}));
        assert_eq!(cast_value(json!({"a": 1}), "jsonb").unwrap(), json!({"a": 1}));
        assert!(cast_value(json!("{broken"), "jsonb").is_err());
    }

    #[test]
    fn bool_cast_accepts_numbers_and_case() {
        for (input, expected) in [("TRUE", true), ("1", true), ("false", false), ("0", false)] {
            assert_eq!(cast_value(json!(input), "bool").unwrap(), json!(expected));
        }
        assert!(cast_value(json!("yes"), "bool").is_err());
    }

    #[test]
    fn length_counts_characters() {
        let params = obj(json!({"length": 4}));
        assert!(validate("max_length", &json!("Müll"), &params).is_ok());
    }

    #[test]
    fn range_check_accepts_numeric_strings() {
        let params = obj(json!({"min": 1, "max": 10}));
        assert!(validate("range_check", &json!("5"), &params).is_ok());
        assert!(validate("range_check", &json!("11"), &params).is_err());
    }

    #[test]
    fn go_mst_date_format_is_parsed() {
        assert_eq!(autodetect_date("2026-10-17 08:15:00.123456789 +0200 CEST").unwrap(), NaiveDate::from_ymd_opt(2026, 10, 17).unwrap());
    }

    #[test]
    fn error_messages_contain_cause_but_not_value() {
        let mut rules = RuleSet::default();
        rules.sources.insert("s".into(), MappingSource { id: "s".into(), name: "ORA_EMPLOYEE".into(), topic: "Employee".into() });
        rules.target_fields.insert("t".into(), target("t", "birth_date", "text"));
        let mut date_rule = rule("s", "t", "BIRTH", 1);
        date_rule.transformations = vec![RuleStep { name: "parse_date".into(), parameters: Map::new() }];
        rules.rules.push(date_rule);

        let secret = "secret-1990-99-99";
        let (_, errors) = process_sources(&sources(&[("ORA_EMPLOYEE", json!({"BIRTH": secret}))]), "Employee", &rules, &[1; 32], &[]);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].error_message.contains("unsupported date format"), "{}", errors[0].error_message);
        assert!(!errors[0].error_message.contains(secret));
    }

    #[test]
    fn regex_is_cached() {
        let first = cached_regex("^a+$").unwrap();
        let second = cached_regex("^a+$").unwrap();
        assert_eq!(first.as_str(), second.as_str());
        assert!(REGEX_CACHE.lock().unwrap().contains_key("^a+$"));
        assert!(cached_regex("(").is_err());
    }
}
