// SPDX-License-Identifier: Apache-2.0

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use lance::Dataset;
use serde_json::{Value, json};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn metrics_enabled() -> Result<bool> {
    match std::env::var("LANCE_METADATA_METRICS").as_deref() {
        Ok("1") => Ok(true),
        Ok("0") | Err(std::env::VarError::NotPresent) => Ok(false),
        other => Err(format!("LANCE_METADATA_METRICS must be 0 or 1, got {other:?}").into()),
    }
}

pub fn export(
    path: &Path,
    record: Option<Value>,
    dataset: Option<&Dataset>,
    extra: Value,
) -> Result<()> {
    let Some(mut record) = record else {
        return Ok(());
    };
    record["build_profile"] = json!(if cfg!(debug_assertions) {
        "dev"
    } else {
        "optimized"
    });
    if let Some(dataset) = dataset {
        record["version"] = json!(dataset.version_id());
        record["fragment_count"] = json!(dataset.fragments().len());
    }
    if let Some(fields) = extra.as_object() {
        for (key, value) in fields {
            record[key] = value.clone();
        }
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{}", serde_json::to_string(&record)?)?;
    Ok(())
}
