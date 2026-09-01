mod fault;
pub(crate) mod inventory;
mod oracle;
mod runtime;
mod schema;
#[cfg(test)]
mod source_policy;
mod transaction;
mod v2_io;

use crate::error::{EvidenceError, Result};
use crate::root::EvidenceRoot;
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) fn foundation_check_command(arguments: &[String]) -> Result<()> {
    let values = parse_exact(arguments, &["--root"])?;
    schema::validate_closed_schema()?;
    fault::validate_registry()?;
    v2_io::validate_source_closure()?;
    inventory::canonical_inventory_names()?;
    oracle::validate_foundation_oracles()?;
    let _ = crate::sha256::digest(b"M03-PR03g1")?;

    let root = EvidenceRoot::prepare(Path::new(values["--root"]))?;
    root.foundation_layout()?;
    let witness = root.with_store_child("v2-executor-foundation", transaction::run_foundation)?;

    root.with_store_child("v1-success-smoke", |success| {
        let success_witness = runtime::success(success, false)?;
        if success_witness.handled != "HANDLED" || success_witness.durable != "DURABLE" {
            return Err(EvidenceError::InvalidHarness);
        }
        Ok(())
    })?;

    for (name, kind, partial) in [
        (
            "v1-pressure-full-smoke",
            och_runtime::__m03_pr03e_native_harness::InjectedErrorKind::StorageFull,
            false,
        ),
        (
            "v1-pressure-quota-smoke",
            och_runtime::__m03_pr03e_native_harness::InjectedErrorKind::QuotaExceeded,
            true,
        ),
    ] {
        root.with_store_child(name, |pressure| runtime::pressure(pressure, kind, partial))?;
    }

    println!("schema={}", schema::FOUNDATION_SCHEMA);
    println!("foundation_status=PASS");
    println!("descriptor_count={}", witness.descriptor_count);
    println!("source_site_count={}", witness.source_site_count);
    println!("source_site_executions={}", witness.site_executions);
    println!("flow_count={}", witness.flow_count);
    println!(
        "deferred_g2_crash_obligations={}",
        witness.deferred_crash_obligations
    );
    println!("COLLECTION_AUTHORIZED=false");
    println!("REPORT_BUNDLE=ABSENT");
    println!("PR03E-M01..M11=UNSATISFIED");
    println!("V2_PRODUCT_AUTHORITY=false");
    Ok(())
}

fn parse_exact<'a>(
    arguments: &'a [String],
    required: &[&str],
) -> Result<BTreeMap<&'a str, &'a str>> {
    let mut values = BTreeMap::new();
    let mut index = 0_usize;
    while index < arguments.len() {
        let option = arguments[index].as_str();
        if !required.contains(&option) || values.contains_key(option) {
            return Err(EvidenceError::Usage);
        }
        let value = arguments.get(index + 1).ok_or(EvidenceError::Usage)?;
        if value.starts_with("--") || value.is_empty() {
            return Err(EvidenceError::Usage);
        }
        values.insert(option, value.as_str());
        index = index.checked_add(2).ok_or(EvidenceError::Bounds)?;
    }
    if values.len() != required.len() || required.iter().any(|name| !values.contains_key(name)) {
        return Err(EvidenceError::Usage);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foundation_parser_is_exact_and_has_no_collection_or_report_arguments() {
        let valid = ["--root".to_owned(), "private-root".to_owned()];
        assert!(parse_exact(&valid, &["--root"]).is_ok());
        for hostile in [
            vec![],
            vec!["--root".to_owned()],
            vec!["--report".to_owned(), "bundle".to_owned()],
            vec![
                "--root".to_owned(),
                "one".to_owned(),
                "--root".to_owned(),
                "two".to_owned(),
            ],
        ] {
            assert!(parse_exact(&hostile, &["--root"]).is_err());
        }
    }
}
