use crate::error::{EvidenceError, Result};
use crate::root::EvidenceRoot;
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) fn foundation_check_command(arguments: &[String]) -> Result<()> {
    let values = parse_exact(arguments, &["--root"])?;
    let root = EvidenceRoot::prepare(Path::new(values["--root"]))?;
    let witness = root.run_v2_foundation()?;
    root.run_v1_success_smoke()?;

    for (kind, partial) in [
        (
            och_runtime::__m03_pr03e_native_harness::InjectedErrorKind::StorageFull,
            false,
        ),
        (
            och_runtime::__m03_pr03e_native_harness::InjectedErrorKind::QuotaExceeded,
            true,
        ),
    ] {
        root.run_v1_pressure_smoke(kind, partial)?;
    }

    println!("schema={}", witness.schema);
    println!("foundation_status=PASS");
    println!("descriptor_count={}", witness.descriptor_count);
    println!("source_site_count={}", witness.source_site_count);
    println!("source_site_executions={}", witness.site_executions);
    println!("flow_count={}", witness.flow_count);
    println!(
        "separate_g2_crash_targets={}",
        witness.registered_g2_crash_targets
    );
    println!("COLLECTION_AUTHORIZED=false");
    println!("REPORT_BUNDLE=ABSENT");
    println!("PR03E-M01..M11=UNSATISFIED");
    println!("V2_PRODUCT_AUTHORITY=false");
    Ok(())
}

pub(crate) fn harness_check_command(arguments: &[String]) -> Result<()> {
    let values = parse_exact(arguments, &["--root"])?;
    let root = EvidenceRoot::prepare(Path::new(values["--root"]))?;
    let witness = root.run_v2_harness()?;
    root.run_v1_success_smoke()?;
    for (kind, partial) in [
        (
            och_runtime::__m03_pr03e_native_harness::InjectedErrorKind::StorageFull,
            false,
        ),
        (
            och_runtime::__m03_pr03e_native_harness::InjectedErrorKind::QuotaExceeded,
            true,
        ),
    ] {
        root.run_v1_pressure_smoke(kind, partial)?;
    }
    print_harness_summary(&witness);
    Ok(())
}

pub(crate) fn collect_command(arguments: &[String]) -> Result<()> {
    let _values = parse_exact(
        arguments,
        &[
            "--root",
            "--harness-sha",
            "--measured-source-sha",
            "--tree-status",
            "--authorization",
        ],
    )?;
    // G2 intentionally has no measured collector. Caller-provided source and
    // cleanliness assertions cannot authorize or manufacture acceptance data.
    // A later accepted implementation must independently inspect git and
    // collect every mandatory repetition/tier before it may prepare a root.
    Err(EvidenceError::Replan)
}

fn print_harness_summary(witness: &crate::root::HarnessSummary) {
    println!("schema={}", witness.schema);
    println!("harness_status=PASS");
    println!("report_classification={}", witness.classification);
    println!("descriptor_count={}", witness.descriptor_count);
    println!("crash_target_count={}", witness.crash_target_count);
    println!("matrix_rows={}", witness.matrix_rows);
    println!("timing_rows={}", witness.timing_rows);
    println!("timing_summary_rows={}", witness.timing_summary_rows);
    println!("resource_rows={}", witness.resource_rows);
    println!("fault_registry_rows={}", witness.registry_rows);
    println!("fault_result_rows={}", witness.fault_result_rows);
    println!("bundle_files={}", witness.bundle_files);
    println!("bundle_bytes={}", witness.bundle_bytes);
    println!("COLLECTION_AUTHORIZED={}", witness.collection_authorized);
    println!(
        "MEASURED_NATIVE_EVIDENCE={}",
        witness.measured_native_evidence
    );
    println!("PR03E-M01..M11=UNSATISFIED");
    println!("NATIVE_THRESHOLDS_BUDGETS_SLOS=UNKNOWN");
    println!("V2_PRODUCT_AUTHORITY=false");
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

    #[test]
    fn collection_parser_and_platform_guard_refuse_without_running_collection() {
        let arguments = [
            "--root".to_owned(),
            "unused".to_owned(),
            "--harness-sha".to_owned(),
            "a".repeat(40),
            "--measured-source-sha".to_owned(),
            "b".repeat(40),
            "--tree-status".to_owned(),
            "CLEAN".to_owned(),
            "--authorization".to_owned(),
            "POST_ACCEPTANCE_G2".to_owned(),
        ];
        assert!(collect_command(&arguments).is_err());
        assert!(!Path::new("unused").exists());
    }

    #[test]
    fn syntactically_complete_collection_request_fails_closed_before_root_creation() {
        let root = format!("collection-must-not-exist-{}", std::process::id());
        let arguments = [
            "--root".to_owned(),
            root.clone(),
            "--harness-sha".to_owned(),
            "a".repeat(40),
            "--measured-source-sha".to_owned(),
            "b".repeat(40),
            "--tree-status".to_owned(),
            "CLEAN".to_owned(),
            "--authorization".to_owned(),
            "POST_ACCEPTANCE_G2".to_owned(),
        ];
        assert!(matches!(
            collect_command(&arguments),
            Err(EvidenceError::Replan)
        ));
        assert!(!Path::new(&root).exists());
    }
}
