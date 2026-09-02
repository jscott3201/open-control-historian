#![forbid(unsafe_code)]
//! Process-level proof for the unsupported private g2 structural harness command.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(1);

struct Temp(PathBuf);

impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "och-v2-g2-cli-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).expect("create CLI temporary parent");
        Self(path)
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn complete_structural_command_is_repeatable_bounded_and_non_authorizing() {
    let temp = Temp::new();
    let root = temp.0.join("evidence");
    for iteration in 1..=2 {
        let output = Command::new(env!("CARGO_BIN_EXE_och-v2-evidence"))
            .args([
                "native-harness-check",
                "--root",
                root.to_str().expect("UTF-8 temporary root"),
            ])
            .output()
            .expect("run private structural harness command");
        assert!(
            output.status.success(),
            "harness iteration {iteration} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("bounded UTF-8 summary");
        for expected in [
            "schema=m03-pr03e-v1",
            "harness_status=PASS",
            "report_classification=STRUCTURAL_SYNTHETIC",
            "descriptor_count=173",
            "crash_target_count=173",
            "matrix_rows=639",
            "timing_rows=173",
            "timing_summary_rows=6",
            "resource_rows=6",
            "fault_registry_rows=173",
            "fault_result_rows=487",
            "bundle_files=8",
            "COLLECTION_AUTHORIZED=false",
            "MEASURED_NATIVE_EVIDENCE=false",
            "PR03E-M01..M11=UNSATISFIED",
            "NATIVE_THRESHOLDS_BUDGETS_SLOS=UNKNOWN",
            "V2_PRODUCT_AUTHORITY=false",
        ] {
            assert!(stdout.lines().any(|line| line == expected), "{expected}");
        }
        assert!(!stdout.contains('/') && !stdout.contains('\\'));
    }

    for name in ["cases", "control"] {
        assert_eq!(
            fs::read_dir(root.join(name))
                .expect("read reclaimed parent directory")
                .count(),
            0,
            "{name}"
        );
    }
    let bundle = root.join("reports/m03-pr03g2-structural");
    let actual = fs::read_dir(&bundle)
        .expect("read structural bundle")
        .map(|entry| {
            entry
                .expect("read structural report entry")
                .file_name()
                .into_string()
                .expect("UTF-8 report name")
        })
        .collect::<BTreeSet<_>>();
    let expected = [
        "SHA256SUMS",
        "fault-registry.tsv",
        "fault-results.tsv",
        "matrix.tsv",
        "resource-ledger.tsv",
        "run.kv",
        "timing-samples.tsv",
        "timing-summary.tsv",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
    let run = fs::read_to_string(bundle.join("run.kv")).expect("read run metadata");
    for expected in [
        "report_classification=STRUCTURAL_SYNTHETIC",
        "collection_authorized=false",
        "measured_native_evidence=false",
        "v2_product_authority=false",
        "pr03e_m01=UNSATISFIED",
        "pr03e_m11=UNSATISFIED",
    ] {
        assert!(run.lines().any(|line| line == expected), "{expected}");
    }
    assert_eq!(
        fs::read_to_string(bundle.join("SHA256SUMS"))
            .expect("read checksums")
            .lines()
            .count(),
        7
    );
    validate_crash_report(&bundle);
}

fn validate_crash_report(bundle: &std::path::Path) {
    let fault_results = fs::read_to_string(bundle.join("fault-results.tsv"))
        .expect("read structural fault results");
    let crash_rows = fault_results
        .lines()
        .skip(1)
        .filter(|line| line.contains("CHILD_CRASH_AFTER_SUCCESS"))
        .collect::<Vec<_>>();
    assert_eq!(crash_rows.len(), 173);
    for row in crash_rows {
        let fields = row.split('\t').collect::<Vec<_>>();
        assert_eq!(fields.len(), 26);
        assert_eq!(fields[7], "1");
        assert_eq!(fields[8], fields[9]);
        assert_ne!(fields[8], "STRUCTURAL_MATCH");
        assert_eq!(fields[17], "KILLED_REAPED");
        for fingerprint in &fields[19..=22] {
            assert_eq!(fingerprint.len(), 64);
        }
        assert_eq!(fields[24], "NOT_EMITTED");
        assert_eq!(fields[25], "1");
    }
}

#[test]
fn hidden_worker_and_collection_refuse_direct_or_malformed_invocation() {
    let temp = Temp::new();
    let root = temp.0.join("must-stay-absent");
    for arguments in [
        vec![
            "__native-child",
            "--root",
            root.to_str().expect("UTF-8 root"),
            "--token",
            "CRASH-000-1",
            "--fault-id",
            "V2IO-P0-INVENTORY-DIRECTORY-OPEN",
        ],
        vec![
            "native-collect",
            "--root",
            root.to_str().expect("UTF-8 root"),
            "--harness-sha",
            "bad",
            "--measured-source-sha",
            "bad",
            "--tree-status",
            "DIRTY",
            "--authorization",
            "POST_ACCEPTANCE_G2",
        ],
        vec![
            "native-collect",
            "--root",
            root.to_str().expect("UTF-8 root"),
            "--harness-sha",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--measured-source-sha",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--tree-status",
            "CLEAN",
            "--authorization",
            "POST_ACCEPTANCE_G2",
        ],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_och-v2-evidence"))
            .args(arguments)
            .output()
            .expect("run malformed private command");
        assert!(!output.status.success());
        assert!(!root.exists());
    }
}
