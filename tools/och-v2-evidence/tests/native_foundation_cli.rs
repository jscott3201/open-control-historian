#![forbid(unsafe_code)]
//! Process-level proof for the unsupported private g1 executor-foundation command.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(1);

struct Temp(PathBuf);

impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "och-v2-g1-cli-{}-{}",
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
fn private_foundation_command_executes_closure_and_emits_no_report_or_collection_claim() {
    let temp = Temp::new();
    let root = temp.0.join("evidence");
    for iteration in 1..=2 {
        let output = Command::new(env!("CARGO_BIN_EXE_och-v2-evidence"))
            .args([
                "native-foundation-check",
                "--root",
                root.to_str().expect("UTF-8 temporary root"),
            ])
            .output()
            .expect("run private foundation command");
        assert!(
            output.status.success(),
            "foundation command iteration {iteration} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("bounded UTF-8 summary");
        for expected in [
            "schema=m03-pr03g1-v1",
            "foundation_status=PASS",
            "descriptor_count=173",
            "source_site_count=173",
            "source_site_executions=487",
            "flow_count=5",
            "COLLECTION_AUTHORIZED=false",
            "REPORT_BUNDLE=ABSENT",
            "PR03E-M01..M11=UNSATISFIED",
            "V2_PRODUCT_AUTHORITY=false",
        ] {
            assert!(stdout.lines().any(|line| line == expected), "{expected}");
        }
        assert!(!stdout.contains('/') && !stdout.contains('\\'));
    }
    assert!(!root.join("reports").exists());
    assert_eq!(
        fs::read_dir(root.join("cases"))
            .expect("read reclaimed cases")
            .count(),
        0
    );
}

#[test]
fn unsupported_or_malformed_private_commands_refuse() {
    for command in [
        "native-run",
        "native-validate",
        "native-collect",
        "__native-child",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_och-v2-evidence"))
            .arg(command)
            .output()
            .expect("run deferred command");
        assert!(!output.status.success(), "{command}");
    }
}
