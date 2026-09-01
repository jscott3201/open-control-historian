#![forbid(unsafe_code)]
//! Integration proof that the checked-in workspace satisfies its own policy.

use std::path::PathBuf;
use std::process::Command;

fn workspace_manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("Cargo.toml")
}

#[test]
fn actual_workspace_native_graph_is_minimal() {
    let summary = och_policy::check_workspace(&workspace_manifest())
        .expect("the actual workspace dependency graph should satisfy policy");
    assert_eq!(summary.native_root_count(), 3);
    assert_eq!(summary.native_closure_package_count(), 5);
}

#[test]
fn command_reports_the_actual_workspace_summary() {
    let output = Command::new(env!("CARGO_BIN_EXE_och-policy"))
        .arg("check")
        .arg("--manifest-path")
        .arg(workspace_manifest())
        .output()
        .expect("policy command should start");
    assert!(
        output.status.success(),
        "policy command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains(
        "dependency policy passed: 3 native root(s), 5 package(s) in the native closure"
    ));
}

#[test]
fn command_rejects_unknown_arguments() {
    let output = Command::new(env!("CARGO_BIN_EXE_och-policy"))
        .arg("unexpected")
        .output()
        .expect("policy command should start");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("usage:"));
}

#[test]
fn native_evidence_feature_is_exact_nondefault_and_does_not_change_the_native_graph() {
    let root = workspace_manifest();
    let workspace = std::fs::read_to_string(&root).expect("read workspace manifest");
    let store = std::fs::read_to_string(
        root.parent()
            .expect("workspace root")
            .join("crates/och-store/Cargo.toml"),
    )
    .expect("read store manifest");
    let runtime = std::fs::read_to_string(
        root.parent()
            .expect("workspace root")
            .join("crates/och-runtime/Cargo.toml"),
    )
    .expect("read runtime manifest");
    let lock = std::fs::read_to_string(root.parent().expect("workspace root").join("Cargo.lock"))
        .expect("read workspace lockfile");

    assert!(store.contains("[features]\ndefault = []\nm03-pr03e-native-harness = []"));
    assert!(runtime.contains(
        "[features]\ndefault = []\nm03-pr03e-native-harness = [\"och-store/m03-pr03e-native-harness\"]"
    ));
    assert!(
        store.contains(
            "[dependencies]\noch-core = { version = \"=0.0.0\", path = \"../och-core\" }"
        )
    );
    assert!(runtime.contains(
        "och-core = { version = \"=0.0.0\", path = \"../och-core\" }\noch-store = { version = \"=0.0.0\", path = \"../och-store\" }\ntokio = { version = \"1.53.1\", default-features = false, features = [\"rt\", \"sync\"] }"
    ));
    assert!(workspace.contains(
        "default-members = [\"crates/och-core\", \"crates/och-runtime\", \"crates/och-store\"]"
    ));
    assert!(!workspace.contains("och-v2-native-harness"));
    assert!(!lock.contains("och-v2-native-harness"));
    assert!(!lock.contains("name = \"sha2\""));

    let summary = och_policy::check_workspace(&root)
        .expect("feature metadata must preserve the checked native graph");
    assert_eq!(summary.native_root_count(), 3);
    assert_eq!(summary.native_closure_package_count(), 5);
}
