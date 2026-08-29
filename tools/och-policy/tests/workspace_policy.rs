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
    assert_eq!(summary.native_root_count(), 2);
    assert_eq!(summary.native_closure_package_count(), 4);
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
        "dependency policy passed: 2 native root(s), 4 package(s) in the native closure"
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
