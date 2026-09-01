use crate::error::{EvidenceError, Result};
use crate::model::valid_case_name;
use std::fs;
use std::path::{Path, PathBuf};

mod pr03c_io;
mod v1_smoke;
mod v2_io;

pub(crate) use pr03c_io::{Pr03cCase, Pr03cSet};

const MAX_PARENT_ENTRIES: usize = 8;
const PARENT_DIRECTORIES: [&str; 5] = ["artifacts", "cases", "control", "fixtures", "reports"];

pub(crate) struct EvidenceRoot {
    path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FoundationSummary {
    pub(crate) schema: &'static str,
    pub(crate) descriptor_count: usize,
    pub(crate) source_site_count: usize,
    pub(crate) site_executions: usize,
    pub(crate) flow_count: usize,
    pub(crate) deferred_crash_obligations: usize,
}

impl EvidenceRoot {
    pub(crate) fn prepare(path: &Path) -> Result<Self> {
        if path.as_os_str().is_empty() {
            return Err(EvidenceError::UnsafeEvidenceRoot);
        }
        let existing = nearest_existing(path)?;
        reject_store_ancestors(&existing)?;
        fs::create_dir_all(path).map_err(|_| EvidenceError::Io)?;
        let canonical = fs::canonicalize(path).map_err(|_| EvidenceError::Io)?;
        reject_store_ancestors(&canonical)?;
        Ok(Self { path: canonical })
    }

    pub(crate) fn open(path: &Path) -> Result<Self> {
        let canonical = fs::canonicalize(path).map_err(|_| EvidenceError::Io)?;
        if !canonical.is_dir() {
            return Err(EvidenceError::UnsafeEvidenceRoot);
        }
        reject_store_ancestors(&canonical)?;
        Ok(Self { path: canonical })
    }

    pub(crate) fn pr03c_case(&self, case: &str) -> Result<Pr03cCase> {
        validate_case(case)?;
        Ok(Pr03cCase::new(&self.path, case))
    }

    pub(crate) fn pr03c_set(&self, set: &str) -> Result<Pr03cSet> {
        validate_case(set)?;
        Ok(Pr03cSet::new(&self.path, set))
    }

    pub(crate) fn ensure_layout(&self) -> Result<()> {
        fs::create_dir_all(self.path.join("fixtures")).map_err(|_| EvidenceError::Io)?;
        fs::create_dir_all(self.path.join("artifacts")).map_err(|_| EvidenceError::Io)?;
        Ok(())
    }

    pub(crate) fn run_v2_foundation(&self) -> Result<FoundationSummary> {
        self.foundation_layout()?;
        v2_io::run_foundation(self)
    }

    pub(crate) fn run_v1_success_smoke(&self) -> Result<()> {
        self.foundation_layout()?;
        v1_smoke::run_success(self)
    }

    pub(crate) fn run_v1_pressure_smoke(
        &self,
        kind: och_runtime::__m03_pr03e_native_harness::InjectedErrorKind,
        partial: bool,
    ) -> Result<()> {
        self.foundation_layout()?;
        v1_smoke::run_pressure(self, kind, partial)
    }

    fn foundation_layout(&self) -> Result<()> {
        self.validate_parent_inventory()?;
        self.validate_cases_empty()?;
        for name in ["cases", "control"] {
            fs::create_dir_all(self.path.join(name)).map_err(|_| EvidenceError::Io)?;
        }
        self.validate_parent_inventory()?;
        self.validate_cases_empty()
    }

    fn validate_parent_inventory(&self) -> Result<()> {
        let mut count = 0_usize;
        for entry in fs::read_dir(&self.path).map_err(|_| EvidenceError::Io)? {
            let entry = entry.map_err(|_| EvidenceError::Io)?;
            count = count.checked_add(1).ok_or(EvidenceError::Bounds)?;
            if count > MAX_PARENT_ENTRIES
                || !entry.file_type().map_err(|_| EvidenceError::Io)?.is_dir()
            {
                return Err(EvidenceError::UnsafeInventory);
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| EvidenceError::UnsafeInventory)?;
            if !PARENT_DIRECTORIES.contains(&name.as_str()) {
                return Err(EvidenceError::UnsafeInventory);
            }
        }
        Ok(())
    }

    fn validate_cases_empty(&self) -> Result<()> {
        let cases = self.path.join("cases");
        if !cases.exists() {
            return Ok(());
        }
        let mut entries = fs::read_dir(cases).map_err(|_| EvidenceError::UnsafeInventory)?;
        if entries
            .next()
            .transpose()
            .map_err(|_| EvidenceError::Io)?
            .is_some()
        {
            return Err(EvidenceError::UnsafeInventory);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn artifact_partials_absent(&self) -> Result<bool> {
        for entry in fs::read_dir(self.path.join("artifacts")).map_err(|_| EvidenceError::Io)? {
            let entry = entry.map_err(|_| EvidenceError::Io)?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| EvidenceError::UnsafeInventory)?;
            if name.ends_with(".partial") {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn validate_case(case: &str) -> Result<()> {
    if valid_case_name(case) {
        Ok(())
    } else {
        Err(EvidenceError::Usage)
    }
}

fn nearest_existing(path: &Path) -> Result<PathBuf> {
    let mut candidate = path;
    while !candidate.exists() {
        candidate = candidate
            .parent()
            .ok_or(EvidenceError::UnsafeEvidenceRoot)?;
    }
    fs::canonicalize(candidate).map_err(|_| EvidenceError::Io)
}

fn reject_store_ancestors(path: &Path) -> Result<()> {
    for ancestor in path.ancestors() {
        if ancestor.is_dir() && contains_recognized_store_name(ancestor)? {
            return Err(EvidenceError::UnsafeEvidenceRoot);
        }
    }
    Ok(())
}

fn contains_recognized_store_name(directory: &Path) -> Result<bool> {
    let entries = fs::read_dir(directory).map_err(|_| EvidenceError::Io)?;
    for entry in entries {
        let entry = entry.map_err(|_| EvidenceError::Io)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Ok(true);
        };
        if is_recognized_store_name(name) || is_authority_like_name(name) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_authority_like_name(name: &str) -> bool {
    [
        "store-format-",
        "manifest-",
        "generation-catalog-",
        "journal-rotation-",
        "active-journal-",
        "sealed-journal-",
        "native-segment-",
        "series-registry-",
        "retry-state-",
        "recovery-state-",
        "store-v1.lock",
    ]
    .into_iter()
    .any(|prefix| name.starts_with(prefix))
}

fn is_recognized_store_name(name: &str) -> bool {
    name == och_store::STORE_LOCK_FILE_NAME
        || name == och_store::STORE_FORMAT_FILE_NAME
        || name == och_store::STORE_FORMAT_STAGING_FILE_NAME
        || name == och_store::ACTIVE_JOURNAL_FILE_NAME
        || name == och_store::ACTIVE_CHECKPOINT_FILE_NAME
        || name == och_store::ROTATION_INTENT_FILE_NAME
        || name == och_store::SEALED_JOURNAL_STAGING_FILE_NAME
        || name == och_store::GENERATION_CATALOG_STAGING_FILE_NAME
        || name == och_store::MANIFEST_STAGING_FILE_NAME
        || name == och_store::REGISTRY_STAGING_FILE_NAME
        || name == och_store::RETRY_STAGING_FILE_NAME
        || name == och_store::RECOVERY_STAGING_FILE_NAME
        || name == "store-format-v2.och"
        || name == "store-format-v2.staging"
        || name == "journal-rotation-v2.intent"
        || name == "native-segment-v1.staging"
        || name == "generation-catalog-v2.staging"
        || name == "manifest-v2.staging"
        || recognized_generation_name(name)
        || recognized_slot_name(name)
}

fn recognized_generation_name(name: &str) -> bool {
    for (prefix, suffix) in [
        ("sealed-journal-v1-g", ".och"),
        ("native-segment-v1-g", ".och"),
        ("active-journal-v1-g", ".och"),
        ("active-journal-v1-g", ".checkpoint"),
    ] {
        if let Some(digits) = name
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
            && digits.len() == 20
            && digits.bytes().all(|byte| byte.is_ascii_digit())
        {
            return true;
        }
    }
    false
}

fn recognized_slot_name(name: &str) -> bool {
    for (prefix, count) in [
        ("manifest-v1-slot-", 2_u8),
        ("manifest-v2-slot-", 2),
        ("generation-catalog-v1-slot-", 3),
        ("generation-catalog-v2-slot-", 3),
        ("series-registry-v1-slot-", 3),
        ("retry-state-v1-slot-", 3),
        ("recovery-state-v1-slot-", 3),
    ] {
        for slot in 0..count {
            if name == format!("{prefix}{slot}.och") {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn v1_and_future_v2_authority_names_are_recognized_but_evidence_names_are_not() {
        for name in [
            "store-format-v1.och",
            "store-format-v2.och",
            "manifest-v2-slot-1.och",
            "native-segment-v1-g00000000000000000001.och",
            "sealed-journal-v1-g00000000000000000064.och",
            "active-journal-v1-g00000000000000000002.och",
            "active-journal-v1-g00000000000000000002.checkpoint",
            "series-registry-v1.staging",
            "retry-state-v1.staging",
            "recovery-state-v1.staging",
        ] {
            assert!(is_recognized_store_name(name), "{name}");
        }
        for wrong_successor_checkpoint in [
            "active-checkpoint-v1-g00000000000000000002-slot-0.och",
            "active-checkpoint-v1-g00000000000000000002-slot-1.och",
        ] {
            assert!(!is_recognized_store_name(wrong_successor_checkpoint));
        }
        assert!(!is_recognized_store_name("min.ochseg01-evidence"));
        assert!(!is_recognized_store_name("min.raw-journal-v1-evidence"));
    }

    #[test]
    fn authority_like_unknown_ancestor_refuses_before_child_creation() {
        let parent = std::env::temp_dir().join(format!(
            "och-v2-evidence-root-hostile-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&parent);
        fs::create_dir(&parent).expect("create hostile parent");
        fs::write(parent.join("manifest-v9-unknown.och"), []).expect("write unknown authority");
        let child = parent.join("evidence");
        assert!(matches!(
            EvidenceRoot::prepare(&child),
            Err(EvidenceError::UnsafeEvidenceRoot)
        ));
        assert!(!child.exists());
        fs::remove_dir_all(parent).expect("remove hostile parent");
    }

    #[test]
    fn nonempty_cases_inventory_refuses_before_foundation_mutation() {
        for (label, names, directory_entry) in [
            ("v1", vec!["store-format-v1.och"], false),
            ("v2", vec!["store-format-v2.och"], false),
            (
                "mixed",
                vec!["store-format-v1.och", "store-format-v2.och"],
                false,
            ),
            ("markerless", vec!["series-registry-v1-slot-0.och"], false),
            ("unknown", vec!["unknown.bin"], false),
            ("non-file", vec!["unknown-directory"], true),
        ] {
            let parent = std::env::temp_dir().join(format!(
                "och-v2-evidence-cases-hostile-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&parent);
            fs::create_dir(&parent).expect("create hostile cases parent");
            let evidence = parent.join("evidence");
            let root =
                EvidenceRoot::prepare(&evidence).expect("prepare initially safe evidence root");
            fs::create_dir(evidence.join("cases")).expect("create cases directory");
            for name in names {
                let entry = evidence.join("cases").join(name);
                if directory_entry {
                    fs::create_dir(entry).expect("create hostile directory entry");
                } else {
                    fs::write(entry, []).expect("create hostile file entry");
                }
            }
            assert!(matches!(
                root.foundation_layout(),
                Err(EvidenceError::UnsafeInventory)
            ));
            assert!(!evidence.join("control").exists());
            fs::remove_dir_all(parent).expect("remove hostile cases parent");
        }
    }

    #[test]
    fn reviewed_root_api_inventory_has_no_path_projection_or_extraction_trait() {
        let root_source = include_str!("root.rs");
        let pr03c_source = include_str!("root/pr03c_io.rs");
        let mut actual = reviewed_api("EvidenceRoot", root_source);
        actual.extend(reviewed_api("Pr03cIo", pr03c_source));
        actual.sort();

        let mut expected = vec![
            ("EvidenceRoot", "artifact_partials_absent", "Result<bool>"),
            ("EvidenceRoot", "ensure_layout", "Result<()>"),
            ("EvidenceRoot", "open", "Result<Self>"),
            ("EvidenceRoot", "pr03c_case", "Result<Pr03cCase>"),
            ("EvidenceRoot", "pr03c_set", "Result<Pr03cSet>"),
            ("EvidenceRoot", "prepare", "Result<Self>"),
            ("EvidenceRoot", "run_v1_pressure_smoke", "Result<()>"),
            ("EvidenceRoot", "run_v1_success_smoke", "Result<()>"),
            (
                "EvidenceRoot",
                "run_v2_foundation",
                "Result<FoundationSummary>",
            ),
            ("Pr03cIo", "create_raw_partial", "Result<File>"),
            ("Pr03cIo", "create_segment_partial", "Result<File>"),
            ("Pr03cIo", "open", "Result<File>"),
            ("Pr03cIo", "open_fixture_meta", "Result<File>"),
            ("Pr03cIo", "open_raw", "Result<File>"),
            ("Pr03cIo", "open_segment", "Result<File>"),
            ("Pr03cIo", "open_segment_identity", "Result<File>"),
            ("Pr03cIo", "publish_raw", "Result<()>"),
            ("Pr03cIo", "publish_segment", "Result<()>"),
            ("Pr03cIo", "remove_raw_partial", "Result<()>"),
            ("Pr03cIo", "remove_segment_partial", "Result<()>"),
            ("Pr03cIo", "replace_raw_from", "Result<()>"),
            ("Pr03cIo", "replace_segment", "Result<()>"),
            ("Pr03cIo", "reset_fixture", "Result<()>"),
            ("Pr03cIo", "reset_segment", "Result<()>"),
            ("Pr03cIo", "write", "Result<()>"),
            ("Pr03cIo", "write_fixture_meta", "Result<()>"),
            ("Pr03cIo", "write_segment_identity", "Result<()>"),
        ]
        .into_iter()
        .map(|(owner, name, result)| (owner.to_owned(), name.to_owned(), result.to_owned()))
        .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(actual, expected);

        for forbidden in [
            ["impl", "AsRef<"].join(" "),
            ["impl", "std::ops::AsRef<"].join(" "),
            ["impl", "Deref"].join(" "),
            ["impl", "std::ops::Deref"].join(" "),
            ["fn", "as_path("].join(" "),
            ["fn", "into_path("].join(" "),
        ] {
            assert!(!root_source.contains(&forbidden), "{forbidden}");
            assert!(!pr03c_source.contains(&forbidden), "{forbidden}");
        }
        assert!(root_source.contains("pub(crate) struct EvidenceRoot {\n    path: PathBuf,\n}"));
        assert!(!pr03c_source.contains("Debug"));
    }

    fn reviewed_api(owner: &str, source: &str) -> Vec<(String, String, String)> {
        let mut signatures = Vec::new();
        let mut lines = source.lines();
        let visibility = ["pub(crate)", "fn"].join(" ");
        while let Some(line) = lines.next() {
            if !line.contains(&visibility) {
                continue;
            }
            let mut signature = line.trim().to_owned();
            while !signature.contains('{') {
                let continuation = lines.next().expect("complete reviewed API signature");
                signature.push(' ');
                signature.push_str(continuation.trim());
            }
            let declaration = signature
                .split_once('{')
                .map(|(declaration, _)| declaration)
                .expect("reviewed function body");
            let name = declaration
                .split_once("fn ")
                .and_then(|(_, rest)| rest.split_once('('))
                .map(|(name, _)| name)
                .expect("reviewed function name");
            let return_type = declaration
                .split_once("->")
                .map_or("()", |(_, value)| value.trim());
            assert!(
                matches!((owner, name), ("EvidenceRoot", "prepare" | "open"))
                    || (!declaration.contains("Path")
                        && !declaration.contains("OsStr")
                        && !declaration.contains("FnOnce")
                        && !declaration.contains("FnMut")
                        && !declaration.contains("Fn(")),
                "path-bearing reviewed API: {declaration}"
            );
            signatures.push((owner.to_owned(), name.to_owned(), return_type.to_owned()));
        }
        signatures
    }
}
