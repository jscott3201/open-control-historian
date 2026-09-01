use crate::error::{EvidenceError, Result};
use crate::model::valid_case_name;
use std::fs;
use std::path::{Path, PathBuf};

const MAX_PARENT_ENTRIES: usize = 8;
const PARENT_DIRECTORIES: [&str; 5] = ["artifacts", "cases", "control", "fixtures", "reports"];

#[derive(Clone, Debug)]
pub(crate) struct EvidenceRoot {
    path: PathBuf,
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

    pub(crate) fn fixtures_dir(&self) -> PathBuf {
        self.path.join("fixtures")
    }

    pub(crate) fn artifacts_dir(&self) -> PathBuf {
        self.path.join("artifacts")
    }

    pub(crate) fn raw_path(&self, case: &str) -> Result<PathBuf> {
        validate_case(case)?;
        Ok(self
            .fixtures_dir()
            .join(format!("{case}.raw-journal-v1-evidence")))
    }

    pub(crate) fn fixture_meta_path(&self, case: &str) -> Result<PathBuf> {
        validate_case(case)?;
        Ok(self.fixtures_dir().join(format!("{case}.fixture-meta")))
    }

    pub(crate) fn segment_path(&self, case: &str) -> Result<PathBuf> {
        validate_case(case)?;
        Ok(self
            .artifacts_dir()
            .join(format!("{case}.ochseg01-evidence")))
    }

    pub(crate) fn segment_temp_path(&self, case: &str) -> Result<PathBuf> {
        validate_case(case)?;
        Ok(self
            .artifacts_dir()
            .join(format!("{case}.ochseg01-evidence.partial")))
    }

    pub(crate) fn segment_identity_path(&self, case: &str) -> Result<PathBuf> {
        validate_case(case)?;
        Ok(self
            .artifacts_dir()
            .join(format!("{case}.segment-identity")))
    }

    pub(crate) fn set_path(&self, set: &str) -> Result<PathBuf> {
        validate_case(set)?;
        Ok(self.fixtures_dir().join(format!("{set}.fixture-set")))
    }

    pub(crate) fn ensure_layout(&self) -> Result<()> {
        fs::create_dir_all(self.fixtures_dir()).map_err(|_| EvidenceError::Io)?;
        fs::create_dir_all(self.artifacts_dir()).map_err(|_| EvidenceError::Io)?;
        Ok(())
    }

    pub(crate) fn foundation_layout(&self) -> Result<()> {
        self.validate_parent_inventory()?;
        for name in ["cases", "control"] {
            fs::create_dir_all(self.path.join(name)).map_err(|_| EvidenceError::Io)?;
        }
        self.validate_parent_inventory()
    }

    pub(crate) fn create_store_child(&self, name: &str) -> Result<PathBuf> {
        validate_case(name)?;
        self.validate_parent_inventory()?;
        let cases = self.path.join("cases");
        fs::create_dir_all(&cases).map_err(|_| EvidenceError::Io)?;
        let child = cases.join(name);
        if child.exists() {
            return Err(EvidenceError::UnsafeInventory);
        }
        reject_store_ancestors(&cases)?;
        fs::create_dir(&child).map_err(|_| EvidenceError::Io)?;
        Ok(child)
    }

    pub(crate) fn dispose_store_child(&self, child: &Path) -> Result<()> {
        let cases = fs::canonicalize(self.path.join("cases")).map_err(|_| EvidenceError::Io)?;
        let canonical = fs::canonicalize(child).map_err(|_| EvidenceError::Io)?;
        if canonical.parent() != Some(cases.as_path()) {
            return Err(EvidenceError::UnsafeInventory);
        }
        fs::remove_dir_all(canonical).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn validate_parent_inventory(&self) -> Result<()> {
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

    #[cfg(test)]
    pub(crate) fn path_for_test(&self) -> String {
        self.path.to_string_lossy().into_owned()
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
}
