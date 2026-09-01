use crate::error::{EvidenceError, Result};
use crate::sha256::{Sha256, hex};
use std::collections::BTreeSet;

pub(super) const MAX_V2_INVENTORY_ENTRIES: usize = 156;
pub(super) const MAX_ARTIFACT_BYTES: u64 = 637_993_128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ArtifactFingerprint {
    pub(super) name: String,
    pub(super) kind: &'static str,
    pub(super) logical_length: u64,
    pub(super) sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct InventoryFingerprint {
    pub(super) aggregate_sha256: String,
    pub(super) artifacts: Vec<ArtifactFingerprint>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InventoryClass {
    Empty,
    CurrentV1,
    ReviewedV2,
    Mixed,
    MarkerlessAuthority,
    Unknown,
    NonFile,
    Excessive,
}

pub(super) fn classify_entries(entries: Vec<(String, bool)>) -> Result<InventoryClass> {
    let mut names = BTreeSet::new();
    let mut non_file = false;
    for (name, is_file) in entries {
        if !is_file {
            non_file = true;
        }
        if !names.insert(name) {
            return Err(EvidenceError::UnsafeInventory);
        }
        if names.len() > MAX_V2_INVENTORY_ENTRIES {
            return Ok(InventoryClass::Excessive);
        }
    }
    if non_file {
        return Ok(InventoryClass::NonFile);
    }
    if names.is_empty() {
        return Ok(InventoryClass::Empty);
    }
    let has_v1 = names.iter().any(|name| is_v1_epoch_name(name));
    let has_v2 = names.iter().any(|name| is_v2_epoch_name(name));
    if has_v1 && has_v2 {
        return Ok(InventoryClass::Mixed);
    }
    if names.iter().any(|name| !is_recognized_name(name)) {
        return Ok(InventoryClass::Unknown);
    }
    if has_v1 {
        return Ok(InventoryClass::CurrentV1);
    }
    if has_v2 {
        return Ok(InventoryClass::ReviewedV2);
    }
    Ok(InventoryClass::MarkerlessAuthority)
}

pub(super) fn finish_fingerprint(
    mut artifacts: Vec<ArtifactFingerprint>,
) -> Result<InventoryFingerprint> {
    if artifacts.len() > MAX_V2_INVENTORY_ENTRIES {
        return Err(EvidenceError::Bounds);
    }
    let mut names = BTreeSet::new();
    for artifact in &artifacts {
        if artifact.name.contains('/')
            || artifact.name.contains('\\')
            || artifact.name == "."
            || artifact.name == ".."
            || artifact.kind != "FILE"
            || artifact.logical_length > MAX_ARTIFACT_BYTES
            || !names.insert(artifact.name.as_str())
        {
            return Err(EvidenceError::UnsafeInventory);
        }
    }
    artifacts.sort_by(|left, right| left.name.cmp(&right.name));
    let mut hash = Sha256::new();
    for artifact in &artifacts {
        let name_length = u16::try_from(artifact.name.len()).map_err(|_| EvidenceError::Bounds)?;
        hash.update(&name_length.to_be_bytes())?;
        hash.update(artifact.name.as_bytes())?;
        hash.update(&[1])?;
        hash.update(&artifact.logical_length.to_be_bytes())?;
        hash.update(&crate::sha256::parse_hex(&artifact.sha256)?)?;
    }
    Ok(InventoryFingerprint {
        aggregate_sha256: hex(&hash.finish()?),
        artifacts,
    })
}

pub(super) fn canonical_inventory_names() -> Result<Vec<String>> {
    let mut names = vec![
        "store-format-v2.och".to_owned(),
        "store-v1.lock".to_owned(),
        "active-journal-v1.och".to_owned(),
        "active-journal-v1.checkpoint".to_owned(),
        "active-journal-v1-g00000000000000000002.och".to_owned(),
        "active-journal-v1-g00000000000000000002.checkpoint".to_owned(),
        "journal-rotation-v2.intent".to_owned(),
        "sealed-journal-v1.staging".to_owned(),
        "native-segment-v1.staging".to_owned(),
        "generation-catalog-v2.staging".to_owned(),
        "manifest-v2.staging".to_owned(),
        "generation-catalog-v2-slot-0.och".to_owned(),
        "generation-catalog-v2-slot-1.och".to_owned(),
        "generation-catalog-v2-slot-2.och".to_owned(),
        "manifest-v2-slot-0.och".to_owned(),
        "manifest-v2-slot-1.och".to_owned(),
        "series-registry-v1.staging".to_owned(),
        "series-registry-v1-slot-0.och".to_owned(),
        "series-registry-v1-slot-1.och".to_owned(),
        "series-registry-v1-slot-2.och".to_owned(),
        "retry-state-v1.staging".to_owned(),
        "retry-state-v1-slot-0.och".to_owned(),
        "retry-state-v1-slot-1.och".to_owned(),
        "retry-state-v1-slot-2.och".to_owned(),
        "recovery-state-v1.staging".to_owned(),
        "recovery-state-v1-slot-0.och".to_owned(),
        "recovery-state-v1-slot-1.och".to_owned(),
        "recovery-state-v1-slot-2.och".to_owned(),
    ];
    names
        .try_reserve_exact(128)
        .map_err(|_| EvidenceError::Bounds)?;
    for generation in 1..=64_u64 {
        names.push(format!("sealed-journal-v1-g{generation:020}.och"));
        names.push(format!("native-segment-v1-g{generation:020}.och"));
    }
    names.sort();
    if names.len() != MAX_V2_INVENTORY_ENTRIES
        || names.iter().collect::<BTreeSet<_>>().len() != names.len()
        || names.iter().any(|name| !is_recognized_name(name))
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(names)
}

pub(super) fn is_recognized_name(name: &str) -> bool {
    matches!(
        name,
        "store-format-v1.och"
            | "store-format-v1.staging"
            | "store-format-v2.och"
            | "store-format-v2.staging"
            | "store-v1.lock"
            | "active-journal-v1.och"
            | "active-journal-v1.checkpoint"
            | "journal-rotation-v1.intent"
            | "journal-rotation-v2.intent"
            | "sealed-journal-v1.staging"
            | "native-segment-v1.staging"
            | "generation-catalog-v1.staging"
            | "generation-catalog-v2.staging"
            | "manifest-v1.staging"
            | "manifest-v2.staging"
            | "series-registry-v1.staging"
            | "retry-state-v1.staging"
            | "recovery-state-v1.staging"
    ) || fixed_slot(name)
        || generation_name(name)
}

fn fixed_slot(name: &str) -> bool {
    [
        ("manifest-v1-slot-", 2_u8),
        ("manifest-v2-slot-", 2),
        ("generation-catalog-v1-slot-", 3),
        ("generation-catalog-v2-slot-", 3),
        ("series-registry-v1-slot-", 3),
        ("retry-state-v1-slot-", 3),
        ("recovery-state-v1-slot-", 3),
    ]
    .into_iter()
    .any(|(prefix, count)| (0..count).any(|slot| name == format!("{prefix}{slot}.och")))
}

fn generation_name(name: &str) -> bool {
    [
        ("sealed-journal-v1-g", ".och", 64_u64),
        ("native-segment-v1-g", ".och", 64),
        ("active-journal-v1-g", ".och", 65),
        ("active-journal-v1-g", ".checkpoint", 65),
    ]
    .into_iter()
    .any(|(prefix, suffix, maximum)| {
        name.strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
            .filter(|digits| digits.len() == 20 && digits.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|digits| digits.parse::<u64>().ok())
            .is_some_and(|generation| (1..=maximum).contains(&generation))
    })
}

fn is_v1_epoch_name(name: &str) -> bool {
    name.starts_with("store-format-v1")
        || name.starts_with("manifest-v1")
        || name.starts_with("generation-catalog-v1")
        || name == "journal-rotation-v1.intent"
}

fn is_v2_epoch_name(name: &str) -> bool {
    name.starts_with("store-format-v2")
        || name.starts_with("manifest-v2")
        || name.starts_with("generation-catalog-v2")
        || name == "journal-rotation-v2.intent"
        || name.starts_with("native-segment-v1")
}

#[cfg(test)]
mod tests {
    use super::super::{V2StoreChild, classify, fingerprint, run_child};
    use super::*;
    use crate::root::EvidenceRoot;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    struct Temp {
        parent: PathBuf,
        root: EvidenceRoot,
    }

    impl Temp {
        fn new(label: &str) -> Self {
            let parent = std::env::temp_dir().join(format!(
                "och-v2-harness-inventory-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&parent);
            fs::create_dir(&parent).expect("create temporary inventory parent");
            let root = EvidenceRoot::prepare(&parent.join("evidence"))
                .expect("prepare inventory evidence root");
            root.foundation_layout().expect("prepare inventory layout");
            Self { parent, root }
        }

        fn run<T>(&self, operation: impl FnOnce(&V2StoreChild) -> Result<T>) -> Result<T> {
            run_child(&self.root, operation)
        }
    }

    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.parent);
        }
    }

    #[test]
    fn canonical_name_oracle_is_exactly_one_hundred_fifty_six() {
        let names = canonical_inventory_names().expect("canonical inventory names");
        assert_eq!(names.len(), 156);
        assert!(names.iter().all(|name| is_recognized_name(name)));
        assert!(!is_recognized_name(
            "native-segment-v1-g00000000000000000065.och"
        ));
    }

    #[test]
    fn compact_canonical_inventory_accepts_156_and_refuses_157_before_fingerprinting() {
        let temp = Temp::new("canonical-bound");
        temp.run(|child| {
            for name in canonical_inventory_names()? {
                fs::write(child.path().join(name), []).map_err(|_| EvidenceError::Io)?;
            }
            assert_eq!(classify(child)?, InventoryClass::ReviewedV2);
            assert_eq!(
                fingerprint(child)?.artifacts.len(),
                MAX_V2_INVENTORY_ENTRIES
            );
            fs::write(child.path().join("unknown-157"), []).map_err(|_| EvidenceError::Io)?;
            assert_eq!(classify(child)?, InventoryClass::Excessive);
            assert!(fingerprint(child).is_err());
            Ok(())
        })
        .expect("classify compact canonical inventory");
    }

    #[test]
    fn v1_v2_mixed_markerless_unknown_non_file_and_excessive_refuse_closed() {
        for (label, entries, expected) in [
            ("v1", vec!["store-format-v1.och"], InventoryClass::CurrentV1),
            (
                "v2",
                vec!["store-format-v2.och"],
                InventoryClass::ReviewedV2,
            ),
            (
                "mixed",
                vec!["store-format-v1.och", "store-format-v2.och"],
                InventoryClass::Mixed,
            ),
            (
                "markerless",
                vec!["series-registry-v1-slot-0.och"],
                InventoryClass::MarkerlessAuthority,
            ),
            ("unknown", vec!["unknown.bin"], InventoryClass::Unknown),
        ] {
            let temp = Temp::new(label);
            temp.run(|child| {
                for entry in entries {
                    fs::write(child.path().join(entry), []).map_err(|_| EvidenceError::Io)?;
                }
                assert_eq!(classify(child)?, expected);
                Ok(())
            })
            .expect("classify inventory");
        }
        let non_file = Temp::new("non-file");
        non_file
            .run(|child| {
                fs::create_dir(child.path().join("store-format-v2.och"))
                    .map_err(|_| EvidenceError::Io)?;
                assert_eq!(classify(child)?, InventoryClass::NonFile);
                Ok(())
            })
            .expect("classify non-file inventory");

        let excessive = Temp::new("excessive");
        excessive
            .run(|child| {
                for index in 0..157 {
                    fs::write(child.path().join(format!("unknown-{index}")), [])
                        .map_err(|_| EvidenceError::Io)?;
                }
                assert_eq!(classify(child)?, InventoryClass::Excessive);
                Ok(())
            })
            .expect("classify excessive inventory");
    }

    #[test]
    fn complete_file_fingerprint_is_stable_and_content_sensitive() {
        let temp = Temp::new("fingerprint");
        temp.run(|child| {
            fs::write(child.path().join("store-format-v2.och"), b"complete bytes")
                .map_err(|_| EvidenceError::Io)?;
            let first = fingerprint(child)?;
            let second = fingerprint(child)?;
            assert_eq!(first, second);
            fs::write(child.path().join("store-format-v2.och"), b"complete byteS")
                .map_err(|_| EvidenceError::Io)?;
            assert_ne!(fingerprint(child)?, first);
            Ok(())
        })
        .expect("fingerprint complete inventory");
    }
}
