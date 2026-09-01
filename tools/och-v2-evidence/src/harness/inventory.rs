use crate::error::{EvidenceError, Result};
use crate::sha256::{Sha256, hex};
use std::collections::BTreeSet;

pub(crate) const MAX_V2_INVENTORY_ENTRIES: usize = 156;
pub(crate) const MAX_ARTIFACT_BYTES: u64 = 637_993_128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactFingerprint {
    pub(crate) name: String,
    pub(crate) kind: &'static str,
    pub(crate) logical_length: u64,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InventoryFingerprint {
    pub(crate) aggregate_sha256: String,
    pub(crate) artifacts: Vec<ArtifactFingerprint>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InventoryClass {
    Empty,
    CurrentV1,
    ReviewedV2,
    Mixed,
    MarkerlessAuthority,
    Unknown,
    NonFile,
    Excessive,
}

pub(crate) fn classify_entries(entries: Vec<(String, bool)>) -> Result<InventoryClass> {
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

pub(crate) fn finish_fingerprint(
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

pub(crate) fn canonical_inventory_names() -> Result<Vec<String>> {
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

pub(crate) fn is_recognized_name(name: &str) -> bool {
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
    use super::*;
    use crate::harness::v2_io::{classify, fingerprint};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn temporary(label: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "och-v2-harness-inventory-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).expect("create temporary inventory");
        path
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
        let path = temporary("canonical-bound");
        for name in canonical_inventory_names().expect("canonical inventory names") {
            fs::write(path.join(name), []).expect("write compact canonical artifact");
        }
        assert_eq!(
            classify(&path).expect("classify canonical inventory"),
            InventoryClass::ReviewedV2
        );
        assert_eq!(
            fingerprint(&path)
                .expect("fingerprint canonical inventory")
                .artifacts
                .len(),
            MAX_V2_INVENTORY_ENTRIES
        );
        fs::write(path.join("unknown-157"), []).expect("write excessive artifact");
        assert_eq!(
            classify(&path).expect("classify excessive canonical inventory"),
            InventoryClass::Excessive
        );
        assert!(fingerprint(&path).is_err());
        fs::remove_dir_all(path).expect("remove canonical inventory");
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
            let path = temporary(label);
            for entry in entries {
                fs::write(path.join(entry), []).expect("write inventory entry");
            }
            assert_eq!(classify(&path).expect("classify inventory"), expected);
            fs::remove_dir_all(path).expect("remove inventory");
        }
        let non_file = temporary("non-file");
        fs::create_dir(non_file.join("store-format-v2.och")).expect("create hostile non-file");
        assert_eq!(
            classify(&non_file).expect("classify non-file"),
            InventoryClass::NonFile
        );
        fs::remove_dir_all(non_file).expect("remove non-file inventory");

        let excessive = temporary("excessive");
        for index in 0..157 {
            fs::write(excessive.join(format!("unknown-{index}")), []).expect("write excessive");
        }
        assert_eq!(
            classify(&excessive).expect("classify excessive"),
            InventoryClass::Excessive
        );
        fs::remove_dir_all(excessive).expect("remove excessive inventory");
    }

    #[test]
    fn complete_file_fingerprint_is_stable_and_content_sensitive() {
        let path = temporary("fingerprint");
        fs::write(path.join("store-format-v2.och"), b"complete bytes")
            .expect("write fingerprint file");
        let first = fingerprint(&path).expect("first fingerprint");
        let second = fingerprint(&path).expect("second fingerprint");
        assert_eq!(first, second);
        fs::write(path.join("store-format-v2.och"), b"complete byteS")
            .expect("change complete file");
        assert_ne!(fingerprint(&path).expect("changed fingerprint"), first);
        fs::remove_dir_all(path).expect("remove fingerprint inventory");
    }
}
