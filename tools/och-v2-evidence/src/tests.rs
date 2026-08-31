use super::*;
use crate::error::EvidenceError;
use crate::fixture::{
    admission_for_test, bound_probe, product_representative_admission_for_test, write_raw_fixture,
};
use crate::ledger::{MAX_FRAME_BYTES, SCRATCH_BYTES, active_controlled_bytes};
use crate::model::FixtureMeta;
use crate::stream::test_support::repair_segment_checksum;
use och_store::{
    ActiveJournalLimits, ActiveJournalOpenMode, ManifestStore, ManifestStoreConfig,
    PreparedAdmissionV1, RegistryPersistenceOptions, RetryPersistenceOptions,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new(label: &str) -> Self {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "och-v2-evidence-{label}-{}-{sequence}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create test evidence directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn setup(case: &str, profile: &str) -> (TempDirectory, EvidenceRoot, FixtureMeta) {
    let directory = TempDirectory::new(case);
    let root = EvidenceRoot::prepare(directory.path()).expect("safe evidence root");
    let meta = write_raw_fixture(&root, case, 7, profile).expect("generate bounded fixture");
    (directory, root, meta)
}

#[test]
fn streaming_minimum_matches_independent_primitive_oracle_and_is_repeatable() {
    let (_directory, root, meta) = setup("oracle-min", "min");
    let first = stream::build(&root, &meta.case, false).expect("build minimum segment");
    let first_bytes =
        fs::read(root.segment_path(&meta.case).expect("segment path")).expect("read test segment");
    let raw = fs::read(root.raw_path(&meta.case).expect("raw path")).expect("read test raw");
    let expected = primitive_min_oracle(&meta, &raw);
    assert_eq!(first_bytes, expected);
    let repeated = stream::build(&root, &meta.case, false).expect("repeat minimum segment");
    assert_eq!(first.identity, repeated.identity);
    assert_eq!(
        fs::read(root.segment_path(&meta.case).expect("segment path")).expect("read repeated"),
        expected
    );
    let validated = stream::validate(&root, &meta.case).expect("stream validate minimum");
    assert_eq!(validated.identity, first.identity);
    assert!(validated.source_stats.max_read_request <= MAX_FRAME_BYTES);
    assert!(validated.segment_stats.max_read_request <= MAX_FRAME_BYTES);
    assert!(validated.max_frame_buffer_bytes <= MAX_FRAME_BYTES);
    assert!(validated.max_reencode_buffer_bytes <= MAX_FRAME_BYTES);
    assert_eq!(validated.external_workspace_bytes, 0);
    assert!(
        validated.metadata_ledger_bytes
            >= u64::try_from(SCRATCH_BYTES).expect("small metadata ledger")
    );
    assert_eq!(validated.controlled_bytes_after, active_controlled_bytes());
    assert_eq!(active_controlled_bytes(), 0);
}

#[test]
fn streaming_bytes_match_current_public_manifest_store_builder_evidence() {
    let store_directory = TempDirectory::new("product-store");
    let store_id = och_core::StoreId::from_bytes(uuid_bytes(9_000))
        .expect("valid deterministic store identity");
    let limits = ActiveJournalLimits::new(
        och_store::MAX_ADMISSION_PAYLOAD_V1,
        och_store::MAX_ACTIVE_JOURNAL_BYTES,
        och_store::MAX_ACTIVE_JOURNAL_RECORDS,
    )
    .expect("maximum store limits");
    let registry = RegistryPersistenceOptions::new(och_core::SeriesRegistryLimits::new(1, 1))
        .expect("bounded registry options");
    let config = ManifestStoreConfig::new(
        store_directory.path().to_path_buf(),
        store_id,
        ActiveJournalOpenMode::CreateNew,
        limits,
        registry,
        RetryPersistenceOptions::new(1, 1).expect("bounded retry options"),
    )
    .expect("valid store config");
    let mut store = ManifestStore::open(config).expect("open product evidence store");
    let admission = admission_for_test(store_id, 11).expect("canonical no-change admission");
    let retry = admission.retry().clone();
    let declaration = admission.declaration().clone();
    store
        .register(
            declaration.series_id(),
            declaration.binding().clone(),
            declaration.payload().clone(),
            declaration.evidence().clone(),
        )
        .expect("register matching declaration");
    let sequence = store.next_append_sequence().expect("next product sequence");
    let frame = PreparedAdmissionV1::new(admission)
        .expect("prepare product admission")
        .into_frame(sequence)
        .expect("frame product admission");
    let end_offset = store.append(&frame).expect("append product evidence");
    store
        .sync_pending(&[och_store::PendingRetryOutcome::new(
            retry,
            sequence.get(),
            end_offset,
        )])
        .expect("durably commit product evidence");
    store.rotate().expect("seal product evidence generation");
    let candidate = store
        .build_segment_candidate_v1(1)
        .expect("build through existing public product bridge");
    let inspection = candidate.inspection();

    let evidence_directory = TempDirectory::new("product-compare");
    let root = EvidenceRoot::prepare(evidence_directory.path()).expect("safe comparison root");
    root.ensure_layout().expect("comparison layout");
    let raw_source = store_directory
        .path()
        .join("sealed-journal-v1-g00000000000000000001.och");
    let case = "product-compare";
    fs::copy(
        raw_source,
        root.raw_path(case).expect("comparison raw path"),
    )
    .expect("copy public source evidence outside store");
    let meta = FixtureMeta {
        case: case.to_owned(),
        seed: 11,
        store_id,
        journal_generation: inspection.source_journal_generation(),
        sequence_floor: inspection.sequence_floor(),
        sequence_cutoff: inspection.sequence_cutoff(),
        registry_generation: inspection.source_registry_generation(),
        source_length: inspection.source_artifact_length(),
        source_checksum: inspection.source_artifact_checksum(),
        frame_count: inspection.frame_count(),
        series_count: inspection.series_count(),
        observation_count: inspection.observation_count(),
    };
    fs::write(
        root.fixture_meta_path(case)
            .expect("comparison metadata path"),
        meta.encode(),
    )
    .expect("write comparison metadata");
    stream::build(&root, case, false).expect("stream-build copied public source");
    let streamed = fs::read(root.segment_path(case).expect("comparison segment path"))
        .expect("read streamed comparison");
    assert_eq!(streamed, candidate.bytes());
    drop(store);
}

#[test]
#[allow(clippy::too_many_lines)]
fn observation_bearing_multi_series_bytes_match_current_public_product_evidence() {
    const CASE: &str = "product-observed-compare";
    const SEED: u64 = 17;
    let evidence_directory = TempDirectory::new("product-observed-source");
    let root = EvidenceRoot::prepare(evidence_directory.path()).expect("safe evidence root");
    let generated = write_raw_fixture(&root, CASE, SEED, "product-representative")
        .expect("generate observed multi-series source");
    assert_eq!(generated.frame_count, 4);
    assert_eq!(generated.series_count, 2);
    assert_eq!(generated.observation_count, 8);

    let store_directory = TempDirectory::new("product-observed-store");
    let limits = ActiveJournalLimits::new(
        och_store::MAX_ADMISSION_PAYLOAD_V1,
        och_store::MAX_ACTIVE_JOURNAL_BYTES,
        och_store::MAX_ACTIVE_JOURNAL_RECORDS,
    )
    .expect("maximum store limits");
    let registry = RegistryPersistenceOptions::new(och_core::SeriesRegistryLimits::new(2, 2))
        .expect("two-series registry options");
    let config = ManifestStoreConfig::new(
        store_directory.path().to_path_buf(),
        generated.store_id,
        ActiveJournalOpenMode::CreateNew,
        limits,
        registry,
        RetryPersistenceOptions::new(4, 4).expect("bounded retry options"),
    )
    .expect("valid observed product config");
    let mut store = ManifestStore::open(config).expect("open observed product store");
    for frame_index in 0..2 {
        let admission =
            product_representative_admission_for_test(generated.store_id, SEED, frame_index)
                .expect("representative registration admission");
        let declaration = admission.declaration();
        store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register representative series");
    }
    let mut pending = Vec::with_capacity(4);
    for frame_index in 0..4 {
        let admission =
            product_representative_admission_for_test(generated.store_id, SEED, frame_index)
                .expect("representative append admission");
        let retry = admission.retry().clone();
        let sequence = store.next_append_sequence().expect("next product sequence");
        let frame = PreparedAdmissionV1::new(admission)
            .expect("prepare representative admission")
            .into_frame(sequence)
            .expect("frame representative admission");
        let end_offset = store.append(&frame).expect("append representative frame");
        pending.push(och_store::PendingRetryOutcome::new(
            retry,
            sequence.get(),
            end_offset,
        ));
    }
    store
        .sync_pending(&pending)
        .expect("durably commit representative source");
    store.rotate().expect("seal representative generation");
    let candidate = store
        .build_segment_candidate_v1(1)
        .expect("build representative product candidate");
    let inspection = candidate.inspection();
    assert_eq!(inspection.frame_count(), 4);
    assert_eq!(inspection.series_count(), 2);
    assert_eq!(inspection.observation_count(), 8);
    let sealed = fs::read(
        store_directory
            .path()
            .join("sealed-journal-v1-g00000000000000000001.och"),
    )
    .expect("read representative sealed source");
    assert_eq!(
        sealed,
        fs::read(root.raw_path(CASE).expect("representative raw path"))
            .expect("read generated representative source")
    );
    let product_meta = FixtureMeta {
        case: CASE.to_owned(),
        seed: SEED,
        store_id: generated.store_id,
        journal_generation: inspection.source_journal_generation(),
        sequence_floor: inspection.sequence_floor(),
        sequence_cutoff: inspection.sequence_cutoff(),
        registry_generation: inspection.source_registry_generation(),
        source_length: inspection.source_artifact_length(),
        source_checksum: inspection.source_artifact_checksum(),
        frame_count: inspection.frame_count(),
        series_count: inspection.series_count(),
        observation_count: inspection.observation_count(),
    };
    fs::write(
        root.fixture_meta_path(CASE)
            .expect("representative metadata path"),
        product_meta.encode(),
    )
    .expect("write product-rooted representative metadata");
    stream::build(&root, CASE, false).expect("stream-build representative source");
    let streamed = fs::read(
        root.segment_path(CASE)
            .expect("representative segment path"),
    )
    .expect("read streamed representative segment");
    assert_eq!(streamed, candidate.bytes());
}

#[test]
fn hostile_segment_classes_refuse_closed_without_unbounded_reads_or_temp_files() {
    let (_directory, root, meta) = setup("hostile-small", "test-small");
    stream::build(&root, &meta.case, false).expect("build hostile baseline");
    let segment_path = root.segment_path(&meta.case).expect("segment path");
    let canonical = fs::read(&segment_path).expect("read canonical segment");

    let mut variants = Vec::new();
    variants.push(canonical[..canonical.len() - 1].to_vec());
    let mut trailing = canonical.clone();
    trailing.push(0);
    variants.push(trailing);
    for offset in [8_usize, 12, 168, 32, 40, 48, 56, 80, 84, 88, 92, 104, 160] {
        let mut hostile = canonical.clone();
        hostile[offset] ^= 1;
        repair_segment_checksum(&mut hostile);
        variants.push(hostile);
    }
    let mut foreign_store_id = canonical.clone();
    foreign_store_id[31] ^= 1;
    repair_segment_checksum(&mut foreign_store_id);
    variants.push(foreign_store_id);
    let append_offset = usize::try_from(read_u64(&canonical, 128)).expect("append offset");
    let mut bad_append_coverage = canonical.clone();
    bad_append_coverage[append_offset + 7] ^= 1;
    repair_segment_checksum(&mut bad_append_coverage);
    variants.push(bad_append_coverage);
    let recent_offset = usize::try_from(read_u64(&canonical, 144)).expect("recent offset");
    let mut bad_recent_location = canonical.clone();
    bad_recent_location[recent_offset + 80] ^= 1;
    repair_segment_checksum(&mut bad_recent_location);
    variants.push(bad_recent_location);
    let block_offset = usize::try_from(read_u64(&canonical, 112)).expect("block offset");
    let mut corrupt_frame = canonical.clone();
    corrupt_frame[block_offset + och_store::JOURNAL_V1_FRAME_PREFIX_LEN] ^= 1;
    repair_segment_checksum(&mut corrupt_frame);
    variants.push(corrupt_frame);
    let mut bad_checksum = canonical.clone();
    let final_byte = bad_checksum.len() - 1;
    bad_checksum[final_byte] ^= 1;
    variants.push(bad_checksum);

    for hostile in variants {
        fs::write(&segment_path, hostile).expect("write hostile segment");
        let error = stream::validate(&root, &meta.case).expect_err("hostile segment must refuse");
        assert!(matches!(
            error,
            EvidenceError::InvalidSegment | EvidenceError::Bounds
        ));
        assert!(!error.to_string().contains(meta.case.as_str()));
        assert_eq!(active_controlled_bytes(), 0);
    }
    fs::write(&segment_path, &canonical).expect("restore canonical segment");
    stream::validate(&root, &meta.case).expect("restored segment validates");
    assert_no_partial_files(&root);
}

#[test]
fn repeated_hostile_and_sixty_four_pair_runs_drop_controlled_state() {
    let (_directory, root, meta) = setup("repeat-min", "min");
    stream::build(&root, &meta.case, false).expect("build repeat baseline");
    let segment_path = root.segment_path(&meta.case).expect("segment path");
    let canonical = fs::read(&segment_path).expect("read canonical segment");
    let mut hostile = canonical.clone();
    hostile[0] ^= 1;
    repair_segment_checksum(&mut hostile);
    for _ in 0..64 {
        fs::write(&segment_path, &hostile).expect("write repeated hostile segment");
        assert!(stream::validate(&root, &meta.case).is_err());
        assert_eq!(active_controlled_bytes(), 0);
    }
    fs::write(&segment_path, canonical).expect("restore repeat segment");
    let mut set = String::from("schema=och-v2-evidence-set-v1\n");
    for _ in 0..64 {
        set.push_str(&meta.case);
        set.push('\n');
    }
    fs::write(root.set_path("repeat-64").expect("set path"), set).expect("write set");
    run(&[
        "validate-set".to_owned(),
        "--root".to_owned(),
        root.path_for_test(),
        "--set".to_owned(),
        "repeat-64".to_owned(),
    ])
    .expect("sequentially validate 64 pairs");
    assert_eq!(active_controlled_bytes(), 0);
    assert_no_partial_files(&root);
}

#[test]
fn roots_containing_current_or_future_store_names_refuse_before_output() {
    let directory = TempDirectory::new("unsafe-root");
    for (index, recognized_name) in [
        "store-format-v2.och",
        "active-journal-v1-g00000000000000000002.checkpoint",
        "series-registry-v1.staging",
        "retry-state-v1.staging",
        "recovery-state-v1.staging",
    ]
    .into_iter()
    .enumerate()
    {
        let store_like = directory.path().join(format!("store-like-{index}"));
        fs::create_dir(&store_like).expect("create store-like test root");
        fs::write(store_like.join(recognized_name), []).expect("write recognized artifact name");
        assert_eq!(
            EvidenceRoot::open(&store_like).expect_err("store-like root must refuse"),
            EvidenceError::UnsafeEvidenceRoot,
            "{recognized_name}"
        );
        let child = store_like.join("evidence-child");
        assert_eq!(
            EvidenceRoot::prepare(&child).expect_err("child of store-like root must refuse"),
            EvidenceError::UnsafeEvidenceRoot,
            "{recognized_name}"
        );
        assert!(!child.exists());
    }
}

#[test]
fn prepare_root_cli_refuses_a_real_v1_store_without_mutation_and_store_reopens() {
    let store_directory = TempDirectory::new("prepare-root-product-store");
    let store_id = och_core::StoreId::from_bytes(uuid_bytes(91_000))
        .expect("valid deterministic store identity");
    let config = |mode| {
        ManifestStoreConfig::new(
            store_directory.path().to_path_buf(),
            store_id,
            mode,
            ActiveJournalLimits::new(
                och_store::MAX_ADMISSION_PAYLOAD_V1,
                och_store::MAX_ACTIVE_JOURNAL_BYTES,
                och_store::MAX_ACTIVE_JOURNAL_RECORDS,
            )
            .expect("maximum store limits"),
            RegistryPersistenceOptions::new(och_core::SeriesRegistryLimits::new(1, 1))
                .expect("bounded registry options"),
            RetryPersistenceOptions::new(1, 1).expect("bounded retry options"),
        )
        .expect("valid product store config")
    };
    let store = ManifestStore::open(config(ActiveJournalOpenMode::CreateNew))
        .expect("create valid V1 store");
    let before = direct_inventory(store_directory.path());
    let error = run(&[
        "prepare-root".to_owned(),
        "--root".to_owned(),
        store_directory.path().to_string_lossy().into_owned(),
    ])
    .expect_err("V1 store must refuse as evidence root");
    assert_eq!(error, EvidenceError::UnsafeEvidenceRoot);
    assert_eq!(direct_inventory(store_directory.path()), before);
    drop(store);
    let reopened = ManifestStore::open(config(ActiveJournalOpenMode::OpenExisting))
        .expect("unchanged V1 store reopens");
    drop(reopened);
}

#[test]
fn prepare_root_cli_creates_only_a_missing_safe_root() {
    let directory = TempDirectory::new("prepare-root-safe-parent");
    let safe_root = directory.path().join("new-evidence-root");
    run(&[
        "prepare-root".to_owned(),
        "--root".to_owned(),
        safe_root.to_string_lossy().into_owned(),
    ])
    .expect("prepare missing safe evidence root");
    assert!(safe_root.is_dir());
    assert_eq!(fs::read_dir(safe_root).expect("read safe root").count(), 0);
}

#[test]
fn oversized_fixture_metadata_refuses_before_pair_state_or_partial_output() {
    let (_directory, root, meta) = setup("oversized-meta", "min");
    fs::write(
        root.fixture_meta_path(&meta.case).expect("metadata path"),
        vec![b'x'; 2_049],
    )
    .expect("write oversized fixture metadata");
    assert_eq!(
        stream::build(&root, &meta.case, false).expect_err("oversized metadata must refuse"),
        EvidenceError::InvalidFixture
    );
    assert_eq!(active_controlled_bytes(), 0);
    assert_no_partial_files(&root);
}

#[test]
fn oversized_segment_identity_refuses_before_pair_state_or_partial_output() {
    let (_directory, root, meta) = setup("oversized-identity", "min");
    stream::build(&root, &meta.case, false).expect("build identity baseline");
    fs::write(
        root.segment_identity_path(&meta.case)
            .expect("identity path"),
        vec![b'x'; 513],
    )
    .expect("write oversized segment identity");
    assert_eq!(
        stream::validate(&root, &meta.case).expect_err("oversized identity must refuse"),
        EvidenceError::InvalidFixture
    );
    assert_eq!(active_controlled_bytes(), 0);
    assert_no_partial_files(&root);
}

#[test]
fn oversized_fixture_set_refuses_before_pair_state_or_partial_output() {
    let (_directory, root, meta) = setup("oversized-set", "min");
    stream::build(&root, &meta.case, false).expect("build set baseline");
    fs::write(
        root.set_path("oversized-set").expect("set path"),
        vec![b'x'; 8_193],
    )
    .expect("write oversized fixture set");
    assert_eq!(
        run(&[
            "validate-set".to_owned(),
            "--root".to_owned(),
            root.path_for_test(),
            "--set".to_owned(),
            "oversized-set".to_owned(),
        ])
        .expect_err("oversized set must refuse"),
        EvidenceError::Bounds
    );
    assert_eq!(active_controlled_bytes(), 0);
    assert_no_partial_files(&root);
}

#[test]
fn measurement_script_fences_before_reports_and_uses_strict_rss_target() {
    let script = include_str!("../../../scripts/measure-v2-evidence.sh");
    let build = script
        .find("cargo \"+${TOOLCHAIN}\" build")
        .expect("release build command");
    let prepare = script
        .find("\"${TOOL}\" prepare-root --root")
        .expect("evidence-root preparation command");
    let report_write = script
        .find("mkdir -p -- \"${REPORT_ROOT}\"")
        .expect("first report-root write");
    assert!(build < prepare && prepare < report_write);
    assert!(script.contains("rss[-1] < target"));
    assert!(!script.contains("rss[-1] <= target"));
}

#[test]
fn maximum_fixture_profiles_are_constructible_within_current_source_bounds() {
    let (max_byte_source, max_observation_source) = bound_probe().expect("probe maximum fixtures");
    assert_eq!(max_byte_source, och_store::MAX_ACTIVE_JOURNAL_BYTES);
    assert!(max_observation_source <= och_store::MAX_ACTIVE_JOURNAL_BYTES);
}

fn primitive_min_oracle(meta: &FixtureMeta, raw: &[u8]) -> Vec<u8> {
    assert_eq!(meta.frame_count, 1);
    assert_eq!(meta.series_count, 1);
    assert_eq!(meta.observation_count, 0);
    let frame = &raw[och_store::JOURNAL_V1_HEADER_LEN..];
    let decoded =
        och_store::decode_admission_frame_v1(frame, och_store::DecodeLimitsV1::maximum(), None)
            .expect("decode primitive-oracle source frame");
    let series_offset = 192_usize;
    let blocks_offset = series_offset + 64;
    let append_offset = blocks_offset + frame.len();
    let recent_offset = append_offset + 48;
    let artifact_length = recent_offset + 4;
    let mut output = vec![0_u8; artifact_length];
    output[..8].copy_from_slice(b"OCHSEG01");
    output[8..10].copy_from_slice(&1_u16.to_be_bytes());
    output[10..12].copy_from_slice(&192_u16.to_be_bytes());
    output[16..32].copy_from_slice(meta.store_id.as_bytes());
    put_u64(&mut output, 32, meta.journal_generation);
    put_u64(&mut output, 40, meta.sequence_floor);
    put_u64(&mut output, 48, meta.sequence_cutoff);
    put_u64(&mut output, 56, meta.registry_generation);
    put_u64(&mut output, 64, meta.source_length);
    put_u64(&mut output, 72, meta.source_length);
    output[80..84].copy_from_slice(&oracle_crc(raw).to_be_bytes());
    put_u32(&mut output, 84, 1);
    put_u32(&mut output, 88, 1);
    put_u64(&mut output, 96, series_offset as u64);
    put_u64(&mut output, 104, 64);
    put_u64(&mut output, 112, blocks_offset as u64);
    put_u64(&mut output, 120, frame.len() as u64);
    put_u64(&mut output, 128, append_offset as u64);
    put_u64(&mut output, 136, 48);
    put_u64(&mut output, 144, recent_offset as u64);
    put_u64(&mut output, 160, artifact_length as u64);
    output[series_offset..series_offset + 16]
        .copy_from_slice(decoded.declaration().series_id().as_bytes());
    put_u64(&mut output, series_offset + 16, blocks_offset as u64);
    put_u64(&mut output, series_offset + 24, frame.len() as u64);
    put_u32(&mut output, series_offset + 32, 1);
    put_u64(&mut output, series_offset + 40, recent_offset as u64);
    output[blocks_offset..append_offset].copy_from_slice(frame);
    put_u64(&mut output, append_offset, decoded.append_sequence());
    output[append_offset + 8..append_offset + 24]
        .copy_from_slice(decoded.declaration().series_id().as_bytes());
    put_u64(&mut output, append_offset + 24, blocks_offset as u64);
    put_u64(&mut output, append_offset + 32, frame.len() as u64);
    let checksum = oracle_crc(&output[..artifact_length - 4]);
    output[artifact_length - 4..].copy_from_slice(&checksum.to_be_bytes());
    output
}

fn oracle_crc(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("u64 field"))
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn uuid_bytes(number: u64) -> [u8; 16] {
    let suffix = number.to_be_bytes();
    [
        0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, suffix[2], suffix[3],
        suffix[4], suffix[5], suffix[6], suffix[7],
    ]
}

fn assert_no_partial_files(root: &EvidenceRoot) {
    let entries = fs::read_dir(root.artifacts_dir()).expect("read evidence artifacts");
    assert!(
        entries
            .filter_map(std::result::Result::ok)
            .all(|entry| { !entry.file_name().to_string_lossy().ends_with(".partial") })
    );
}

fn direct_inventory(directory: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(directory)
        .expect("read direct inventory")
        .map(|entry| {
            let entry = entry.expect("read inventory entry");
            assert!(entry.file_type().expect("read entry type").is_file());
            let name = entry
                .file_name()
                .into_string()
                .expect("store artifact name is UTF-8");
            let bytes = fs::read(entry.path()).expect("read store artifact bytes");
            (name, bytes)
        })
        .collect()
}
