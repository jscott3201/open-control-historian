#![forbid(unsafe_code)]
//! Native Segment V1 exact-byte, hostile-parser, and read-only store evidence.

mod support;

use och_core::{ExactValue, SeriesRegistryLimits, ValueFamily};
use och_store::{
    ActiveJournalLimits, ActiveJournalOpenMode, AppendSequenceV1, ManifestStore,
    ManifestStoreConfig, PendingRetryOutcome, PreparedAdmissionV1, RegistryPersistenceOptions,
    RetryPersistenceOptions, SegmentV1Error, parse_segment_v1,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use support::segment_oracle;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "och-segment-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create unique segment fixture directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn config(directory: &TestDirectory, mode: ActiveJournalOpenMode) -> ManifestStoreConfig {
    ManifestStoreConfig::new(
        directory.path().to_path_buf(),
        support::store_id(1),
        mode,
        ActiveJournalLimits::new(och_store::MAX_ADMISSION_PAYLOAD_V1, 32 * 1_024 * 1_024, 16)
            .expect("segment fixture journal limits"),
        RegistryPersistenceOptions::new(SeriesRegistryLimits::new(4, 8))
            .expect("segment fixture registry limits"),
        RetryPersistenceOptions::new(4, 4).expect("segment fixture retry limits"),
    )
    .expect("segment fixture store config")
}

fn frame(admission: och_core::CanonicalAdmission, sequence: u64) -> och_store::PreparedFrameV1 {
    PreparedAdmissionV1::new(admission)
        .expect("bounded segment fixture admission")
        .into_frame(AppendSequenceV1::new(sequence).expect("positive fixture sequence"))
        .expect("bounded segment fixture frame")
}

fn inventory(directory: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = fs::read_dir(directory)
        .expect("read segment fixture inventory")
        .map(|entry| {
            let entry = entry.expect("read segment fixture entry");
            (
                entry
                    .file_name()
                    .into_string()
                    .expect("store artifact name is UTF-8"),
                fs::read(entry.path()).expect("read exact store artifact"),
            )
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn build_store_candidate() -> (
    TestDirectory,
    och_store::PreparedSegmentV1,
    Vec<u8>,
    och_store::PreparedFrameV1,
    och_store::PreparedFrameV1,
) {
    let directory = TestDirectory::new("committed");
    let observed = support::observed_admission(
        vec![ExactValue::Boolean(true), ExactValue::Boolean(false)],
        ValueFamily::Boolean,
        1,
        false,
    );
    let no_change = support::no_change_admission();
    let observed_declaration = observed.declaration().clone();
    let no_change_declaration = no_change.declaration().clone();
    let observed = frame(observed, 1);
    let no_change = frame(no_change, 2);
    let mut store = ManifestStore::open(config(&directory, ActiveJournalOpenMode::CreateNew))
        .expect("create segment fixture store");
    for declaration in [&observed_declaration, &no_change_declaration] {
        store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register segment fixture declaration");
    }
    let observed_end = store.append(&observed).expect("append observed frame");
    let no_change_end = store.append(&no_change).expect("append no-change frame");
    store
        .sync_pending(&[
            PendingRetryOutcome::new(observed.admission().retry().clone(), 1, observed_end),
            PendingRetryOutcome::new(no_change.admission().retry().clone(), 2, no_change_end),
        ])
        .expect("commit segment fixture range");
    store.rotate().expect("seal segment fixture generation");
    let sealed_name = "sealed-journal-v1-g00000000000000000001.och";
    let raw = fs::read(directory.path().join(sealed_name)).expect("read sealed fixture bytes");
    let before = inventory(directory.path());
    let candidate = store
        .build_segment_candidate_v1(1)
        .expect("build committed read-only candidate");
    assert_eq!(
        store
            .build_segment_candidate_v1(2)
            .expect_err("active generation is not a sealed source"),
        SegmentV1Error::InvalidSource
    );
    assert_eq!(
        store
            .build_segment_candidate_v1(99)
            .expect_err("unknown generation is not a sealed source"),
        SegmentV1Error::InvalidSource
    );
    assert_eq!(inventory(directory.path()), before);
    drop(store);
    (directory, candidate, raw, observed, no_change)
}

#[test]
fn committed_generation_matches_primitive_oracle_and_bridge_is_read_only() {
    let (directory, candidate, raw, observed, no_change) = build_store_candidate();
    let metadata = candidate.inspection();
    let observed_index = [
        segment_oracle::Observation {
            effective_seconds: 9,
            effective_nanos: 12,
            receive_seconds: 10,
            receive_nanos: 11,
            id: support::uuid_bytes(10_000),
            ordinal: 0,
        },
        segment_oracle::Observation {
            effective_seconds: 9,
            effective_nanos: 12,
            receive_seconds: 10,
            receive_nanos: 11,
            id: support::uuid_bytes(10_001),
            ordinal: 1,
        },
    ];
    let frames = [
        segment_oracle::Frame {
            sequence: 1,
            series_id: support::uuid_bytes(2),
            bytes: observed.bytes(),
            observations: &observed_index,
        },
        segment_oracle::Frame {
            sequence: 2,
            series_id: support::uuid_bytes(4),
            bytes: no_change.bytes(),
            observations: &[],
        },
    ];
    let expected = segment_oracle::build(&segment_oracle::Source {
        store_id: support::uuid_bytes(1),
        journal_generation: metadata.source_journal_generation(),
        sequence_floor: metadata.sequence_floor(),
        sequence_cutoff: metadata.sequence_cutoff(),
        registry_generation: metadata.source_registry_generation(),
        raw_journal: &raw,
        frames: &frames,
    });
    assert_eq!(candidate.bytes(), expected);
    let repeated = parse_segment_v1(candidate.bytes(), support::store_id(1))
        .expect("parse exact Segment V1 candidate");
    assert_eq!(repeated.inspection(), metadata);
    assert_eq!(repeated.series_directory().len(), 2);
    assert_eq!(
        repeated.series_directory()[0].series_id(),
        support::series_id(2)
    );
    assert_eq!(
        repeated.series_directory()[1].series_id(),
        support::series_id(4)
    );
    assert_eq!(repeated.append_directory().len(), 2);
    assert_eq!(
        repeated
            .frame_bytes(&repeated.append_directory()[0])
            .expect("first indexed frame"),
        observed.bytes()
    );
    assert_eq!(
        repeated
            .decode_frame(&repeated.append_directory()[1])
            .expect("decode second indexed frame")
            .append_sequence(),
        2
    );
    assert_eq!(repeated.recent_observations().len(), 2);
    assert_eq!(
        repeated.recent_observations()[0].observation_id(),
        support::observation_id(10_001)
    );
    assert_eq!(
        repeated.recent_observations()[1].observation_id(),
        support::observation_id(10_000)
    );

    let before_reopen = inventory(directory.path());
    let reopened = ManifestStore::open(config(&directory, ActiveJournalOpenMode::OpenExisting))
        .expect("reopen store unchanged after offline candidate build");
    assert_eq!(reopened.inspection().generations().sealed_count(), 1);
    assert_eq!(inventory(directory.path()), before_reopen);
    assert!(
        inventory(directory.path())
            .iter()
            .all(|(name, _)| !name.contains("segment"))
    );
}

#[test]
fn hostile_segment_bytes_refuse_closed_without_panics_or_unbounded_counts() {
    let (_directory, candidate, _raw, _observed, _no_change) = build_store_candidate();
    let canonical = candidate.bytes();
    assert_eq!(
        parse_segment_v1(canonical, support::store_id(2))
            .expect_err("foreign expected store must refuse"),
        SegmentV1Error::StoreMismatch
    );
    assert!(parse_segment_v1(&canonical[..canonical.len() - 1], support::store_id(1)).is_err());
    let mut trailing = canonical.to_vec();
    trailing.push(0);
    assert!(parse_segment_v1(&trailing, support::store_id(1)).is_err());

    for (offset, length, value) in [
        (8_usize, 2_usize, 2_u8),
        (10, 2, 0),
        (12, 4, 1),
        (32, 8, 0),
        (40, 8, 2),
        (48, 8, 1),
        (56, 8, 0),
        (64, 8, 0),
        (72, 8, 0),
        (80, 4, 0),
        (84, 4, 0),
        (88, 4, 0),
        (92, 4, u8::MAX),
        (96, 8, 0),
        (104, 8, u8::MAX),
        (112, 8, 0),
        (128, 8, 0),
        (144, 8, 0),
        (160, 8, 0),
        (168, 1, 1),
    ] {
        let mut hostile = canonical.to_vec();
        hostile[offset..offset + length].fill(value);
        segment_oracle::repair_checksum(&mut hostile);
        assert!(
            parse_segment_v1(&hostile, support::store_id(1)).is_err(),
            "hostile header field at {offset} must refuse"
        );
    }

    let inspection = candidate.inspection();
    let series = usize::try_from(inspection.series_directory_offset())
        .expect("bounded series directory offset");
    let append = usize::try_from(inspection.append_directory_offset())
        .expect("bounded append directory offset");
    let recent = usize::try_from(inspection.recent_directory_offset())
        .expect("bounded recent directory offset");
    for hostile in [
        swap_entries(canonical, series, 64),
        swap_entries(canonical, append, 48),
        swap_entries(canonical, recent, 96),
        overwrite(canonical, append + 8, &support::uuid_bytes(4)),
        overwrite(canonical, append + 24, &0_u64.to_be_bytes()),
        overwrite(canonical, append + 40, &1_u32.to_be_bytes()),
        overwrite(canonical, recent + 56, &2_u64.to_be_bytes()),
    ] {
        assert!(parse_segment_v1(&hostile, support::store_id(1)).is_err());
    }

    let mut checksum = canonical.to_vec();
    let last = checksum.len() - 1;
    checksum[last] ^= 1;
    assert_eq!(
        parse_segment_v1(&checksum, support::store_id(1))
            .expect_err("damaged segment checksum must refuse"),
        SegmentV1Error::InvalidSegment
    );
}

#[test]
fn parser_refuses_canonical_slice_redistribution_between_two_series() {
    let (_directory, candidate, _raw, _observed, _no_change) = build_store_candidate();
    let inspection = candidate.inspection();
    let series_offset = usize::try_from(inspection.series_directory_offset())
        .expect("bounded series directory offset");
    let recent_offset = inspection.recent_directory_offset();
    let mut hostile = candidate.bytes().to_vec();

    hostile[series_offset + 36..series_offset + 40].copy_from_slice(&1_u32.to_be_bytes());
    hostile[series_offset + 48..series_offset + 56].copy_from_slice(
        &u64::try_from(och_store::SEGMENT_V1_OBSERVATION_ENTRY_LEN)
            .expect("fixed observation entry length")
            .to_be_bytes(),
    );
    let second = series_offset + och_store::SEGMENT_V1_SERIES_ENTRY_LEN;
    hostile[second + 36..second + 40].copy_from_slice(&1_u32.to_be_bytes());
    hostile[second + 40..second + 48].copy_from_slice(
        &recent_offset
            .checked_add(
                u64::try_from(och_store::SEGMENT_V1_OBSERVATION_ENTRY_LEN)
                    .expect("fixed observation entry length"),
            )
            .expect("bounded recent slice offset")
            .to_be_bytes(),
    );
    hostile[second + 48..second + 56].copy_from_slice(
        &u64::try_from(och_store::SEGMENT_V1_OBSERVATION_ENTRY_LEN)
            .expect("fixed observation entry length")
            .to_be_bytes(),
    );
    segment_oracle::repair_checksum(&mut hostile);

    assert_eq!(
        parse_segment_v1(&hostile, support::store_id(1))
            .expect_err("per-series observation redistribution must refuse"),
        SegmentV1Error::InvalidSegment
    );
}

fn swap_entries(source: &[u8], offset: usize, length: usize) -> Vec<u8> {
    let mut bytes = source.to_vec();
    let first = bytes[offset..offset + length].to_vec();
    let second = bytes[offset + length..offset + length * 2].to_vec();
    bytes[offset..offset + length].copy_from_slice(&second);
    bytes[offset + length..offset + length * 2].copy_from_slice(&first);
    segment_oracle::repair_checksum(&mut bytes);
    bytes
}

fn overwrite(source: &[u8], offset: usize, value: &[u8]) -> Vec<u8> {
    let mut bytes = source.to_vec();
    bytes[offset..offset + value.len()].copy_from_slice(value);
    segment_oracle::repair_checksum(&mut bytes);
    bytes
}
