#![forbid(unsafe_code)]
//! Read-only one-generation `ManifestStore` observation-query bridge evidence.

mod support;

use och_core::{ExactValue, SeriesRegistryLimits, ValueFamily};
use och_store::{
    ActiveJournalLimits, ActiveJournalOpenMode, AppendSequenceV1, ManifestIoOperation,
    ManifestStore, ManifestStoreConfig, ManifestStoreSegmentQueryV1Error, PendingRetryOutcome,
    PreparedAdmissionV1, RegistryPersistenceOptions, RetryPersistenceOptions,
    SegmentObservationQueryV1, SegmentV1Error, StoreWriteState,
};
use std::error::Error as _;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "och-segment-store-query-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create unique store-query fixture directory");
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
            .expect("store-query fixture journal limits"),
        RegistryPersistenceOptions::new(SeriesRegistryLimits::new(2, 4))
            .expect("store-query fixture registry limits"),
        RetryPersistenceOptions::new(4, 4).expect("store-query fixture retry limits"),
    )
    .expect("store-query fixture config")
}

fn inventory(directory: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = fs::read_dir(directory)
        .expect("read store-query fixture inventory")
        .map(|entry| {
            let entry = entry.expect("read store-query fixture entry");
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

fn sealed_path(directory: &TestDirectory, generation: u64) -> PathBuf {
    directory
        .path()
        .join(format!("sealed-journal-v1-g{generation:020}.och"))
}

fn append_durable(store: &mut ManifestStore, admission: och_core::CanonicalAdmission) {
    let sequence = store
        .next_append_sequence()
        .expect("next store-query append sequence");
    let frame = PreparedAdmissionV1::new(admission)
        .expect("bounded store-query admission")
        .into_frame(AppendSequenceV1::new(sequence.get()).expect("positive append sequence"))
        .expect("bounded store-query frame");
    let end = store.append(&frame).expect("append store-query frame");
    store
        .sync_pending(&[PendingRetryOutcome::new(
            frame.admission().retry().clone(),
            sequence.get(),
            end,
        )])
        .expect("commit store-query frame");
}

fn two_sealed_generations() -> (TestDirectory, ManifestStore) {
    let directory = TestDirectory::new("two-sealed");
    let first = support::observed_admission_with_query_evidence(
        vec![ExactValue::Signed(11), ExactValue::Signed(12)],
        ValueFamily::Signed,
        11_000,
        7,
        "store-query-generation-one",
    );
    let second = support::observed_admission_with_query_evidence(
        vec![ExactValue::Signed(21), ExactValue::Signed(22)],
        ValueFamily::Signed,
        12_000,
        9,
        "store-query-generation-two",
    );
    assert_eq!(first.declaration(), second.declaration());

    let declaration = first.declaration().clone();
    let mut store = ManifestStore::open(config(&directory, ActiveJournalOpenMode::CreateNew))
        .expect("create store-query fixture");
    store
        .register(
            declaration.series_id(),
            declaration.binding().clone(),
            declaration.payload().clone(),
            declaration.evidence().clone(),
        )
        .expect("register store-query declaration");
    append_durable(&mut store, first);
    store.rotate().expect("seal first query generation");
    append_durable(&mut store, second);
    store.rotate().expect("seal second query generation");
    assert_eq!(store.inspection().generations().sealed_count(), 2);
    assert_eq!(store.inspection().active().journal().generation(), 3);
    (directory, store)
}

#[test]
#[allow(clippy::too_many_lines)]
fn exact_generations_query_independently_and_owned_limit_one_result_outlives_bridge() {
    let (directory, store) = two_sealed_generations();
    let inspection = store.inspection();
    let registry = store.registry_snapshot();
    let retry = store.retry_state_snapshot();
    let artifacts = inventory(directory.path());
    let all = SegmentObservationQueryV1::new(support::series_id(2), None, 16)
        .expect("valid complete store query");

    let first = store
        .query_sealed_generation_observations_v1(1, &all)
        .expect("query first sealed generation");
    let second = store
        .query_sealed_generation_observations_v1(2, &all)
        .expect("query second sealed generation");
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    assert!(!first.is_truncated());
    assert!(!second.is_truncated());
    assert_eq!(
        first
            .items()
            .iter()
            .map(|item| (
                item.observation().observation_id(),
                item.observation().value(),
                item.entry().append_sequence(),
                item.lineage().ordinal(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                support::observation_id(11_001),
                &ExactValue::Signed(12),
                1,
                8,
            ),
            (
                support::observation_id(11_000),
                &ExactValue::Signed(11),
                1,
                7,
            ),
        ]
    );
    assert_eq!(
        second
            .items()
            .iter()
            .map(|item| (
                item.observation().observation_id(),
                item.observation().value(),
                item.entry().append_sequence(),
                item.lineage().ordinal(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                support::observation_id(12_001),
                &ExactValue::Signed(22),
                2,
                10,
            ),
            (
                support::observation_id(12_000),
                &ExactValue::Signed(21),
                2,
                9,
            ),
        ]
    );

    let one = SegmentObservationQueryV1::new(support::series_id(2), None, 1)
        .expect("valid one-result store query");
    let owned = store
        .query_sealed_generation_observations_v1(1, &one)
        .expect("query owned truncated result");
    assert_eq!(owned.len(), 1);
    assert!(owned.is_truncated());
    assert!(owned.has_more());

    for generation in [1_u64, 2] {
        let repeated = store
            .query_sealed_generation_observations_v1(generation, &all)
            .expect("repeat successful exact-generation query");
        assert_eq!(repeated.len(), 2);
    }
    assert_eq!(store.inspection(), inspection);
    assert_eq!(store.registry_snapshot(), registry);
    assert_eq!(store.retry_state_snapshot(), retry);
    assert_eq!(inventory(directory.path()), artifacts);

    drop(first);
    drop(second);
    drop(store);
    assert_eq!(
        owned.items()[0].observation().value(),
        &ExactValue::Signed(12)
    );
    assert_eq!(owned.items()[0].lineage().ordinal(), 8);

    let reopened = ManifestStore::open(config(&directory, ActiveJournalOpenMode::OpenExisting))
        .expect("reopen after successful read-only queries");
    assert_eq!(reopened.inspection(), inspection);
    assert_eq!(reopened.registry_snapshot(), registry);
    assert_eq!(reopened.retry_state_snapshot(), retry);
    assert_eq!(inventory(directory.path()), artifacts);
}

#[test]
fn active_unknown_missing_and_corrupt_sources_refuse_closed_without_state_changes() {
    let (directory, store) = two_sealed_generations();
    let query = SegmentObservationQueryV1::new(support::series_id(2), None, 16)
        .expect("valid refusal store query");
    let inspection = store.inspection();
    let registry = store.registry_snapshot();
    let retry = store.retry_state_snapshot();
    let artifacts = inventory(directory.path());

    for generation in [3_u64, 99] {
        let error = store
            .query_sealed_generation_observations_v1(generation, &query)
            .expect_err("active or unknown generation must refuse");
        assert_eq!(
            error,
            ManifestStoreSegmentQueryV1Error::Segment(SegmentV1Error::InvalidSource)
        );
        assert_eq!(error.segment_error(), Some(SegmentV1Error::InvalidSource));
        assert_eq!(error.query_error(), None);
        assert!(error.source().is_some());
        assert_eq!(error.to_string(), "invalid sealed Journal V1 source");
    }
    assert_eq!(store.inspection(), inspection);
    assert_eq!(store.registry_snapshot(), registry);
    assert_eq!(store.retry_state_snapshot(), retry);
    assert_eq!(inventory(directory.path()), artifacts);

    let selected = sealed_path(&directory, 1);
    let raw = fs::read(&selected).expect("read selected sealed source fixture");
    fs::remove_file(&selected).expect("remove selected sealed source fixture");
    let missing_artifacts = inventory(directory.path());
    for _ in 0..2 {
        let error = store
            .query_sealed_generation_observations_v1(1, &query)
            .expect_err("missing selected source must refuse");
        let Some(SegmentV1Error::Io(evidence)) = error.segment_error() else {
            panic!("missing source must retain segment I/O evidence");
        };
        assert_eq!(evidence.operation(), ManifestIoOperation::Read);
        assert_eq!(evidence.kind(), ErrorKind::NotFound);
        assert_eq!(error.query_error(), None);
        assert_eq!(store.inspection(), inspection);
        assert_eq!(store.registry_snapshot(), registry);
        assert_eq!(store.retry_state_snapshot(), retry);
        assert_eq!(inventory(directory.path()), missing_artifacts);
    }

    fs::write(&selected, &raw).expect("restore selected sealed source fixture");
    let mut corrupt = raw.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    fs::write(&selected, &corrupt).expect("corrupt selected sealed source fixture");
    let corrupt_artifacts = inventory(directory.path());
    for _ in 0..2 {
        let error = store
            .query_sealed_generation_observations_v1(1, &query)
            .expect_err("corrupt selected source must refuse");
        assert_eq!(
            error,
            ManifestStoreSegmentQueryV1Error::Segment(SegmentV1Error::InvalidSource)
        );
        assert_eq!(store.inspection(), inspection);
        assert_eq!(store.registry_snapshot(), registry);
        assert_eq!(store.retry_state_snapshot(), retry);
        assert_eq!(inventory(directory.path()), corrupt_artifacts);
    }

    fs::write(&selected, &raw).expect("restore exact sealed source bytes");
    assert_eq!(inventory(directory.path()), artifacts);
    assert_eq!(store.inspection().write_state(), StoreWriteState::Writable);
    assert_eq!(
        store
            .query_sealed_generation_observations_v1(1, &query)
            .expect("restored source remains queryable")
            .len(),
        2
    );
    assert_eq!(inventory(directory.path()), artifacts);
    drop(store);

    let reopened = ManifestStore::open(config(&directory, ActiveJournalOpenMode::OpenExisting))
        .expect("reopen unchanged store after read refusals");
    assert_eq!(reopened.inspection(), inspection);
    assert_eq!(reopened.registry_snapshot(), registry);
    assert_eq!(reopened.retry_state_snapshot(), retry);
    assert_eq!(inventory(directory.path()), artifacts);
    assert_eq!(
        reopened
            .query_sealed_generation_observations_v1(2, &query)
            .expect("other exact generation remains queryable after reopen")
            .len(),
        2
    );
    assert_eq!(inventory(directory.path()), artifacts);
}
