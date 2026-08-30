#![forbid(unsafe_code)]
//! Manifest-rooted registry/retry, bootstrap, lifecycle, and hostile-byte evidence.

#[path = "support/manifest_oracle.rs"]
mod manifest_oracle;
mod support;

use och_core::{
    CanonicalAdmission, DeclarationEvidence, DeclarationRevision, ExactValue, ModelError,
    SeriesRegistryLimits, Timestamp, ValueFamily,
};
use och_store::{
    ACTIVE_CHECKPOINT_FILE_NAME, ACTIVE_JOURNAL_FILE_NAME, ActiveJournal, ActiveJournalConfig,
    ActiveJournalLimits, ActiveJournalOpenMode, AppendSequenceV1, JOURNAL_V1_HEADER_LEN,
    JournalHeaderV1, MANIFEST_SLOT_0_FILE_NAME, MANIFEST_SLOT_1_FILE_NAME,
    MANIFEST_STAGING_FILE_NAME, MAX_ADMISSION_PAYLOAD_V1, ManifestStore, ManifestStoreConfig,
    ManifestStoreError, PendingRetryOutcome, PreparedAdmissionV1, RECOVERY_SLOT_0_FILE_NAME,
    REGISTRY_SLOT_0_FILE_NAME, REGISTRY_STAGING_FILE_NAME, RETRY_SLOT_0_FILE_NAME,
    RETRY_SLOT_1_FILE_NAME, RETRY_SLOT_2_FILE_NAME, RegistryPersistenceOptions,
    RetryPersistenceOptions, STORE_FORMAT_FILE_NAME, STORE_LOCK_FILE_NAME,
};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

const ORACLE_SOURCE: &str = include_str!("support/manifest_oracle.rs");
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "och-manifest-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("unique test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
        let _ = fs::remove_file(self.0.with_extension("ready"));
    }
}

fn journal_limits() -> ActiveJournalLimits {
    ActiveJournalLimits::new(MAX_ADMISSION_PAYLOAD_V1, 32 * 1_024 * 1_024, 32)
        .expect("bounded journal limits")
}

fn registry_limits() -> SeriesRegistryLimits {
    SeriesRegistryLimits::new(4, 8)
}

fn registry_options() -> RegistryPersistenceOptions {
    RegistryPersistenceOptions::new(registry_limits()).expect("bounded registry limits")
}

fn manifest_config(
    directory: &TestDirectory,
    mode: ActiveJournalOpenMode,
    registry: RegistryPersistenceOptions,
) -> ManifestStoreConfig {
    ManifestStoreConfig::new(
        directory.path().to_path_buf(),
        support::store_id(1),
        mode,
        journal_limits(),
        registry,
        RetryPersistenceOptions::new(2, 2).expect("bounded retry limits"),
    )
    .expect("valid manifest config")
}

fn frame(admission: CanonicalAdmission, sequence: u64) -> och_store::PreparedFrameV1 {
    PreparedAdmissionV1::new(admission)
        .expect("bounded fixture")
        .into_frame(AppendSequenceV1::new(sequence).expect("positive sequence"))
        .expect("bounded frame")
}

fn crc32c(bytes: &[u8]) -> u32 {
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

fn rewrite_trailing_crc(bytes: &mut [u8]) {
    let checksum_offset = bytes.len() - 4;
    let checksum = crc32c(&bytes[..checksum_offset]).to_be_bytes();
    bytes[checksum_offset..].copy_from_slice(&checksum);
}

fn exact_file_inventory(directory: &Path) -> Vec<(String, Vec<u8>)> {
    let mut inventory = fs::read_dir(directory)
        .expect("read exact fixture inventory")
        .map(|entry| {
            let entry = entry.expect("read fixture entry");
            assert!(
                entry
                    .file_type()
                    .expect("read fixture entry type")
                    .is_file(),
                "manifest fixture inventory must contain only files"
            );
            (
                entry
                    .file_name()
                    .into_string()
                    .expect("fixture artifact name must be UTF-8"),
                fs::read(entry.path()).expect("read exact fixture artifact"),
            )
        })
        .collect::<Vec<_>>();
    inventory.sort_by(|first, second| first.0.cmp(&second.0));
    inventory
}

fn assert_unsupported_unchanged(directory: &TestDirectory) {
    let before = exact_file_inventory(directory.path());
    for mode in [
        ActiveJournalOpenMode::OpenExisting,
        ActiveJournalOpenMode::CreateNew,
    ] {
        let Err(error) = ManifestStore::open(manifest_config(directory, mode, registry_options()))
        else {
            panic!("pre-reset evidence must refuse");
        };
        assert_eq!(error, ManifestStoreError::UnsupportedStoreFormat);
        assert!(
            !error
                .to_string()
                .contains(&directory.path().display().to_string())
        );
        assert_eq!(exact_file_inventory(directory.path()), before);
    }
}

fn install_format_marker(directory: &TestDirectory) {
    fs::write(directory.path().join(STORE_LOCK_FILE_NAME), [])
        .expect("install pre-existing stable lock fixture");
    fs::write(
        directory.path().join(STORE_FORMAT_FILE_NAME),
        manifest_oracle::store_format_v1(support::uuid_bytes(1)),
    )
    .expect("install forged current marker fixture");
}

fn create_active_genesis(directory: &TestDirectory) -> ActiveJournal {
    ActiveJournal::open(
        ActiveJournalConfig::new(
            directory.path().to_path_buf(),
            support::store_id(1),
            ActiveJournalOpenMode::CreateNew,
            journal_limits(),
        )
        .expect("active genesis config"),
    )
    .expect("create active genesis")
}

fn assert_premanifest_checkpoint_refuses(name: &str, mutate: impl FnOnce(&mut Vec<u8>)) {
    let directory = TestDirectory::new(name);
    drop(create_active_genesis(&directory));
    let checkpoint_path = directory.path().join(ACTIVE_CHECKPOINT_FILE_NAME);
    let mut checkpoint = fs::read(&checkpoint_path).expect("read genesis checkpoint");
    mutate(&mut checkpoint);
    fs::write(checkpoint_path, checkpoint).expect("install hostile checkpoint");
    install_format_marker(&directory);
    assert_unsupported_unchanged(&directory);
}

#[test]
fn genesis_bytes_match_the_independent_primitive_oracle() {
    let directory = TestDirectory::new("oracle");
    let store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create manifest store");
    let inspection = store.inspection();
    assert_eq!(inspection.committed().manifest_generation(), 1);
    assert_eq!(inspection.committed().registry_generation(), 1);
    assert_eq!(inspection.committed().registry_slot(), 0);
    assert_eq!(inspection.committed().durable_cutoff().append_sequence(), 0);
    drop(store);

    let marker = fs::read(directory.path().join(STORE_FORMAT_FILE_NAME))
        .expect("read Store Format V1 marker");
    assert_eq!(
        marker,
        manifest_oracle::store_format_v1(support::uuid_bytes(1))
    );

    let registry = fs::read(directory.path().join(REGISTRY_SLOT_0_FILE_NAME))
        .expect("read empty registry bytes");
    let expected_registry = manifest_oracle::empty_registry(
        support::uuid_bytes(1),
        1,
        u32::try_from(registry_limits().max_series()).expect("oracle series bound"),
        u32::try_from(registry_limits().max_declaration_revisions())
            .expect("oracle revision bound"),
    );
    assert_eq!(registry, expected_registry);
    let retry =
        fs::read(directory.path().join(RETRY_SLOT_0_FILE_NAME)).expect("read empty retry bytes");
    let retry_options = RetryPersistenceOptions::new(2, 2).expect("oracle retry options");
    let expected_retry = manifest_oracle::empty_retry(
        support::uuid_bytes(1),
        1,
        u32::try_from(retry_options.replay_capacity()).expect("oracle replay capacity"),
        u32::try_from(retry_options.guard_capacity()).expect("oracle guard capacity"),
    );
    assert_eq!(retry, expected_retry);
    let manifest = fs::read(directory.path().join(MANIFEST_SLOT_0_FILE_NAME))
        .expect("read genesis manifest bytes");
    assert_eq!(
        manifest,
        manifest_oracle::current_manifest_v1(
            support::uuid_bytes(1),
            1,
            1,
            1,
            0,
            JOURNAL_V1_HEADER_LEN as u64,
            0,
            1,
            &expected_registry,
            0,
            1,
            &expected_retry,
            0,
            None,
        )
    );

    for forbidden in ["och_store", "och_core", "encode_manifest"] {
        assert!(
            !ORACLE_SOURCE.contains(forbidden),
            "primitive oracle must not contain {forbidden}"
        );
    }
}

#[test]
fn recovery_state_and_present_manifest_reference_match_primitive_oracles() {
    let directory = TestDirectory::new("recovery-oracle");
    let store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create recovery oracle store");
    let source = store.inspection().committed();
    drop(store);
    let source_manifest =
        fs::read(directory.path().join(MANIFEST_SLOT_0_FILE_NAME)).expect("read source manifest");
    let source_checksum = u32::from_be_bytes(
        source_manifest[156..160]
            .try_into()
            .expect("source manifest checksum"),
    );
    let original_length = JOURNAL_V1_HEADER_LEN as u64 + 11;
    let mut journal = fs::OpenOptions::new()
        .append(true)
        .open(directory.path().join(ACTIVE_JOURNAL_FILE_NAME))
        .expect("open oracle journal suffix");
    journal
        .write_all(b"OCHF\0\x01\x01\0\0\0\0")
        .expect("write oracle journal suffix");
    journal.sync_all().expect("sync oracle journal suffix");
    drop(journal);

    let recovered = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::OpenExisting,
        registry_options(),
    ))
    .expect("recover oracle suffix");
    let committed = recovered.inspection().committed();
    let report = recovered
        .inspection()
        .latest_recovery()
        .expect("oracle recovery report");
    drop(recovered);

    let report_bytes =
        fs::read(directory.path().join(RECOVERY_SLOT_0_FILE_NAME)).expect("read Recovery State V1");
    let expected_report = manifest_oracle::recovery_state_v1(
        support::uuid_bytes(1),
        report.report_generation(),
        source.manifest_generation(),
        source_checksum,
        report.active_generation(),
        report.active_sequence_floor(),
        report.checkpoint_generation(),
        report.append_sequence(),
        report.committed_end_offset(),
        original_length,
        1,
    );
    assert_eq!(report_bytes, expected_report);

    let registry =
        fs::read(directory.path().join(REGISTRY_SLOT_0_FILE_NAME)).expect("read recovery registry");
    let retry =
        fs::read(directory.path().join(RETRY_SLOT_0_FILE_NAME)).expect("read recovery retry");
    let manifest =
        fs::read(directory.path().join(MANIFEST_SLOT_1_FILE_NAME)).expect("read recovery manifest");
    assert_eq!(
        manifest,
        manifest_oracle::current_manifest_v1_with_recovery(
            support::uuid_bytes(1),
            committed.manifest_generation(),
            committed.durable_cutoff().journal().generation(),
            committed.durable_cutoff().checkpoint_generation(),
            committed.durable_cutoff().append_sequence(),
            committed.durable_cutoff().end_offset(),
            committed.registry_slot(),
            committed.registry_generation(),
            &registry,
            committed.retry_state().slot(),
            committed.retry_state().generation(),
            &retry,
            committed.sequence_floor(),
            None,
            Some(manifest_oracle::RecoveryReference {
                slot: 0,
                checksum: manifest_oracle::checksum(&expected_report),
            }),
        )
    );
}

#[test]
fn recovery_subtype_tags_match_primitive_only_oracles() {
    let candidate = frame(support::no_change_admission(), 1);
    let mut invalid_prefix = [0_u8; och_store::JOURNAL_V1_FRAME_PREFIX_LEN];
    invalid_prefix[..4].copy_from_slice(b"OCHF");
    invalid_prefix[4..6].copy_from_slice(&1_u16.to_be_bytes());
    invalid_prefix[6] = 1;
    invalid_prefix[8..16].copy_from_slice(&1_u64.to_be_bytes());
    invalid_prefix[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
    let mut invalid_complete = candidate.bytes().to_vec();
    let final_byte = invalid_complete.len() - 1;
    invalid_complete[final_byte] ^= 1;
    for (name, suffix, tag) in [
        ("invalid-prefix-oracle", invalid_prefix.to_vec(), 2_u8),
        (
            "truncated-frame-oracle",
            candidate.bytes()[..25].to_vec(),
            3,
        ),
        ("invalid-frame-oracle", invalid_complete, 4),
    ] {
        let directory = TestDirectory::new(name);
        let store = ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::CreateNew,
            registry_options(),
        ))
        .expect("create subtype oracle store");
        let source = store.inspection().committed();
        drop(store);
        let source_manifest = fs::read(directory.path().join(MANIFEST_SLOT_0_FILE_NAME))
            .expect("read subtype source manifest");
        let source_checksum = u32::from_be_bytes(
            source_manifest[156..160]
                .try_into()
                .expect("subtype source checksum"),
        );
        let mut journal = fs::OpenOptions::new()
            .append(true)
            .open(directory.path().join(ACTIVE_JOURNAL_FILE_NAME))
            .expect("open subtype suffix");
        journal.write_all(&suffix).expect("write subtype suffix");
        journal.sync_all().expect("sync subtype suffix");
        drop(journal);

        let recovered = ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::OpenExisting,
            registry_options(),
        ))
        .expect("recover subtype oracle suffix");
        let report = recovered
            .inspection()
            .latest_recovery()
            .expect("subtype report");
        drop(recovered);
        let bytes = fs::read(directory.path().join(RECOVERY_SLOT_0_FILE_NAME))
            .expect("read subtype report");
        assert_eq!(
            bytes,
            manifest_oracle::recovery_state_v1(
                support::uuid_bytes(1),
                report.report_generation(),
                source.manifest_generation(),
                source_checksum,
                report.active_generation(),
                report.active_sequence_floor(),
                report.checkpoint_generation(),
                report.append_sequence(),
                report.committed_end_offset(),
                JOURNAL_V1_HEADER_LEN as u64 + suffix.len() as u64,
                tag,
            ),
            "closed subtype tag {tag} must match the primitive oracle"
        );
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn pre_reset_and_mixed_formats_refuse_path_free_before_any_mutation() {
    let store = support::uuid_bytes(1);
    let registry = manifest_oracle::empty_registry(store, 1, 4, 8);
    let retry = manifest_oracle::empty_retry(store, 1, 2, 2);
    let old_v1 = manifest_oracle::manifest_v1(store, 1, 1, 0, 28, 0, 1, &registry);
    let old_v2 = manifest_oracle::manifest(store, 1, 1, 0, 28, 0, 1, &registry, 0, 1, &retry);
    let old_v3 = manifest_oracle::historical_manifest_v3(
        store,
        2,
        2,
        1,
        1,
        28,
        0,
        1,
        &registry,
        0,
        1,
        &retry,
        1,
        manifest_oracle::CatalogReference {
            slot: 0,
            generation: 1,
            length: 132,
            checksum: 7,
        },
    );
    for (name, bytes) in [
        ("old-manifest-v1", old_v1.to_vec()),
        ("old-manifest-v2", old_v2.to_vec()),
        ("old-manifest-v3", old_v3.to_vec()),
    ] {
        let directory = TestDirectory::new(name);
        install_format_marker(&directory);
        fs::write(directory.path().join(MANIFEST_SLOT_0_FILE_NAME), bytes)
            .expect("install historical manifest");
        assert_unsupported_unchanged(&directory);
    }

    let old_header_v2 = TestDirectory::new("old-header-v2");
    install_format_marker(&old_header_v2);
    let mut header_v2 = JournalHeaderV1::new(support::store_id(1)).encode();
    header_v2[8..10].copy_from_slice(&2_u16.to_be_bytes());
    fs::write(
        old_header_v2.path().join(ACTIVE_JOURNAL_FILE_NAME),
        header_v2,
    )
    .expect("install historical active header");
    assert_unsupported_unchanged(&old_header_v2);

    let admission = support::no_change_admission();
    let qualification = admission.retry();
    let old_retry_v1 = manifest_oracle::historical_retry_v1_with_one_replay(
        store,
        1,
        2,
        2,
        *qualification.series_id().as_bytes(),
        *qualification.producer_id().as_bytes(),
        qualification.key().as_str(),
        qualification.content().format().as_str(),
        qualification.content().version().get(),
        *qualification.content().sha256(),
        1,
        100,
        1,
        0,
        1,
        1,
        1,
        100,
        0,
        1,
    );
    let current_extended = manifest_oracle::retry_with_one_replay(
        store,
        1,
        2,
        2,
        *qualification.series_id().as_bytes(),
        *qualification.producer_id().as_bytes(),
        qualification.key().as_str(),
        qualification.content().format().as_str(),
        qualification.content().version().get(),
        *qualification.content().sha256(),
        1,
        100,
        1,
        0,
        1,
        1,
        1,
        100,
        0,
        1,
    );
    for (name, bytes) in [
        ("old-retry-v1-nonempty", old_retry_v1),
        (
            "old-retry-v2-empty",
            manifest_oracle::historical_retry_v2(retry.clone()),
        ),
        (
            "old-retry-v2-nonempty",
            manifest_oracle::historical_retry_v2(current_extended),
        ),
    ] {
        let directory = TestDirectory::new(name);
        install_format_marker(&directory);
        fs::write(directory.path().join(RETRY_SLOT_0_FILE_NAME), bytes)
            .expect("install historical retry state");
        assert_unsupported_unchanged(&directory);
    }

    let old_retry_v1_empty = TestDirectory::new("old-retry-v1-empty");
    install_format_marker(&old_retry_v1_empty);
    fs::write(
        old_retry_v1_empty.path().join(RETRY_SLOT_0_FILE_NAME),
        retry,
    )
    .expect("install historical empty Retry V1");
    fs::write(
        old_retry_v1_empty.path().join(MANIFEST_SLOT_0_FILE_NAME),
        old_v1,
    )
    .expect("bind historical empty Retry V1 to old root");
    assert_unsupported_unchanged(&old_retry_v1_empty);

    for version in [1_u16, 2] {
        let directory = TestDirectory::new(if version == 1 {
            "markerless-premanifest-v1"
        } else {
            "markerless-premanifest-v2"
        });
        let mut header = JournalHeaderV1::new(support::store_id(1)).encode();
        header[8..10].copy_from_slice(&version.to_be_bytes());
        fs::write(directory.path().join(ACTIVE_JOURNAL_FILE_NAME), header)
            .expect("install markerless premanifest header");
        assert_unsupported_unchanged(&directory);
        assert!(!directory.path().join(STORE_LOCK_FILE_NAME).exists());
    }

    let mixed = TestDirectory::new("forged-marker-mixed-current-old");
    let current = ManifestStore::open(manifest_config(
        &mixed,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create current fixture");
    drop(current);
    fs::write(mixed.path().join(MANIFEST_SLOT_0_FILE_NAME), old_v2)
        .expect("replace current root with historical bytes");
    assert_unsupported_unchanged(&mixed);
}

#[test]
fn malformed_marker_and_lock_only_refuse_unchanged() {
    let malformed = TestDirectory::new("malformed-marker");
    install_format_marker(&malformed);
    let marker_path = malformed.path().join(STORE_FORMAT_FILE_NAME);
    let mut marker = fs::read(&marker_path).expect("read marker fixture");
    marker[8..10].copy_from_slice(&2_u16.to_be_bytes());
    let checksum = crc32c(&marker[..28]);
    marker[28..32].copy_from_slice(&checksum.to_be_bytes());
    fs::write(marker_path, marker).expect("install checksummed unsupported marker");
    assert_unsupported_unchanged(&malformed);

    let lock_only = TestDirectory::new("lock-only");
    fs::write(lock_only.path().join(STORE_LOCK_FILE_NAME), []).expect("write interrupted lock");
    assert_unsupported_unchanged(&lock_only);
}

#[test]
fn premanifest_suffix_and_nongenesis_checkpoint_refuse_before_active_mutation() {
    let valid_suffix = TestDirectory::new("premanifest-valid-suffix");
    let mut retained = create_active_genesis(&valid_suffix);
    retained
        .append(&frame(support::no_change_admission(), 1))
        .expect("append valid premanifest suffix");
    install_format_marker(&valid_suffix);
    assert_unsupported_unchanged(&valid_suffix);
    drop(retained);

    let torn_suffix = TestDirectory::new("premanifest-torn-suffix");
    drop(create_active_genesis(&torn_suffix));
    let journal_path = torn_suffix.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let mut journal_bytes = fs::read(&journal_path).expect("read active genesis");
    journal_bytes.extend_from_slice(&frame(support::no_change_admission(), 1).bytes()[..11]);
    fs::write(&journal_path, journal_bytes).expect("install torn premanifest suffix");
    install_format_marker(&torn_suffix);
    assert_unsupported_unchanged(&torn_suffix);

    let torn_without_checkpoint = TestDirectory::new("premanifest-torn-no-checkpoint");
    drop(create_active_genesis(&torn_without_checkpoint));
    let checkpoint_path = torn_without_checkpoint
        .path()
        .join(ACTIVE_CHECKPOINT_FILE_NAME);
    fs::remove_file(&checkpoint_path).expect("remove checkpoint fixture");
    let journal_path = torn_without_checkpoint
        .path()
        .join(ACTIVE_JOURNAL_FILE_NAME);
    let mut journal_bytes = fs::read(&journal_path).expect("read active genesis");
    journal_bytes.extend_from_slice(&[0xa5; 11]);
    fs::write(&journal_path, journal_bytes).expect("install malformed premanifest suffix");
    install_format_marker(&torn_without_checkpoint);
    assert_unsupported_unchanged(&torn_without_checkpoint);
    assert!(!checkpoint_path.exists());

    assert_premanifest_checkpoint_refuses("premanifest-nongenesis-checkpoint", |checkpoint| {
        checkpoint[36..44].copy_from_slice(&3_u64.to_be_bytes());
        let checksum = crc32c(&checkpoint[..60]);
        checkpoint[60..64].copy_from_slice(&checksum.to_be_bytes());
    });
    assert_premanifest_checkpoint_refuses("premanifest-foreign-checkpoint", |checkpoint| {
        checkpoint[12..28].copy_from_slice(&support::uuid_bytes(2));
        let checksum = crc32c(&checkpoint[..60]);
        checkpoint[60..64].copy_from_slice(&checksum.to_be_bytes());
    });
    assert_premanifest_checkpoint_refuses("premanifest-trailing-checkpoint", |checkpoint| {
        checkpoint.push(0);
    });
    assert_premanifest_checkpoint_refuses("premanifest-ambiguous-checkpoint", |checkpoint| {
        let first_slot = checkpoint[..64].to_vec();
        checkpoint[64..128].copy_from_slice(&first_slot);
    });
    assert_premanifest_checkpoint_refuses("premanifest-malformed-checkpoint", |checkpoint| {
        checkpoint[0] ^= 1;
    });
}

#[test]
fn exact_premanifest_genesis_checkpoint_shapes_converge_and_reopen() {
    for mode in [
        ActiveJournalOpenMode::OpenExisting,
        ActiveJournalOpenMode::CreateNew,
    ] {
        for checkpoint_shape in ["absent", "zero", "canonical"] {
            let directory = TestDirectory::new(&format!(
                "premanifest-{checkpoint_shape}-{}",
                if mode == ActiveJournalOpenMode::OpenExisting {
                    "open"
                } else {
                    "create"
                }
            ));
            drop(create_active_genesis(&directory));
            let checkpoint_path = directory.path().join(ACTIVE_CHECKPOINT_FILE_NAME);
            match checkpoint_shape {
                "absent" => fs::remove_file(&checkpoint_path).expect("remove checkpoint"),
                "zero" => fs::write(&checkpoint_path, []).expect("zero checkpoint"),
                "canonical" => {}
                _ => unreachable!("closed checkpoint fixture"),
            }
            install_format_marker(&directory);
            let store = ManifestStore::open(manifest_config(&directory, mode, registry_options()))
                .expect("exact premanifest genesis converges");
            assert_eq!(store.inspection().committed().manifest_generation(), 1);
            assert_eq!(
                store
                    .inspection()
                    .committed()
                    .durable_cutoff()
                    .append_sequence(),
                0
            );
            drop(store);
            ManifestStore::open(manifest_config(
                &directory,
                ActiveJournalOpenMode::OpenExisting,
                registry_options(),
            ))
            .expect("converged genesis reopens");
        }
    }
}

#[test]
fn one_replay_retry_and_manifest_v1_match_the_primitive_oracle_exactly() {
    let directory = TestDirectory::new("retry-oracle");
    let admission = support::no_change_admission();
    let declaration = admission.declaration().clone();
    let prepared = frame(admission, 1);
    let mut store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create retry oracle store");
    store
        .register(
            declaration.series_id(),
            declaration.binding().clone(),
            declaration.payload().clone(),
            declaration.evidence().clone(),
        )
        .expect("register retry oracle declaration");
    let end_offset = store.append(&prepared).expect("append retry oracle frame");
    let (commit, state) = store
        .sync_pending(&[PendingRetryOutcome::new(
            prepared.admission().retry().clone(),
            1,
            end_offset,
        )])
        .expect("commit retry oracle outcome");
    assert_eq!(state.replay().len(), 1);
    let retry_reference = commit.retry_state();
    drop(store);

    let retry_name = match retry_reference.slot() {
        0 => RETRY_SLOT_0_FILE_NAME,
        1 => RETRY_SLOT_1_FILE_NAME,
        2 => RETRY_SLOT_2_FILE_NAME,
        _ => panic!("validated retry slot"),
    };
    let retry_bytes =
        fs::read(directory.path().join(retry_name)).expect("read one-outcome retry bytes");
    let qualification = prepared.admission().retry();
    let options = RetryPersistenceOptions::new(2, 2).expect("oracle retry options");
    let expected_retry = manifest_oracle::retry_with_one_replay(
        support::uuid_bytes(1),
        retry_reference.generation(),
        u32::try_from(options.replay_capacity()).expect("oracle replay capacity"),
        u32::try_from(options.guard_capacity()).expect("oracle guard capacity"),
        *qualification.series_id().as_bytes(),
        *qualification.producer_id().as_bytes(),
        qualification.key().as_str(),
        qualification.content().format().as_str(),
        qualification.content().version().get(),
        *qualification.content().sha256(),
        1,
        end_offset,
        commit.manifest_generation(),
        commit.registry_slot(),
        commit.registry_generation(),
        commit.durable_cutoff().checkpoint_generation(),
        commit.durable_cutoff().append_sequence(),
        commit.durable_cutoff().end_offset(),
        retry_reference.slot(),
        retry_reference.generation(),
    );
    assert_eq!(retry_bytes, expected_retry);

    let registry_name = match commit.registry_slot() {
        0 => REGISTRY_SLOT_0_FILE_NAME,
        1 => och_store::REGISTRY_SLOT_1_FILE_NAME,
        2 => och_store::REGISTRY_SLOT_2_FILE_NAME,
        _ => panic!("validated registry slot"),
    };
    let registry_bytes =
        fs::read(directory.path().join(registry_name)).expect("read oracle registry bytes");
    let manifest_bytes = fs::read(directory.path().join(MANIFEST_SLOT_0_FILE_NAME))
        .expect("read oracle V2 manifest bytes");
    assert_eq!(
        manifest_bytes,
        manifest_oracle::current_manifest_v1(
            support::uuid_bytes(1),
            commit.manifest_generation(),
            commit.durable_cutoff().journal().generation(),
            commit.durable_cutoff().checkpoint_generation(),
            commit.durable_cutoff().append_sequence(),
            commit.durable_cutoff().end_offset(),
            commit.registry_slot(),
            commit.registry_generation(),
            &registry_bytes,
            retry_reference.slot(),
            retry_reference.generation(),
            &expected_retry,
            commit.sequence_floor(),
            None,
        )
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn rotation_manifest_catalog_seal_and_retry_v1_match_primitive_oracles_exactly() {
    let directory = TestDirectory::new("rotation-oracles");
    let first_admission = support::no_change_admission_with_retry_key("rotation-first");
    let declaration = first_admission.declaration().clone();
    let first = frame(first_admission, 1);
    let mut store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create rotation oracle store");
    store
        .register(
            declaration.series_id(),
            declaration.binding().clone(),
            declaration.payload().clone(),
            declaration.evidence().clone(),
        )
        .expect("register rotation oracle declaration");
    let first_end = store.append(&first).expect("append first oracle frame");
    let (first_commit, _) = store
        .sync_pending(&[PendingRetryOutcome::new(
            first.admission().retry().clone(),
            1,
            first_end,
        )])
        .expect("commit first oracle frame");
    let expected_sealed =
        manifest_oracle::sealed_raw_journal_v1(support::uuid_bytes(1), &[first.bytes()]);
    let rotated = store.rotate().expect("commit oracle rotation");
    let catalog_reference = rotated
        .generation_catalog()
        .expect("rotation commits catalog identity");
    let sealed_name = "sealed-journal-v1-g00000000000000000001.och";
    let sealed = fs::read(directory.path().join(sealed_name)).expect("read sealed raw journal");
    assert_eq!(sealed, expected_sealed);
    let catalog_name = format!(
        "generation-catalog-v1-slot-{}.och",
        catalog_reference.slot()
    );
    let catalog = fs::read(directory.path().join(catalog_name)).expect("read generation catalog");
    let expected_catalog = manifest_oracle::generation_catalog_v1(
        support::uuid_bytes(1),
        catalog_reference.generation(),
        &[manifest_oracle::CatalogEntry {
            journal_generation: 1,
            sequence_floor: 0,
            sequence_cutoff: 1,
            end_offset: first_end,
            registry_generation: rotated.registry_generation(),
            artifact_length: sealed.len() as u64,
            artifact_checksum: manifest_oracle::checksum(&sealed),
        }],
    );
    assert_eq!(catalog, expected_catalog);
    assert_eq!(catalog_reference.length(), catalog.len() as u64);
    assert_eq!(
        catalog_reference.checksum(),
        manifest_oracle::checksum(&catalog)
    );

    let second_admission = support::no_change_admission_with_retry_key("rotation-second");
    let second = frame(second_admission, 2);
    let second_end = store
        .append(&second)
        .expect("append successor oracle frame");
    let (second_commit, retry_state) = store
        .sync_pending(&[PendingRetryOutcome::new(
            second.admission().retry().clone(),
            2,
            second_end,
        )])
        .expect("commit successor oracle frame");
    assert_eq!(retry_state.replay().len(), 2);
    assert_eq!(retry_state.replay()[0].manifest_commit(), first_commit);
    assert_eq!(second_commit.generation_catalog(), Some(catalog_reference));
    let retry_reference = second_commit.retry_state();
    drop(store);

    let retry_name = format!("retry-state-v1-slot-{}.och", retry_reference.slot());
    let retry_bytes = fs::read(directory.path().join(retry_name)).expect("read Retry State V1");
    let oracle_catalog = manifest_oracle::CatalogReference {
        slot: catalog_reference.slot(),
        generation: catalog_reference.generation(),
        length: catalog_reference.length(),
        checksum: catalog_reference.checksum(),
    };
    let oracle_outcomes = retry_state
        .replay()
        .iter()
        .map(|outcome| {
            let qualification = outcome.qualification();
            let commit = outcome.manifest_commit();
            let cutoff = commit.durable_cutoff();
            let retry = commit.retry_state();
            manifest_oracle::RetryV1Outcome {
                series: *qualification.series_id().as_bytes(),
                producer: *qualification.producer_id().as_bytes(),
                key: qualification.key().as_str(),
                content_format: qualification.content().format().as_str(),
                content_version: qualification.content().version().get(),
                digest: *qualification.content().sha256(),
                append_sequence: outcome.append_sequence(),
                end_offset: outcome.end_offset(),
                manifest_generation: commit.manifest_generation(),
                registry_slot: commit.registry_slot(),
                registry_generation: commit.registry_generation(),
                journal_generation: cutoff.journal().generation(),
                checkpoint_generation: cutoff.checkpoint_generation(),
                cutoff_sequence: cutoff.append_sequence(),
                cutoff_end: cutoff.end_offset(),
                retry_slot: retry.slot(),
                retry_generation: retry.generation(),
                sequence_floor: commit.sequence_floor(),
                catalog: commit.generation_catalog().map(|_| oracle_catalog),
            }
        })
        .collect::<Vec<_>>();
    let retry_options = RetryPersistenceOptions::new(2, 2).expect("oracle retry options");
    let expected_retry = manifest_oracle::retry_v1(
        support::uuid_bytes(1),
        retry_reference.generation(),
        u32::try_from(retry_options.replay_capacity()).expect("replay capacity fits u32"),
        u32::try_from(retry_options.guard_capacity()).expect("guard capacity fits u32"),
        &oracle_outcomes,
    );
    assert_eq!(retry_bytes, expected_retry);

    let registry_name = format!(
        "series-registry-v1-slot-{}.och",
        second_commit.registry_slot()
    );
    let registry_bytes = fs::read(directory.path().join(registry_name)).expect("read registry");
    let manifest_name = if (second_commit.manifest_generation() - 1).is_multiple_of(2) {
        MANIFEST_SLOT_0_FILE_NAME
    } else {
        MANIFEST_SLOT_1_FILE_NAME
    };
    let manifest_bytes = fs::read(directory.path().join(manifest_name)).expect("read Manifest V1");
    let cutoff = second_commit.durable_cutoff();
    assert_eq!(
        manifest_bytes,
        manifest_oracle::current_manifest_v1(
            support::uuid_bytes(1),
            second_commit.manifest_generation(),
            cutoff.journal().generation(),
            cutoff.checkpoint_generation(),
            cutoff.append_sequence(),
            cutoff.end_offset(),
            second_commit.registry_slot(),
            second_commit.registry_generation(),
            &registry_bytes,
            retry_reference.slot(),
            retry_reference.generation(),
            &expected_retry,
            second_commit.sequence_floor(),
            Some(oracle_catalog),
        )
    );

    for forbidden in ["och_store", "och_core", "encode_manifest", "encode_catalog"] {
        assert!(!ORACLE_SOURCE.contains(forbidden));
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn reopen_rejects_checksummed_retry_state_inconsistent_with_current_or_older_manifest_root() {
    for hostile_current in [true, false] {
        let directory = TestDirectory::new(if hostile_current {
            "retry-current-root"
        } else {
            "retry-older-root"
        });
        let admission = support::no_change_admission();
        let declaration = admission.declaration().clone();
        let prepared = frame(admission, 1);
        let mut store = ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::CreateNew,
            registry_options(),
        ))
        .expect("create root validation store");
        store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register root validation declaration");
        let end_offset = store
            .append(&prepared)
            .expect("append root validation frame");
        let (commit, _) = store
            .sync_pending(&[PendingRetryOutcome::new(
                prepared.admission().retry().clone(),
                1,
                end_offset,
            )])
            .expect("commit root validation outcome");
        drop(store);

        if hostile_current {
            let retry_reference = commit.retry_state();
            let retry_name = match retry_reference.slot() {
                0 => RETRY_SLOT_0_FILE_NAME,
                1 => RETRY_SLOT_1_FILE_NAME,
                2 => RETRY_SLOT_2_FILE_NAME,
                _ => panic!("validated retry slot"),
            };
            let retry_path = directory.path().join(retry_name);
            let mut retry = fs::read(&retry_path).expect("read current retry bytes");
            let qualification = prepared.admission().retry();
            let qualification_len = 16
                + 16
                + 4
                + qualification.key().as_str().len()
                + 4
                + qualification.content().format().as_str().len()
                + 16
                + 32;
            let manifest_generation_offset = 64 + qualification_len + 16;
            retry[manifest_generation_offset..manifest_generation_offset + 8].copy_from_slice(
                &commit
                    .manifest_generation()
                    .checked_add(1)
                    .expect("bounded future generation")
                    .to_be_bytes(),
            );
            rewrite_trailing_crc(&mut retry);
            fs::write(&retry_path, &retry).expect("write checksummed future retry outcome");

            let manifest_path = directory.path().join(MANIFEST_SLOT_0_FILE_NAME);
            let mut manifest = fs::read(&manifest_path).expect("read current manifest bytes");
            manifest[112..116].copy_from_slice(&crc32c(&retry).to_be_bytes());
            rewrite_trailing_crc(&mut manifest);
            fs::write(manifest_path, manifest).expect("root future retry from current manifest");
        } else {
            let retry_path = directory.path().join(RETRY_SLOT_0_FILE_NAME);
            let qualification = prepared.admission().retry();
            let retry = manifest_oracle::retry_with_one_replay(
                support::uuid_bytes(1),
                1,
                2,
                2,
                *qualification.series_id().as_bytes(),
                *qualification.producer_id().as_bytes(),
                qualification.key().as_str(),
                qualification.content().format().as_str(),
                qualification.content().version().get(),
                *qualification.content().sha256(),
                1,
                end_offset,
                2,
                commit.registry_slot(),
                commit.registry_generation(),
                commit.durable_cutoff().checkpoint_generation(),
                1,
                end_offset,
                0,
                1,
            );
            fs::write(&retry_path, &retry).expect("write future outcome under older root");

            let manifest_path = directory.path().join(MANIFEST_SLOT_1_FILE_NAME);
            let mut manifest = fs::read(&manifest_path).expect("read older manifest bytes");
            assert_eq!(u64::from_be_bytes(manifest[28..36].try_into().unwrap()), 2);
            manifest[104..112].copy_from_slice(
                &u64::try_from(retry.len())
                    .expect("bounded hostile retry length")
                    .to_be_bytes(),
            );
            manifest[112..116].copy_from_slice(&crc32c(&retry).to_be_bytes());
            rewrite_trailing_crc(&mut manifest);
            fs::write(manifest_path, manifest).expect("root hostile older retry snapshot");
        }

        let before = exact_file_inventory(directory.path());
        let Err(error) = ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::OpenExisting,
            registry_options(),
        )) else {
            panic!("root-inconsistent retry must refuse on reopen");
        };
        assert_eq!(
            error,
            ManifestStoreError::InvalidRetry,
            "hostile current case: {hostile_current}"
        );
        assert_eq!(exact_file_inventory(directory.path()), before);
    }
}

#[test]
fn foreign_store_registry_referenced_by_valid_manifest_refuses_unchanged() {
    let directory = TestDirectory::new("foreign-registry");
    let store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create local manifest store");
    drop(store);

    let foreign_directory = TestDirectory::new("foreign-registry-source");
    let foreign_config = ManifestStoreConfig::new(
        foreign_directory.path().to_path_buf(),
        support::store_id(2),
        ActiveJournalOpenMode::CreateNew,
        journal_limits(),
        registry_options(),
        RetryPersistenceOptions::new(2, 2).expect("bounded retry limits"),
    )
    .expect("valid foreign manifest config");
    let foreign = ManifestStore::open(foreign_config).expect("create foreign manifest store");
    drop(foreign);

    let foreign_registry = fs::read(foreign_directory.path().join(REGISTRY_SLOT_0_FILE_NAME))
        .expect("read valid foreign registry artifact");
    fs::write(
        directory.path().join(REGISTRY_SLOT_0_FILE_NAME),
        &foreign_registry,
    )
    .expect("install foreign registry artifact");
    let manifest_path = directory.path().join(MANIFEST_SLOT_0_FILE_NAME);
    let mut manifest = fs::read(&manifest_path).expect("read local manifest");
    manifest[80..88].copy_from_slice(
        &u64::try_from(foreign_registry.len())
            .expect("bounded foreign registry length")
            .to_be_bytes(),
    );
    manifest[88..92].copy_from_slice(&crc32c(&foreign_registry).to_be_bytes());
    rewrite_trailing_crc(&mut manifest);
    fs::write(&manifest_path, manifest).expect("write valid foreign registry reference");

    let before = exact_file_inventory(directory.path());
    let result = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::OpenExisting,
        registry_options(),
    ));
    assert!(matches!(result, Err(ManifestStoreError::StoreMismatch)));
    assert_eq!(exact_file_inventory(directory.path()), before);
}

#[test]
#[allow(clippy::too_many_lines)]
fn lifecycle_commits_restart_and_historical_admission_survives_retirement() {
    let directory = TestDirectory::new("lifecycle");
    let initial = support::observed_admission(
        vec![ExactValue::Boolean(true)],
        ValueFamily::Boolean,
        0,
        false,
    );
    let corrected = support::observed_admission(
        vec![ExactValue::Boolean(true)],
        ValueFamily::Boolean,
        0,
        true,
    );
    let initial_envelope = initial.envelope().clone();
    let initial_declaration = initial.declaration().clone();
    let corrected_declaration = corrected.declaration().clone();
    let initial_frame = frame(initial, 1);
    let retirement_evidence =
        DeclarationEvidence::new(Timestamp::new(7, 0).expect("retirement timestamp"), None);

    let mut store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create store");
    let (registered, register_commit) = store
        .register(
            initial_declaration.series_id(),
            initial_declaration.binding().clone(),
            initial_declaration.payload().clone(),
            initial_declaration.evidence().clone(),
        )
        .expect("register revision one");
    assert_eq!(registered, initial_declaration);
    assert_eq!(register_commit.manifest_generation(), 2);
    let (_, replay_commit) = store
        .register(
            initial_declaration.series_id(),
            initial_declaration.binding().clone(),
            initial_declaration.payload().clone(),
            initial_declaration.evidence().clone(),
        )
        .expect("exact registration replay");
    assert_eq!(replay_commit, register_commit);
    assert_eq!(
        store
            .bind(initial_envelope.clone())
            .expect("active declaration binds")
            .declaration(),
        &initial_declaration
    );

    let (revised, revise_commit) = store
        .revise(
            initial_declaration.series_id(),
            DeclarationRevision::FIRST,
            corrected_declaration.payload().clone(),
            corrected_declaration.evidence().clone(),
        )
        .expect("commit revision two");
    assert_eq!(revised, corrected_declaration);
    assert_eq!(revise_commit.manifest_generation(), 3);
    let (_, revise_replay) = store
        .revise(
            initial_declaration.series_id(),
            DeclarationRevision::FIRST,
            corrected_declaration.payload().clone(),
            corrected_declaration.evidence().clone(),
        )
        .expect("exact revision replay");
    assert_eq!(revise_replay, revise_commit);

    let (retirement, retire_commit) = store
        .retire(
            initial_declaration.series_id(),
            corrected_declaration.revision(),
            retirement_evidence.clone(),
        )
        .expect("terminal retirement");
    assert_eq!(retire_commit.manifest_generation(), 4);
    let (_, retire_replay) = store
        .retire(
            initial_declaration.series_id(),
            corrected_declaration.revision(),
            retirement_evidence,
        )
        .expect("exact retirement replay");
    assert_eq!(retire_replay, retire_commit);
    assert_eq!(
        retirement.declaration_revision(),
        corrected_declaration.revision()
    );
    assert_eq!(
        store.bind(initial_envelope),
        Err(ManifestStoreError::Model(ModelError::SeriesRetired))
    );

    let end_offset = store
        .append(&initial_frame)
        .expect("retained revision-one declaration remains admissible");
    let pending = [PendingRetryOutcome::new(
        initial_frame.admission().retry().clone(),
        initial_frame.append_sequence().get(),
        end_offset,
    )];
    let (durable, _) = store
        .sync_pending(&pending)
        .expect("manifest-backed durability");
    assert_eq!(durable.manifest_generation(), 5);
    assert_eq!(durable.registry_generation(), 4);
    let snapshot = store.registry_snapshot();
    drop(store);

    let reopened = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::OpenExisting,
        registry_options(),
    ))
    .expect("reopen complete history and tombstone");
    assert_eq!(reopened.registry_snapshot(), snapshot);
    assert_eq!(reopened.recovered_records().len(), 1);
    assert_eq!(reopened.inspection().committed(), durable);
}

#[test]
fn unknown_historical_declaration_and_core_capacity_refuse_unchanged() {
    let directory = TestDirectory::new("refuse");
    let known = support::no_change_admission();
    let known_declaration = known.declaration().clone();
    let unknown = support::observed_admission(
        vec![ExactValue::Boolean(true)],
        ValueFamily::Boolean,
        0,
        false,
    );
    let mut store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create store");
    store
        .register(
            known_declaration.series_id(),
            known_declaration.binding().clone(),
            known_declaration.payload().clone(),
            known_declaration.evidence().clone(),
        )
        .expect("register known declaration");
    let before = store.inspection();
    let bytes_before = fs::metadata(directory.path().join(ACTIVE_JOURNAL_FILE_NAME))
        .expect("journal metadata")
        .len();
    assert_eq!(
        store.append(&frame(unknown, 1)),
        Err(ManifestStoreError::HistoricalDeclarationMismatch)
    );
    assert_eq!(store.inspection(), before);
    assert_eq!(
        fs::metadata(directory.path().join(ACTIVE_JOURNAL_FILE_NAME))
            .expect("journal metadata after refusal")
            .len(),
        bytes_before
    );

    let tiny_directory = TestDirectory::new("capacity");
    let tiny_options =
        RegistryPersistenceOptions::new(SeriesRegistryLimits::new(1, 1)).expect("tiny limits");
    let mut tiny = ManifestStore::open(manifest_config(
        &tiny_directory,
        ActiveJournalOpenMode::CreateNew,
        tiny_options,
    ))
    .expect("create tiny store");
    tiny.register(
        known_declaration.series_id(),
        known_declaration.binding().clone(),
        known_declaration.payload().clone(),
        known_declaration.evidence().clone(),
    )
    .expect("fill series and revision capacity");
    let before = tiny.inspection();
    assert_eq!(
        tiny.revise(
            known_declaration.series_id(),
            DeclarationRevision::FIRST,
            support::observed_admission(
                vec![ExactValue::Boolean(true)],
                ValueFamily::Boolean,
                0,
                true,
            )
            .declaration()
            .payload()
            .clone(),
            DeclarationEvidence::new(Timestamp::new(9, 0).expect("timestamp"), None),
        ),
        Err(ManifestStoreError::Model(
            ModelError::RegistryRevisionCapacityExceeded
        ))
    );
    assert_eq!(tiny.inspection(), before);
}

#[test]
#[allow(clippy::too_many_lines)]
fn hostile_manifest_registry_inventory_and_cutoff_refuse_without_repair() {
    for (name, target, offset) in [
        ("manifest-version", MANIFEST_SLOT_0_FILE_NAME, 9_usize),
        ("manifest-checksum", MANIFEST_SLOT_0_FILE_NAME, 127_usize),
        ("registry-version", REGISTRY_SLOT_0_FILE_NAME, 9_usize),
        ("registry-checksum", REGISTRY_SLOT_0_FILE_NAME, 67_usize),
        ("retry-version", RETRY_SLOT_0_FILE_NAME, 9_usize),
        ("retry-checksum", RETRY_SLOT_0_FILE_NAME, 67_usize),
    ] {
        let directory = TestDirectory::new(name);
        let store = ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::CreateNew,
            registry_options(),
        ))
        .expect("create hostile-byte fixture");
        drop(store);
        let path = directory.path().join(target);
        let mut bytes = fs::read(&path).expect("read fixture bytes");
        bytes[offset] ^= 0xff;
        fs::write(path, bytes).expect("write hostile bytes");
        assert!(matches!(
            ManifestStore::open(manifest_config(
                &directory,
                ActiveJournalOpenMode::OpenExisting,
                registry_options(),
            )),
            Err(ManifestStoreError::InvalidManifest
                | ManifestStoreError::InvalidRegistry
                | ManifestStoreError::InvalidRetry
                | ManifestStoreError::UnsupportedStoreFormat)
        ));
    }

    let cutoff = TestDirectory::new("cutoff");
    let store = ManifestStore::open(manifest_config(
        &cutoff,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create cutoff fixture");
    drop(store);
    let path = cutoff.path().join(MANIFEST_SLOT_0_FILE_NAME);
    let mut bytes = fs::read(&path).expect("read manifest");
    bytes[52..60].copy_from_slice(&1_u64.to_be_bytes());
    bytes[60..68].copy_from_slice(&29_u64.to_be_bytes());
    rewrite_trailing_crc(&mut bytes);
    fs::write(path, bytes).expect("write mismatched manifest cutoff");
    assert!(matches!(
        ManifestStore::open(manifest_config(
            &cutoff,
            ActiveJournalOpenMode::OpenExisting,
            registry_options(),
        )),
        Err(ManifestStoreError::InvalidManifest)
    ));

    for artifact in [
        "unknown.och",
        MANIFEST_STAGING_FILE_NAME,
        REGISTRY_STAGING_FILE_NAME,
        och_store::RETRY_STAGING_FILE_NAME,
    ] {
        let directory = TestDirectory::new("inventory");
        let store = ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::CreateNew,
            registry_options(),
        ))
        .expect("create inventory fixture");
        drop(store);
        fs::write(directory.path().join(artifact), b"evidence")
            .expect("write hostile inventory evidence");
        let expected = if artifact == "unknown.och" {
            ManifestStoreError::UnsupportedStoreFormat
        } else {
            ManifestStoreError::InterruptedPublication
        };
        assert!(matches!(
            ManifestStore::open(manifest_config(
                &directory,
                ActiveJournalOpenMode::OpenExisting,
                registry_options(),
            )),
            Err(error) if error == expected
        ));
    }

    let unreferenced = TestDirectory::new("unreferenced-retry");
    let store = ManifestStore::open(manifest_config(
        &unreferenced,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create unreferenced retry fixture");
    drop(store);
    fs::copy(
        unreferenced.path().join(RETRY_SLOT_0_FILE_NAME),
        unreferenced.path().join(RETRY_SLOT_1_FILE_NAME),
    )
    .expect("copy valid but unreferenced retry snapshot");
    assert!(matches!(
        ManifestStore::open(manifest_config(
            &unreferenced,
            ActiveJournalOpenMode::OpenExisting,
            registry_options(),
        )),
        Err(ManifestStoreError::InvalidRetry)
    ));

    let mismatch = TestDirectory::new("retry-options-mismatch");
    let store = ManifestStore::open(manifest_config(
        &mismatch,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("create retry options fixture");
    drop(store);
    let mismatch_config = ManifestStoreConfig::new(
        mismatch.path().to_path_buf(),
        support::store_id(1),
        ActiveJournalOpenMode::OpenExisting,
        journal_limits(),
        registry_options(),
        RetryPersistenceOptions::new(1, 1).expect("other valid retry options"),
    )
    .expect("construct mismatched retry options");
    assert!(matches!(
        ManifestStore::open(mismatch_config),
        Err(ManifestStoreError::InvalidRetry)
    ));
}

#[test]
fn unrelated_invalid_directory_refuses_without_creating_the_stable_lock() {
    let directory = TestDirectory::new("unrelated");
    let unrelated = directory.path().join("belongs-to-caller.bin");
    let original = b"unrelated bytes remain exact";
    fs::write(&unrelated, original).expect("write unrelated caller artifact");
    let names_before = fs::read_dir(directory.path())
        .expect("read original inventory")
        .map(|entry| entry.expect("inventory entry").file_name())
        .collect::<Vec<_>>();
    assert!(matches!(
        ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::CreateNew,
            registry_options(),
        )),
        Err(ManifestStoreError::UnsupportedStoreFormat)
    ));
    let names_after = fs::read_dir(directory.path())
        .expect("read refused inventory")
        .map(|entry| entry.expect("inventory entry").file_name())
        .collect::<Vec<_>>();
    assert_eq!(names_after, names_before);
    assert_eq!(fs::read(unrelated).expect("read unrelated bytes"), original);
    assert!(!directory.path().join(STORE_LOCK_FILE_NAME).exists());
}

#[test]
fn child_process_manifest_lock_probe() {
    let Ok(directory) = std::env::var("OCH_MANIFEST_LOCK_DIRECTORY") else {
        return;
    };
    let directory = TestDirectory(PathBuf::from(directory));
    let _store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::CreateNew,
        registry_options(),
    ))
    .expect("child owns manifest store");
    fs::write(directory.path().with_extension("ready"), b"ready")
        .expect("publish readiness outside store inventory");
    loop {
        std::thread::park();
    }
}

#[test]
fn stable_store_lock_excludes_a_real_child_process() {
    let directory = TestDirectory::new("process-lock");
    let marker = directory.path().with_extension("ready");
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "child_process_manifest_lock_probe",
            "--nocapture",
        ])
        .env("OCH_MANIFEST_LOCK_DIRECTORY", directory.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn manifest lock child");
    let mut ready = false;
    for _ in 0..5_000 {
        if marker.is_file() {
            ready = true;
            break;
        }
        assert!(
            child.try_wait().expect("inspect child").is_none(),
            "child exited before acquiring lock"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(ready, "child must acquire stable store lock");
    assert!(matches!(
        ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::OpenExisting,
            registry_options(),
        )),
        Err(ManifestStoreError::AlreadyOpen)
    ));
    child.kill().expect("kill lock child");
    let status = child.wait().expect("reap lock child");
    assert!(!status.success());
    assert!(directory.path().join(STORE_LOCK_FILE_NAME).exists());
}
