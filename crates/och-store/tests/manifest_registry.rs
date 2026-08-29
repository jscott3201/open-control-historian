#![forbid(unsafe_code)]
//! Manifest-rooted registry, bootstrap, lifecycle, and hostile-byte evidence.

#[path = "support/manifest_oracle.rs"]
mod manifest_oracle;
mod support;

use och_core::{
    CanonicalAdmission, DeclarationEvidence, DeclarationRevision, ExactValue, ModelError,
    SeriesRegistry, SeriesRegistryLimits, SeriesRegistrySnapshot, Timestamp, ValueFamily,
};
use och_store::{
    ACTIVE_JOURNAL_FILE_NAME, ActiveJournal, ActiveJournalConfig, ActiveJournalLimits,
    ActiveJournalOpenMode, AppendSequenceV1, JOURNAL_V1_HEADER_LEN, JournalHeaderV1,
    JournalHeaderV2, MANIFEST_SLOT_0_FILE_NAME, MANIFEST_STAGING_FILE_NAME,
    MAX_ADMISSION_PAYLOAD_V1, ManifestStore, ManifestStoreConfig, ManifestStoreError,
    PreparedAdmissionV1, REGISTRY_SLOT_0_FILE_NAME, REGISTRY_STAGING_FILE_NAME,
    RegistryPersistenceOptions, STORE_LOCK_FILE_NAME,
};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
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

fn registry_options_with(snapshot: SeriesRegistrySnapshot) -> RegistryPersistenceOptions {
    registry_options()
        .with_bootstrap_snapshot(snapshot)
        .expect("matching bounded bootstrap")
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
    )
    .expect("valid manifest config")
}

fn active_config(directory: &TestDirectory, mode: ActiveJournalOpenMode) -> ActiveJournalConfig {
    ActiveJournalConfig::new(
        directory.path().to_path_buf(),
        support::store_id(1),
        mode,
        journal_limits(),
    )
    .expect("valid active config")
}

fn snapshot_for(admission: &CanonicalAdmission) -> SeriesRegistrySnapshot {
    let declaration = admission.declaration();
    let mut registry = SeriesRegistry::new(admission.store_id(), registry_limits());
    let actual = registry
        .register(
            declaration.series_id(),
            declaration.binding().clone(),
            declaration.payload().clone(),
            declaration.evidence().clone(),
        )
        .expect("fixture declaration registers");
    assert_eq!(&actual, declaration);
    registry.snapshot()
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
    let manifest = fs::read(directory.path().join(MANIFEST_SLOT_0_FILE_NAME))
        .expect("read genesis manifest bytes");
    assert_eq!(
        manifest,
        manifest_oracle::manifest(
            support::uuid_bytes(1),
            1,
            1,
            0,
            JOURNAL_V1_HEADER_LEN as u64,
            0,
            1,
            &expected_registry,
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
fn nonempty_v1_or_v2_premanifest_requires_and_verifies_complete_snapshot() {
    let directory = TestDirectory::new("bootstrap");
    let admission = support::no_change_admission();
    let snapshot = snapshot_for(&admission);
    let mut journal =
        ActiveJournal::open(active_config(&directory, ActiveJournalOpenMode::CreateNew))
            .expect("create legacy active journal");
    journal.append(&frame(admission, 1)).expect("legacy append");
    journal.sync_pending().expect("legacy durable cutoff");
    drop(journal);

    assert!(matches!(
        ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::OpenExisting,
            registry_options(),
        )),
        Err(ManifestStoreError::BootstrapSnapshotRequired)
    ));
    assert!(
        !directory.path().join(MANIFEST_SLOT_0_FILE_NAME).exists(),
        "a refused bootstrap must not publish a manifest"
    );

    let journal_path = directory.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&journal_path)
        .expect("open legacy header");
    let mut header = [0_u8; JOURNAL_V1_HEADER_LEN];
    file.read_exact(&mut header).expect("read legacy header");
    assert_eq!(
        JournalHeaderV1::decode(&header),
        Ok(JournalHeaderV1::new(support::store_id(1)))
    );
    file.seek(SeekFrom::Start(0)).expect("rewind header");
    file.write_all(&JournalHeaderV2::new(support::store_id(1)).encode())
        .expect("simulate interrupted header-only bootstrap fence");
    file.sync_all().expect("sync v2 header");
    drop(file);

    assert!(matches!(
        ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::OpenExisting,
            registry_options(),
        )),
        Err(ManifestStoreError::BootstrapSnapshotRequired)
    ));
    let empty = SeriesRegistry::new(support::store_id(1), registry_limits()).snapshot();
    assert!(matches!(
        ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::OpenExisting,
            registry_options_with(empty),
        )),
        Err(ManifestStoreError::BootstrapSnapshotMismatch)
    ));

    let store = ManifestStore::open(manifest_config(
        &directory,
        ActiveJournalOpenMode::OpenExisting,
        registry_options_with(snapshot.clone()),
    ))
    .expect("matching complete snapshot bootstraps");
    assert_eq!(store.registry_snapshot(), snapshot);
    assert_eq!(store.recovered_records().len(), 1);
    assert_eq!(store.inspection().committed().manifest_generation(), 1);
}

#[test]
fn exact_header_only_v1_and_v2_stores_bootstrap_empty() {
    for v2 in [false, true] {
        let directory = TestDirectory::new(if v2 { "empty-v2" } else { "empty-v1" });
        let journal =
            ActiveJournal::open(active_config(&directory, ActiveJournalOpenMode::CreateNew))
                .expect("create header-only legacy store");
        drop(journal);
        if v2 {
            let path = directory.path().join(ACTIVE_JOURNAL_FILE_NAME);
            let mut file = OpenOptions::new()
                .write(true)
                .open(path)
                .expect("open header-only journal");
            file.write_all(&JournalHeaderV2::new(support::store_id(1)).encode())
                .expect("write v2 header");
            file.sync_all().expect("sync v2 header");
        }
        let store = ManifestStore::open(manifest_config(
            &directory,
            ActiveJournalOpenMode::OpenExisting,
            registry_options(),
        ))
        .expect("header-only store auto-bootstraps empty registry");
        assert!(store.registry_snapshot().series().is_empty());
    }
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

    store
        .append(&initial_frame)
        .expect("retained revision-one declaration remains admissible");
    let durable = store.sync_pending().expect("manifest-backed durability");
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
fn hostile_manifest_registry_inventory_and_cutoff_refuse_without_repair() {
    for (name, target, offset) in [
        ("manifest-version", MANIFEST_SLOT_0_FILE_NAME, 9_usize),
        ("manifest-checksum", MANIFEST_SLOT_0_FILE_NAME, 127_usize),
        ("registry-version", REGISTRY_SLOT_0_FILE_NAME, 9_usize),
        ("registry-checksum", REGISTRY_SLOT_0_FILE_NAME, 67_usize),
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
            Err(ManifestStoreError::InvalidManifest | ManifestStoreError::InvalidRegistry)
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
            ManifestStoreError::InvalidInventory
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
        Err(ManifestStoreError::InvalidInventory)
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
