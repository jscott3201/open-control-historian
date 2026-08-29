#![forbid(unsafe_code)]
//! Focused active-journal ownership, cutoff, and bounded-reopen evidence.

mod support;

use och_store::{
    ACTIVE_CHECKPOINT_FILE_NAME, ACTIVE_JOURNAL_FILE_NAME, ActiveJournal, ActiveJournalConfig,
    ActiveJournalError, ActiveJournalLimits, ActiveJournalOpenMode, AppendSequenceV1,
    JOURNAL_V1_HEADER_LEN, JournalHeaderV1, MAX_ADMISSION_PAYLOAD_V1, PreparedAdmissionV1,
};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "och-store-{name}-{}-{sequence}",
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
    }
}

fn limits(bytes: u64, records: usize) -> ActiveJournalLimits {
    ActiveJournalLimits::new(MAX_ADMISSION_PAYLOAD_V1, bytes, records).expect("valid active limits")
}

fn config(
    directory: &TestDirectory,
    store_number: u64,
    mode: ActiveJournalOpenMode,
    limits: ActiveJournalLimits,
) -> ActiveJournalConfig {
    ActiveJournalConfig::new(
        directory.path().to_path_buf(),
        support::store_id(store_number),
        mode,
        limits,
    )
    .expect("valid active configuration")
}

fn frame(sequence: u64) -> och_store::PreparedFrameV1 {
    PreparedAdmissionV1::new(support::no_change_admission())
        .expect("bounded preparation")
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

#[test]
fn create_append_sync_and_bounded_reopen_preserve_exact_cutoff() {
    let directory = TestDirectory::new("committed");
    let limits = limits(4 * 1_024 * 1_024, 8);
    let mut journal = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::CreateNew,
        limits,
    ))
    .expect("create active journal");
    assert_eq!(
        journal.inspection().active_bytes(),
        JOURNAL_V1_HEADER_LEN as u64
    );
    assert_eq!(journal.inspection().durable_cutoff().append_sequence(), 0);
    assert_eq!(
        journal
            .inspection()
            .durable_cutoff()
            .checkpoint_generation(),
        1
    );
    assert!(journal.recovered_records().is_empty());

    let expected_retry = frame(1).admission().retry().clone();
    let end = journal.append(&frame(1)).expect("append first frame");
    assert_eq!(journal.inspection().last_append_sequence(), 1);
    assert_eq!(journal.inspection().durable_cutoff().append_sequence(), 0);
    let cutoff = journal.sync_pending().expect("durable barrier");
    assert_eq!(cutoff.append_sequence(), 1);
    assert_eq!(cutoff.end_offset(), end);
    assert_eq!(cutoff.checkpoint_generation(), 2);
    assert_eq!(journal.inspection().sync_count(), 1);
    drop(journal);

    let reopened = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::OpenExisting,
        limits,
    ))
    .expect("reopen committed prefix");
    assert_eq!(reopened.inspection().active_records(), 1);
    assert_eq!(reopened.inspection().active_bytes(), end);
    assert_eq!(reopened.inspection().durable_cutoff().append_sequence(), 1);
    assert_eq!(
        reopened
            .inspection()
            .durable_cutoff()
            .checkpoint_generation(),
        2
    );
    assert_eq!(reopened.recovered_records()[0].retry(), &expected_retry);
}

#[test]
fn lock_store_scope_sequence_and_capacity_refuse_before_logical_mutation() {
    let directory = TestDirectory::new("refusals");
    let prepared = frame(1);
    let exact_bytes = JOURNAL_V1_HEADER_LEN as u64
        + u64::try_from(prepared.len()).expect("frame length fits u64");
    let exact = limits(exact_bytes, 1);
    let mut journal = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::CreateNew,
        exact,
    ))
    .expect("create exact-bound journal");
    assert!(matches!(
        ActiveJournal::open(config(
            &directory,
            1,
            ActiveJournalOpenMode::OpenExisting,
            exact,
        )),
        Err(ActiveJournalError::AlreadyOpen)
    ));
    assert_eq!(journal.append(&prepared), Ok(exact_bytes));
    let before = journal.inspection();
    assert_eq!(
        journal.append(&frame(1)),
        Err(ActiveJournalError::SequenceMismatch)
    );
    assert_eq!(
        journal.append(&frame(2)),
        Err(ActiveJournalError::RotationRequired)
    );
    assert_eq!(journal.inspection(), before);
    drop(journal);

    assert!(matches!(
        ActiveJournal::open(config(
            &directory,
            2,
            ActiveJournalOpenMode::OpenExisting,
            exact,
        )),
        Err(ActiveJournalError::StoreMismatch)
    ));

    let other = TestDirectory::new("scope");
    let mut other_journal = ActiveJournal::open(config(
        &other,
        2,
        ActiveJournalOpenMode::CreateNew,
        limits(4 * 1_024 * 1_024, 8),
    ))
    .expect("other store journal");
    let before = other_journal.inspection();
    assert_eq!(
        other_journal.append(&frame(1)),
        Err(ActiveJournalError::StoreMismatch)
    );
    assert_eq!(other_journal.inspection(), before);
}

#[test]
fn valid_unacknowledged_suffix_is_adopted_but_torn_suffix_is_truncated() {
    let directory = TestDirectory::new("suffix");
    let limits = limits(4 * 1_024 * 1_024, 8);
    let mut journal = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::CreateNew,
        limits,
    ))
    .expect("create active journal");
    let adopted_end = journal.append(&frame(1)).expect("append valid suffix");
    drop(journal);

    let reopened = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::OpenExisting,
        limits,
    ))
    .expect("adopt and checkpoint valid suffix");
    assert_eq!(reopened.inspection().durable_cutoff().append_sequence(), 1);
    assert_eq!(
        reopened
            .inspection()
            .durable_cutoff()
            .checkpoint_generation(),
        2
    );
    assert_eq!(reopened.inspection().active_bytes(), adopted_end);
    drop(reopened);

    let journal_path = directory.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&journal_path)
        .expect("open unlocked journal");
    file.seek(SeekFrom::End(0)).expect("seek to suffix");
    file.write_all(&frame(2).bytes()[..11])
        .expect("write torn suffix");
    file.sync_all().expect("make hostile suffix visible");
    drop(file);

    let reopened = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::OpenExisting,
        limits,
    ))
    .expect("truncate proven unacknowledged suffix");
    assert_eq!(reopened.inspection().active_bytes(), adopted_end);
    assert_eq!(
        fs::metadata(journal_path).expect("journal metadata").len(),
        adopted_end
    );
}

#[test]
fn durable_corruption_and_ambiguous_checkpoint_corruption_refuse() {
    let directory = TestDirectory::new("corruption");
    let limits = limits(4 * 1_024 * 1_024, 8);
    let mut journal = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::CreateNew,
        limits,
    ))
    .expect("create active journal");
    journal.append(&frame(1)).expect("append frame");
    journal.sync_pending().expect("commit frame");
    drop(journal);

    let journal_path = directory.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&journal_path)
        .expect("open journal");
    file.seek(SeekFrom::Start(JOURNAL_V1_HEADER_LEN as u64 + 8))
        .expect("seek into durable frame");
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).expect("read durable byte");
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(JOURNAL_V1_HEADER_LEN as u64 + 8))
        .expect("seek back");
    file.write_all(&byte).expect("corrupt durable byte");
    file.sync_all().expect("sync corruption");
    drop(file);
    assert!(matches!(
        ActiveJournal::open(config(
            &directory,
            1,
            ActiveJournalOpenMode::OpenExisting,
            limits,
        )),
        Err(ActiveJournalError::InvalidLayout)
    ));

    let checkpoint_directory = TestDirectory::new("checkpoint");
    let journal = ActiveJournal::open(config(
        &checkpoint_directory,
        1,
        ActiveJournalOpenMode::CreateNew,
        limits,
    ))
    .expect("create checkpoint fixture");
    drop(journal);
    let checkpoint_path = checkpoint_directory
        .path()
        .join(ACTIVE_CHECKPOINT_FILE_NAME);
    let mut checkpoint = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&checkpoint_path)
        .expect("open checkpoint");
    checkpoint
        .seek(SeekFrom::Start(7))
        .expect("seek checkpoint");
    checkpoint.write_all(&[1]).expect("corrupt nonzero slot");
    checkpoint.sync_all().expect("sync checkpoint corruption");
    drop(checkpoint);
    assert!(matches!(
        ActiveJournal::open(config(
            &checkpoint_directory,
            1,
            ActiveJournalOpenMode::OpenExisting,
            limits,
        )),
        Err(ActiveJournalError::InvalidLayout)
    ));
}

#[test]
fn checkpoint_slots_require_strict_cutoff_progress_even_with_a_valid_crc() {
    let directory = TestDirectory::new("checkpoint-progress");
    let limits = limits(4 * 1_024 * 1_024, 8);
    let mut journal = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::CreateNew,
        limits,
    ))
    .expect("create checkpoint progress fixture");
    journal.append(&frame(1)).expect("append first frame");
    journal.sync_pending().expect("publish second slot");
    drop(journal);

    let checkpoint_path = directory.path().join(ACTIVE_CHECKPOINT_FILE_NAME);
    let journal_path = directory.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let journal_before = fs::read(&journal_path).expect("read journal before hostile open");
    let mut checkpoint_bytes = fs::read(&checkpoint_path).expect("read checkpoint slots");
    let newer = 64;
    checkpoint_bytes[newer + 44..newer + 52].copy_from_slice(&0_u64.to_be_bytes());
    checkpoint_bytes[newer + 52..newer + 60]
        .copy_from_slice(&(JOURNAL_V1_HEADER_LEN as u64).to_be_bytes());
    let checksum = crc32c(&checkpoint_bytes[newer..newer + 60]);
    checkpoint_bytes[newer + 60..newer + 64].copy_from_slice(&checksum.to_be_bytes());
    let mut checkpoint = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&checkpoint_path)
        .expect("open checkpoint for hostile rewrite");
    checkpoint
        .write_all(&checkpoint_bytes)
        .expect("write recomputed checkpoint CRC");
    checkpoint.sync_all().expect("sync hostile checkpoint");
    drop(checkpoint);

    assert!(matches!(
        ActiveJournal::open(config(
            &directory,
            1,
            ActiveJournalOpenMode::OpenExisting,
            limits,
        )),
        Err(ActiveJournalError::InvalidLayout)
    ));
    assert_eq!(
        fs::read(&checkpoint_path).expect("checkpoint remains unchanged"),
        checkpoint_bytes
    );
    assert_eq!(
        fs::read(journal_path).expect("journal remains unchanged"),
        journal_before
    );
}

#[test]
fn malformed_complete_suffix_with_later_candidate_refuses_without_truncation() {
    let directory = TestDirectory::new("ambiguous-suffix");
    let limits = limits(4 * 1_024 * 1_024, 8);
    let mut journal = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::CreateNew,
        limits,
    ))
    .expect("create ambiguous suffix fixture");
    journal.append(&frame(1)).expect("append durable prefix");
    journal.sync_pending().expect("commit durable prefix");
    drop(journal);

    let journal_path = directory.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let mut malformed = frame(2).bytes().to_vec();
    let checksum_byte = malformed.len() - 1;
    malformed[checksum_byte] ^= 0xff;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&journal_path)
        .expect("open journal for ambiguous suffix");
    file.seek(SeekFrom::End(0)).expect("seek suffix end");
    file.write_all(&malformed)
        .expect("write complete malformed frame");
    file.write_all(frame(2).bytes())
        .expect("write later valid candidate");
    file.sync_all().expect("sync ambiguous suffix fixture");
    drop(file);
    let before = fs::read(&journal_path).expect("read ambiguous journal");

    assert!(matches!(
        ActiveJournal::open(config(
            &directory,
            1,
            ActiveJournalOpenMode::OpenExisting,
            limits,
        )),
        Err(ActiveJournalError::InvalidLayout)
    ));
    assert_eq!(
        fs::read(journal_path).expect("ambiguous journal remains unchanged"),
        before
    );
}

#[test]
fn interrupted_genesis_recovers_only_an_exact_header_only_journal() {
    let directory = TestDirectory::new("interrupted-genesis");
    let limits = limits(4 * 1_024 * 1_024, 8);
    let journal_path = directory.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let checkpoint_path = directory.path().join(ACTIVE_CHECKPOINT_FILE_NAME);
    let mut journal_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&journal_path)
        .expect("create interrupted journal");
    journal_file
        .write_all(&JournalHeaderV1::new(support::store_id(1)).encode())
        .expect("write exact journal header");
    journal_file.sync_all().expect("sync journal genesis");
    drop(journal_file);

    let journal = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::OpenExisting,
        limits,
    ))
    .expect("initialize missing checkpoint after exact header");
    assert_eq!(journal.inspection().active_records(), 0);
    assert_eq!(
        journal
            .inspection()
            .durable_cutoff()
            .checkpoint_generation(),
        1
    );
    assert!(checkpoint_path.is_file());
    drop(journal);

    let invalid = TestDirectory::new("invalid-interrupted-genesis");
    let journal_path = invalid.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let checkpoint_path = invalid.path().join(ACTIVE_CHECKPOINT_FILE_NAME);
    let mut invalid_header = JournalHeaderV1::new(support::store_id(1)).encode();
    invalid_header[0] ^= 0xff;
    let mut journal_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&journal_path)
        .expect("create invalid interrupted journal");
    journal_file
        .write_all(&invalid_header)
        .expect("write invalid exact-length header");
    journal_file.sync_all().expect("sync invalid header");
    drop(journal_file);
    assert!(
        ActiveJournal::open(config(
            &invalid,
            1,
            ActiveJournalOpenMode::OpenExisting,
            limits,
        ))
        .is_err()
    );
    assert!(!checkpoint_path.exists());

    let nonempty = TestDirectory::new("missing-checkpoint-nonempty");
    let mut journal = ActiveJournal::open(config(
        &nonempty,
        1,
        ActiveJournalOpenMode::CreateNew,
        limits,
    ))
    .expect("create nonempty missing-checkpoint fixture");
    journal
        .append(&frame(1))
        .expect("append uncheckpointed frame");
    drop(journal);
    let checkpoint_path = nonempty.path().join(ACTIVE_CHECKPOINT_FILE_NAME);
    fs::remove_file(&checkpoint_path).expect("remove test-owned checkpoint");
    let journal_path = nonempty.path().join(ACTIVE_JOURNAL_FILE_NAME);
    let before = fs::read(&journal_path).expect("read nonempty journal");
    assert!(matches!(
        ActiveJournal::open(config(
            &nonempty,
            1,
            ActiveJournalOpenMode::OpenExisting,
            limits,
        )),
        Err(ActiveJournalError::MissingArtifact)
    ));
    assert!(!checkpoint_path.exists());
    assert_eq!(
        fs::read(journal_path).expect("nonempty journal remains unchanged"),
        before
    );
}

#[test]
fn exact_counting_preflight_matches_prepared_frame_and_recovers_admission() {
    let admission = support::observed_admission(
        vec![och_core::ExactValue::Boolean(true)],
        och_core::ValueFamily::Boolean,
        1,
        true,
    );
    let expected = admission.clone();
    let counted = och_store::admission_frame_len_v1(&admission).expect("countable admission");
    let prepared = PreparedAdmissionV1::new(admission).expect("prepared admission");
    assert_eq!(prepared.frame_len(), counted);
    let framed = prepared
        .into_frame(AppendSequenceV1::new(1).expect("sequence"))
        .expect("prepared frame");
    assert_eq!(framed.len(), counted);
    assert_eq!(framed.into_admission(), expected);
}

#[test]
fn child_process_lock_probe() {
    let Ok(directory) = std::env::var("OCH_STORE_LOCK_PROBE") else {
        return;
    };
    let limits = limits(4 * 1_024 * 1_024, 8);
    let config = ActiveJournalConfig::new(
        PathBuf::from(directory),
        support::store_id(1),
        ActiveJournalOpenMode::OpenExisting,
        limits,
    )
    .expect("valid child probe configuration");
    assert!(matches!(
        ActiveJournal::open(config),
        Err(ActiveJournalError::AlreadyOpen)
    ));
}

#[test]
fn active_writer_lock_excludes_a_real_child_process() {
    let directory = TestDirectory::new("process-lock");
    let limits = limits(4 * 1_024 * 1_024, 8);
    let _journal = ActiveJournal::open(config(
        &directory,
        1,
        ActiveJournalOpenMode::CreateNew,
        limits,
    ))
    .expect("parent retains active lock");
    let output = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "child_process_lock_probe", "--nocapture"])
        .env("OCH_STORE_LOCK_PROBE", directory.path())
        .output()
        .expect("run child lock probe");
    assert!(
        output.status.success(),
        "child lock probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
