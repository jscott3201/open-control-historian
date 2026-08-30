//! Bounded durable retry projection and current Retry State V1 codec.

use crate::codec::{Cursor, Encoder, crc32c, decode_retry, encode_retry};
use crate::{
    ACTIVE_JOURNAL_GENERATION, DurableCutoff, GenerationCatalogReference,
    GenerationCatalogSnapshot, JournalV1Error, ManifestCommit,
};
use och_core::{RetryClassification, RetryQualification, StoreId};
use std::error::Error;
use std::fmt;

/// Hard maximum replay plus guard entries in one durable retry projection.
pub const MAX_PERSISTED_RETRY_ENTRIES: usize = 4_096;
/// Hard maximum bytes in one Retry State V1 artifact.
pub const MAX_RETRY_STATE_BYTES: usize = 2 * 1_024 * 1_024;

pub(crate) const RETRY_MAGIC: [u8; 8] = *b"OCHRET01";
pub(crate) const RETRY_VERSION: u16 = 1;
pub(crate) const RETRY_HEADER_LEN: usize = 64;
const RETRY_HEADER_LEN_U16: u16 = 64;
const RETRY_CRC_LEN: usize = 4;

/// Sanitized invalid durable-retry persistence configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryOptionsError;

impl fmt::Display for RetryOptionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid durable retry persistence options")
    }
}

impl Error for RetryOptionsError {}

/// Explicit finite outcome-replay and expired/conflict guard capacities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPersistenceOptions {
    replay_capacity: usize,
    guard_capacity: usize,
}

impl RetryPersistenceOptions {
    /// Validates positive tier capacities and the combined hard bound.
    ///
    /// # Errors
    ///
    /// Refuses either zero tier or a combined capacity above 4,096.
    pub const fn new(
        replay_capacity: usize,
        guard_capacity: usize,
    ) -> Result<Self, RetryOptionsError> {
        if replay_capacity == 0
            || guard_capacity == 0
            || replay_capacity > MAX_PERSISTED_RETRY_ENTRIES
            || guard_capacity > MAX_PERSISTED_RETRY_ENTRIES - replay_capacity
        {
            return Err(RetryOptionsError);
        }
        Ok(Self {
            replay_capacity,
            guard_capacity,
        })
    }

    /// Returns exact replayable-outcome capacity.
    #[must_use]
    pub const fn replay_capacity(self) -> usize {
        self.replay_capacity
    }

    /// Returns exact expired/conflict guard capacity.
    #[must_use]
    pub const fn guard_capacity(self) -> usize {
        self.guard_capacity
    }
}

impl Default for RetryPersistenceOptions {
    fn default() -> Self {
        Self {
            replay_capacity: 256,
            guard_capacity: 256,
        }
    }
}

/// Public non-recursive identity of one committed Retry State V1 snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryStateReference {
    slot: u8,
    generation: u64,
}

impl RetryStateReference {
    pub(crate) const fn new(slot: u8, generation: u64) -> Self {
        Self { slot, generation }
    }

    /// Returns the reusable retry slot in `0..3`.
    #[must_use]
    pub const fn slot(self) -> u8 {
        self.slot
    }

    /// Returns the positive retry snapshot generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

/// One writer-confirmed append awaiting inclusion in a durable retry snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingRetryOutcome {
    qualification: RetryQualification,
    append_sequence: u64,
    end_offset: u64,
}

impl PendingRetryOutcome {
    /// Retains exact retry and append identity selected by the sole writer.
    #[must_use]
    pub const fn new(
        qualification: RetryQualification,
        append_sequence: u64,
        end_offset: u64,
    ) -> Self {
        Self {
            qualification,
            append_sequence,
            end_offset,
        }
    }

    /// Borrows exact content-qualified retry evidence.
    #[must_use]
    pub const fn qualification(&self) -> &RetryQualification {
        &self.qualification
    }

    /// Returns the original writer-assigned append sequence.
    #[must_use]
    pub const fn append_sequence(&self) -> u64 {
        self.append_sequence
    }

    /// Returns the original exact frame end offset.
    #[must_use]
    pub const fn end_offset(&self) -> u64 {
        self.end_offset
    }
}

/// One replayable durable retry result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryReplayOutcome {
    qualification: RetryQualification,
    append_sequence: u64,
    end_offset: u64,
    committed: ManifestCommit,
}

impl RetryReplayOutcome {
    /// Borrows exact retry qualification.
    #[must_use]
    pub const fn qualification(&self) -> &RetryQualification {
        &self.qualification
    }

    /// Returns original append sequence.
    #[must_use]
    pub const fn append_sequence(&self) -> u64 {
        self.append_sequence
    }

    /// Returns original frame end offset.
    #[must_use]
    pub const fn end_offset(&self) -> u64 {
        self.end_offset
    }

    /// Returns the exact first manifest commit covering this outcome.
    #[must_use]
    pub const fn manifest_commit(&self) -> ManifestCommit {
        self.committed
    }
}

/// One non-replayable retained retry guard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryGuardEntry {
    qualification: RetryQualification,
    append_sequence: u64,
}

impl RetryGuardEntry {
    /// Borrows exact retry qualification.
    #[must_use]
    pub const fn qualification(&self) -> &RetryQualification {
        &self.qualification
    }

    /// Returns original append sequence/order evidence.
    #[must_use]
    pub const fn append_sequence(&self) -> u64 {
        self.append_sequence
    }
}

/// Exact classification against an immutable committed retry projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetryStateMatch {
    /// Exact scope, key, and content retain a replayable outcome.
    Replay(Box<RetryReplayOutcome>),
    /// Exact scope and key remain guarded with the same content.
    Expired,
    /// Exact scope and key remain retained with changed content.
    Conflict,
    /// Scope and key have expired from both bounded tiers.
    Fresh,
}

/// Immutable store-scoped committed two-tier retry projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryStateSnapshot {
    store_id: StoreId,
    options: RetryPersistenceOptions,
    reference: Option<RetryStateReference>,
    replay: Box<[RetryReplayOutcome]>,
    guard: Box<[RetryGuardEntry]>,
}

impl RetryStateSnapshot {
    /// Creates an empty in-memory projection before startup installs durable state.
    #[must_use]
    pub fn empty(store_id: StoreId, options: RetryPersistenceOptions) -> Self {
        Self {
            store_id,
            options,
            reference: None,
            replay: Box::new([]),
            guard: Box::new([]),
        }
    }

    pub(crate) fn empty_persisted(
        store_id: StoreId,
        options: RetryPersistenceOptions,
        reference: RetryStateReference,
    ) -> Self {
        Self {
            store_id,
            options,
            reference: Some(reference),
            replay: Box::new([]),
            guard: Box::new([]),
        }
    }

    /// Returns immutable store scope.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns exact configured tier capacities.
    #[must_use]
    pub const fn options(&self) -> RetryPersistenceOptions {
        self.options
    }

    /// Returns durable snapshot identity, absent only before startup installation.
    #[must_use]
    pub const fn reference(&self) -> Option<RetryStateReference> {
        self.reference
    }

    /// Returns replay entries oldest first by durable append sequence.
    #[must_use]
    pub const fn replay(&self) -> &[RetryReplayOutcome] {
        &self.replay
    }

    /// Returns guard entries oldest first by original append sequence.
    #[must_use]
    pub const fn guard(&self) -> &[RetryGuardEntry] {
        &self.guard
    }

    /// Classifies by the reviewed core retry comparison without refreshing order.
    #[must_use]
    pub fn classify(&self, qualification: &RetryQualification) -> RetryStateMatch {
        for outcome in &self.replay {
            match outcome.qualification.classify(qualification) {
                RetryClassification::Equivalent => {
                    return RetryStateMatch::Replay(Box::new(outcome.clone()));
                }
                RetryClassification::Conflict => return RetryStateMatch::Conflict,
                RetryClassification::Distinct => {}
            }
        }
        for entry in &self.guard {
            match entry.qualification.classify(qualification) {
                RetryClassification::Equivalent => return RetryStateMatch::Expired,
                RetryClassification::Conflict => return RetryStateMatch::Conflict,
                RetryClassification::Distinct => {}
            }
        }
        RetryStateMatch::Fresh
    }

    /// Verifies one proposed writer-ordered durable batch transition exactly.
    ///
    /// This is read-only evidence for the runtime's atomic handoff. It neither
    /// mutates this projection nor grants a second retry authority.
    #[must_use]
    pub fn verifies_transition(
        &self,
        candidate: &Self,
        pending: &[PendingRetryOutcome],
        committed: ManifestCommit,
    ) -> bool {
        let Some(reference) = candidate.reference else {
            return false;
        };
        self.advance(reference, pending, committed)
            .is_ok_and(|expected| expected == *candidate)
    }

    pub(crate) fn advance(
        &self,
        reference: RetryStateReference,
        pending: &[PendingRetryOutcome],
        committed: ManifestCommit,
    ) -> Result<Self, RetryStateCodecError> {
        validate_transition_preflight(self, reference, pending, committed)?;
        let mut replay = self.replay.to_vec();
        let mut guard = self.guard.to_vec();
        let mut previous = replay
            .last()
            .map(RetryReplayOutcome::append_sequence)
            .or_else(|| guard.last().map(RetryGuardEntry::append_sequence))
            .unwrap_or(0);
        for entry in pending {
            if entry.append_sequence == 0
                || entry.append_sequence <= previous
                || entry.end_offset == 0
                || !matches!(self.classify(&entry.qualification), RetryStateMatch::Fresh)
                || replay.iter().any(|retained| {
                    retained.qualification.classify(&entry.qualification)
                        != RetryClassification::Distinct
                })
                || guard.iter().any(|retained| {
                    retained.qualification.classify(&entry.qualification)
                        != RetryClassification::Distinct
                })
            {
                return Err(RetryStateCodecError::Invalid);
            }
            replay.push(RetryReplayOutcome {
                qualification: entry.qualification.clone(),
                append_sequence: entry.append_sequence,
                end_offset: entry.end_offset,
                committed,
            });
            previous = entry.append_sequence;
            if replay.len() > self.options.replay_capacity {
                let promoted = replay.remove(0);
                guard.push(RetryGuardEntry {
                    qualification: promoted.qualification,
                    append_sequence: promoted.append_sequence,
                });
                if guard.len() > self.options.guard_capacity {
                    guard.remove(0);
                }
            }
        }
        let candidate = Self {
            store_id: self.store_id,
            options: self.options,
            reference: Some(reference),
            replay: replay.into_boxed_slice(),
            guard: guard.into_boxed_slice(),
        };
        validate_state(&candidate)?;
        validate_root(&candidate, committed, None)?;
        Ok(candidate)
    }

    #[cfg(test)]
    pub(crate) fn validates_root(&self, root: ManifestCommit) -> bool {
        validate_root(self, root, None).is_ok()
    }

    pub(crate) fn validates_root_with_catalog(
        &self,
        root: ManifestCommit,
        catalog: &GenerationCatalogSnapshot,
    ) -> bool {
        validate_root(self, root, Some(catalog)).is_ok()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryStateCodecError {
    Invalid,
    StoreMismatch,
    OptionsMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetryArtifactReference {
    pub(crate) public: RetryStateReference,
    pub(crate) length: u64,
    pub(crate) checksum: u32,
}

pub(crate) fn encode_retry_state(
    snapshot: &RetryStateSnapshot,
) -> Result<Vec<u8>, RetryStateCodecError> {
    encode_retry_state_with_limit(snapshot, MAX_RETRY_STATE_BYTES)
}

pub(crate) fn encode_retry_state_with_limit(
    snapshot: &RetryStateSnapshot,
    maximum: usize,
) -> Result<Vec<u8>, RetryStateCodecError> {
    validate_state(snapshot)?;
    let reference = snapshot.reference.ok_or(RetryStateCodecError::Invalid)?;
    let mut counter = Encoder::counting();
    encode_payload(&mut counter, snapshot)?;
    let payload_len = counter.len();
    let total = RETRY_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(RETRY_CRC_LEN))
        .ok_or(RetryStateCodecError::Invalid)?;
    if total > maximum || total > MAX_RETRY_STATE_BYTES {
        return Err(RetryStateCodecError::Invalid);
    }
    let mut payload = Encoder::new();
    encode_payload(&mut payload, snapshot)?;
    let payload = payload.finish();
    if payload.len() != payload_len {
        return Err(RetryStateCodecError::Invalid);
    }
    let mut bytes = vec![0_u8; total];
    bytes[..8].copy_from_slice(&RETRY_MAGIC);
    bytes[8..10].copy_from_slice(&RETRY_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&RETRY_HEADER_LEN_U16.to_be_bytes());
    bytes[12..28].copy_from_slice(snapshot.store_id.as_bytes());
    bytes[28..36].copy_from_slice(&reference.generation.to_be_bytes());
    bytes[36..40].copy_from_slice(
        &u32::try_from(snapshot.options.replay_capacity)
            .map_err(|_| RetryStateCodecError::Invalid)?
            .to_be_bytes(),
    );
    bytes[40..44].copy_from_slice(
        &u32::try_from(snapshot.options.guard_capacity)
            .map_err(|_| RetryStateCodecError::Invalid)?
            .to_be_bytes(),
    );
    bytes[44..48].copy_from_slice(
        &u32::try_from(snapshot.replay.len())
            .map_err(|_| RetryStateCodecError::Invalid)?
            .to_be_bytes(),
    );
    bytes[48..52].copy_from_slice(
        &u32::try_from(snapshot.guard.len())
            .map_err(|_| RetryStateCodecError::Invalid)?
            .to_be_bytes(),
    );
    bytes[52..60].copy_from_slice(
        &u64::try_from(payload_len)
            .map_err(|_| RetryStateCodecError::Invalid)?
            .to_be_bytes(),
    );
    bytes[RETRY_HEADER_LEN..RETRY_HEADER_LEN + payload_len].copy_from_slice(&payload);
    let checksum_offset = total - RETRY_CRC_LEN;
    let checksum = crc32c(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

fn encode_payload(
    encoder: &mut Encoder,
    snapshot: &RetryStateSnapshot,
) -> Result<(), RetryStateCodecError> {
    for outcome in &snapshot.replay {
        encode_retry(encoder, &outcome.qualification).map_err(invalid_journal)?;
        encoder.u64(outcome.append_sequence);
        encoder.u64(outcome.end_offset);
        let commit = outcome.committed;
        encoder.u64(commit.manifest_generation());
        encoder.u8(commit.registry_slot());
        encoder.bytes(&[0; 7]);
        encoder.u64(commit.registry_generation());
        let cutoff = commit.durable_cutoff();
        encoder.u64(cutoff.journal().generation());
        encoder.u64(cutoff.checkpoint_generation());
        encoder.u64(cutoff.append_sequence());
        encoder.u64(cutoff.end_offset());
        let retry = commit.retry_state();
        encoder.u8(retry.slot());
        encoder.bytes(&[0; 7]);
        encoder.u64(retry.generation());
        encoder.u64(commit.sequence_floor());
        match commit.generation_catalog() {
            Some(catalog) => {
                encoder.u8(1);
                encoder.u8(catalog.slot());
                encoder.bytes(&[0; 6]);
                encoder.u64(catalog.generation());
                encoder.u64(catalog.length());
                encoder.u32(catalog.checksum());
                encoder.bytes(&[0; 12]);
            }
            None => encoder.bytes(&[0; 40]),
        }
    }
    for entry in &snapshot.guard {
        encode_retry(encoder, &entry.qualification).map_err(invalid_journal)?;
        encoder.u64(entry.append_sequence);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(crate) fn decode_retry_state_at_slot(
    bytes: &[u8],
    slot: u8,
    expected_store: StoreId,
    expected_options: RetryPersistenceOptions,
) -> Result<(RetryArtifactReference, RetryStateSnapshot), RetryStateCodecError> {
    if slot >= 3
        || bytes.len() < RETRY_HEADER_LEN + RETRY_CRC_LEN
        || bytes.len() > MAX_RETRY_STATE_BYTES
        || bytes[..8] != RETRY_MAGIC
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default()) != RETRY_HEADER_LEN_U16
        || bytes[60..64].iter().any(|byte| *byte != 0)
    {
        return Err(RetryStateCodecError::Invalid);
    }
    if u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != RETRY_VERSION {
        return Err(RetryStateCodecError::Invalid);
    }
    let checksum_offset = bytes.len() - RETRY_CRC_LEN;
    if crc32c(&bytes[..checksum_offset])
        != u32::from_be_bytes(bytes[checksum_offset..].try_into().unwrap_or_default())
    {
        return Err(RetryStateCodecError::Invalid);
    }
    let store_id = StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default())
        .map_err(|_| RetryStateCodecError::Invalid)?;
    if store_id != expected_store {
        return Err(RetryStateCodecError::StoreMismatch);
    }
    let generation = u64::from_be_bytes(bytes[28..36].try_into().unwrap_or_default());
    let replay_capacity = usize::try_from(u32::from_be_bytes(
        bytes[36..40].try_into().unwrap_or_default(),
    ))
    .map_err(|_| RetryStateCodecError::Invalid)?;
    let guard_capacity = usize::try_from(u32::from_be_bytes(
        bytes[40..44].try_into().unwrap_or_default(),
    ))
    .map_err(|_| RetryStateCodecError::Invalid)?;
    let options = RetryPersistenceOptions::new(replay_capacity, guard_capacity)
        .map_err(|_| RetryStateCodecError::Invalid)?;
    if options != expected_options {
        return Err(RetryStateCodecError::OptionsMismatch);
    }
    let replay_count = usize::try_from(u32::from_be_bytes(
        bytes[44..48].try_into().unwrap_or_default(),
    ))
    .map_err(|_| RetryStateCodecError::Invalid)?;
    let guard_count = usize::try_from(u32::from_be_bytes(
        bytes[48..52].try_into().unwrap_or_default(),
    ))
    .map_err(|_| RetryStateCodecError::Invalid)?;
    let payload_len = usize::try_from(u64::from_be_bytes(
        bytes[52..60].try_into().unwrap_or_default(),
    ))
    .map_err(|_| RetryStateCodecError::Invalid)?;
    if generation == 0
        || replay_count > replay_capacity
        || guard_count > guard_capacity
        || replay_count.saturating_add(guard_count) > MAX_PERSISTED_RETRY_ENTRIES
        || RETRY_HEADER_LEN.checked_add(payload_len) != Some(checksum_offset)
    {
        return Err(RetryStateCodecError::Invalid);
    }
    let reference = RetryStateReference::new(slot, generation);
    let mut cursor = Cursor::new(&bytes[RETRY_HEADER_LEN..checksum_offset]);
    let mut replay = Vec::with_capacity(replay_count);
    for _ in 0..replay_count {
        let qualification = decode_retry(&mut cursor).map_err(invalid_journal)?;
        let append_sequence = cursor.u64().map_err(invalid_journal)?;
        let end_offset = cursor.u64().map_err(invalid_journal)?;
        let manifest_generation = cursor.u64().map_err(invalid_journal)?;
        let registry_slot = cursor.u8().map_err(invalid_journal)?;
        if cursor
            .take(7)
            .map_err(invalid_journal)?
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(RetryStateCodecError::Invalid);
        }
        let registry_generation = cursor.u64().map_err(invalid_journal)?;
        let journal_generation = cursor.u64().map_err(invalid_journal)?;
        let checkpoint_generation = cursor.u64().map_err(invalid_journal)?;
        let cutoff_sequence = cursor.u64().map_err(invalid_journal)?;
        let cutoff_end = cursor.u64().map_err(invalid_journal)?;
        let retry_slot = cursor.u8().map_err(invalid_journal)?;
        if cursor
            .take(7)
            .map_err(invalid_journal)?
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(RetryStateCodecError::Invalid);
        }
        let retry_generation = cursor.u64().map_err(invalid_journal)?;
        let sequence_floor = cursor.u64().map_err(invalid_journal)?;
        let present = cursor.u8().map_err(invalid_journal)?;
        let catalog_slot = cursor.u8().map_err(invalid_journal)?;
        if cursor
            .take(6)
            .map_err(invalid_journal)?
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(RetryStateCodecError::Invalid);
        }
        let catalog_generation = cursor.u64().map_err(invalid_journal)?;
        let catalog_length = cursor.u64().map_err(invalid_journal)?;
        let catalog_checksum = cursor.u32().map_err(invalid_journal)?;
        if cursor
            .take(12)
            .map_err(invalid_journal)?
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(RetryStateCodecError::Invalid);
        }
        let catalog = match present {
            0 if catalog_slot == 0
                && catalog_generation == 0
                && catalog_length == 0
                && catalog_checksum == 0 =>
            {
                None
            }
            1 if catalog_slot < 3 && catalog_generation > 0 && catalog_length > 0 => {
                Some(GenerationCatalogReference::new(
                    catalog_slot,
                    catalog_generation,
                    catalog_length,
                    catalog_checksum,
                ))
            }
            _ => return Err(RetryStateCodecError::Invalid),
        };
        let cutoff = DurableCutoff::from_manifest(
            store_id,
            journal_generation,
            checkpoint_generation,
            cutoff_sequence,
            cutoff_end,
        );
        let committed = ManifestCommit::from_generation_parts(
            manifest_generation,
            registry_generation,
            registry_slot,
            cutoff,
            RetryStateReference::new(retry_slot, retry_generation),
            sequence_floor,
            catalog,
        );
        replay.push(RetryReplayOutcome {
            qualification,
            append_sequence,
            end_offset,
            committed,
        });
    }
    let mut guard = Vec::with_capacity(guard_count);
    for _ in 0..guard_count {
        guard.push(RetryGuardEntry {
            qualification: decode_retry(&mut cursor).map_err(invalid_journal)?,
            append_sequence: cursor.u64().map_err(invalid_journal)?,
        });
    }
    cursor.finish().map_err(invalid_journal)?;
    let snapshot = RetryStateSnapshot {
        store_id,
        options,
        reference: Some(reference),
        replay: replay.into_boxed_slice(),
        guard: guard.into_boxed_slice(),
    };
    validate_state(&snapshot)?;
    if encode_retry_state(&snapshot)? != bytes {
        return Err(RetryStateCodecError::Invalid);
    }
    Ok((
        RetryArtifactReference {
            public: reference,
            length: u64::try_from(bytes.len()).map_err(|_| RetryStateCodecError::Invalid)?,
            checksum: crc32c(bytes),
        },
        snapshot,
    ))
}

#[allow(clippy::too_many_lines)]
fn validate_state(snapshot: &RetryStateSnapshot) -> Result<(), RetryStateCodecError> {
    if snapshot.replay.len() > snapshot.options.replay_capacity
        || snapshot.guard.len() > snapshot.options.guard_capacity
        || snapshot.replay.len().saturating_add(snapshot.guard.len()) > MAX_PERSISTED_RETRY_ENTRIES
        || snapshot
            .reference
            .is_some_and(|reference| reference.slot >= 3 || reference.generation == 0)
    {
        return Err(RetryStateCodecError::Invalid);
    }
    let mut previous = 0_u64;
    // Guards are older than replay outcomes even though the wire orders replay
    // first. Validate each tier and the cross-tier chronology separately.
    for entry in &snapshot.guard {
        if entry.append_sequence == 0 || entry.append_sequence <= previous {
            return Err(RetryStateCodecError::Invalid);
        }
        previous = entry.append_sequence;
    }
    let mut previous_end = 0_u64;
    let mut previous_journal = 0_u64;
    let mut previous_manifest = 0_u64;
    let mut previous_registry = 0_u64;
    let mut previous_checkpoint = 0_u64;
    let mut previous_cutoff_sequence = 0_u64;
    let mut previous_retry_generation = 0_u64;
    for outcome in &snapshot.replay {
        if outcome.append_sequence == 0
            || outcome.append_sequence <= previous
            || outcome.end_offset == 0
            || outcome.committed.manifest_generation() == 0
            || outcome.committed.registry_generation() == 0
            || outcome.committed.registry_slot() >= 3
        {
            return Err(RetryStateCodecError::Invalid);
        }
        let cutoff = outcome.committed.durable_cutoff();
        let retry = outcome.committed.retry_state();
        if cutoff.journal().store_id() != snapshot.store_id
            || cutoff.journal().generation() == 0
            || cutoff.checkpoint_generation() == 0
            || cutoff.append_sequence() == 0
            || cutoff.end_offset() == 0
            || outcome.append_sequence > cutoff.append_sequence()
            || outcome.end_offset > cutoff.end_offset()
            || (outcome.append_sequence == cutoff.append_sequence()
                && outcome.end_offset != cutoff.end_offset())
            || (outcome.append_sequence < cutoff.append_sequence()
                && outcome.end_offset >= cutoff.end_offset())
            || retry.slot() >= 3
            || retry.generation() == 0
            || outcome.committed.manifest_generation() < outcome.committed.registry_generation()
            || outcome.committed.manifest_generation() < retry.generation()
            || (cutoff.journal().generation() == previous_journal
                && outcome.end_offset <= previous_end)
            || cutoff.journal().generation() < previous_journal
            || outcome.committed.manifest_generation() < previous_manifest
            || outcome.committed.registry_generation() < previous_registry
            || (cutoff.journal().generation() == previous_journal
                && cutoff.checkpoint_generation() < previous_checkpoint)
            || cutoff.append_sequence() < previous_cutoff_sequence
            || retry.generation() < previous_retry_generation
            || snapshot
                .reference
                .is_some_and(|current| retry.generation() > current.generation())
        {
            return Err(RetryStateCodecError::Invalid);
        }
        if cutoff.journal().generation() == ACTIVE_JOURNAL_GENERATION {
            if outcome.committed.sequence_floor() != 0
                || outcome.committed.generation_catalog().is_some()
            {
                return Err(RetryStateCodecError::Invalid);
            }
        } else if outcome.committed.sequence_floor() == 0
            || outcome.committed.generation_catalog().is_none()
            || cutoff.append_sequence() < outcome.committed.sequence_floor()
        {
            return Err(RetryStateCodecError::Invalid);
        }
        previous = outcome.append_sequence;
        previous_end = outcome.end_offset;
        previous_journal = cutoff.journal().generation();
        previous_manifest = outcome.committed.manifest_generation();
        previous_registry = outcome.committed.registry_generation();
        previous_checkpoint = cutoff.checkpoint_generation();
        previous_cutoff_sequence = cutoff.append_sequence();
        previous_retry_generation = retry.generation();
    }
    let mut retained: Vec<&RetryQualification> =
        Vec::with_capacity(snapshot.replay.len().saturating_add(snapshot.guard.len()));
    for qualification in snapshot
        .replay
        .iter()
        .map(RetryReplayOutcome::qualification)
        .chain(snapshot.guard.iter().map(RetryGuardEntry::qualification))
    {
        if retained
            .iter()
            .any(|prior| prior.classify(qualification) != RetryClassification::Distinct)
        {
            return Err(RetryStateCodecError::Invalid);
        }
        retained.push(qualification);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn validate_root(
    snapshot: &RetryStateSnapshot,
    root: ManifestCommit,
    catalog: Option<&GenerationCatalogSnapshot>,
) -> Result<(), RetryStateCodecError> {
    validate_state(snapshot)?;
    let root_reference = root.retry_state();
    let root_cutoff = root.durable_cutoff();
    if snapshot.reference != Some(root_reference)
        || snapshot.store_id != root_cutoff.journal().store_id()
        || (root_cutoff.journal().generation() == ACTIVE_JOURNAL_GENERATION
            && (root.sequence_floor() != 0 || root.generation_catalog().is_some()))
        || (root_cutoff.journal().generation() > ACTIVE_JOURNAL_GENERATION
            && (root.sequence_floor() == 0
                || root.generation_catalog().is_none()
                || root_cutoff.append_sequence() < root.sequence_floor()))
        || catalog.is_some_and(|catalog| catalog.reference() != root.generation_catalog())
    {
        return Err(RetryStateCodecError::Invalid);
    }
    let Some(newest) = snapshot.replay.last() else {
        return if snapshot.guard.is_empty() && root_reference.generation() == 1 {
            Ok(())
        } else {
            Err(RetryStateCodecError::Invalid)
        };
    };
    let newest_generation = newest.committed.durable_cutoff().journal().generation();
    let newest_covered = if newest_generation == root_cutoff.journal().generation() {
        newest.end_offset == root_cutoff.end_offset()
    } else {
        root_cutoff.append_sequence() == root.sequence_floor()
            && newest_generation < root_cutoff.journal().generation()
            && catalog.map_or(root.generation_catalog().is_some(), |catalog| {
                catalog.covers_commit(newest_generation, newest.append_sequence, newest.end_offset)
            })
    };
    if (!snapshot.guard.is_empty() && snapshot.replay.len() != snapshot.options.replay_capacity)
        || newest.append_sequence != root_cutoff.append_sequence()
        || !newest_covered
        || newest.committed.retry_state() != root_reference
    {
        return Err(RetryStateCodecError::Invalid);
    }

    let mut chronological = snapshot
        .guard
        .iter()
        .map(RetryGuardEntry::append_sequence)
        .chain(
            snapshot
                .replay
                .iter()
                .map(RetryReplayOutcome::append_sequence),
        );
    let mut previous_sequence = chronological.next().ok_or(RetryStateCodecError::Invalid)?;
    if previous_sequence > root_cutoff.append_sequence() {
        return Err(RetryStateCodecError::Invalid);
    }
    for sequence in chronological {
        if previous_sequence.checked_add(1) != Some(sequence)
            || sequence > root_cutoff.append_sequence()
        {
            return Err(RetryStateCodecError::Invalid);
        }
        previous_sequence = sequence;
    }

    let mut prior_commit = None;
    for (index, outcome) in snapshot.replay.iter().enumerate() {
        let commit = outcome.committed;
        let cutoff = commit.durable_cutoff();
        let reference = commit.retry_state();
        if commit.manifest_generation() > root.manifest_generation()
            || commit.registry_generation() > root.registry_generation()
            || (commit.registry_generation() == root.registry_generation()
                && commit.registry_slot() != root.registry_slot())
            || cutoff.journal().store_id() != root_cutoff.journal().store_id()
            || cutoff.journal().generation() > root_cutoff.journal().generation()
            || (cutoff.journal().generation() == root_cutoff.journal().generation()
                && cutoff.checkpoint_generation() > root_cutoff.checkpoint_generation())
            || cutoff.append_sequence() > root_cutoff.append_sequence()
            || (cutoff.journal().generation() == root_cutoff.journal().generation()
                && cutoff.end_offset() > root_cutoff.end_offset())
            || (cutoff.journal().generation() < root_cutoff.journal().generation()
                && catalog.map_or(root.generation_catalog().is_none(), |catalog| {
                    !catalog.covers_commit(
                        cutoff.journal().generation(),
                        outcome.append_sequence,
                        outcome.end_offset,
                    )
                }))
            || reference.generation() > root_reference.generation()
            || (commit.manifest_generation() == root.manifest_generation() && commit != root)
        {
            return Err(RetryStateCodecError::Invalid);
        }
        if let Some(prior) = prior_commit {
            validate_commit_progression(prior, commit)?;
        }
        let group_ends = snapshot
            .replay
            .get(index + 1)
            .is_none_or(|next| next.committed.retry_state() != reference);
        if group_ends
            && (outcome.append_sequence != cutoff.append_sequence()
                || outcome.end_offset != cutoff.end_offset())
        {
            return Err(RetryStateCodecError::Invalid);
        }
        prior_commit = Some(commit);
    }
    Ok(())
}

fn validate_transition_preflight(
    snapshot: &RetryStateSnapshot,
    reference: RetryStateReference,
    pending: &[PendingRetryOutcome],
    committed: ManifestCommit,
) -> Result<(), RetryStateCodecError> {
    let cutoff = committed.durable_cutoff();
    let pending_len = u64::try_from(pending.len()).map_err(|_| RetryStateCodecError::Invalid)?;
    let first = pending.first().ok_or(RetryStateCodecError::Invalid)?;
    let last = pending.last().ok_or(RetryStateCodecError::Invalid)?;
    let expected_first = cutoff
        .append_sequence()
        .checked_sub(
            pending_len
                .checked_sub(1)
                .ok_or(RetryStateCodecError::Invalid)?,
        )
        .ok_or(RetryStateCodecError::Invalid)?;
    if committed.retry_state() != reference
        || pending.len() > MAX_PERSISTED_RETRY_ENTRIES
        || cutoff.journal().store_id() != snapshot.store_id
        || first.append_sequence != expected_first
        || last.append_sequence != cutoff.append_sequence()
        || last.end_offset != cutoff.end_offset()
    {
        return Err(RetryStateCodecError::Invalid);
    }
    if let Some(prior) = snapshot.replay.last() {
        let prior_cutoff = prior.committed.durable_cutoff();
        if cutoff
            .append_sequence()
            .checked_sub(prior_cutoff.append_sequence())
            != Some(pending_len)
        {
            return Err(RetryStateCodecError::Invalid);
        }
    }
    Ok(())
}

fn validate_commit_progression(
    prior: ManifestCommit,
    current: ManifestCommit,
) -> Result<(), RetryStateCodecError> {
    let prior_reference = prior.retry_state();
    let current_reference = current.retry_state();
    if current_reference.generation() == prior_reference.generation() {
        return if current == prior {
            Ok(())
        } else {
            Err(RetryStateCodecError::Invalid)
        };
    }
    let prior_cutoff = prior.durable_cutoff();
    let current_cutoff = current.durable_cutoff();
    let crosses_generation =
        current_cutoff.journal().generation() != prior_cutoff.journal().generation();
    if prior_reference.generation().checked_add(1) != Some(current_reference.generation())
        || current_reference.slot() == prior_reference.slot()
        || current.manifest_generation() <= prior.manifest_generation()
        || current_cutoff.append_sequence() <= prior_cutoff.append_sequence()
        || (!crosses_generation
            && (prior_cutoff.checkpoint_generation().checked_add(1)
                != Some(current_cutoff.checkpoint_generation())
                || current_cutoff.end_offset() <= prior_cutoff.end_offset()))
        || (crosses_generation
            && (prior_cutoff.journal().generation().checked_add(1)
                != Some(current_cutoff.journal().generation())
                || current.sequence_floor() != prior_cutoff.append_sequence()
                || current.generation_catalog().is_none()))
        || current.registry_generation() < prior.registry_generation()
        || (current.registry_generation() == prior.registry_generation()
            && current.registry_slot() != prior.registry_slot())
    {
        return Err(RetryStateCodecError::Invalid);
    }
    Ok(())
}

fn invalid_journal(_: JournalV1Error) -> RetryStateCodecError {
    RetryStateCodecError::Invalid
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use och_core::RetryKey;

    const REPLAY_SUFFIX_BYTES: usize = 136;

    fn qualification(key: &str) -> RetryQualification {
        let base = test_support::no_change_admission().retry().clone();
        RetryQualification::new(
            base.series_id(),
            base.producer_id(),
            RetryKey::new(key.to_owned()).expect("bounded test retry key"),
            base.content().clone(),
        )
    }

    fn qualification_len(value: &RetryQualification) -> usize {
        let mut counter = Encoder::counting();
        encode_retry(&mut counter, value).expect("count test retry qualification");
        counter.len()
    }

    fn repair_checksum(bytes: &mut [u8]) {
        let checksum_offset = bytes.len() - RETRY_CRC_LEN;
        let checksum = crc32c(&bytes[..checksum_offset]);
        bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    }

    #[test]
    fn options_require_two_positive_bounded_tiers() {
        assert_eq!(RetryPersistenceOptions::new(0, 1), Err(RetryOptionsError));
        assert_eq!(RetryPersistenceOptions::new(1, 0), Err(RetryOptionsError));
        assert_eq!(
            RetryPersistenceOptions::new(2_048, 2_048)
                .expect("exact combined bound")
                .replay_capacity(),
            2_048
        );
        assert_eq!(
            RetryPersistenceOptions::new(2_048, 2_049),
            Err(RetryOptionsError)
        );
    }

    #[test]
    fn transition_preflight_refuses_oversized_or_non_suffix_pending_before_copy() {
        let store_id = test_support::store_id(1);
        let options = RetryPersistenceOptions::new(2, 2).expect("retry options");
        let initial =
            RetryStateSnapshot::empty_persisted(store_id, options, RetryStateReference::new(0, 1));
        let reference = RetryStateReference::new(1, 2);
        let cutoff = DurableCutoff::from_manifest(
            store_id,
            1,
            2,
            (MAX_PERSISTED_RETRY_ENTRIES + 1) as u64,
            500,
        );
        let commit = ManifestCommit::from_parts(2, 1, 0, cutoff, reference);
        let repeated = PendingRetryOutcome::new(qualification("bounded"), 1, 1);
        let oversized = vec![repeated; MAX_PERSISTED_RETRY_ENTRIES + 1];
        assert_eq!(
            initial.advance(reference, &oversized, commit),
            Err(RetryStateCodecError::Invalid)
        );

        let wrong_suffix = [PendingRetryOutcome::new(qualification("suffix"), 2, 500)];
        let suffix_commit = ManifestCommit::from_parts(
            2,
            1,
            0,
            DurableCutoff::from_manifest(store_id, 1, 2, 3, 500),
            reference,
        );
        assert_eq!(
            initial.advance(reference, &wrong_suffix, suffix_commit),
            Err(RetryStateCodecError::Invalid)
        );
    }

    #[test]
    fn owning_manifest_root_rejects_checksummed_future_and_unreachable_shapes() {
        let store_id = test_support::store_id(1);
        let options = RetryPersistenceOptions::new(2, 2).expect("retry options");
        let reference = RetryStateReference::new(1, 2);
        let root = ManifestCommit::from_parts(
            4,
            1,
            0,
            DurableCutoff::from_manifest(store_id, 1, 2, 5, 500),
            reference,
        );
        let initial =
            RetryStateSnapshot::empty_persisted(store_id, options, RetryStateReference::new(0, 1));
        let pending = (1_u64..=5)
            .map(|sequence| {
                PendingRetryOutcome::new(
                    qualification(&format!("root-{sequence:04}")),
                    sequence,
                    sequence * 100,
                )
            })
            .collect::<Vec<_>>();
        let candidate = initial
            .advance(reference, &pending, root)
            .expect("canonical retained suffix");
        assert!(candidate.validates_root(root));
        let encoded = encode_retry_state(&candidate).expect("canonical root bytes");
        let qualification_bytes = qualification_len(candidate.replay[0].qualification());
        assert_eq!(
            qualification_bytes,
            qualification_len(candidate.replay[1].qualification())
        );
        let first_append = RETRY_HEADER_LEN + qualification_bytes;
        let second_append = first_append + REPLAY_SUFFIX_BYTES + qualification_bytes;

        let mut future = encoded.clone();
        future[first_append + 16..first_append + 24].copy_from_slice(&5_u64.to_be_bytes());
        future[second_append + 16..second_append + 24].copy_from_slice(&5_u64.to_be_bytes());
        repair_checksum(&mut future);
        let future = decode_retry_state_at_slot(&future, 1, store_id, options)
            .expect("internally canonical checksummed future bytes")
            .1;
        assert!(!future.validates_root(root));

        let first_guard_append = second_append
            + REPLAY_SUFFIX_BYTES
            + qualification_len(candidate.guard[0].qualification());
        let mut gapped = encoded.clone();
        gapped[first_guard_append..first_guard_append + 8].copy_from_slice(&1_u64.to_be_bytes());
        repair_checksum(&mut gapped);
        let gapped = decode_retry_state_at_slot(&gapped, 1, store_id, options)
            .expect("internally canonical checksummed gap bytes")
            .1;
        assert!(!gapped.validates_root(root));

        let mut nonfull = candidate.clone();
        nonfull.replay = nonfull.replay[1..].to_vec().into_boxed_slice();
        let nonfull_bytes = encode_retry_state(&nonfull).expect("checksummed nonfull replay shape");
        let nonfull = decode_retry_state_at_slot(&nonfull_bytes, 1, store_id, options)
            .expect("internally canonical nonfull replay bytes")
            .1;
        assert!(!nonfull.validates_root(root));

        let mut unequal_same_generation = candidate.clone();
        unequal_same_generation.replay[0].committed =
            ManifestCommit::from_parts(3, 1, 0, root.durable_cutoff(), reference);
        let unequal_bytes = encode_retry_state(&unequal_same_generation)
            .expect("checksummed unequal same-generation commit");
        let unequal_same_generation =
            decode_retry_state_at_slot(&unequal_bytes, 1, store_id, options)
                .expect("internally canonical unequal same-generation bytes")
                .1;
        assert!(!unequal_same_generation.validates_root(root));

        let skipped_generation = ManifestCommit::from_parts(
            4,
            1,
            0,
            DurableCutoff::from_manifest(store_id, 1, 4, 5, 500),
            RetryStateReference::new(1, 4),
        );
        let earlier_generation = ManifestCommit::from_parts(
            2,
            1,
            0,
            DurableCutoff::from_manifest(store_id, 1, 2, 4, 400),
            RetryStateReference::new(0, 2),
        );
        assert_eq!(
            validate_commit_progression(earlier_generation, skipped_generation),
            Err(RetryStateCodecError::Invalid)
        );
    }

    #[test]
    fn empty_persisted_snapshot_is_canonical_only_at_generation_one() {
        let store_id = test_support::store_id(1);
        let options = RetryPersistenceOptions::new(2, 2).expect("retry options");
        let root = ManifestCommit::from_parts(
            2,
            1,
            0,
            DurableCutoff::from_manifest(store_id, 1, 2, 0, 64),
            RetryStateReference::new(1, 2),
        );
        let hostile =
            RetryStateSnapshot::empty_persisted(store_id, options, RetryStateReference::new(1, 2));
        let bytes = encode_retry_state(&hostile).expect("checksummed empty generation two");
        let hostile = decode_retry_state_at_slot(&bytes, 1, store_id, options)
            .expect("internally canonical empty generation two")
            .1;
        assert!(!hostile.validates_root(root));
    }

    #[test]
    fn empty_snapshot_exact_byte_preflight_and_hostile_header_refuse() {
        let options = RetryPersistenceOptions::new(2, 2).expect("test retry options");
        let mut snapshot = RetryStateSnapshot::empty(test_support::store_id(1), options);
        snapshot.reference = Some(RetryStateReference::new(0, 1));
        let encoded = encode_retry_state(&snapshot).expect("empty retry snapshot");
        assert_eq!(encoded.len(), RETRY_HEADER_LEN + RETRY_CRC_LEN);
        assert_eq!(
            encode_retry_state_with_limit(&snapshot, encoded.len() - 1),
            Err(RetryStateCodecError::Invalid)
        );
        assert_eq!(
            encode_retry_state_with_limit(&snapshot, encoded.len()),
            Ok(encoded.clone())
        );
        assert_eq!(
            decode_retry_state_at_slot(&encoded, 0, test_support::store_id(1), options)
                .expect("canonical empty retry")
                .1,
            snapshot
        );
        for offset in [0_usize, 9, 10, 36, 40, 44, 48, 52, 60, encoded.len() - 1] {
            let mut hostile = encoded.clone();
            hostile[offset] ^= 0xff;
            if offset != encoded.len() - 1 {
                let checksum_offset = hostile.len() - 4;
                let checksum = crc32c(&hostile[..checksum_offset]);
                hostile[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
            }
            assert!(
                decode_retry_state_at_slot(&hostile, 0, test_support::store_id(1), options)
                    .is_err()
            );
        }
        assert_eq!(
            decode_retry_state_at_slot(&encoded, 0, test_support::store_id(2), options),
            Err(RetryStateCodecError::StoreMismatch)
        );
        assert_eq!(
            decode_retry_state_at_slot(
                &encoded,
                0,
                test_support::store_id(1),
                RetryPersistenceOptions::new(1, 1).expect("other options")
            ),
            Err(RetryStateCodecError::OptionsMismatch)
        );
        assert!(
            decode_retry_state_at_slot(&encoded, 3, test_support::store_id(1), options).is_err()
        );
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(
            decode_retry_state_at_slot(&trailing, 0, test_support::store_id(1), options).is_err()
        );
        assert!(
            decode_retry_state_at_slot(
                &encoded[..encoded.len() - 1],
                0,
                test_support::store_id(1),
                options
            )
            .is_err()
        );
    }

    #[test]
    fn replay_codec_refuses_duplicate_scope_order_reserved_and_outcome_mismatch() {
        let store_id = test_support::store_id(1);
        let options = RetryPersistenceOptions::new(2, 2).expect("retry options");
        let initial_reference = RetryStateReference::new(0, 1);
        let initial = RetryStateSnapshot::empty_persisted(store_id, options, initial_reference);
        let reference = RetryStateReference::new(1, 2);
        let qualification = test_support::no_change_admission().retry().clone();
        let cutoff = DurableCutoff::from_manifest(store_id, 1, 2, 1, 500);
        let commit = ManifestCommit::from_parts(2, 1, 0, cutoff, reference);
        let candidate = initial
            .advance(
                reference,
                &[PendingRetryOutcome::new(qualification.clone(), 1, 500)],
                commit,
            )
            .expect("one replay candidate");
        let encoded = encode_retry_state(&candidate).expect("one replay encoding");
        assert_eq!(
            decode_retry_state_at_slot(&encoded, 1, store_id, options)
                .expect("one replay canonical decode")
                .1,
            candidate
        );

        let mut qualification_counter = Encoder::counting();
        encode_retry(&mut qualification_counter, &qualification).expect("count retry grammar");
        let append_offset = RETRY_HEADER_LEN + qualification_counter.len();
        assert_eq!(
            &encoded[append_offset..append_offset + 8],
            &1_u64.to_be_bytes()
        );
        assert_eq!(encoded[append_offset + 24], 0);
        assert!(
            encoded[append_offset + 25..append_offset + 32]
                .iter()
                .all(|byte| *byte == 0)
        );
        let mut hostile_offsets = vec![
            (append_offset, 8_usize, 0_u8),
            (append_offset + 8, 8, 0),
            (append_offset + 16, 8, 0),
            (append_offset + 25, 7, 1),
            (append_offset + 32, 8, 0),
            (append_offset + 40, 8, 0),
            (append_offset + 48, 8, 0),
            (append_offset + 56, 8, 0),
            (append_offset + 64, 8, 0),
            (append_offset + 72, 1, 3),
            (append_offset + 73, 7, 1),
            (append_offset + 80, 8, 0),
        ];
        for (offset, length, value) in hostile_offsets.drain(..) {
            let mut hostile = encoded.clone();
            hostile[offset..offset + length].fill(value);
            let checksum_offset = hostile.len() - 4;
            let checksum = crc32c(&hostile[..checksum_offset]);
            hostile[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
            assert!(
                decode_retry_state_at_slot(&hostile, 1, store_id, options).is_err(),
                "hostile replay field at {offset} must refuse"
            );
        }

        assert_eq!(
            initial.advance(
                reference,
                &[
                    PendingRetryOutcome::new(qualification.clone(), 1, 400),
                    PendingRetryOutcome::new(qualification, 2, 500),
                ],
                ManifestCommit::from_parts(
                    2,
                    1,
                    0,
                    DurableCutoff::from_manifest(store_id, 1, 2, 2, 500),
                    reference,
                ),
            ),
            Err(RetryStateCodecError::Invalid)
        );
    }

    #[test]
    fn current_retry_v1_round_trips_exact_commits_and_refuses_hostile_catalog_fields() {
        let store_id = test_support::store_id(1);
        let options = RetryPersistenceOptions::new(2, 2).expect("retry options");
        let first_reference = RetryStateReference::new(1, 2);
        let root_reference = RetryStateReference::new(2, 3);
        let first_commit = ManifestCommit::from_parts(
            3,
            2,
            1,
            DurableCutoff::from_manifest(store_id, 1, 2, 1, 400),
            first_reference,
        );
        let catalog = GenerationCatalogReference::new(0, 1, 132, 7);
        let root = ManifestCommit::from_generation_parts(
            5,
            2,
            1,
            DurableCutoff::from_manifest(store_id, 2, 2, 2, 500),
            root_reference,
            1,
            Some(catalog),
        );
        let first_qualification = qualification("current-first");
        let second_qualification = qualification("current-second");
        let snapshot = RetryStateSnapshot {
            store_id,
            options,
            reference: Some(root_reference),
            replay: vec![
                RetryReplayOutcome {
                    qualification: first_qualification.clone(),
                    append_sequence: 1,
                    end_offset: 400,
                    committed: first_commit,
                },
                RetryReplayOutcome {
                    qualification: second_qualification.clone(),
                    append_sequence: 2,
                    end_offset: 500,
                    committed: root,
                },
            ]
            .into_boxed_slice(),
            guard: Box::new([]),
        };
        let canonical = encode_retry_state(&snapshot).expect("current Retry State V1 bytes");
        assert_eq!(&canonical[8..10], &RETRY_VERSION.to_be_bytes());
        assert_eq!(
            decode_retry_state_at_slot(&canonical, 2, store_id, options)
                .expect("current Retry State V1 decode")
                .1,
            snapshot
        );
        let first_extension = RETRY_HEADER_LEN + qualification_len(&first_qualification) + 88;
        let second_extension = first_extension + 48 + qualification_len(&second_qualification) + 88;
        assert_eq!(canonical[first_extension + 8], 0);
        assert_eq!(canonical[second_extension + 8], 1);

        for (offset, length, value) in [
            (first_extension + 8, 1_usize, 1_u8),
            (first_extension + 10, 1, 1),
            (second_extension, 8, 0),
            (second_extension + 8, 1, 2),
            (second_extension + 9, 1, 3),
            (second_extension + 10, 1, 1),
            (second_extension + 16, 8, 0),
            (second_extension + 24, 8, 0),
            (second_extension + 36, 1, 1),
        ] {
            let mut hostile = canonical.clone();
            hostile[offset..offset + length].fill(value);
            repair_checksum(&mut hostile);
            assert!(
                decode_retry_state_at_slot(&hostile, 2, store_id, options).is_err(),
                "hostile current Retry State V1 field at {offset} must refuse"
            );
        }
        let mut unknown_version = canonical.clone();
        unknown_version[8..10].copy_from_slice(&3_u16.to_be_bytes());
        repair_checksum(&mut unknown_version);
        assert!(decode_retry_state_at_slot(&unknown_version, 2, store_id, options).is_err());
        let mut trailing = canonical.clone();
        trailing.push(0);
        assert!(decode_retry_state_at_slot(&trailing, 2, store_id, options).is_err());
        let mut checksum = canonical;
        let last = checksum.len() - 1;
        checksum[last] ^= 1;
        assert!(decode_retry_state_at_slot(&checksum, 2, store_id, options).is_err());
    }
}
