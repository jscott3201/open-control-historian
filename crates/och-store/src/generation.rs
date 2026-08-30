//! Bounded generation catalog, rotation intent, and sealed Journal V1 identity.

use crate::MAX_ACTIVE_JOURNAL_BYTES;
use crate::codec::crc32c;
use och_core::StoreId;

/// Maximum immutable raw Journal V1 generations retained by one store.
pub const MAX_SEALED_GENERATIONS: usize = 64;
/// Exact Generation Catalog V1 staging artifact.
pub const GENERATION_CATALOG_STAGING_FILE_NAME: &str = "generation-catalog-v1.staging";
/// Exact bounded rotation intent artifact.
pub const ROTATION_INTENT_FILE_NAME: &str = "journal-rotation-v1.intent";
/// Exact bounded sealed Journal V1 staging artifact.
pub const SEALED_JOURNAL_STAGING_FILE_NAME: &str = "sealed-journal-v1.staging";

pub(crate) const CATALOG_SLOT_NAMES: [&str; 3] = [
    "generation-catalog-v1-slot-0.och",
    "generation-catalog-v1-slot-1.och",
    "generation-catalog-v1-slot-2.och",
];
pub(crate) const CATALOG_MAGIC: [u8; 8] = *b"OCHCAT01";
pub(crate) const CATALOG_VERSION: u16 = 1;
pub(crate) const CATALOG_HEADER_LEN: usize = 64;
const CATALOG_HEADER_LEN_U16: u16 = 64;
pub(crate) const CATALOG_ENTRY_LEN: usize = 64;
pub(crate) const MAX_GENERATION_CATALOG_BYTES: usize =
    CATALOG_HEADER_LEN + CATALOG_ENTRY_LEN * MAX_SEALED_GENERATIONS + 4;
const SEALED_FORMAT_RAW_JOURNAL_V1: u16 = 1;
const ROTATION_INTENT_MAGIC: [u8; 8] = *b"OCHROT01";
const ROTATION_INTENT_VERSION: u16 = 1;
const ROTATION_INTENT_LEN: usize = 96;
const ROTATION_INTENT_LEN_U16: u16 = 96;

/// Complete public identity of one committed Generation Catalog V1 snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationCatalogReference {
    slot: u8,
    generation: u64,
    length: u64,
    checksum: u32,
}

impl GenerationCatalogReference {
    pub(crate) const fn new(slot: u8, generation: u64, length: u64, checksum: u32) -> Self {
        Self {
            slot,
            generation,
            length,
            checksum,
        }
    }

    /// Returns the reusable catalog slot in `0..3`.
    #[must_use]
    pub const fn slot(self) -> u8 {
        self.slot
    }

    /// Returns the positive catalog generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the exact complete catalog artifact length.
    #[must_use]
    pub const fn length(self) -> u64 {
        self.length
    }

    /// Returns CRC-32C over the complete catalog artifact.
    #[must_use]
    pub const fn checksum(self) -> u32 {
        self.checksum
    }
}

/// Sanitized immutable evidence for one sealed raw Journal V1 generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedGeneration {
    journal_generation: u64,
    sequence_floor: u64,
    sequence_cutoff: u64,
    end_offset: u64,
    registry_generation: u64,
    artifact_length: u64,
    artifact_checksum: u32,
}

impl SealedGeneration {
    pub(crate) const fn new(
        journal_generation: u64,
        sequence_floor: u64,
        sequence_cutoff: u64,
        end_offset: u64,
        registry_generation: u64,
        artifact_length: u64,
        artifact_checksum: u32,
    ) -> Self {
        Self {
            journal_generation,
            sequence_floor,
            sequence_cutoff,
            end_offset,
            registry_generation,
            artifact_length,
            artifact_checksum,
        }
    }

    /// Returns the sealed source journal generation.
    #[must_use]
    pub const fn journal_generation(self) -> u64 {
        self.journal_generation
    }

    /// Returns the exclusive store-global append sequence floor.
    #[must_use]
    pub const fn sequence_floor(self) -> u64 {
        self.sequence_floor
    }

    /// Returns the inclusive store-global append sequence cutoff.
    #[must_use]
    pub const fn sequence_cutoff(self) -> u64 {
        self.sequence_cutoff
    }

    /// Returns the exact source journal durable end offset.
    #[must_use]
    pub const fn end_offset(self) -> u64 {
        self.end_offset
    }

    /// Returns the registry generation that interprets the complete range.
    #[must_use]
    pub const fn registry_generation(self) -> u64 {
        self.registry_generation
    }

    /// Returns the exact immutable artifact byte length.
    #[must_use]
    pub const fn artifact_length(self) -> u64 {
        self.artifact_length
    }

    /// Returns CRC-32C over the complete immutable artifact.
    #[must_use]
    pub const fn artifact_checksum(self) -> u32 {
        self.artifact_checksum
    }
}

/// Immutable bounded Generation Catalog V1 snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationCatalogSnapshot {
    store_id: StoreId,
    reference: Option<GenerationCatalogReference>,
    entries: Box<[SealedGeneration]>,
}

impl GenerationCatalogSnapshot {
    pub(crate) fn empty(store_id: StoreId) -> Self {
        Self {
            store_id,
            reference: None,
            entries: Box::new([]),
        }
    }

    pub(crate) fn advance(
        &self,
        reference: GenerationCatalogReference,
        entry: SealedGeneration,
    ) -> Result<Self, GenerationCodecError> {
        let expected_generation = match self.reference {
            Some(prior) => prior.generation.checked_add(1),
            None => Some(1),
        };
        if self.entries.len() >= MAX_SEALED_GENERATIONS
            || reference.slot >= 3
            || reference.generation == 0
            || Some(reference.generation) != expected_generation
            || self
                .reference
                .is_some_and(|prior| prior.slot == reference.slot)
            || self.entries.last().is_some_and(|prior| {
                prior.journal_generation.checked_add(1) != Some(entry.journal_generation)
                    || prior.sequence_cutoff != entry.sequence_floor
            })
            || (self.entries.is_empty()
                && (entry.journal_generation != 1 || entry.sequence_floor != 0))
            || entry.sequence_cutoff <= entry.sequence_floor
            || entry.end_offset <= crate::JOURNAL_V1_HEADER_LEN as u64
            || entry.artifact_length != entry.end_offset
            || entry.artifact_length > MAX_ACTIVE_JOURNAL_BYTES
            || entry.registry_generation == 0
        {
            return Err(GenerationCodecError::Invalid);
        }
        let mut entries = self.entries.to_vec();
        entries.push(entry);
        Ok(Self {
            store_id: self.store_id,
            reference: Some(reference),
            entries: entries.into_boxed_slice(),
        })
    }

    pub(crate) fn with_reference(
        mut self,
        reference: GenerationCatalogReference,
    ) -> Result<Self, GenerationCodecError> {
        if self.reference.is_none_or(|current| {
            current.slot != reference.slot || current.generation != reference.generation
        }) || reference.slot >= 3
            || reference.generation == 0
            || reference.length == 0
        {
            return Err(GenerationCodecError::Invalid);
        }
        self.reference = Some(reference);
        Ok(self)
    }

    /// Returns the exact store scope.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns committed catalog identity, absent before first rotation.
    #[must_use]
    pub const fn reference(&self) -> Option<GenerationCatalogReference> {
        self.reference
    }

    /// Returns canonically ordered sealed-generation evidence.
    #[must_use]
    pub const fn entries(&self) -> &[SealedGeneration] {
        &self.entries
    }

    pub(crate) fn covers_commit(
        &self,
        journal_generation: u64,
        sequence: u64,
        end_offset: u64,
    ) -> bool {
        self.entries.iter().any(|entry| {
            entry.journal_generation == journal_generation
                && sequence > entry.sequence_floor
                && sequence <= entry.sequence_cutoff
                && end_offset > crate::JOURNAL_V1_HEADER_LEN as u64
                && end_offset <= entry.end_offset
        })
    }
}

/// Bounded path-free generation facts exposed by store inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationInventory {
    active_generation: u64,
    sealed_count: usize,
    covered_sequence_floor: u64,
    covered_sequence_cutoff: u64,
    sealed_bytes: u64,
}

impl GenerationInventory {
    pub(crate) fn from_catalog(
        active_generation: u64,
        catalog: &GenerationCatalogSnapshot,
    ) -> Self {
        Self {
            active_generation,
            sealed_count: catalog.entries.len(),
            covered_sequence_floor: catalog
                .entries
                .first()
                .map_or(0, |entry| entry.sequence_floor),
            covered_sequence_cutoff: catalog
                .entries
                .last()
                .map_or(0, |entry| entry.sequence_cutoff),
            sealed_bytes: catalog.entries.iter().fold(0_u64, |total, entry| {
                total.saturating_add(entry.artifact_length)
            }),
        }
    }

    /// Returns the current mutable journal generation.
    #[must_use]
    pub const fn active_generation(self) -> u64 {
        self.active_generation
    }

    /// Returns the exact retained sealed generation count.
    #[must_use]
    pub const fn sealed_count(self) -> usize {
        self.sealed_count
    }

    /// Returns the exclusive floor of the first sealed range, or zero.
    #[must_use]
    pub const fn covered_sequence_floor(self) -> u64 {
        self.covered_sequence_floor
    }

    /// Returns the inclusive cutoff of the last sealed range, or zero.
    #[must_use]
    pub const fn covered_sequence_cutoff(self) -> u64 {
        self.covered_sequence_cutoff
    }

    /// Returns the exact sum of sealed raw artifact bytes.
    #[must_use]
    pub const fn sealed_bytes(self) -> u64 {
        self.sealed_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GenerationCodecError {
    Invalid,
    StoreMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RotationIntent {
    pub(crate) store_id: StoreId,
    pub(crate) source_generation: u64,
    pub(crate) successor_generation: u64,
    pub(crate) sequence_cutoff: u64,
    pub(crate) source_end_offset: u64,
    pub(crate) registry_generation: u64,
    pub(crate) catalog_generation: u64,
    pub(crate) source_checkpoint_generation: u64,
}

pub(crate) fn sealed_journal_file_name(generation: u64) -> String {
    format!("sealed-journal-v1-g{generation:020}.och")
}

pub(crate) fn parse_active_journal_generation_name(name: &str) -> Option<u64> {
    parse_generation_name(name, "active-journal-v1-g", ".och", 2)
}

pub(crate) fn parse_active_checkpoint_generation_name(name: &str) -> Option<u64> {
    parse_generation_name(name, "active-journal-v1-g", ".checkpoint", 2)
}

pub(crate) fn parse_sealed_generation_name(name: &str) -> Option<u64> {
    parse_generation_name(name, "sealed-journal-v1-g", ".och", 1)
}

fn parse_generation_name(name: &str, prefix: &str, suffix: &str, minimum: u64) -> Option<u64> {
    let digits = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let generation = digits.parse::<u64>().ok()?;
    (generation >= minimum && format!("{generation:020}") == digits).then_some(generation)
}

pub(crate) fn encode_catalog(
    snapshot: &GenerationCatalogSnapshot,
) -> Result<Vec<u8>, GenerationCodecError> {
    let reference = snapshot.reference.ok_or(GenerationCodecError::Invalid)?;
    if reference.slot >= 3
        || reference.generation == 0
        || snapshot.entries.is_empty()
        || snapshot.entries.len() > MAX_SEALED_GENERATIONS
    {
        return Err(GenerationCodecError::Invalid);
    }
    let payload_len = snapshot
        .entries
        .len()
        .checked_mul(CATALOG_ENTRY_LEN)
        .ok_or(GenerationCodecError::Invalid)?;
    let total = CATALOG_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(4))
        .ok_or(GenerationCodecError::Invalid)?;
    if total > MAX_GENERATION_CATALOG_BYTES {
        return Err(GenerationCodecError::Invalid);
    }
    let mut bytes = vec![0_u8; total];
    bytes[..8].copy_from_slice(&CATALOG_MAGIC);
    bytes[8..10].copy_from_slice(&CATALOG_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&CATALOG_HEADER_LEN_U16.to_be_bytes());
    bytes[12..28].copy_from_slice(snapshot.store_id.as_bytes());
    bytes[28..36].copy_from_slice(&reference.generation.to_be_bytes());
    bytes[36..40].copy_from_slice(
        &u32::try_from(snapshot.entries.len())
            .map_err(|_| GenerationCodecError::Invalid)?
            .to_be_bytes(),
    );
    bytes[40..48].copy_from_slice(
        &u64::try_from(payload_len)
            .map_err(|_| GenerationCodecError::Invalid)?
            .to_be_bytes(),
    );
    for (index, entry) in snapshot.entries.iter().enumerate() {
        let offset = CATALOG_HEADER_LEN + index * CATALOG_ENTRY_LEN;
        bytes[offset..offset + 8].copy_from_slice(&entry.journal_generation.to_be_bytes());
        bytes[offset + 8..offset + 16].copy_from_slice(&entry.sequence_floor.to_be_bytes());
        bytes[offset + 16..offset + 24].copy_from_slice(&entry.sequence_cutoff.to_be_bytes());
        bytes[offset + 24..offset + 32].copy_from_slice(&entry.end_offset.to_be_bytes());
        bytes[offset + 32..offset + 40].copy_from_slice(&entry.registry_generation.to_be_bytes());
        bytes[offset + 40..offset + 48].copy_from_slice(&entry.artifact_length.to_be_bytes());
        bytes[offset + 48..offset + 52].copy_from_slice(&entry.artifact_checksum.to_be_bytes());
        bytes[offset + 52..offset + 54]
            .copy_from_slice(&SEALED_FORMAT_RAW_JOURNAL_V1.to_be_bytes());
    }
    let checksum_offset = bytes.len() - 4;
    let checksum = crc32c(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn decode_catalog(
    bytes: &[u8],
    slot: u8,
    expected_store: StoreId,
) -> Result<GenerationCatalogSnapshot, GenerationCodecError> {
    if slot >= 3
        || bytes.len() < CATALOG_HEADER_LEN + CATALOG_ENTRY_LEN + 4
        || bytes.len() > MAX_GENERATION_CATALOG_BYTES
        || bytes[..8] != CATALOG_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != CATALOG_VERSION
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
            != CATALOG_HEADER_LEN_U16
        || bytes[48..64].iter().any(|byte| *byte != 0)
    {
        return Err(GenerationCodecError::Invalid);
    }
    let checksum_offset = bytes.len() - 4;
    if crc32c(&bytes[..checksum_offset])
        != u32::from_be_bytes(bytes[checksum_offset..].try_into().unwrap_or_default())
    {
        return Err(GenerationCodecError::Invalid);
    }
    let store_id = StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default())
        .map_err(|_| GenerationCodecError::Invalid)?;
    if store_id != expected_store {
        return Err(GenerationCodecError::StoreMismatch);
    }
    let generation = u64::from_be_bytes(bytes[28..36].try_into().unwrap_or_default());
    let count = usize::try_from(u32::from_be_bytes(
        bytes[36..40].try_into().unwrap_or_default(),
    ))
    .map_err(|_| GenerationCodecError::Invalid)?;
    let payload_len = usize::try_from(u64::from_be_bytes(
        bytes[40..48].try_into().unwrap_or_default(),
    ))
    .map_err(|_| GenerationCodecError::Invalid)?;
    if generation == 0
        || count == 0
        || count > MAX_SEALED_GENERATIONS
        || count.checked_mul(CATALOG_ENTRY_LEN) != Some(payload_len)
        || CATALOG_HEADER_LEN.checked_add(payload_len) != Some(checksum_offset)
    {
        return Err(GenerationCodecError::Invalid);
    }
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let offset = CATALOG_HEADER_LEN + index * CATALOG_ENTRY_LEN;
        if bytes[offset + 54..offset + 64]
            .iter()
            .any(|byte| *byte != 0)
            || u16::from_be_bytes(
                bytes[offset + 52..offset + 54]
                    .try_into()
                    .unwrap_or_default(),
            ) != SEALED_FORMAT_RAW_JOURNAL_V1
        {
            return Err(GenerationCodecError::Invalid);
        }
        let entry = SealedGeneration::new(
            u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap_or_default()),
            u64::from_be_bytes(
                bytes[offset + 8..offset + 16]
                    .try_into()
                    .unwrap_or_default(),
            ),
            u64::from_be_bytes(
                bytes[offset + 16..offset + 24]
                    .try_into()
                    .unwrap_or_default(),
            ),
            u64::from_be_bytes(
                bytes[offset + 24..offset + 32]
                    .try_into()
                    .unwrap_or_default(),
            ),
            u64::from_be_bytes(
                bytes[offset + 32..offset + 40]
                    .try_into()
                    .unwrap_or_default(),
            ),
            u64::from_be_bytes(
                bytes[offset + 40..offset + 48]
                    .try_into()
                    .unwrap_or_default(),
            ),
            u32::from_be_bytes(
                bytes[offset + 48..offset + 52]
                    .try_into()
                    .unwrap_or_default(),
            ),
        );
        if entries.last().is_some_and(|prior: &SealedGeneration| {
            prior.journal_generation.checked_add(1) != Some(entry.journal_generation)
                || prior.sequence_cutoff != entry.sequence_floor
        }) || (entries.is_empty()
            && (entry.journal_generation != 1 || entry.sequence_floor != 0))
            || entry.sequence_cutoff <= entry.sequence_floor
            || entry.end_offset <= crate::JOURNAL_V1_HEADER_LEN as u64
            || entry.artifact_length != entry.end_offset
            || entry.artifact_length > MAX_ACTIVE_JOURNAL_BYTES
            || entry.registry_generation == 0
        {
            return Err(GenerationCodecError::Invalid);
        }
        entries.push(entry);
    }
    if usize::try_from(generation).ok() != Some(count) {
        return Err(GenerationCodecError::Invalid);
    }
    let snapshot = GenerationCatalogSnapshot {
        store_id,
        reference: Some(GenerationCatalogReference::new(
            slot,
            generation,
            u64::try_from(bytes.len()).map_err(|_| GenerationCodecError::Invalid)?,
            crc32c(bytes),
        )),
        entries: entries.into_boxed_slice(),
    };
    if encode_catalog(&snapshot)? != bytes {
        return Err(GenerationCodecError::Invalid);
    }
    Ok(snapshot)
}

pub(crate) fn encode_rotation_intent(intent: RotationIntent) -> [u8; ROTATION_INTENT_LEN] {
    let mut bytes = [0_u8; ROTATION_INTENT_LEN];
    bytes[..8].copy_from_slice(&ROTATION_INTENT_MAGIC);
    bytes[8..10].copy_from_slice(&ROTATION_INTENT_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&ROTATION_INTENT_LEN_U16.to_be_bytes());
    bytes[12..28].copy_from_slice(intent.store_id.as_bytes());
    bytes[28..36].copy_from_slice(&intent.source_generation.to_be_bytes());
    bytes[36..44].copy_from_slice(&intent.successor_generation.to_be_bytes());
    bytes[44..52].copy_from_slice(&intent.sequence_cutoff.to_be_bytes());
    bytes[52..60].copy_from_slice(&intent.source_end_offset.to_be_bytes());
    bytes[60..68].copy_from_slice(&intent.registry_generation.to_be_bytes());
    bytes[68..76].copy_from_slice(&intent.catalog_generation.to_be_bytes());
    bytes[76..84].copy_from_slice(&intent.source_checkpoint_generation.to_be_bytes());
    let checksum = crc32c(&bytes[..92]);
    bytes[92..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

pub(crate) fn decode_rotation_intent(
    bytes: &[u8],
    expected_store: StoreId,
) -> Result<RotationIntent, GenerationCodecError> {
    if bytes.len() != ROTATION_INTENT_LEN
        || bytes[..8] != ROTATION_INTENT_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default())
            != ROTATION_INTENT_VERSION
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
            != ROTATION_INTENT_LEN_U16
        || bytes[84..92].iter().any(|byte| *byte != 0)
        || crc32c(&bytes[..92]) != u32::from_be_bytes(bytes[92..].try_into().unwrap_or_default())
    {
        return Err(GenerationCodecError::Invalid);
    }
    let store_id = StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default())
        .map_err(|_| GenerationCodecError::Invalid)?;
    if store_id != expected_store {
        return Err(GenerationCodecError::StoreMismatch);
    }
    let intent = RotationIntent {
        store_id,
        source_generation: u64::from_be_bytes(bytes[28..36].try_into().unwrap_or_default()),
        successor_generation: u64::from_be_bytes(bytes[36..44].try_into().unwrap_or_default()),
        sequence_cutoff: u64::from_be_bytes(bytes[44..52].try_into().unwrap_or_default()),
        source_end_offset: u64::from_be_bytes(bytes[52..60].try_into().unwrap_or_default()),
        registry_generation: u64::from_be_bytes(bytes[60..68].try_into().unwrap_or_default()),
        catalog_generation: u64::from_be_bytes(bytes[68..76].try_into().unwrap_or_default()),
        source_checkpoint_generation: u64::from_be_bytes(
            bytes[76..84].try_into().unwrap_or_default(),
        ),
    };
    if intent.source_generation == 0
        || intent.source_generation.checked_add(1) != Some(intent.successor_generation)
        || intent.sequence_cutoff == 0
        || intent.source_end_offset <= crate::JOURNAL_V1_HEADER_LEN as u64
        || intent.registry_generation == 0
        || intent.catalog_generation == 0
        || intent.source_checkpoint_generation == 0
    {
        return Err(GenerationCodecError::Invalid);
    }
    Ok(intent)
}

pub(crate) struct StreamingCrc32c(u32);

impl StreamingCrc32c {
    pub(crate) const fn new() -> Self {
        Self(u32::MAX)
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = 0_u32.wrapping_sub(self.0 & 1);
                self.0 = (self.0 >> 1) ^ (0x82f6_3b78 & mask);
            }
        }
    }

    pub(crate) const fn finish(self) -> u32 {
        !self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;

    #[test]
    fn two_generation_catalog_round_trips_canonically() {
        let store_id = test_support::store_id(1);
        let first_reference = GenerationCatalogReference::new(0, 1, 1, 1);
        let first = GenerationCatalogSnapshot::empty(store_id)
            .advance(
                first_reference,
                SealedGeneration::new(1, 0, 1, 700, 2, 700, 11),
            )
            .expect("first catalog entry");
        let first_bytes = encode_catalog(&first).expect("first catalog bytes");
        let first = first
            .with_reference(GenerationCatalogReference::new(
                0,
                1,
                first_bytes.len() as u64,
                crc32c(&first_bytes),
            ))
            .expect("first final reference");
        let second = first
            .advance(
                GenerationCatalogReference::new(1, 2, 1, 1),
                SealedGeneration::new(2, 1, 2, 710, 2, 710, 12),
            )
            .expect("second catalog entry");
        let second_bytes = encode_catalog(&second).expect("second catalog bytes");
        let second = second
            .with_reference(GenerationCatalogReference::new(
                1,
                2,
                second_bytes.len() as u64,
                crc32c(&second_bytes),
            ))
            .expect("second final reference");
        assert_eq!(
            decode_catalog(
                &encode_catalog(&second).expect("canonical bytes"),
                1,
                store_id
            ),
            Ok(second)
        );
    }

    fn finalized(snapshot: GenerationCatalogSnapshot) -> GenerationCatalogSnapshot {
        let reference = snapshot.reference().expect("provisional catalog reference");
        let bytes = encode_catalog(&snapshot).expect("provisional catalog bytes");
        snapshot
            .with_reference(GenerationCatalogReference::new(
                reference.slot(),
                reference.generation(),
                bytes.len() as u64,
                crc32c(&bytes),
            ))
            .expect("final catalog identity")
    }

    #[test]
    fn maximum_generations_never_saturate_into_equal_or_wrapped_successors() {
        let store_id = test_support::store_id(1);
        let exhausted = GenerationCatalogSnapshot {
            store_id,
            reference: Some(GenerationCatalogReference::new(0, u64::MAX, 132, 7)),
            entries: vec![SealedGeneration::new(1, 0, 1, 100, 1, 100, 9)].into_boxed_slice(),
        };
        assert_eq!(
            exhausted.advance(
                GenerationCatalogReference::new(1, u64::MAX, 1, 1),
                SealedGeneration::new(2, 1, 2, 101, 1, 101, 10),
            ),
            Err(GenerationCodecError::Invalid)
        );

        let intent = RotationIntent {
            store_id,
            source_generation: u64::MAX,
            successor_generation: u64::MAX,
            sequence_cutoff: 1,
            source_end_offset: 100,
            registry_generation: 1,
            catalog_generation: 1,
            source_checkpoint_generation: 1,
        };
        assert_eq!(
            decode_rotation_intent(&encode_rotation_intent(intent), store_id),
            Err(GenerationCodecError::Invalid)
        );
        let wrapped = RotationIntent {
            successor_generation: 0,
            ..intent
        };
        assert_eq!(
            decode_rotation_intent(&encode_rotation_intent(wrapped), store_id),
            Err(GenerationCodecError::Invalid)
        );
        let exact_maximum_successor = RotationIntent {
            source_generation: u64::MAX - 1,
            successor_generation: u64::MAX,
            ..intent
        };
        assert_eq!(
            decode_rotation_intent(&encode_rotation_intent(exact_maximum_successor), store_id),
            Ok(exact_maximum_successor)
        );
    }

    fn repair_checksum(bytes: &mut [u8]) {
        let offset = bytes.len() - 4;
        let checksum = crc32c(&bytes[..offset]);
        bytes[offset..].copy_from_slice(&checksum.to_be_bytes());
    }

    #[test]
    fn exact_catalog_capacity_is_sixty_four_without_overwrite_or_reclamation() {
        let store_id = test_support::store_id(1);
        let mut snapshot = GenerationCatalogSnapshot::empty(store_id);
        for generation in 1_u64..=MAX_SEALED_GENERATIONS as u64 {
            let slot = u8::try_from((generation - 1) % 3).expect("catalog slot");
            snapshot = finalized(
                snapshot
                    .advance(
                        GenerationCatalogReference::new(slot, generation, 1, 1),
                        SealedGeneration::new(
                            generation,
                            generation - 1,
                            generation,
                            29,
                            1,
                            29,
                            u32::try_from(generation).expect("bounded catalog generation"),
                        ),
                    )
                    .expect("bounded catalog advance"),
            );
        }
        assert_eq!(snapshot.entries().len(), MAX_SEALED_GENERATIONS);
        let full_bytes = encode_catalog(&snapshot).expect("full bounded catalog bytes");
        assert_eq!(full_bytes.len(), MAX_GENERATION_CATALOG_BYTES);
        let prior = snapshot.clone();
        assert_eq!(
            snapshot.advance(
                GenerationCatalogReference::new(1, 65, 1, 1),
                SealedGeneration::new(65, 64, 65, 29, 1, 29, 65),
            ),
            Err(GenerationCodecError::Invalid)
        );
        assert_eq!(snapshot, prior);
    }

    #[test]
    fn catalog_parser_refuses_hostile_lengths_versions_scope_order_ranges_and_reserved_bytes() {
        let store_id = test_support::store_id(1);
        let snapshot = finalized(
            GenerationCatalogSnapshot::empty(store_id)
                .advance(
                    GenerationCatalogReference::new(0, 1, 1, 1),
                    SealedGeneration::new(1, 0, 1, 100, 2, 100, 7),
                )
                .expect("canonical catalog"),
        );
        let canonical = encode_catalog(&snapshot).expect("canonical catalog bytes");
        assert_eq!(decode_catalog(&canonical, 0, store_id), Ok(snapshot));
        assert_eq!(
            decode_catalog(&canonical, 3, store_id),
            Err(GenerationCodecError::Invalid)
        );
        assert_eq!(
            decode_catalog(&canonical, 0, test_support::store_id(2)),
            Err(GenerationCodecError::StoreMismatch)
        );
        assert!(decode_catalog(&canonical[..canonical.len() - 1], 0, store_id).is_err());
        let mut trailing = canonical.clone();
        trailing.push(0);
        assert!(decode_catalog(&trailing, 0, store_id).is_err());

        for (offset, length, value) in [
            (8_usize, 2_usize, 2_u8),
            (10, 2, 0),
            (28, 8, 0),
            (36, 4, 2),
            (40, 8, 0),
            (48, 1, 1),
            (64, 8, 2),
            (72, 8, 1),
            (80, 8, 0),
            (88, 8, 28),
            (96, 8, 0),
            (104, 8, 99),
            (116, 2, 2),
            (118, 1, 1),
        ] {
            let mut hostile = canonical.clone();
            hostile[offset..offset + length].fill(value);
            repair_checksum(&mut hostile);
            assert!(
                decode_catalog(&hostile, 0, store_id).is_err(),
                "hostile catalog field at {offset} must refuse"
            );
        }
        let mut checksum = canonical;
        let last = checksum.len() - 1;
        checksum[last] ^= 1;
        assert!(decode_catalog(&checksum, 0, store_id).is_err());
    }
}
