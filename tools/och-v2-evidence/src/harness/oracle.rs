use crate::crc32c::Crc32c;
use crate::error::{EvidenceError, Result};

pub(crate) const MARKER_BYTES: usize = 32;
pub(crate) const INTENT_BYTES: usize = 128;
pub(crate) const MANIFEST_BYTES: usize = 160;
pub(crate) const CATALOG_MAX_ENTRIES: usize = 64;
pub(crate) const CATALOG_HEADER_BYTES: usize = 64;
pub(crate) const CATALOG_ENTRY_BYTES: usize = 80;
pub(crate) const CATALOG_MAX_BYTES: usize = 5_188;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactIdentity {
    pub(crate) length: u64,
    pub(crate) checksum: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CatalogEntry {
    pub(crate) journal_generation: u64,
    pub(crate) sequence_floor: u64,
    pub(crate) sequence_cutoff: u64,
    pub(crate) source_end: u64,
    pub(crate) registry_generation: u64,
    pub(crate) raw: ArtifactIdentity,
    pub(crate) segment: ArtifactIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionRelation {
    pub(crate) store_id: [u8; 16],
    pub(crate) source_generation: u64,
    pub(crate) successor_generation: u64,
    pub(crate) sequence_floor: u64,
    pub(crate) sequence_cutoff: u64,
    pub(crate) source_end: u64,
    pub(crate) registry_generation: u64,
    pub(crate) catalog_generation: u64,
    pub(crate) checkpoint_generation: u64,
    pub(crate) raw: ArtifactIdentity,
    pub(crate) segment: ArtifactIdentity,
}

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = Crc32c::new();
    crc.update(bytes);
    crc.finish()
}

pub(crate) fn marker(store_id: [u8; 16]) -> [u8; MARKER_BYTES] {
    let mut bytes = [0_u8; MARKER_BYTES];
    bytes[..8].copy_from_slice(b"OCHFMT02");
    bytes[8..10].copy_from_slice(&2_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&32_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store_id);
    let checksum = crc32c(&bytes[..28]);
    bytes[28..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

pub(crate) fn validate_marker(bytes: &[u8], store_id: [u8; 16]) -> Result<()> {
    if bytes != marker(store_id) {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

pub(crate) fn intent(relation: TransactionRelation) -> Result<[u8; INTENT_BYTES]> {
    validate_relation(relation)?;
    let mut bytes = [0_u8; INTENT_BYTES];
    bytes[..8].copy_from_slice(b"OCHROT02");
    put_u16(&mut bytes, 8, 2);
    put_u16(&mut bytes, 10, 128);
    bytes[12..28].copy_from_slice(&relation.store_id);
    put_u64(&mut bytes, 28, relation.source_generation);
    put_u64(&mut bytes, 36, relation.successor_generation);
    put_u64(&mut bytes, 44, relation.sequence_cutoff);
    put_u64(&mut bytes, 52, relation.source_end);
    put_u64(&mut bytes, 60, relation.registry_generation);
    put_u64(&mut bytes, 68, relation.catalog_generation);
    put_u64(&mut bytes, 76, relation.checkpoint_generation);
    put_u16(&mut bytes, 84, 1);
    put_u64(&mut bytes, 86, relation.raw.length);
    put_u32(&mut bytes, 94, relation.raw.checksum);
    put_u16(&mut bytes, 98, 1);
    put_u64(&mut bytes, 100, relation.segment.length);
    put_u32(&mut bytes, 108, relation.segment.checksum);
    let checksum = crc32c(&bytes[..124]);
    put_u32(&mut bytes, 124, checksum);
    Ok(bytes)
}

pub(crate) fn validate_intent(bytes: &[u8], relation: TransactionRelation) -> Result<()> {
    if bytes != intent(relation)? {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

pub(crate) fn catalog(store_id: [u8; 16], entries: &[CatalogEntry]) -> Result<Vec<u8>> {
    if entries.is_empty() || entries.len() > CATALOG_MAX_ENTRIES {
        return Err(EvidenceError::Bounds);
    }
    let payload = entries
        .len()
        .checked_mul(CATALOG_ENTRY_BYTES)
        .ok_or(EvidenceError::Bounds)?;
    let total = CATALOG_HEADER_BYTES
        .checked_add(payload)
        .and_then(|value| value.checked_add(4))
        .ok_or(EvidenceError::Bounds)?;
    if total > CATALOG_MAX_BYTES {
        return Err(EvidenceError::Bounds);
    }
    let mut bytes = vec![0_u8; total];
    bytes[..8].copy_from_slice(b"OCHCAT02");
    put_u16(&mut bytes, 8, 2);
    put_u16(&mut bytes, 10, 64);
    bytes[12..28].copy_from_slice(&store_id);
    put_u64(
        &mut bytes,
        28,
        u64::try_from(entries.len()).map_err(|_| EvidenceError::Bounds)?,
    );
    put_u32(
        &mut bytes,
        36,
        u32::try_from(entries.len()).map_err(|_| EvidenceError::Bounds)?,
    );
    put_u64(
        &mut bytes,
        40,
        u64::try_from(payload).map_err(|_| EvidenceError::Bounds)?,
    );
    let mut previous = None;
    for (index, entry) in entries.iter().enumerate() {
        validate_catalog_entry(*entry, previous)?;
        let offset = CATALOG_HEADER_BYTES + index * CATALOG_ENTRY_BYTES;
        put_u64(&mut bytes, offset, entry.journal_generation);
        put_u64(&mut bytes, offset + 8, entry.sequence_floor);
        put_u64(&mut bytes, offset + 16, entry.sequence_cutoff);
        put_u64(&mut bytes, offset + 24, entry.source_end);
        put_u64(&mut bytes, offset + 32, entry.registry_generation);
        put_u64(&mut bytes, offset + 40, entry.raw.length);
        put_u32(&mut bytes, offset + 48, entry.raw.checksum);
        put_u16(&mut bytes, offset + 52, 1);
        put_u16(&mut bytes, offset + 54, 1);
        put_u64(&mut bytes, offset + 56, entry.segment.length);
        put_u32(&mut bytes, offset + 64, entry.segment.checksum);
        previous = Some(*entry);
    }
    let checksum = crc32c(&bytes[..total - 4]);
    put_u32(&mut bytes, total - 4, checksum);
    Ok(bytes)
}

pub(crate) fn validate_catalog(
    bytes: &[u8],
    store_id: [u8; 16],
    entries: &[CatalogEntry],
) -> Result<()> {
    if bytes != catalog(store_id, entries)? {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

pub(crate) fn manifest(
    relation: TransactionRelation,
    manifest_generation: u64,
    catalog_slot: u8,
    catalog_identity: ArtifactIdentity,
) -> Result<[u8; MANIFEST_BYTES]> {
    validate_relation(relation)?;
    if manifest_generation == 0 || catalog_slot > 2 || catalog_identity.length == 0 {
        return Err(EvidenceError::InvalidHarness);
    }
    let mut bytes = [0_u8; MANIFEST_BYTES];
    bytes[..8].copy_from_slice(b"OCHMAN02");
    put_u16(&mut bytes, 8, 2);
    put_u16(&mut bytes, 10, 160);
    bytes[12..28].copy_from_slice(&relation.store_id);
    put_u64(&mut bytes, 28, manifest_generation);
    put_u64(&mut bytes, 36, relation.successor_generation);
    put_u64(&mut bytes, 44, 1);
    put_u64(&mut bytes, 52, relation.sequence_cutoff);
    put_u64(&mut bytes, 60, och_store::JOURNAL_V1_HEADER_LEN as u64);
    bytes[68] = 0;
    put_u64(&mut bytes, 72, relation.registry_generation);
    put_u64(&mut bytes, 80, 68);
    put_u32(&mut bytes, 88, 0x11aa_22bb);
    bytes[92] = 0;
    put_u64(&mut bytes, 96, 1);
    put_u64(&mut bytes, 104, 68);
    put_u32(&mut bytes, 112, 0x33cc_44dd);
    put_u64(&mut bytes, 124, relation.sequence_cutoff);
    bytes[132] = catalog_slot;
    put_u64(&mut bytes, 136, relation.catalog_generation);
    put_u64(&mut bytes, 144, catalog_identity.length);
    put_u32(&mut bytes, 152, catalog_identity.checksum);
    let checksum = crc32c(&bytes[..156]);
    put_u32(&mut bytes, 156, checksum);
    Ok(bytes)
}

pub(crate) fn validate_manifest(
    bytes: &[u8],
    relation: TransactionRelation,
    manifest_generation: u64,
    catalog_slot: u8,
    catalog_identity: ArtifactIdentity,
) -> Result<()> {
    if bytes
        != manifest(
            relation,
            manifest_generation,
            catalog_slot,
            catalog_identity,
        )?
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

pub(crate) fn identity(bytes: &[u8]) -> Result<ArtifactIdentity> {
    Ok(ArtifactIdentity {
        length: u64::try_from(bytes.len()).map_err(|_| EvidenceError::Bounds)?,
        checksum: crc32c(bytes),
    })
}

pub(crate) fn validate_foundation_oracles() -> Result<()> {
    let store_id = [3_u8; 16];
    validate_marker(&marker(store_id), store_id)?;
    let raw = identity(&[0x5a; och_store::JOURNAL_V1_HEADER_LEN + 1])?;
    let segment = identity(b"OCHSEG01-foundation")?;
    let relation = TransactionRelation {
        store_id,
        source_generation: 1,
        successor_generation: 2,
        sequence_floor: 0,
        sequence_cutoff: 2,
        source_end: raw.length,
        registry_generation: 1,
        catalog_generation: 1,
        checkpoint_generation: 1,
        raw,
        segment,
    };
    validate_intent(&intent(relation)?, relation)?;
    let entry = CatalogEntry {
        journal_generation: 1,
        sequence_floor: 0,
        sequence_cutoff: 2,
        source_end: raw.length,
        registry_generation: 1,
        raw,
        segment,
    };
    let catalog_bytes = catalog(store_id, &[entry])?;
    validate_catalog(&catalog_bytes, store_id, &[entry])?;
    let catalog_identity = identity(&catalog_bytes)?;
    validate_manifest(
        &manifest(relation, 1, 0, catalog_identity)?,
        relation,
        1,
        0,
        catalog_identity,
    )
}

fn validate_relation(relation: TransactionRelation) -> Result<()> {
    if relation.source_generation == 0
        || relation.successor_generation != relation.source_generation.checked_add(1).unwrap_or(0)
        || relation.sequence_cutoff <= relation.sequence_floor
        || relation.source_end <= och_store::JOURNAL_V1_HEADER_LEN as u64
        || relation.registry_generation == 0
        || relation.catalog_generation == 0
        || relation.checkpoint_generation == 0
        || relation.raw.length != relation.source_end
        || relation.segment.length <= 4
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn validate_catalog_entry(entry: CatalogEntry, previous: Option<CatalogEntry>) -> Result<()> {
    let expected_generation = previous.map_or(1, |entry| entry.journal_generation + 1);
    let expected_floor = previous.map_or(0, |entry| entry.sequence_cutoff);
    if entry.journal_generation != expected_generation
        || entry.sequence_floor != expected_floor
        || entry.sequence_cutoff <= entry.sequence_floor
        || entry.source_end <= och_store::JOURNAL_V1_HEADER_LEN as u64
        || entry.registry_generation == 0
        || entry.raw.length != entry.source_end
        || entry.raw.length > och_store::MAX_ACTIVE_JOURNAL_BYTES
        || entry.segment.length <= 4
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relation() -> TransactionRelation {
        TransactionRelation {
            store_id: [7; 16],
            source_generation: 1,
            successor_generation: 2,
            sequence_floor: 0,
            sequence_cutoff: 1,
            source_end: 128,
            registry_generation: 1,
            catalog_generation: 1,
            checkpoint_generation: 1,
            raw: ArtifactIdentity {
                length: 128,
                checksum: 9,
            },
            segment: ArtifactIdentity {
                length: 256,
                checksum: 10,
            },
        }
    }

    #[test]
    fn primitive_lengths_magic_endian_reserved_and_checksums_are_exact() {
        let relation = relation();
        let marker = marker(relation.store_id);
        assert_eq!(marker.len(), 32);
        assert_eq!(&marker[..8], b"OCHFMT02");
        validate_marker(&marker, relation.store_id).expect("marker oracle");
        let intent = intent(relation).expect("intent oracle");
        assert_eq!(intent.len(), 128);
        assert_eq!(&intent[..8], b"OCHROT02");
        assert!(intent[112..124].iter().all(|byte| *byte == 0));
        validate_intent(&intent, relation).expect("intent validation");
        let entry = CatalogEntry {
            journal_generation: 1,
            sequence_floor: 0,
            sequence_cutoff: 1,
            source_end: 128,
            registry_generation: 1,
            raw: relation.raw,
            segment: relation.segment,
        };
        let catalog = catalog(relation.store_id, &[entry]).expect("catalog oracle");
        assert_eq!(catalog.len(), 148);
        assert!(catalog[48..64].iter().all(|byte| *byte == 0));
        validate_catalog(&catalog, relation.store_id, &[entry]).expect("catalog validation");
        let catalog_identity = identity(&catalog).expect("catalog identity");
        let manifest = manifest(relation, 2, 1, catalog_identity).expect("manifest oracle");
        assert_eq!(manifest.len(), 160);
        assert_eq!(&manifest[..8], b"OCHMAN02");
        validate_manifest(&manifest, relation, 2, 1, catalog_identity)
            .expect("manifest validation");
    }

    #[test]
    fn every_primitive_hostile_mutation_refuses() {
        let relation = relation();
        let marker = marker(relation.store_id);
        for index in [0, 8, 10, 12, 28, 31] {
            let mut hostile = marker;
            hostile[index] ^= 1;
            assert!(validate_marker(&hostile, relation.store_id).is_err());
        }
        let intent = intent(relation).expect("intent");
        for index in [
            0, 8, 10, 28, 36, 44, 52, 60, 68, 76, 84, 86, 94, 98, 100, 108, 112, 124,
        ] {
            let mut hostile = intent;
            hostile[index] ^= 1;
            assert!(validate_intent(&hostile, relation).is_err());
        }
    }

    #[test]
    fn catalog_entries_one_and_sixty_four_succeed_and_sixty_five_refuses_before_allocation() {
        let relation = relation();
        for count in [1_usize, 64] {
            let entries = (0..count)
                .map(|index| CatalogEntry {
                    journal_generation: u64::try_from(index + 1).expect("small generation"),
                    sequence_floor: u64::try_from(index).expect("small floor"),
                    sequence_cutoff: u64::try_from(index + 1).expect("small cutoff"),
                    source_end: 128,
                    registry_generation: 1,
                    raw: relation.raw,
                    segment: relation.segment,
                })
                .collect::<Vec<_>>();
            let bytes = catalog(relation.store_id, &entries).expect("bounded catalog");
            validate_catalog(&bytes, relation.store_id, &entries)
                .expect("bounded catalog validation");
        }
        let entries = vec![
            CatalogEntry {
                journal_generation: 1,
                sequence_floor: 0,
                sequence_cutoff: 1,
                source_end: 128,
                registry_generation: 1,
                raw: relation.raw,
                segment: relation.segment,
            };
            65
        ];
        assert_eq!(
            catalog(relation.store_id, &entries),
            Err(EvidenceError::Bounds)
        );
    }
}
