//! Primitive-only manifest, registry, retry, catalog, and sealed-journal oracle.

const MANIFEST_LEN: usize = 128;
const MANIFEST_V3_LEN: usize = 160;

pub struct CatalogEntry {
    pub journal_generation: u64,
    pub sequence_floor: u64,
    pub sequence_cutoff: u64,
    pub end_offset: u64,
    pub registry_generation: u64,
    pub artifact_length: u64,
    pub artifact_checksum: u32,
}

#[derive(Clone, Copy)]
pub struct CatalogReference {
    pub slot: u8,
    pub generation: u64,
    pub length: u64,
    pub checksum: u32,
}

pub struct RetryV2Outcome<'a> {
    pub series: [u8; 16],
    pub producer: [u8; 16],
    pub key: &'a str,
    pub content_format: &'a str,
    pub content_version: u128,
    pub digest: [u8; 32],
    pub append_sequence: u64,
    pub end_offset: u64,
    pub manifest_generation: u64,
    pub registry_slot: u8,
    pub registry_generation: u64,
    pub journal_generation: u64,
    pub checkpoint_generation: u64,
    pub cutoff_sequence: u64,
    pub cutoff_end: u64,
    pub retry_slot: u8,
    pub retry_generation: u64,
    pub sequence_floor: u64,
    pub catalog: Option<CatalogReference>,
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

pub fn checksum(bytes: &[u8]) -> u32 {
    crc32c(bytes)
}

pub fn sealed_raw_journal_v1(store: [u8; 16], frames: &[&[u8]]) -> Vec<u8> {
    let capacity = 28 + frames.iter().map(|frame| frame.len()).sum::<usize>();
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(b"OCHJNL01");
    bytes.extend_from_slice(&2_u16.to_be_bytes());
    bytes.extend_from_slice(&28_u16.to_be_bytes());
    bytes.extend_from_slice(&store);
    for frame in frames {
        bytes.extend_from_slice(frame);
    }
    bytes
}

pub fn generation_catalog_v1(
    store: [u8; 16],
    generation: u64,
    entries: &[CatalogEntry],
) -> Vec<u8> {
    let payload_len = entries.len() * 64;
    let mut bytes = vec![0_u8; 64 + payload_len + 4];
    bytes[..8].copy_from_slice(b"OCHCAT01");
    bytes[8..10].copy_from_slice(&1_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&64_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store);
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..40].copy_from_slice(
        &u32::try_from(entries.len())
            .expect("bounded oracle catalog count")
            .to_be_bytes(),
    );
    bytes[40..48].copy_from_slice(
        &u64::try_from(payload_len)
            .expect("bounded oracle catalog payload")
            .to_be_bytes(),
    );
    for (index, entry) in entries.iter().enumerate() {
        let offset = 64 + index * 64;
        bytes[offset..offset + 8].copy_from_slice(&entry.journal_generation.to_be_bytes());
        bytes[offset + 8..offset + 16].copy_from_slice(&entry.sequence_floor.to_be_bytes());
        bytes[offset + 16..offset + 24].copy_from_slice(&entry.sequence_cutoff.to_be_bytes());
        bytes[offset + 24..offset + 32].copy_from_slice(&entry.end_offset.to_be_bytes());
        bytes[offset + 32..offset + 40].copy_from_slice(&entry.registry_generation.to_be_bytes());
        bytes[offset + 40..offset + 48].copy_from_slice(&entry.artifact_length.to_be_bytes());
        bytes[offset + 48..offset + 52].copy_from_slice(&entry.artifact_checksum.to_be_bytes());
        bytes[offset + 52..offset + 54].copy_from_slice(&1_u16.to_be_bytes());
    }
    let checksum_offset = bytes.len() - 4;
    let checksum = crc32c(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

#[allow(clippy::too_many_arguments)]
pub fn manifest_v3(
    store: [u8; 16],
    generation: u64,
    journal_generation: u64,
    checkpoint_generation: u64,
    append_sequence: u64,
    end_offset: u64,
    registry_slot: u8,
    registry_generation: u64,
    registry_bytes: &[u8],
    retry_slot: u8,
    retry_generation: u64,
    retry_bytes: &[u8],
    sequence_floor: u64,
    catalog: CatalogReference,
) -> [u8; MANIFEST_V3_LEN] {
    let mut bytes = [0_u8; MANIFEST_V3_LEN];
    bytes[..8].copy_from_slice(b"OCHMAN01");
    bytes[8..10].copy_from_slice(&3_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&160_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store);
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..44].copy_from_slice(&journal_generation.to_be_bytes());
    bytes[44..52].copy_from_slice(&checkpoint_generation.to_be_bytes());
    bytes[52..60].copy_from_slice(&append_sequence.to_be_bytes());
    bytes[60..68].copy_from_slice(&end_offset.to_be_bytes());
    bytes[68] = registry_slot;
    bytes[72..80].copy_from_slice(&registry_generation.to_be_bytes());
    bytes[80..88].copy_from_slice(&(registry_bytes.len() as u64).to_be_bytes());
    bytes[88..92].copy_from_slice(&crc32c(registry_bytes).to_be_bytes());
    bytes[92] = retry_slot;
    bytes[96..104].copy_from_slice(&retry_generation.to_be_bytes());
    bytes[104..112].copy_from_slice(&(retry_bytes.len() as u64).to_be_bytes());
    bytes[112..116].copy_from_slice(&crc32c(retry_bytes).to_be_bytes());
    bytes[124..132].copy_from_slice(&sequence_floor.to_be_bytes());
    bytes[132] = catalog.slot;
    bytes[136..144].copy_from_slice(&catalog.generation.to_be_bytes());
    bytes[144..152].copy_from_slice(&catalog.length.to_be_bytes());
    bytes[152..156].copy_from_slice(&catalog.checksum.to_be_bytes());
    let checksum = crc32c(&bytes[..156]);
    bytes[156..160].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

pub fn retry_v2(
    store: [u8; 16],
    generation: u64,
    replay_capacity: u32,
    guard_capacity: u32,
    outcomes: &[RetryV2Outcome<'_>],
) -> Vec<u8> {
    fn string(bytes: &mut Vec<u8>, value: &str) {
        let length = u32::try_from(value.len()).expect("oracle string length fits u32");
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    let mut payload = Vec::new();
    for outcome in outcomes {
        payload.extend_from_slice(&outcome.series);
        payload.extend_from_slice(&outcome.producer);
        string(&mut payload, outcome.key);
        string(&mut payload, outcome.content_format);
        payload.extend_from_slice(&outcome.content_version.to_be_bytes());
        payload.extend_from_slice(&outcome.digest);
        payload.extend_from_slice(&outcome.append_sequence.to_be_bytes());
        payload.extend_from_slice(&outcome.end_offset.to_be_bytes());
        payload.extend_from_slice(&outcome.manifest_generation.to_be_bytes());
        payload.push(outcome.registry_slot);
        payload.extend_from_slice(&[0; 7]);
        payload.extend_from_slice(&outcome.registry_generation.to_be_bytes());
        payload.extend_from_slice(&outcome.journal_generation.to_be_bytes());
        payload.extend_from_slice(&outcome.checkpoint_generation.to_be_bytes());
        payload.extend_from_slice(&outcome.cutoff_sequence.to_be_bytes());
        payload.extend_from_slice(&outcome.cutoff_end.to_be_bytes());
        payload.push(outcome.retry_slot);
        payload.extend_from_slice(&[0; 7]);
        payload.extend_from_slice(&outcome.retry_generation.to_be_bytes());
        payload.extend_from_slice(&outcome.sequence_floor.to_be_bytes());
        match outcome.catalog {
            Some(catalog) => {
                payload.push(1);
                payload.push(catalog.slot);
                payload.extend_from_slice(&[0; 6]);
                payload.extend_from_slice(&catalog.generation.to_be_bytes());
                payload.extend_from_slice(&catalog.length.to_be_bytes());
                payload.extend_from_slice(&catalog.checksum.to_be_bytes());
                payload.extend_from_slice(&[0; 12]);
            }
            None => payload.extend_from_slice(&[0; 40]),
        }
    }
    let mut bytes = vec![0_u8; 64 + payload.len() + 4];
    bytes[..8].copy_from_slice(b"OCHRET01");
    bytes[8..10].copy_from_slice(&2_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&64_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store);
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..40].copy_from_slice(&replay_capacity.to_be_bytes());
    bytes[40..44].copy_from_slice(&guard_capacity.to_be_bytes());
    let outcome_count = u32::try_from(outcomes.len()).expect("oracle outcome count fits u32");
    bytes[44..48].copy_from_slice(&outcome_count.to_be_bytes());
    bytes[52..60].copy_from_slice(&(payload.len() as u64).to_be_bytes());
    bytes[64..64 + payload.len()].copy_from_slice(&payload);
    let checksum_offset = bytes.len() - 4;
    let checksum = crc32c(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

pub fn empty_registry(
    store: [u8; 16],
    generation: u64,
    max_series: u32,
    max_revisions: u32,
) -> Vec<u8> {
    let mut bytes = vec![0_u8; 68];
    bytes[..8].copy_from_slice(b"OCHREG01");
    bytes[8..10].copy_from_slice(&1_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&64_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store);
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..40].copy_from_slice(&max_series.to_be_bytes());
    bytes[40..44].copy_from_slice(&max_revisions.to_be_bytes());
    let checksum = crc32c(&bytes[..64]);
    bytes[64..68].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

pub fn empty_retry(
    store: [u8; 16],
    generation: u64,
    replay_capacity: u32,
    guard_capacity: u32,
) -> Vec<u8> {
    let mut bytes = vec![0_u8; 68];
    bytes[..8].copy_from_slice(b"OCHRET01");
    bytes[8..10].copy_from_slice(&1_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&64_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store);
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..40].copy_from_slice(&replay_capacity.to_be_bytes());
    bytes[40..44].copy_from_slice(&guard_capacity.to_be_bytes());
    let checksum = crc32c(&bytes[..64]);
    bytes[64..68].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

#[allow(clippy::too_many_arguments)]
pub fn retry_with_one_replay(
    store: [u8; 16],
    generation: u64,
    replay_capacity: u32,
    guard_capacity: u32,
    series: [u8; 16],
    producer: [u8; 16],
    key: &str,
    content_format: &str,
    content_version: u128,
    digest: [u8; 32],
    append_sequence: u64,
    end_offset: u64,
    manifest_generation: u64,
    registry_slot: u8,
    registry_generation: u64,
    checkpoint_generation: u64,
    cutoff_sequence: u64,
    cutoff_end: u64,
    retry_slot: u8,
    retry_generation: u64,
) -> Vec<u8> {
    fn string(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(
            &u32::try_from(value.len())
                .expect("bounded oracle string")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(value.as_bytes());
    }

    let mut payload = Vec::new();
    payload.extend_from_slice(&series);
    payload.extend_from_slice(&producer);
    string(&mut payload, key);
    string(&mut payload, content_format);
    payload.extend_from_slice(&content_version.to_be_bytes());
    payload.extend_from_slice(&digest);
    payload.extend_from_slice(&append_sequence.to_be_bytes());
    payload.extend_from_slice(&end_offset.to_be_bytes());
    payload.extend_from_slice(&manifest_generation.to_be_bytes());
    payload.push(registry_slot);
    payload.extend_from_slice(&[0; 7]);
    payload.extend_from_slice(&registry_generation.to_be_bytes());
    payload.extend_from_slice(&1_u64.to_be_bytes());
    payload.extend_from_slice(&checkpoint_generation.to_be_bytes());
    payload.extend_from_slice(&cutoff_sequence.to_be_bytes());
    payload.extend_from_slice(&cutoff_end.to_be_bytes());
    payload.push(retry_slot);
    payload.extend_from_slice(&[0; 7]);
    payload.extend_from_slice(&retry_generation.to_be_bytes());

    let mut bytes = vec![0_u8; 64 + payload.len() + 4];
    bytes[..8].copy_from_slice(b"OCHRET01");
    bytes[8..10].copy_from_slice(&1_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&64_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store);
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..40].copy_from_slice(&replay_capacity.to_be_bytes());
    bytes[40..44].copy_from_slice(&guard_capacity.to_be_bytes());
    bytes[44..48].copy_from_slice(&1_u32.to_be_bytes());
    bytes[52..60].copy_from_slice(
        &u64::try_from(payload.len())
            .expect("bounded oracle payload")
            .to_be_bytes(),
    );
    bytes[64..64 + payload.len()].copy_from_slice(&payload);
    let checksum_offset = bytes.len() - 4;
    let checksum = crc32c(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

#[allow(clippy::too_many_arguments)]
pub fn manifest(
    store: [u8; 16],
    generation: u64,
    checkpoint_generation: u64,
    append_sequence: u64,
    end_offset: u64,
    registry_slot: u8,
    registry_generation: u64,
    registry_bytes: &[u8],
    retry_slot: u8,
    retry_generation: u64,
    retry_bytes: &[u8],
) -> [u8; MANIFEST_LEN] {
    let mut bytes = [0_u8; MANIFEST_LEN];
    bytes[..8].copy_from_slice(b"OCHMAN01");
    bytes[8..10].copy_from_slice(&2_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&128_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store);
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..44].copy_from_slice(&1_u64.to_be_bytes());
    bytes[44..52].copy_from_slice(&checkpoint_generation.to_be_bytes());
    bytes[52..60].copy_from_slice(&append_sequence.to_be_bytes());
    bytes[60..68].copy_from_slice(&end_offset.to_be_bytes());
    bytes[68] = registry_slot;
    bytes[72..80].copy_from_slice(&registry_generation.to_be_bytes());
    bytes[80..88].copy_from_slice(
        &u64::try_from(registry_bytes.len())
            .expect("bounded oracle registry length")
            .to_be_bytes(),
    );
    bytes[88..92].copy_from_slice(&crc32c(registry_bytes).to_be_bytes());
    bytes[92] = retry_slot;
    bytes[96..104].copy_from_slice(&retry_generation.to_be_bytes());
    bytes[104..112].copy_from_slice(
        &u64::try_from(retry_bytes.len())
            .expect("bounded oracle retry length")
            .to_be_bytes(),
    );
    bytes[112..116].copy_from_slice(&crc32c(retry_bytes).to_be_bytes());
    let checksum = crc32c(&bytes[..124]);
    bytes[124..128].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

#[allow(clippy::too_many_arguments)]
pub fn manifest_v1(
    store: [u8; 16],
    generation: u64,
    checkpoint_generation: u64,
    append_sequence: u64,
    end_offset: u64,
    registry_slot: u8,
    registry_generation: u64,
    registry_bytes: &[u8],
) -> [u8; MANIFEST_LEN] {
    let mut bytes = [0_u8; MANIFEST_LEN];
    bytes[..8].copy_from_slice(b"OCHMAN01");
    bytes[8..10].copy_from_slice(&1_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&128_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(&store);
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..44].copy_from_slice(&1_u64.to_be_bytes());
    bytes[44..52].copy_from_slice(&checkpoint_generation.to_be_bytes());
    bytes[52..60].copy_from_slice(&append_sequence.to_be_bytes());
    bytes[60..68].copy_from_slice(&end_offset.to_be_bytes());
    bytes[68] = registry_slot;
    bytes[72..80].copy_from_slice(&registry_generation.to_be_bytes());
    bytes[80..88].copy_from_slice(
        &u64::try_from(registry_bytes.len())
            .expect("bounded oracle registry length")
            .to_be_bytes(),
    );
    bytes[88..92].copy_from_slice(&crc32c(registry_bytes).to_be_bytes());
    let checksum = crc32c(&bytes[..124]);
    bytes[124..128].copy_from_slice(&checksum.to_be_bytes());
    bytes
}
