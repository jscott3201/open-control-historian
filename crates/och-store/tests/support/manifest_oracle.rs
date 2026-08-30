//! Primitive-only Manifest V1/V2, registry, and Retry State V1 byte oracle.

const MANIFEST_LEN: usize = 128;

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
