//! Primitive-only Manifest V1 and empty registry snapshot byte oracle.

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
