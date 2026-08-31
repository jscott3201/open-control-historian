//! Primitive-only independent Native Segment V1 byte oracle.
//!
//! This module deliberately imports no product crate and calls no product
//! segment, Journal, identity, or checksum implementation.

use std::collections::BTreeMap;

const HEADER_LEN: usize = 192;
const SERIES_ENTRY_LEN: usize = 64;
const APPEND_ENTRY_LEN: usize = 48;
const OBSERVATION_ENTRY_LEN: usize = 96;

/// Primitive raw-order evidence for one indexed observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Observation {
    /// Signed floor Unix second of effective time.
    pub effective_seconds: i64,
    /// Effective-time nanosecond fraction.
    pub effective_nanos: u32,
    /// Signed floor Unix second of receive time.
    pub receive_seconds: i64,
    /// Receive-time nanosecond fraction.
    pub receive_nanos: u32,
    /// Exact observation UUID bytes.
    pub id: [u8; 16],
    /// Original ordinal inside the frame envelope.
    pub ordinal: u32,
}

/// Primitive source-frame evidence consumed by the independent oracle.
pub struct Frame<'a> {
    /// Store-global append sequence.
    pub sequence: u64,
    /// Exact series UUID bytes.
    pub series_id: [u8; 16],
    /// Complete original Journal V1 frame bytes.
    pub bytes: &'a [u8],
    /// Raw-order evidence for observations retained by the frame.
    pub observations: &'a [Observation],
}

/// Primitive source identity consumed by the independent oracle.
pub struct Source<'a> {
    /// Exact `StoreId` UUID bytes.
    pub store_id: [u8; 16],
    /// Positive raw journal generation.
    pub journal_generation: u64,
    /// Exclusive append-sequence floor.
    pub sequence_floor: u64,
    /// Inclusive append-sequence cutoff.
    pub sequence_cutoff: u64,
    /// Positive source registry generation.
    pub registry_generation: u64,
    /// Complete raw Journal V1 bytes.
    pub raw_journal: &'a [u8],
    /// Complete source frames in append order.
    pub frames: &'a [Frame<'a>],
}

#[derive(Clone, Copy)]
struct FrameLocation<'a> {
    sequence: u64,
    series_id: [u8; 16],
    bytes: &'a [u8],
    observations: &'a [Observation],
    offset: u64,
    ordinal: u32,
}

#[derive(Clone, Copy)]
struct RecentLocation {
    series_id: [u8; 16],
    observation: Observation,
    sequence: u64,
    frame_ordinal: u32,
    frame_offset: u64,
    frame_length: u64,
}

/// Returns CRC-32C using the published Segment/Journal Castagnoli parameters.
pub fn checksum(bytes: &[u8]) -> u32 {
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

/// Rewrites only the complete-artifact Segment V1 checksum trailer.
pub fn repair_checksum(bytes: &mut [u8]) {
    let offset = bytes.len() - 4;
    let value = checksum(&bytes[..offset]);
    bytes[offset..].copy_from_slice(&value.to_be_bytes());
}

/// Builds the exact independently specified Native Segment V1 bytes.
#[allow(clippy::too_many_lines)]
pub fn build(source: &Source<'_>) -> Vec<u8> {
    let mut grouped = BTreeMap::<[u8; 16], Vec<&Frame<'_>>>::new();
    for frame in source.frames {
        grouped.entry(frame.series_id).or_default().push(frame);
    }
    let observation_count = source
        .frames
        .iter()
        .map(|frame| frame.observations.len())
        .sum::<usize>();
    let series_offset = HEADER_LEN;
    let series_length = grouped.len() * SERIES_ENTRY_LEN;
    let blocks_offset = series_offset + series_length;
    let blocks_length = source
        .frames
        .iter()
        .map(|frame| frame.bytes.len())
        .sum::<usize>();
    let append_offset = blocks_offset + blocks_length;
    let append_length = source.frames.len() * APPEND_ENTRY_LEN;
    let recent_offset = append_offset + append_length;
    let recent_length = observation_count * OBSERVATION_ENTRY_LEN;
    let artifact_length = recent_offset + recent_length + 4;

    let mut bytes = vec![0_u8; artifact_length];
    bytes[..8].copy_from_slice(b"OCHSEG01");
    bytes[8..10].copy_from_slice(&1_u16.to_be_bytes());
    bytes[10..12].copy_from_slice(&192_u16.to_be_bytes());
    bytes[16..32].copy_from_slice(&source.store_id);
    bytes[32..40].copy_from_slice(&source.journal_generation.to_be_bytes());
    bytes[40..48].copy_from_slice(&source.sequence_floor.to_be_bytes());
    bytes[48..56].copy_from_slice(&source.sequence_cutoff.to_be_bytes());
    bytes[56..64].copy_from_slice(&source.registry_generation.to_be_bytes());
    bytes[64..72].copy_from_slice(&(source.raw_journal.len() as u64).to_be_bytes());
    bytes[72..80].copy_from_slice(&(source.raw_journal.len() as u64).to_be_bytes());
    bytes[80..84].copy_from_slice(&checksum(source.raw_journal).to_be_bytes());
    bytes[84..88].copy_from_slice(&bounded_u32(source.frames.len()).to_be_bytes());
    bytes[88..92].copy_from_slice(&bounded_u32(grouped.len()).to_be_bytes());
    bytes[92..96].copy_from_slice(&bounded_u32(observation_count).to_be_bytes());
    put_u64(&mut bytes, 96, series_offset as u64);
    put_u64(&mut bytes, 104, series_length as u64);
    put_u64(&mut bytes, 112, blocks_offset as u64);
    put_u64(&mut bytes, 120, blocks_length as u64);
    put_u64(&mut bytes, 128, append_offset as u64);
    put_u64(&mut bytes, 136, append_length as u64);
    put_u64(&mut bytes, 144, recent_offset as u64);
    put_u64(&mut bytes, 152, recent_length as u64);
    put_u64(&mut bytes, 160, artifact_length as u64);

    let mut block_cursor = blocks_offset;
    let mut recent_cursor = recent_offset;
    let mut append_entries = Vec::with_capacity(source.frames.len());
    for (series_index, (series_id, frames)) in grouped.iter().enumerate() {
        let directory_offset = series_offset + series_index * SERIES_ENTRY_LEN;
        let block_start = block_cursor;
        let series_observations = frames
            .iter()
            .map(|frame| frame.observations.len())
            .sum::<usize>();
        let recent_start = recent_cursor;
        let mut recent = Vec::with_capacity(series_observations);
        for (frame_ordinal, frame) in frames.iter().enumerate() {
            let frame_ordinal = bounded_u32(frame_ordinal);
            let frame_offset = block_cursor;
            bytes[frame_offset..frame_offset + frame.bytes.len()].copy_from_slice(frame.bytes);
            block_cursor += frame.bytes.len();
            let location = FrameLocation {
                sequence: frame.sequence,
                series_id: *series_id,
                bytes: frame.bytes,
                observations: frame.observations,
                offset: frame_offset as u64,
                ordinal: frame_ordinal,
            };
            append_entries.push(location);
            for observation in frame.observations {
                recent.push(RecentLocation {
                    series_id: *series_id,
                    observation: *observation,
                    sequence: frame.sequence,
                    frame_ordinal,
                    frame_offset: frame_offset as u64,
                    frame_length: frame.bytes.len() as u64,
                });
            }
        }
        recent.sort_by(|left, right| {
            raw_key(right)
                .cmp(&raw_key(left))
                .then_with(|| right.sequence.cmp(&left.sequence))
                .then_with(|| left.observation.ordinal.cmp(&right.observation.ordinal))
        });
        for location in recent {
            encode_recent(
                &mut bytes[recent_cursor..recent_cursor + OBSERVATION_ENTRY_LEN],
                location,
            );
            recent_cursor += OBSERVATION_ENTRY_LEN;
        }

        bytes[directory_offset..directory_offset + 16].copy_from_slice(series_id);
        put_u64(&mut bytes, directory_offset + 16, block_start as u64);
        put_u64(
            &mut bytes,
            directory_offset + 24,
            (block_cursor - block_start) as u64,
        );
        put_u32(&mut bytes, directory_offset + 32, bounded_u32(frames.len()));
        put_u32(
            &mut bytes,
            directory_offset + 36,
            bounded_u32(series_observations),
        );
        put_u64(&mut bytes, directory_offset + 40, recent_start as u64);
        put_u64(
            &mut bytes,
            directory_offset + 48,
            (recent_cursor - recent_start) as u64,
        );
    }

    append_entries.sort_by_key(|entry| entry.sequence);
    for (index, entry) in append_entries.iter().enumerate() {
        let offset = append_offset + index * APPEND_ENTRY_LEN;
        put_u64(&mut bytes, offset, entry.sequence);
        bytes[offset + 8..offset + 24].copy_from_slice(&entry.series_id);
        put_u64(&mut bytes, offset + 24, entry.offset);
        put_u64(&mut bytes, offset + 32, entry.bytes.len() as u64);
        put_u32(&mut bytes, offset + 40, entry.ordinal);
        let _ = entry.observations;
    }
    repair_checksum(&mut bytes);
    bytes
}

fn raw_key(location: &RecentLocation) -> (i64, u32, i64, u32, [u8; 16]) {
    let observation = location.observation;
    (
        observation.effective_seconds,
        observation.effective_nanos,
        observation.receive_seconds,
        observation.receive_nanos,
        observation.id,
    )
}

fn encode_recent(bytes: &mut [u8], location: RecentLocation) {
    let observation = location.observation;
    bytes[..16].copy_from_slice(&location.series_id);
    bytes[16..24].copy_from_slice(&observation.effective_seconds.to_be_bytes());
    bytes[24..28].copy_from_slice(&observation.effective_nanos.to_be_bytes());
    bytes[28..36].copy_from_slice(&observation.receive_seconds.to_be_bytes());
    bytes[36..40].copy_from_slice(&observation.receive_nanos.to_be_bytes());
    bytes[40..56].copy_from_slice(&observation.id);
    bytes[56..64].copy_from_slice(&location.sequence.to_be_bytes());
    bytes[64..68].copy_from_slice(&observation.ordinal.to_be_bytes());
    bytes[68..72].copy_from_slice(&location.frame_ordinal.to_be_bytes());
    bytes[72..80].copy_from_slice(&location.frame_offset.to_be_bytes());
    bytes[80..88].copy_from_slice(&location.frame_length.to_be_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn bounded_u32(value: usize) -> u32 {
    u32::try_from(value).expect("primitive oracle fixture count fits u32")
}
