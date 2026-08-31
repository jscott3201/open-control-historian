//! Bounded, current-only, non-authorizing Native Segment V1 candidates.

use crate::codec::{crc32c, frame_len_from_prefix_v1};
use crate::generation::StreamingCrc32c;
use crate::{
    AppendSequenceV1, DecodeLimitsV1, DecodedAdmissionV1, JOURNAL_V1_FRAME_PREFIX_LEN,
    JOURNAL_V1_HEADER_LEN, JournalHeaderV1, MAX_ACTIVE_JOURNAL_BYTES, MAX_ACTIVE_JOURNAL_RECORDS,
    ManifestIoEvidence, SealedGeneration, decode_admission_frame_v1,
    encode_decoded_admission_frame_v1,
};
use och_core::{
    MAX_SOURCE_OBSERVATION_CONTEXTS, ObservationId, RawObservationOrderKey, SeriesId, StoreId,
    Timestamp,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

/// Exact eight-byte Native Segment V1 magic.
pub const SEGMENT_V1_MAGIC: [u8; 8] = *b"OCHSEG01";
/// Current and sole Native Segment format version.
pub const SEGMENT_V1_VERSION: u16 = 1;
/// Exact Native Segment V1 header length.
pub const SEGMENT_V1_HEADER_LEN: usize = 192;
/// Exact fixed series-directory entry length.
pub const SEGMENT_V1_SERIES_ENTRY_LEN: usize = 64;
/// Exact fixed global append-directory entry length.
pub const SEGMENT_V1_APPEND_ENTRY_LEN: usize = 48;
/// Exact fixed recent-observation-directory entry length.
pub const SEGMENT_V1_OBSERVATION_ENTRY_LEN: usize = 96;
/// Exact Native Segment V1 complete-artifact checksum trailer length.
pub const SEGMENT_V1_CRC_LEN: usize = 4;
/// Maximum distinct series representable by one Segment V1 candidate.
pub const MAX_SEGMENT_V1_SERIES: usize = MAX_ACTIVE_JOURNAL_RECORDS;
/// Maximum indexed observations representable by one Segment V1 candidate.
pub const MAX_SEGMENT_V1_OBSERVATIONS: usize =
    MAX_ACTIVE_JOURNAL_RECORDS * MAX_SOURCE_OBSERVATION_CONTEXTS;
/// Maximum complete Native Segment V1 candidate length.
pub const MAX_SEGMENT_V1_BYTES: u64 = SEGMENT_V1_HEADER_LEN as u64
    + SEGMENT_V1_SERIES_ENTRY_LEN as u64 * MAX_SEGMENT_V1_SERIES as u64
    + (MAX_ACTIVE_JOURNAL_BYTES - JOURNAL_V1_HEADER_LEN as u64)
    + SEGMENT_V1_APPEND_ENTRY_LEN as u64 * MAX_ACTIVE_JOURNAL_RECORDS as u64
    + SEGMENT_V1_OBSERVATION_ENTRY_LEN as u64 * MAX_SEGMENT_V1_OBSERVATIONS as u64
    + SEGMENT_V1_CRC_LEN as u64;

const SEGMENT_FLAGS_NONE: u32 = 0;

/// Closed, path-free Native Segment V1 refusal classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentV1Error {
    /// The sealed raw Journal V1 source does not match its supplied evidence.
    InvalidSource,
    /// Segment bytes are malformed, noncanonical, corrupt, or inconsistent.
    InvalidSegment,
    /// Supplied or embedded bytes belong to a different store.
    StoreMismatch,
    /// A fixed source or segment hard bound would be exceeded.
    Bounds,
    /// Read-only candidate construction observed sanitized store I/O failure.
    Io(ManifestIoEvidence),
}

impl fmt::Display for SegmentV1Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidSource => "invalid sealed Journal V1 source",
            Self::InvalidSegment => "invalid Native Segment V1 bytes",
            Self::StoreMismatch => "Native Segment V1 store identity mismatch",
            Self::Bounds => "Native Segment V1 hard bound exceeded",
            Self::Io(_) => "Native Segment V1 source read failed",
        })
    }
}

impl Error for SegmentV1Error {}

/// Complete bounded metadata carried by one Native Segment V1 header and trailer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentV1Inspection {
    store_id: StoreId,
    source_journal_generation: u64,
    sequence_floor: u64,
    sequence_cutoff: u64,
    source_registry_generation: u64,
    source_end_offset: u64,
    source_artifact_length: u64,
    source_artifact_checksum: u32,
    frame_count: usize,
    series_count: usize,
    observation_count: usize,
    series_directory_offset: u64,
    series_directory_length: u64,
    block_region_offset: u64,
    block_region_length: u64,
    append_directory_offset: u64,
    append_directory_length: u64,
    recent_directory_offset: u64,
    recent_directory_length: u64,
    artifact_length: u64,
    artifact_checksum: u32,
}

impl SegmentV1Inspection {
    /// Returns the exact store scope.
    #[must_use]
    pub const fn store_id(self) -> StoreId {
        self.store_id
    }

    /// Returns the positive source raw-journal generation.
    #[must_use]
    pub const fn source_journal_generation(self) -> u64 {
        self.source_journal_generation
    }

    /// Returns the exclusive source append-sequence floor.
    #[must_use]
    pub const fn sequence_floor(self) -> u64 {
        self.sequence_floor
    }

    /// Returns the inclusive source append-sequence cutoff.
    #[must_use]
    pub const fn sequence_cutoff(self) -> u64 {
        self.sequence_cutoff
    }

    /// Returns the source registry generation retained by the sealed catalog entry.
    #[must_use]
    pub const fn source_registry_generation(self) -> u64 {
        self.source_registry_generation
    }

    /// Returns the exact source durable end offset.
    #[must_use]
    pub const fn source_end_offset(self) -> u64 {
        self.source_end_offset
    }

    /// Returns the exact complete source raw-journal length.
    #[must_use]
    pub const fn source_artifact_length(self) -> u64 {
        self.source_artifact_length
    }

    /// Returns CRC-32C over the complete source raw journal.
    #[must_use]
    pub const fn source_artifact_checksum(self) -> u32 {
        self.source_artifact_checksum
    }

    /// Returns the exact number of complete source frames.
    #[must_use]
    pub const fn frame_count(self) -> usize {
        self.frame_count
    }

    /// Returns the exact number of one-series blocks.
    #[must_use]
    pub const fn series_count(self) -> usize {
        self.series_count
    }

    /// Returns the exact number of recent-observation index entries.
    #[must_use]
    pub const fn observation_count(self) -> usize {
        self.observation_count
    }

    /// Returns the absolute series-directory offset.
    #[must_use]
    pub const fn series_directory_offset(self) -> u64 {
        self.series_directory_offset
    }

    /// Returns the exact series-directory byte length.
    #[must_use]
    pub const fn series_directory_length(self) -> u64 {
        self.series_directory_length
    }

    /// Returns the absolute one-series block-region offset.
    #[must_use]
    pub const fn block_region_offset(self) -> u64 {
        self.block_region_offset
    }

    /// Returns the exact one-series block-region byte length.
    #[must_use]
    pub const fn block_region_length(self) -> u64 {
        self.block_region_length
    }

    /// Returns the absolute global append-directory offset.
    #[must_use]
    pub const fn append_directory_offset(self) -> u64 {
        self.append_directory_offset
    }

    /// Returns the exact global append-directory byte length.
    #[must_use]
    pub const fn append_directory_length(self) -> u64 {
        self.append_directory_length
    }

    /// Returns the absolute recent-observation-directory offset.
    #[must_use]
    pub const fn recent_directory_offset(self) -> u64 {
        self.recent_directory_offset
    }

    /// Returns the exact recent-observation-directory byte length.
    #[must_use]
    pub const fn recent_directory_length(self) -> u64 {
        self.recent_directory_length
    }

    /// Returns the exact complete segment artifact length.
    #[must_use]
    pub const fn artifact_length(self) -> u64 {
        self.artifact_length
    }

    /// Returns the CRC-32C trailer over all preceding segment bytes.
    #[must_use]
    pub const fn artifact_checksum(self) -> u32 {
        self.artifact_checksum
    }
}

/// Immutable in-memory Native Segment V1 candidate produced from a sealed source.
pub struct PreparedSegmentV1 {
    bytes: Box<[u8]>,
    inspection: SegmentV1Inspection,
}

impl PreparedSegmentV1 {
    /// Borrows the exact complete candidate bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the bounded candidate metadata.
    #[must_use]
    pub const fn inspection(&self) -> SegmentV1Inspection {
        self.inspection
    }
}

impl fmt::Debug for PreparedSegmentV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedSegmentV1")
            .field("inspection", &self.inspection)
            .finish_non_exhaustive()
    }
}

/// One canonical fixed-size series-directory entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentSeriesEntryV1 {
    series_id: SeriesId,
    block_offset: u64,
    block_length: u64,
    frame_count: u32,
    observation_count: u32,
    recent_offset: u64,
    recent_length: u64,
}

impl SegmentSeriesEntryV1 {
    /// Returns the exact series identity.
    #[must_use]
    pub const fn series_id(self) -> SeriesId {
        self.series_id
    }

    /// Returns the absolute offset of this series' sole frame block.
    #[must_use]
    pub const fn block_offset(self) -> u64 {
        self.block_offset
    }

    /// Returns the exact byte length of this series' frame block.
    #[must_use]
    pub const fn block_length(self) -> u64 {
        self.block_length
    }

    /// Returns the number of complete frames in this series block.
    #[must_use]
    pub const fn frame_count(self) -> u32 {
        self.frame_count
    }

    /// Returns the number of recent-observation entries for this series.
    #[must_use]
    pub const fn observation_count(self) -> u32 {
        self.observation_count
    }

    /// Returns the absolute offset of this series' recent-observation slice.
    #[must_use]
    pub const fn recent_offset(self) -> u64 {
        self.recent_offset
    }

    /// Returns the exact byte length of this series' recent-observation slice.
    #[must_use]
    pub const fn recent_length(self) -> u64 {
        self.recent_length
    }
}

/// One canonical fixed-size global append-directory entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentAppendEntryV1 {
    append_sequence: u64,
    series_id: SeriesId,
    frame_offset: u64,
    frame_length: u64,
    frame_ordinal: u32,
}

impl SegmentAppendEntryV1 {
    /// Returns the exact store-global append sequence.
    #[must_use]
    pub const fn append_sequence(self) -> u64 {
        self.append_sequence
    }

    /// Returns the exact owning series identity.
    #[must_use]
    pub const fn series_id(self) -> SeriesId {
        self.series_id
    }

    /// Returns the absolute complete-frame offset in the series block region.
    #[must_use]
    pub const fn frame_offset(self) -> u64 {
        self.frame_offset
    }

    /// Returns the exact complete original Journal V1 frame length.
    #[must_use]
    pub const fn frame_length(self) -> u64 {
        self.frame_length
    }

    /// Returns the zero-based frame ordinal inside the owning series block.
    #[must_use]
    pub const fn frame_ordinal(self) -> u32 {
        self.frame_ordinal
    }
}

/// One canonical fixed-size recent-observation-directory entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentObservationEntryV1 {
    series_id: SeriesId,
    raw_order_key: RawObservationOrderKey,
    append_sequence: u64,
    observation_ordinal: u32,
    frame_ordinal: u32,
    frame_offset: u64,
    frame_length: u64,
}

impl SegmentObservationEntryV1 {
    /// Returns the exact owning series identity.
    #[must_use]
    pub const fn series_id(self) -> SeriesId {
        self.series_id
    }

    /// Returns the canonical `(effective, receive, ObservationId)` raw-order key.
    #[must_use]
    pub const fn raw_order_key(self) -> RawObservationOrderKey {
        self.raw_order_key
    }

    /// Returns the indexed canonical observation identity.
    #[must_use]
    pub const fn observation_id(self) -> ObservationId {
        self.raw_order_key.observation_id()
    }

    /// Returns the stable source frame append sequence.
    #[must_use]
    pub const fn append_sequence(self) -> u64 {
        self.append_sequence
    }

    /// Returns the zero-based observation ordinal inside the frame envelope.
    #[must_use]
    pub const fn observation_ordinal(self) -> u32 {
        self.observation_ordinal
    }

    /// Returns the zero-based frame ordinal inside the owning series block.
    #[must_use]
    pub const fn frame_ordinal(self) -> u32 {
        self.frame_ordinal
    }

    /// Returns the absolute complete-frame offset.
    #[must_use]
    pub const fn frame_offset(self) -> u64 {
        self.frame_offset
    }

    /// Returns the exact complete original frame length.
    #[must_use]
    pub const fn frame_length(self) -> u64 {
        self.frame_length
    }
}

/// Hostile-input-validated, borrowing Native Segment V1 inspection view.
pub struct SegmentV1<'a> {
    bytes: &'a [u8],
    inspection: SegmentV1Inspection,
    series: Box<[SegmentSeriesEntryV1]>,
    appends: Box<[SegmentAppendEntryV1]>,
    observations: Box<[SegmentObservationEntryV1]>,
}

impl SegmentV1<'_> {
    /// Returns the complete validated metadata.
    #[must_use]
    pub const fn inspection(&self) -> SegmentV1Inspection {
        self.inspection
    }

    /// Returns the exact SeriesId-ascending series directory.
    #[must_use]
    pub fn series_directory(&self) -> &[SegmentSeriesEntryV1] {
        &self.series
    }

    /// Returns the strict append-sequence-ascending global directory.
    #[must_use]
    pub fn append_directory(&self) -> &[SegmentAppendEntryV1] {
        &self.appends
    }

    /// Returns series-grouped raw-order-descending recent-observation entries.
    #[must_use]
    pub fn recent_observations(&self) -> &[SegmentObservationEntryV1] {
        &self.observations
    }

    /// Borrows the exact complete original Journal V1 frame for an indexed entry.
    ///
    /// # Errors
    ///
    /// Refuses an entry that is not present in this validated append directory.
    pub fn frame_bytes(&self, entry: &SegmentAppendEntryV1) -> Result<&[u8], SegmentV1Error> {
        if !self.appends.contains(entry) {
            return Err(SegmentV1Error::InvalidSegment);
        }
        checked_slice(self.bytes, entry.frame_offset, entry.frame_length)
            .ok_or(SegmentV1Error::InvalidSegment)
    }

    /// Decodes one exact indexed frame into non-authorizing Journal V1 evidence.
    ///
    /// # Errors
    ///
    /// Refuses a foreign entry or any loss of the parser's previously proven
    /// complete-frame invariant.
    pub fn decode_frame(
        &self,
        entry: &SegmentAppendEntryV1,
    ) -> Result<DecodedAdmissionV1, SegmentV1Error> {
        decode_admission_frame_v1(self.frame_bytes(entry)?, DecodeLimitsV1::maximum(), None)
            .map_err(|_| SegmentV1Error::InvalidSegment)
    }
}

impl fmt::Debug for SegmentV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentV1")
            .field("inspection", &self.inspection)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct SourceFrame {
    append_sequence: u64,
    source_offset: usize,
    length: usize,
    observations: Vec<SourceObservation>,
}

#[derive(Clone, Copy)]
struct SourceObservation {
    raw_order_key: RawObservationOrderKey,
    ordinal: u32,
}

#[derive(Clone, Copy)]
struct FrameLocation {
    append_sequence: u64,
    series_id: SeriesId,
    frame_offset: u64,
    frame_length: u64,
    frame_ordinal: u32,
}

#[derive(Clone, Copy)]
struct ObservationLocation {
    series_id: SeriesId,
    raw_order_key: RawObservationOrderKey,
    append_sequence: u64,
    observation_ordinal: u32,
    frame_ordinal: u32,
    frame_offset: u64,
    frame_length: u64,
}

#[derive(Clone, Copy)]
struct Layout {
    series_offset: usize,
    series_length: usize,
    blocks_offset: usize,
    blocks_length: usize,
    append_offset: usize,
    append_length: usize,
    recent_offset: usize,
    recent_length: usize,
    artifact_length: usize,
}

impl Layout {
    fn new(
        frame_count: usize,
        series_count: usize,
        observation_count: usize,
        blocks_length: usize,
    ) -> Result<Self, SegmentV1Error> {
        if frame_count == 0
            || frame_count > MAX_ACTIVE_JOURNAL_RECORDS
            || series_count == 0
            || series_count > MAX_SEGMENT_V1_SERIES
            || series_count > frame_count
            || observation_count > MAX_SEGMENT_V1_OBSERVATIONS
            || blocks_length == 0
            || u64::try_from(blocks_length).map_err(|_| SegmentV1Error::Bounds)?
                > MAX_ACTIVE_JOURNAL_BYTES - JOURNAL_V1_HEADER_LEN as u64
        {
            return Err(SegmentV1Error::Bounds);
        }
        let series_length = series_count
            .checked_mul(SEGMENT_V1_SERIES_ENTRY_LEN)
            .ok_or(SegmentV1Error::Bounds)?;
        let blocks_offset = SEGMENT_V1_HEADER_LEN
            .checked_add(series_length)
            .ok_or(SegmentV1Error::Bounds)?;
        let append_offset = blocks_offset
            .checked_add(blocks_length)
            .ok_or(SegmentV1Error::Bounds)?;
        let append_length = frame_count
            .checked_mul(SEGMENT_V1_APPEND_ENTRY_LEN)
            .ok_or(SegmentV1Error::Bounds)?;
        let recent_offset = append_offset
            .checked_add(append_length)
            .ok_or(SegmentV1Error::Bounds)?;
        let recent_length = observation_count
            .checked_mul(SEGMENT_V1_OBSERVATION_ENTRY_LEN)
            .ok_or(SegmentV1Error::Bounds)?;
        let artifact_length = recent_offset
            .checked_add(recent_length)
            .and_then(|length| length.checked_add(SEGMENT_V1_CRC_LEN))
            .ok_or(SegmentV1Error::Bounds)?;
        if u64::try_from(artifact_length).map_err(|_| SegmentV1Error::Bounds)?
            > MAX_SEGMENT_V1_BYTES
        {
            return Err(SegmentV1Error::Bounds);
        }
        Ok(Self {
            series_offset: SEGMENT_V1_HEADER_LEN,
            series_length,
            blocks_offset,
            blocks_length,
            append_offset,
            append_length,
            recent_offset,
            recent_length,
            artifact_length,
        })
    }
}

/// Builds one exact deterministic Native Segment V1 candidate from a complete
/// committed sealed raw Journal V1 generation.
///
/// The returned bytes are offline, in-memory evidence only. They are not store
/// inventory, publication, query, retention, or reclamation authority.
///
/// # Errors
///
/// Refuses source metadata, length, checksum, header, frame boundary, sequence,
/// store/series scope, canonical re-encoding, suffix, or hard-bound mismatch.
#[allow(clippy::too_many_lines)]
pub fn build_segment_v1(
    store_id: StoreId,
    sealed: SealedGeneration,
    source: &[u8],
) -> Result<PreparedSegmentV1, SegmentV1Error> {
    let source_length = u64::try_from(source.len()).map_err(|_| SegmentV1Error::Bounds)?;
    if source_length > MAX_ACTIVE_JOURNAL_BYTES {
        return Err(SegmentV1Error::Bounds);
    }
    if sealed.journal_generation() == 0
        || sealed.registry_generation() == 0
        || sealed.sequence_cutoff() <= sealed.sequence_floor()
        || sealed.end_offset() != sealed.artifact_length()
        || sealed.artifact_length() != source_length
        || sealed.end_offset() <= JOURNAL_V1_HEADER_LEN as u64
        || crc32c(source) != sealed.artifact_checksum()
    {
        return Err(SegmentV1Error::InvalidSource);
    }
    let expected_count = sealed
        .sequence_cutoff()
        .checked_sub(sealed.sequence_floor())
        .and_then(|count| usize::try_from(count).ok())
        .ok_or(SegmentV1Error::Bounds)?;
    if expected_count == 0 || expected_count > MAX_ACTIVE_JOURNAL_RECORDS {
        return Err(SegmentV1Error::Bounds);
    }
    let header = source
        .get(..JOURNAL_V1_HEADER_LEN)
        .ok_or(SegmentV1Error::InvalidSource)?;
    let decoded_header =
        JournalHeaderV1::decode(header).map_err(|_| SegmentV1Error::InvalidSource)?;
    if decoded_header.store_id() != store_id {
        return Err(SegmentV1Error::StoreMismatch);
    }

    let mut source_frames = Vec::with_capacity(expected_count);
    let mut grouped = BTreeMap::<SeriesId, Vec<usize>>::new();
    let mut source_offset = JOURNAL_V1_HEADER_LEN;
    let mut previous = if sealed.sequence_floor() == 0 {
        None
    } else {
        Some(
            AppendSequenceV1::new(sealed.sequence_floor())
                .map_err(|_| SegmentV1Error::InvalidSource)?,
        )
    };
    let mut observation_count = 0_usize;
    while source_offset < source.len() {
        if source_frames.len() >= expected_count {
            return Err(SegmentV1Error::InvalidSource);
        }
        let prefix_end = source_offset
            .checked_add(JOURNAL_V1_FRAME_PREFIX_LEN)
            .ok_or(SegmentV1Error::Bounds)?;
        let prefix = source
            .get(source_offset..prefix_end)
            .ok_or(SegmentV1Error::InvalidSource)?;
        let frame_length = frame_len_from_prefix_v1(prefix, DecodeLimitsV1::maximum())
            .map_err(|_| SegmentV1Error::InvalidSource)?;
        let frame_end = source_offset
            .checked_add(frame_length)
            .ok_or(SegmentV1Error::Bounds)?;
        let frame = source
            .get(source_offset..frame_end)
            .ok_or(SegmentV1Error::InvalidSource)?;
        let decoded = decode_admission_frame_v1(frame, DecodeLimitsV1::maximum(), previous)
            .map_err(|_| SegmentV1Error::InvalidSource)?;
        if decoded.store_id() != store_id || decoded.declaration().store_id() != store_id {
            return Err(SegmentV1Error::StoreMismatch);
        }
        let series_id = decoded.declaration().series_id();
        if decoded.envelope().series().series_id() != series_id
            || decoded.retry().series_id() != series_id
            || encode_decoded_admission_frame_v1(&decoded)
                .map_err(|_| SegmentV1Error::InvalidSource)?
                != frame
        {
            return Err(SegmentV1Error::InvalidSource);
        }
        let observations = decoded
            .envelope()
            .observations()
            .iter()
            .enumerate()
            .map(|(ordinal, observation)| {
                Ok(SourceObservation {
                    raw_order_key: observation.raw_order_key(),
                    ordinal: u32::try_from(ordinal).map_err(|_| SegmentV1Error::Bounds)?,
                })
            })
            .collect::<Result<Vec<_>, SegmentV1Error>>()?;
        observation_count = observation_count
            .checked_add(observations.len())
            .ok_or(SegmentV1Error::Bounds)?;
        if observation_count > MAX_SEGMENT_V1_OBSERVATIONS {
            return Err(SegmentV1Error::Bounds);
        }
        let append_sequence = decoded.append_sequence();
        let index = source_frames.len();
        source_frames.push(SourceFrame {
            append_sequence,
            source_offset,
            length: frame_length,
            observations,
        });
        grouped.entry(series_id).or_default().push(index);
        previous = Some(
            AppendSequenceV1::new(append_sequence).map_err(|_| SegmentV1Error::InvalidSource)?,
        );
        source_offset = frame_end;
    }
    if source_offset != source.len()
        || source_frames.len() != expected_count
        || previous.map(AppendSequenceV1::get) != Some(sealed.sequence_cutoff())
    {
        return Err(SegmentV1Error::InvalidSource);
    }
    let blocks_length = source
        .len()
        .checked_sub(JOURNAL_V1_HEADER_LEN)
        .ok_or(SegmentV1Error::InvalidSource)?;
    let layout = Layout::new(
        source_frames.len(),
        grouped.len(),
        observation_count,
        blocks_length,
    )?;
    let mut bytes = vec![0_u8; layout.artifact_length];
    encode_header(
        &mut bytes,
        store_id,
        sealed,
        source_frames.len(),
        grouped.len(),
        observation_count,
        layout,
    )?;

    let mut block_cursor = layout.blocks_offset;
    let mut recent_cursor = layout.recent_offset;
    let mut append_entries = Vec::with_capacity(source_frames.len());
    for (series_index, (series_id, frame_indices)) in grouped.iter().enumerate() {
        let directory_offset = layout.series_offset + series_index * SEGMENT_V1_SERIES_ENTRY_LEN;
        let block_start = block_cursor;
        let recent_start = recent_cursor;
        let mut recent = Vec::new();
        for (frame_ordinal, source_index) in frame_indices.iter().enumerate() {
            let source_frame = &source_frames[*source_index];
            let source_bytes = &source
                [source_frame.source_offset..source_frame.source_offset + source_frame.length];
            let frame_offset = block_cursor;
            bytes[frame_offset..frame_offset + source_frame.length].copy_from_slice(source_bytes);
            block_cursor += source_frame.length;
            let location = FrameLocation {
                append_sequence: source_frame.append_sequence,
                series_id: *series_id,
                frame_offset: u64::try_from(frame_offset).map_err(|_| SegmentV1Error::Bounds)?,
                frame_length: u64::try_from(source_frame.length)
                    .map_err(|_| SegmentV1Error::Bounds)?,
                frame_ordinal: u32::try_from(frame_ordinal).map_err(|_| SegmentV1Error::Bounds)?,
            };
            append_entries.push(location);
            for observation in &source_frame.observations {
                recent.push(ObservationLocation {
                    series_id: *series_id,
                    raw_order_key: observation.raw_order_key,
                    append_sequence: source_frame.append_sequence,
                    observation_ordinal: observation.ordinal,
                    frame_ordinal: location.frame_ordinal,
                    frame_offset: location.frame_offset,
                    frame_length: location.frame_length,
                });
            }
        }
        sort_recent(&mut recent);
        for location in &recent {
            encode_observation_entry(
                &mut bytes[recent_cursor..recent_cursor + SEGMENT_V1_OBSERVATION_ENTRY_LEN],
                *location,
            );
            recent_cursor += SEGMENT_V1_OBSERVATION_ENTRY_LEN;
        }
        let entry = SegmentSeriesEntryV1 {
            series_id: *series_id,
            block_offset: u64::try_from(block_start).map_err(|_| SegmentV1Error::Bounds)?,
            block_length: u64::try_from(block_cursor - block_start)
                .map_err(|_| SegmentV1Error::Bounds)?,
            frame_count: u32::try_from(frame_indices.len()).map_err(|_| SegmentV1Error::Bounds)?,
            observation_count: u32::try_from(recent.len()).map_err(|_| SegmentV1Error::Bounds)?,
            recent_offset: u64::try_from(recent_start).map_err(|_| SegmentV1Error::Bounds)?,
            recent_length: u64::try_from(recent_cursor - recent_start)
                .map_err(|_| SegmentV1Error::Bounds)?,
        };
        encode_series_entry(
            &mut bytes[directory_offset..directory_offset + SEGMENT_V1_SERIES_ENTRY_LEN],
            entry,
        );
    }
    if block_cursor != layout.append_offset || recent_cursor + SEGMENT_V1_CRC_LEN != bytes.len() {
        return Err(SegmentV1Error::InvalidSource);
    }
    append_entries.sort_by_key(|entry| entry.append_sequence);
    for (index, entry) in append_entries.iter().enumerate() {
        let offset = layout.append_offset + index * SEGMENT_V1_APPEND_ENTRY_LEN;
        encode_append_entry(
            &mut bytes[offset..offset + SEGMENT_V1_APPEND_ENTRY_LEN],
            *entry,
        );
    }
    let checksum_offset = bytes.len() - SEGMENT_V1_CRC_LEN;
    let artifact_checksum = crc32c(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&artifact_checksum.to_be_bytes());
    let inspection = inspection(
        store_id,
        sealed,
        source_frames.len(),
        grouped.len(),
        observation_count,
        layout,
        artifact_checksum,
    )?;
    Ok(PreparedSegmentV1 {
        bytes: bytes.into_boxed_slice(),
        inspection,
    })
}

/// Parses and exhaustively validates complete hostile Native Segment V1 bytes.
///
/// The result borrows the candidate and remains structurally non-authorizing.
/// It cannot become a canonical admission, registry mutation, runtime submit,
/// manifest reference, or raw-journal reclamation decision.
///
/// # Errors
///
/// Refuses unknown versions/flags, nonzero reserved bytes, store mismatch,
/// truncation/trailing bytes, impossible counts/layout, overlap, checksum,
/// directory order/coverage, frame mismatch, sequence holes, noncanonical frame
/// re-encoding, source metadata mismatch, and every hard-bound excess.
#[allow(clippy::too_many_lines)]
pub fn parse_segment_v1(
    bytes: &[u8],
    expected_store_id: StoreId,
) -> Result<SegmentV1<'_>, SegmentV1Error> {
    if u64::try_from(bytes.len()).map_err(|_| SegmentV1Error::Bounds)? > MAX_SEGMENT_V1_BYTES {
        return Err(SegmentV1Error::Bounds);
    }
    if bytes.len() < SEGMENT_V1_HEADER_LEN + SEGMENT_V1_CRC_LEN {
        return Err(SegmentV1Error::InvalidSegment);
    }
    if bytes[..8] != SEGMENT_V1_MAGIC
        || read_u16(bytes, 8) != Some(SEGMENT_V1_VERSION)
        || read_u16(bytes, 10) != u16::try_from(SEGMENT_V1_HEADER_LEN).ok()
        || read_u32(bytes, 12) != Some(SEGMENT_FLAGS_NONE)
        || bytes[168..SEGMENT_V1_HEADER_LEN]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(SegmentV1Error::InvalidSegment);
    }
    let store_id = StoreId::from_bytes(
        bytes[16..32]
            .try_into()
            .map_err(|_| SegmentV1Error::InvalidSegment)?,
    )
    .map_err(|_| SegmentV1Error::InvalidSegment)?;
    if store_id != expected_store_id {
        return Err(SegmentV1Error::StoreMismatch);
    }
    let source_journal_generation = read_u64(bytes, 32).ok_or(SegmentV1Error::InvalidSegment)?;
    let sequence_floor = read_u64(bytes, 40).ok_or(SegmentV1Error::InvalidSegment)?;
    let sequence_cutoff = read_u64(bytes, 48).ok_or(SegmentV1Error::InvalidSegment)?;
    let source_registry_generation = read_u64(bytes, 56).ok_or(SegmentV1Error::InvalidSegment)?;
    let source_end_offset = read_u64(bytes, 64).ok_or(SegmentV1Error::InvalidSegment)?;
    let source_artifact_length = read_u64(bytes, 72).ok_or(SegmentV1Error::InvalidSegment)?;
    let source_artifact_checksum = read_u32(bytes, 80).ok_or(SegmentV1Error::InvalidSegment)?;
    let frame_count = read_count(bytes, 84, MAX_ACTIVE_JOURNAL_RECORDS)?;
    let series_count = read_count(bytes, 88, MAX_SEGMENT_V1_SERIES)?;
    let observation_count = read_count(bytes, 92, MAX_SEGMENT_V1_OBSERVATIONS)?;
    if source_journal_generation == 0
        || source_registry_generation == 0
        || sequence_cutoff <= sequence_floor
        || source_end_offset != source_artifact_length
        || source_end_offset <= JOURNAL_V1_HEADER_LEN as u64
        || source_artifact_length > MAX_ACTIVE_JOURNAL_BYTES
        || frame_count == 0
        || series_count == 0
        || series_count > frame_count
        || sequence_cutoff
            .checked_sub(sequence_floor)
            .and_then(|count| usize::try_from(count).ok())
            != Some(frame_count)
    {
        return Err(SegmentV1Error::InvalidSegment);
    }
    let blocks_length = read_u64(bytes, 120)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(SegmentV1Error::Bounds)?;
    let layout = Layout::new(frame_count, series_count, observation_count, blocks_length)?;
    for (offset, expected) in [
        (96, layout.series_offset),
        (104, layout.series_length),
        (112, layout.blocks_offset),
        (120, layout.blocks_length),
        (128, layout.append_offset),
        (136, layout.append_length),
        (144, layout.recent_offset),
        (152, layout.recent_length),
        (160, layout.artifact_length),
    ] {
        if read_u64(bytes, offset).and_then(|value| usize::try_from(value).ok()) != Some(expected) {
            return Err(SegmentV1Error::InvalidSegment);
        }
    }
    if bytes.len() != layout.artifact_length {
        return Err(SegmentV1Error::InvalidSegment);
    }
    let checksum_offset = bytes.len() - SEGMENT_V1_CRC_LEN;
    let artifact_checksum =
        read_u32(bytes, checksum_offset).ok_or(SegmentV1Error::InvalidSegment)?;
    if crc32c(&bytes[..checksum_offset]) != artifact_checksum {
        return Err(SegmentV1Error::InvalidSegment);
    }

    let mut series = Vec::with_capacity(series_count);
    let mut expected_block_offset = layout.blocks_offset;
    let mut expected_recent_offset = layout.recent_offset;
    let mut total_frames = 0_usize;
    let mut total_observations = 0_usize;
    for index in 0..series_count {
        let offset = layout.series_offset + index * SEGMENT_V1_SERIES_ENTRY_LEN;
        let entry_bytes = &bytes[offset..offset + SEGMENT_V1_SERIES_ENTRY_LEN];
        let entry = decode_series_entry(entry_bytes)?;
        if series
            .last()
            .is_some_and(|prior: &SegmentSeriesEntryV1| prior.series_id >= entry.series_id)
            || entry.frame_count == 0
            || usize::try_from(entry.block_offset).ok() != Some(expected_block_offset)
            || usize::try_from(entry.recent_offset).ok() != Some(expected_recent_offset)
            || usize::try_from(entry.recent_length).ok()
                != usize::try_from(entry.observation_count)
                    .ok()
                    .and_then(|count| count.checked_mul(SEGMENT_V1_OBSERVATION_ENTRY_LEN))
        {
            return Err(SegmentV1Error::InvalidSegment);
        }
        expected_block_offset = expected_block_offset
            .checked_add(usize::try_from(entry.block_length).map_err(|_| SegmentV1Error::Bounds)?)
            .ok_or(SegmentV1Error::Bounds)?;
        expected_recent_offset = expected_recent_offset
            .checked_add(usize::try_from(entry.recent_length).map_err(|_| SegmentV1Error::Bounds)?)
            .ok_or(SegmentV1Error::Bounds)?;
        if expected_block_offset > layout.append_offset || expected_recent_offset > checksum_offset
        {
            return Err(SegmentV1Error::InvalidSegment);
        }
        total_frames = total_frames
            .checked_add(usize::try_from(entry.frame_count).map_err(|_| SegmentV1Error::Bounds)?)
            .ok_or(SegmentV1Error::Bounds)?;
        total_observations = total_observations
            .checked_add(
                usize::try_from(entry.observation_count).map_err(|_| SegmentV1Error::Bounds)?,
            )
            .ok_or(SegmentV1Error::Bounds)?;
        series.push(entry);
    }
    if expected_block_offset != layout.append_offset
        || expected_recent_offset != checksum_offset
        || total_frames != frame_count
        || total_observations != observation_count
    {
        return Err(SegmentV1Error::InvalidSegment);
    }

    let mut proofs = Vec::with_capacity(frame_count);
    let mut expected_observations = Vec::with_capacity(observation_count);
    for entry in &series {
        let mut frame_offset =
            usize::try_from(entry.block_offset).map_err(|_| SegmentV1Error::Bounds)?;
        let block_end = frame_offset
            .checked_add(usize::try_from(entry.block_length).map_err(|_| SegmentV1Error::Bounds)?)
            .ok_or(SegmentV1Error::Bounds)?;
        let mut prior_series_sequence = None;
        let mut decoded_observation_count = 0_usize;
        for frame_ordinal in 0..entry.frame_count {
            let prefix_end = frame_offset
                .checked_add(JOURNAL_V1_FRAME_PREFIX_LEN)
                .ok_or(SegmentV1Error::Bounds)?;
            let prefix = bytes
                .get(frame_offset..prefix_end)
                .filter(|_| prefix_end <= block_end)
                .ok_or(SegmentV1Error::InvalidSegment)?;
            let frame_length = frame_len_from_prefix_v1(prefix, DecodeLimitsV1::maximum())
                .map_err(|_| SegmentV1Error::InvalidSegment)?;
            let frame_end = frame_offset
                .checked_add(frame_length)
                .ok_or(SegmentV1Error::Bounds)?;
            let frame = bytes
                .get(frame_offset..frame_end)
                .filter(|_| frame_end <= block_end)
                .ok_or(SegmentV1Error::InvalidSegment)?;
            let decoded = decode_admission_frame_v1(frame, DecodeLimitsV1::maximum(), None)
                .map_err(|_| SegmentV1Error::InvalidSegment)?;
            let sequence = decoded.append_sequence();
            if decoded.store_id() != store_id
                || decoded.declaration().store_id() != store_id
                || decoded.declaration().series_id() != entry.series_id
                || decoded.envelope().series().series_id() != entry.series_id
                || decoded.retry().series_id() != entry.series_id
                || prior_series_sequence.is_some_and(|prior| prior >= sequence)
                || encode_decoded_admission_frame_v1(&decoded)
                    .map_err(|_| SegmentV1Error::InvalidSegment)?
                    != frame
            {
                return Err(SegmentV1Error::InvalidSegment);
            }
            let proof = FrameLocation {
                append_sequence: sequence,
                series_id: entry.series_id,
                frame_offset: u64::try_from(frame_offset).map_err(|_| SegmentV1Error::Bounds)?,
                frame_length: u64::try_from(frame_length).map_err(|_| SegmentV1Error::Bounds)?,
                frame_ordinal,
            };
            let frame_observations = decoded.envelope().observations();
            decoded_observation_count = decoded_observation_count
                .checked_add(frame_observations.len())
                .ok_or(SegmentV1Error::Bounds)?;
            for (ordinal, observation) in frame_observations.iter().enumerate() {
                expected_observations.push(ObservationLocation {
                    series_id: entry.series_id,
                    raw_order_key: observation.raw_order_key(),
                    append_sequence: sequence,
                    observation_ordinal: u32::try_from(ordinal)
                        .map_err(|_| SegmentV1Error::Bounds)?,
                    frame_ordinal,
                    frame_offset: proof.frame_offset,
                    frame_length: proof.frame_length,
                });
            }
            proofs.push(proof);
            prior_series_sequence = Some(sequence);
            frame_offset = frame_end;
        }
        if frame_offset != block_end
            || usize::try_from(entry.observation_count).ok() != Some(decoded_observation_count)
        {
            return Err(SegmentV1Error::InvalidSegment);
        }
    }
    if proofs.len() != frame_count || expected_observations.len() != observation_count {
        return Err(SegmentV1Error::InvalidSegment);
    }
    proofs.sort_by_key(|proof| proof.append_sequence);
    let mut expected_sequence = sequence_floor
        .checked_add(1)
        .ok_or(SegmentV1Error::InvalidSegment)?;
    let mut source_checksum = StreamingCrc32c::new();
    source_checksum.update(&JournalHeaderV1::new(store_id).encode());
    let mut reconstructed_source_length = JOURNAL_V1_HEADER_LEN as u64;
    let mut appends = Vec::with_capacity(frame_count);
    for (index, proof) in proofs.iter().enumerate() {
        if proof.append_sequence != expected_sequence {
            return Err(SegmentV1Error::InvalidSegment);
        }
        let offset = layout.append_offset + index * SEGMENT_V1_APPEND_ENTRY_LEN;
        let entry = decode_append_entry(&bytes[offset..offset + SEGMENT_V1_APPEND_ENTRY_LEN])?;
        let expected = SegmentAppendEntryV1 {
            append_sequence: proof.append_sequence,
            series_id: proof.series_id,
            frame_offset: proof.frame_offset,
            frame_length: proof.frame_length,
            frame_ordinal: proof.frame_ordinal,
        };
        if entry != expected {
            return Err(SegmentV1Error::InvalidSegment);
        }
        let frame = checked_slice(bytes, proof.frame_offset, proof.frame_length)
            .ok_or(SegmentV1Error::InvalidSegment)?;
        source_checksum.update(frame);
        reconstructed_source_length = reconstructed_source_length
            .checked_add(proof.frame_length)
            .ok_or(SegmentV1Error::Bounds)?;
        appends.push(entry);
        expected_sequence = expected_sequence.saturating_add(1);
    }
    if proofs.last().map(|proof| proof.append_sequence) != Some(sequence_cutoff)
        || reconstructed_source_length != source_artifact_length
        || source_checksum.finish() != source_artifact_checksum
    {
        return Err(SegmentV1Error::InvalidSegment);
    }

    expected_observations.sort_by(|left, right| {
        left.series_id
            .cmp(&right.series_id)
            .then_with(|| recent_order(left, right))
    });
    let mut observations = Vec::with_capacity(observation_count);
    for (index, expected) in expected_observations.iter().enumerate() {
        let offset = layout.recent_offset + index * SEGMENT_V1_OBSERVATION_ENTRY_LEN;
        let entry = decode_observation_entry(
            &bytes[offset..offset + SEGMENT_V1_OBSERVATION_ENTRY_LEN],
            *expected,
        )?;
        observations.push(entry);
    }
    let sealed = SealedGeneration::new(
        source_journal_generation,
        sequence_floor,
        sequence_cutoff,
        source_end_offset,
        source_registry_generation,
        source_artifact_length,
        source_artifact_checksum,
    );
    let inspection = inspection(
        store_id,
        sealed,
        frame_count,
        series_count,
        observation_count,
        layout,
        artifact_checksum,
    )?;
    Ok(SegmentV1 {
        bytes,
        inspection,
        series: series.into_boxed_slice(),
        appends: appends.into_boxed_slice(),
        observations: observations.into_boxed_slice(),
    })
}

#[allow(clippy::too_many_arguments)]
fn inspection(
    store_id: StoreId,
    sealed: SealedGeneration,
    frame_count: usize,
    series_count: usize,
    observation_count: usize,
    layout: Layout,
    artifact_checksum: u32,
) -> Result<SegmentV1Inspection, SegmentV1Error> {
    Ok(SegmentV1Inspection {
        store_id,
        source_journal_generation: sealed.journal_generation(),
        sequence_floor: sealed.sequence_floor(),
        sequence_cutoff: sealed.sequence_cutoff(),
        source_registry_generation: sealed.registry_generation(),
        source_end_offset: sealed.end_offset(),
        source_artifact_length: sealed.artifact_length(),
        source_artifact_checksum: sealed.artifact_checksum(),
        frame_count,
        series_count,
        observation_count,
        series_directory_offset: to_u64(layout.series_offset)?,
        series_directory_length: to_u64(layout.series_length)?,
        block_region_offset: to_u64(layout.blocks_offset)?,
        block_region_length: to_u64(layout.blocks_length)?,
        append_directory_offset: to_u64(layout.append_offset)?,
        append_directory_length: to_u64(layout.append_length)?,
        recent_directory_offset: to_u64(layout.recent_offset)?,
        recent_directory_length: to_u64(layout.recent_length)?,
        artifact_length: to_u64(layout.artifact_length)?,
        artifact_checksum,
    })
}

#[allow(clippy::too_many_arguments)]
fn encode_header(
    bytes: &mut [u8],
    store_id: StoreId,
    sealed: SealedGeneration,
    frame_count: usize,
    series_count: usize,
    observation_count: usize,
    layout: Layout,
) -> Result<(), SegmentV1Error> {
    bytes[..8].copy_from_slice(&SEGMENT_V1_MAGIC);
    put_u16(bytes, 8, SEGMENT_V1_VERSION);
    put_u16(
        bytes,
        10,
        u16::try_from(SEGMENT_V1_HEADER_LEN).map_err(|_| SegmentV1Error::Bounds)?,
    );
    bytes[16..32].copy_from_slice(store_id.as_bytes());
    put_u64(bytes, 32, sealed.journal_generation());
    put_u64(bytes, 40, sealed.sequence_floor());
    put_u64(bytes, 48, sealed.sequence_cutoff());
    put_u64(bytes, 56, sealed.registry_generation());
    put_u64(bytes, 64, sealed.end_offset());
    put_u64(bytes, 72, sealed.artifact_length());
    put_u32(bytes, 80, sealed.artifact_checksum());
    put_u32(bytes, 84, to_u32(frame_count)?);
    put_u32(bytes, 88, to_u32(series_count)?);
    put_u32(bytes, 92, to_u32(observation_count)?);
    put_u64(bytes, 96, to_u64(layout.series_offset)?);
    put_u64(bytes, 104, to_u64(layout.series_length)?);
    put_u64(bytes, 112, to_u64(layout.blocks_offset)?);
    put_u64(bytes, 120, to_u64(layout.blocks_length)?);
    put_u64(bytes, 128, to_u64(layout.append_offset)?);
    put_u64(bytes, 136, to_u64(layout.append_length)?);
    put_u64(bytes, 144, to_u64(layout.recent_offset)?);
    put_u64(bytes, 152, to_u64(layout.recent_length)?);
    put_u64(bytes, 160, to_u64(layout.artifact_length)?);
    Ok(())
}

fn encode_series_entry(bytes: &mut [u8], entry: SegmentSeriesEntryV1) {
    bytes[..16].copy_from_slice(entry.series_id.as_bytes());
    put_u64(bytes, 16, entry.block_offset);
    put_u64(bytes, 24, entry.block_length);
    put_u32(bytes, 32, entry.frame_count);
    put_u32(bytes, 36, entry.observation_count);
    put_u64(bytes, 40, entry.recent_offset);
    put_u64(bytes, 48, entry.recent_length);
}

fn decode_series_entry(bytes: &[u8]) -> Result<SegmentSeriesEntryV1, SegmentV1Error> {
    if bytes.len() != SEGMENT_V1_SERIES_ENTRY_LEN || bytes[56..64].iter().any(|byte| *byte != 0) {
        return Err(SegmentV1Error::InvalidSegment);
    }
    Ok(SegmentSeriesEntryV1 {
        series_id: SeriesId::from_bytes(
            bytes[..16]
                .try_into()
                .map_err(|_| SegmentV1Error::InvalidSegment)?,
        )
        .map_err(|_| SegmentV1Error::InvalidSegment)?,
        block_offset: read_u64(bytes, 16).ok_or(SegmentV1Error::InvalidSegment)?,
        block_length: read_u64(bytes, 24).ok_or(SegmentV1Error::InvalidSegment)?,
        frame_count: read_u32(bytes, 32).ok_or(SegmentV1Error::InvalidSegment)?,
        observation_count: read_u32(bytes, 36).ok_or(SegmentV1Error::InvalidSegment)?,
        recent_offset: read_u64(bytes, 40).ok_or(SegmentV1Error::InvalidSegment)?,
        recent_length: read_u64(bytes, 48).ok_or(SegmentV1Error::InvalidSegment)?,
    })
}

fn encode_append_entry(bytes: &mut [u8], entry: FrameLocation) {
    put_u64(bytes, 0, entry.append_sequence);
    bytes[8..24].copy_from_slice(entry.series_id.as_bytes());
    put_u64(bytes, 24, entry.frame_offset);
    put_u64(bytes, 32, entry.frame_length);
    put_u32(bytes, 40, entry.frame_ordinal);
}

fn decode_append_entry(bytes: &[u8]) -> Result<SegmentAppendEntryV1, SegmentV1Error> {
    if bytes.len() != SEGMENT_V1_APPEND_ENTRY_LEN || bytes[44..48].iter().any(|byte| *byte != 0) {
        return Err(SegmentV1Error::InvalidSegment);
    }
    Ok(SegmentAppendEntryV1 {
        append_sequence: read_u64(bytes, 0).ok_or(SegmentV1Error::InvalidSegment)?,
        series_id: SeriesId::from_bytes(
            bytes[8..24]
                .try_into()
                .map_err(|_| SegmentV1Error::InvalidSegment)?,
        )
        .map_err(|_| SegmentV1Error::InvalidSegment)?,
        frame_offset: read_u64(bytes, 24).ok_or(SegmentV1Error::InvalidSegment)?,
        frame_length: read_u64(bytes, 32).ok_or(SegmentV1Error::InvalidSegment)?,
        frame_ordinal: read_u32(bytes, 40).ok_or(SegmentV1Error::InvalidSegment)?,
    })
}

fn encode_observation_entry(bytes: &mut [u8], entry: ObservationLocation) {
    let effective = entry.raw_order_key.effective();
    let receive = entry.raw_order_key.receive();
    bytes[..16].copy_from_slice(entry.series_id.as_bytes());
    bytes[16..24].copy_from_slice(&effective.unix_seconds().to_be_bytes());
    put_u32(bytes, 24, effective.nanosecond());
    bytes[28..36].copy_from_slice(&receive.unix_seconds().to_be_bytes());
    put_u32(bytes, 36, receive.nanosecond());
    bytes[40..56].copy_from_slice(entry.raw_order_key.observation_id().as_bytes());
    put_u64(bytes, 56, entry.append_sequence);
    put_u32(bytes, 64, entry.observation_ordinal);
    put_u32(bytes, 68, entry.frame_ordinal);
    put_u64(bytes, 72, entry.frame_offset);
    put_u64(bytes, 80, entry.frame_length);
}

fn decode_observation_entry(
    bytes: &[u8],
    expected: ObservationLocation,
) -> Result<SegmentObservationEntryV1, SegmentV1Error> {
    if bytes.len() != SEGMENT_V1_OBSERVATION_ENTRY_LEN
        || bytes[88..96].iter().any(|byte| *byte != 0)
    {
        return Err(SegmentV1Error::InvalidSegment);
    }
    let series_id = SeriesId::from_bytes(
        bytes[..16]
            .try_into()
            .map_err(|_| SegmentV1Error::InvalidSegment)?,
    )
    .map_err(|_| SegmentV1Error::InvalidSegment)?;
    let effective = Timestamp::new(
        read_i64(bytes, 16).ok_or(SegmentV1Error::InvalidSegment)?,
        read_u32(bytes, 24).ok_or(SegmentV1Error::InvalidSegment)?,
    )
    .map_err(|_| SegmentV1Error::InvalidSegment)?;
    let receive = Timestamp::new(
        read_i64(bytes, 28).ok_or(SegmentV1Error::InvalidSegment)?,
        read_u32(bytes, 36).ok_or(SegmentV1Error::InvalidSegment)?,
    )
    .map_err(|_| SegmentV1Error::InvalidSegment)?;
    let observation_id = ObservationId::from_bytes(
        bytes[40..56]
            .try_into()
            .map_err(|_| SegmentV1Error::InvalidSegment)?,
    )
    .map_err(|_| SegmentV1Error::InvalidSegment)?;
    let actual = SegmentObservationEntryV1 {
        series_id,
        raw_order_key: expected.raw_order_key,
        append_sequence: read_u64(bytes, 56).ok_or(SegmentV1Error::InvalidSegment)?,
        observation_ordinal: read_u32(bytes, 64).ok_or(SegmentV1Error::InvalidSegment)?,
        frame_ordinal: read_u32(bytes, 68).ok_or(SegmentV1Error::InvalidSegment)?,
        frame_offset: read_u64(bytes, 72).ok_or(SegmentV1Error::InvalidSegment)?,
        frame_length: read_u64(bytes, 80).ok_or(SegmentV1Error::InvalidSegment)?,
    };
    if effective != expected.raw_order_key.effective()
        || receive != expected.raw_order_key.receive()
        || observation_id != expected.raw_order_key.observation_id()
        || actual.series_id != expected.series_id
        || actual.append_sequence != expected.append_sequence
        || actual.observation_ordinal != expected.observation_ordinal
        || actual.frame_ordinal != expected.frame_ordinal
        || actual.frame_offset != expected.frame_offset
        || actual.frame_length != expected.frame_length
    {
        return Err(SegmentV1Error::InvalidSegment);
    }
    Ok(actual)
}

fn sort_recent(entries: &mut [ObservationLocation]) {
    entries.sort_by(recent_order);
}

fn recent_order(left: &ObservationLocation, right: &ObservationLocation) -> std::cmp::Ordering {
    right
        .raw_order_key
        .cmp(&left.raw_order_key)
        .then_with(|| right.append_sequence.cmp(&left.append_sequence))
        .then_with(|| left.observation_ordinal.cmp(&right.observation_ordinal))
}

fn checked_slice(bytes: &[u8], offset: u64, length: u64) -> Option<&[u8]> {
    let start = usize::try_from(offset).ok()?;
    let length = usize::try_from(length).ok()?;
    let end = start.checked_add(length)?;
    bytes.get(start..end)
}

fn read_count(bytes: &[u8], offset: usize, maximum: usize) -> Result<usize, SegmentV1Error> {
    let count = read_u32(bytes, offset)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(SegmentV1Error::InvalidSegment)?;
    if count > maximum {
        return Err(SegmentV1Error::Bounds);
    }
    Ok(count)
}

fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    bytes
        .get(offset..offset.checked_add(2)?)?
        .try_into()
        .ok()
        .map(u16::from_be_bytes)
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    bytes
        .get(offset..offset.checked_add(4)?)?
        .try_into()
        .ok()
        .map(u32::from_be_bytes)
}

fn read_u64(bytes: &[u8], offset: usize) -> Option<u64> {
    bytes
        .get(offset..offset.checked_add(8)?)?
        .try_into()
        .ok()
        .map(u64::from_be_bytes)
}

fn read_i64(bytes: &[u8], offset: usize) -> Option<i64> {
    bytes
        .get(offset..offset.checked_add(8)?)?
        .try_into()
        .ok()
        .map(i64::from_be_bytes)
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

fn to_u32(value: usize) -> Result<u32, SegmentV1Error> {
    u32::try_from(value).map_err(|_| SegmentV1Error::Bounds)
}

fn to_u64(value: usize) -> Result<u64, SegmentV1Error> {
    u64::try_from(value).map_err(|_| SegmentV1Error::Bounds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{self, segment_oracle};
    use crate::{PreparedAdmissionV1, PreparedFrameV1};
    use och_core::{ExactValue, ValueFamily};

    fn frame(admission: och_core::CanonicalAdmission, sequence: u64) -> PreparedFrameV1 {
        PreparedAdmissionV1::new(admission)
            .expect("bounded Segment V1 unit fixture")
            .into_frame(AppendSequenceV1::new(sequence).expect("positive fixture sequence"))
            .expect("bounded Segment V1 fixture frame")
    }

    fn raw_journal(store_id: StoreId, frames: &[&PreparedFrameV1]) -> Vec<u8> {
        let mut raw = JournalHeaderV1::new(store_id).encode().to_vec();
        for frame in frames {
            raw.extend_from_slice(frame.bytes());
        }
        raw
    }

    fn sealed(
        raw: &[u8],
        generation: u64,
        floor: u64,
        cutoff: u64,
        registry_generation: u64,
    ) -> SealedGeneration {
        SealedGeneration::new(
            generation,
            floor,
            cutoff,
            raw.len() as u64,
            registry_generation,
            raw.len() as u64,
            segment_oracle::checksum(raw),
        )
    }

    #[test]
    fn one_series_revision_gap_and_repetition_match_the_primitive_oracle_exactly() {
        let first = frame(
            test_support::observed_admission(
                vec![ExactValue::Boolean(true), ExactValue::Boolean(false)],
                ValueFamily::Boolean,
                1,
                false,
            ),
            9,
        );
        let revised = frame(
            test_support::observed_admission(
                vec![ExactValue::Boolean(true)],
                ValueFamily::Boolean,
                0,
                true,
            ),
            10,
        );
        let gap_only = frame(
            test_support::observed_admission(Vec::new(), ValueFamily::Boolean, 1, true),
            11,
        );
        let raw = raw_journal(test_support::store_id(1), &[&first, &revised, &gap_only]);
        let sealed = sealed(&raw, 3, 8, 11, 4);
        let first_observations = [
            segment_oracle::Observation {
                effective_seconds: 9,
                effective_nanos: 12,
                receive_seconds: 10,
                receive_nanos: 11,
                id: test_support::uuid_bytes(10_000),
                ordinal: 0,
            },
            segment_oracle::Observation {
                effective_seconds: 9,
                effective_nanos: 12,
                receive_seconds: 10,
                receive_nanos: 11,
                id: test_support::uuid_bytes(10_001),
                ordinal: 1,
            },
        ];
        let revised_observations = [segment_oracle::Observation {
            effective_seconds: 9,
            effective_nanos: 12,
            receive_seconds: 10,
            receive_nanos: 11,
            id: test_support::uuid_bytes(10_000),
            ordinal: 0,
        }];
        let oracle_frames = [
            segment_oracle::Frame {
                sequence: 9,
                series_id: test_support::uuid_bytes(2),
                bytes: first.bytes(),
                observations: &first_observations,
            },
            segment_oracle::Frame {
                sequence: 10,
                series_id: test_support::uuid_bytes(2),
                bytes: revised.bytes(),
                observations: &revised_observations,
            },
            segment_oracle::Frame {
                sequence: 11,
                series_id: test_support::uuid_bytes(2),
                bytes: gap_only.bytes(),
                observations: &[],
            },
        ];
        let expected = segment_oracle::build(&segment_oracle::Source {
            store_id: test_support::uuid_bytes(1),
            journal_generation: 3,
            sequence_floor: 8,
            sequence_cutoff: 11,
            registry_generation: 4,
            raw_journal: &raw,
            frames: &oracle_frames,
        });
        let first_build = build_segment_v1(test_support::store_id(1), sealed, &raw)
            .expect("build exact one-series candidate");
        let repeated = build_segment_v1(test_support::store_id(1), sealed, &raw)
            .expect("repeat exact one-series candidate");
        assert_eq!(first_build.bytes(), expected);
        assert_eq!(repeated.bytes(), first_build.bytes());
        let parsed = parse_segment_v1(first_build.bytes(), test_support::store_id(1))
            .expect("parse exact one-series candidate");
        assert_eq!(parsed.series_directory().len(), 1);
        assert_eq!(parsed.append_directory().len(), 3);
        assert_eq!(parsed.recent_observations().len(), 3);
        assert_eq!(parsed.recent_observations()[0].append_sequence(), 9);
        assert_eq!(
            parsed.recent_observations()[0].observation_id(),
            test_support::observation_id(10_001)
        );
        assert_eq!(parsed.recent_observations()[1].append_sequence(), 10);
        assert_eq!(parsed.recent_observations()[2].append_sequence(), 9);
    }

    #[test]
    fn recent_index_orders_genuinely_out_of_order_times_then_stable_ties() {
        let out_of_order = frame(
            test_support::observed_admission_with_raw_times(&[
                (10_000, 9, 0, 5, 0),
                (10_001, 1, 0, 7, 0),
                (10_002, 3, 0, 7, 0),
            ]),
            30,
        );
        let repeated_key = frame(
            test_support::observed_admission_with_raw_times(&[(10_001, 1, 0, 7, 0)]),
            31,
        );
        let raw = raw_journal(test_support::store_id(1), &[&out_of_order, &repeated_key]);
        let candidate =
            build_segment_v1(test_support::store_id(1), sealed(&raw, 4, 29, 31, 5), &raw)
                .expect("build out-of-order-time candidate");
        let parsed = parse_segment_v1(candidate.bytes(), test_support::store_id(1))
            .expect("parse out-of-order-time candidate");
        let recent = parsed.recent_observations();
        assert_eq!(recent.len(), 4);
        assert_eq!(
            recent[0].observation_id(),
            test_support::observation_id(10_002)
        );
        assert_eq!(recent[0].raw_order_key().effective().unix_seconds(), 7);
        assert_eq!(recent[0].raw_order_key().receive().unix_seconds(), 3);
        assert_eq!(recent[0].observation_ordinal(), 2);
        assert_eq!(
            recent[1].observation_id(),
            test_support::observation_id(10_001)
        );
        assert_eq!(recent[1].append_sequence(), 31);
        assert_eq!(recent[1].observation_ordinal(), 0);
        assert_eq!(
            recent[2].observation_id(),
            test_support::observation_id(10_001)
        );
        assert_eq!(recent[2].append_sequence(), 30);
        assert_eq!(recent[2].observation_ordinal(), 1);
        assert_eq!(
            recent[3].observation_id(),
            test_support::observation_id(10_000)
        );
        assert_eq!(recent[3].raw_order_key().effective().unix_seconds(), 5);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn source_proof_refuses_empty_mismatched_corrupt_torn_suffix_and_sequence_evidence() {
        let first = frame(test_support::no_change_admission(), 1);
        let second = frame(
            test_support::no_change_admission_with_retry_key("second"),
            2,
        );
        let raw = raw_journal(test_support::store_id(1), &[&first, &second]);
        let canonical = sealed(&raw, 1, 0, 2, 2);
        assert!(build_segment_v1(test_support::store_id(1), canonical, &raw).is_ok());

        let header_only = JournalHeaderV1::new(test_support::store_id(1)).encode();
        assert_eq!(
            build_segment_v1(
                test_support::store_id(1),
                sealed(&header_only, 1, 0, 1, 1),
                &header_only,
            )
            .expect_err("header-only sealed sources are forbidden"),
            SegmentV1Error::InvalidSource
        );
        for hostile in [
            SealedGeneration::new(
                0,
                0,
                2,
                raw.len() as u64,
                2,
                raw.len() as u64,
                segment_oracle::checksum(&raw),
            ),
            SealedGeneration::new(
                1,
                0,
                3,
                raw.len() as u64,
                2,
                raw.len() as u64,
                segment_oracle::checksum(&raw),
            ),
            SealedGeneration::new(
                1,
                0,
                2,
                raw.len() as u64 + 1,
                2,
                raw.len() as u64 + 1,
                segment_oracle::checksum(&raw),
            ),
            SealedGeneration::new(
                1,
                0,
                2,
                raw.len() as u64,
                2,
                raw.len() as u64,
                segment_oracle::checksum(&raw) ^ 1,
            ),
        ] {
            assert!(build_segment_v1(test_support::store_id(1), hostile, &raw).is_err());
        }
        assert_eq!(
            build_segment_v1(test_support::store_id(2), canonical, &raw,)
                .expect_err("foreign Journal header must refuse"),
            SegmentV1Error::StoreMismatch
        );

        let mut corrupt = raw.clone();
        corrupt[JOURNAL_V1_HEADER_LEN + JOURNAL_V1_FRAME_PREFIX_LEN] ^= 1;
        assert_eq!(
            build_segment_v1(
                test_support::store_id(1),
                sealed(&corrupt, 1, 0, 2, 2),
                &corrupt,
            )
            .expect_err("interior frame corruption must refuse"),
            SegmentV1Error::InvalidSource
        );
        let torn = &raw[..raw.len() - 1];
        assert_eq!(
            build_segment_v1(test_support::store_id(1), sealed(torn, 1, 0, 2, 2), torn,)
                .expect_err("torn final frame must refuse"),
            SegmentV1Error::InvalidSource
        );
        let mut suffix = raw.clone();
        suffix.push(0);
        assert_eq!(
            build_segment_v1(
                test_support::store_id(1),
                sealed(&suffix, 1, 0, 2, 2),
                &suffix,
            )
            .expect_err("trailing source suffix must refuse"),
            SegmentV1Error::InvalidSource
        );
        let mut sequence_three = second.bytes().to_vec();
        sequence_three[8..16].copy_from_slice(&3_u64.to_be_bytes());
        let checksum_offset = sequence_three.len() - crate::JOURNAL_V1_FRAME_CRC_LEN;
        let checksum = segment_oracle::checksum(&sequence_three[..checksum_offset]);
        sequence_three[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
        let mut sequence_gap = JournalHeaderV1::new(test_support::store_id(1))
            .encode()
            .to_vec();
        sequence_gap.extend_from_slice(first.bytes());
        sequence_gap.extend_from_slice(&sequence_three);
        sequence_gap.extend_from_slice(second.bytes());
        assert_eq!(
            build_segment_v1(
                test_support::store_id(1),
                sealed(&sequence_gap, 1, 0, 3, 2),
                &sequence_gap,
            )
            .expect_err("interior append-sequence gap must refuse"),
            SegmentV1Error::InvalidSource
        );
        let excessive_range = SealedGeneration::new(
            1,
            0,
            MAX_ACTIVE_JOURNAL_RECORDS as u64 + 1,
            raw.len() as u64,
            2,
            raw.len() as u64,
            segment_oracle::checksum(&raw),
        );
        assert_eq!(
            build_segment_v1(test_support::store_id(1), excessive_range, &raw)
                .expect_err("record hard bound must refuse before grouping"),
            SegmentV1Error::Bounds
        );
    }
}
