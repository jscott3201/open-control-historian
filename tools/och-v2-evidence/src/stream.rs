use crate::crc32c::Crc32c;
use crate::error::{EvidenceError, Result};
use crate::ledger::{
    ControlledGuard, FRAME_META_LEDGER_BYTES, MAX_FRAME_BYTES, MAX_FRAMES, MAX_OBSERVATIONS,
    MAX_SERIES, OBSERVATION_WORK_BYTES, SCRATCH_BYTES, active_controlled_bytes,
    actual_metadata_bytes,
};
use crate::model::{FixtureMeta, SegmentIdentity};
use crate::root::EvidenceRoot;
use och_store::{
    AppendSequenceV1, DecodeLimitsV1, JOURNAL_V1_FRAME_CRC_LEN, JOURNAL_V1_FRAME_MAGIC,
    JOURNAL_V1_FRAME_PREFIX_LEN, JOURNAL_V1_HEADER_LEN, JOURNAL_V1_VERSION, JournalHeaderV1,
    MAX_ADMISSION_PAYLOAD_V1, MAX_SEGMENT_V1_BYTES, SEGMENT_V1_APPEND_ENTRY_LEN,
    SEGMENT_V1_CRC_LEN, SEGMENT_V1_HEADER_LEN, SEGMENT_V1_MAGIC, SEGMENT_V1_OBSERVATION_ENTRY_LEN,
    SEGMENT_V1_SERIES_ENTRY_LEN, SEGMENT_V1_VERSION, decode_admission_frame_v1,
    encode_decoded_admission_frame_v1,
};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FrameMeta {
    series_id: [u8; 16],
    append_sequence: u64,
    source_offset: u64,
    frame_length: u64,
    segment_offset: u64,
    append_index: u32,
    frame_ordinal: u32,
    observation_start: u32,
    observation_count: u16,
    reserved: u16,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ObservationWork {
    series_id: [u8; 16],
    observation_id: [u8; 16],
    effective_seconds: i64,
    receive_seconds: i64,
    append_sequence: u64,
    frame_offset: u64,
    frame_length: u64,
    effective_nanos: u32,
    receive_nanos: u32,
    observation_ordinal: u32,
    frame_ordinal: u32,
    reserved: u32,
}

const _: () = assert!(size_of::<FrameMeta>() == FRAME_META_LEDGER_BYTES);
const _: () = assert!(size_of::<ObservationWork>() == OBSERVATION_WORK_BYTES);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct IoStats {
    pub(crate) bytes_read: u64,
    pub(crate) read_calls: u64,
    pub(crate) max_read_request: usize,
}

impl IoStats {
    fn add(&mut self, other: Self) -> Result<()> {
        self.bytes_read = self
            .bytes_read
            .checked_add(other.bytes_read)
            .ok_or(EvidenceError::Bounds)?;
        self.read_calls = self
            .read_calls
            .checked_add(other.read_calls)
            .ok_or(EvidenceError::Bounds)?;
        self.max_read_request = self.max_read_request.max(other.max_read_request);
        Ok(())
    }
}

struct CountedFile {
    file: File,
    stats: IoStats,
}

impl CountedFile {
    fn new(file: File) -> Self {
        Self {
            file,
            stats: IoStats::default(),
        }
    }

    fn length(&self) -> Result<u64> {
        self.file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|_| EvidenceError::Io)
    }

    fn read_exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.stats.max_read_request = self.stats.max_read_request.max(bytes.len());
        self.stats.read_calls = self
            .stats
            .read_calls
            .checked_add(1)
            .ok_or(EvidenceError::Bounds)?;
        self.stats.bytes_read = self
            .stats
            .bytes_read
            .checked_add(u64::try_from(bytes.len()).map_err(|_| EvidenceError::Bounds)?)
            .ok_or(EvidenceError::Bounds)?;
        self.file
            .read_exact(bytes)
            .map_err(|_| EvidenceError::InvalidSegment)
    }

    fn read_exact_source(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.stats.max_read_request = self.stats.max_read_request.max(bytes.len());
        self.stats.read_calls = self
            .stats
            .read_calls
            .checked_add(1)
            .ok_or(EvidenceError::Bounds)?;
        self.stats.bytes_read = self
            .stats
            .bytes_read
            .checked_add(u64::try_from(bytes.len()).map_err(|_| EvidenceError::Bounds)?)
            .ok_or(EvidenceError::Bounds)?;
        self.file
            .read_exact(bytes)
            .map_err(|_| EvidenceError::InvalidSource)
    }

    fn seek(&mut self, offset: u64) -> Result<()> {
        self.file
            .seek(SeekFrom::Start(offset))
            .map(|_| ())
            .map_err(|_| EvidenceError::Io)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Layout {
    series_offset: u64,
    series_length: u64,
    blocks_offset: u64,
    blocks_length: u64,
    append_offset: u64,
    append_length: u64,
    recent_offset: u64,
    recent_length: u64,
    artifact_length: u64,
}

impl Layout {
    fn new(meta: &FixtureMeta) -> Result<Self> {
        meta.validate()?;
        let blocks_length = meta
            .source_length
            .checked_sub(JOURNAL_V1_HEADER_LEN as u64)
            .ok_or(EvidenceError::Bounds)?;
        let series_length = checked_product(meta.series_count, SEGMENT_V1_SERIES_ENTRY_LEN)?;
        let series_offset = SEGMENT_V1_HEADER_LEN as u64;
        let blocks_offset = series_offset
            .checked_add(series_length)
            .ok_or(EvidenceError::Bounds)?;
        let append_offset = blocks_offset
            .checked_add(blocks_length)
            .ok_or(EvidenceError::Bounds)?;
        let append_length = checked_product(meta.frame_count, SEGMENT_V1_APPEND_ENTRY_LEN)?;
        let recent_offset = append_offset
            .checked_add(append_length)
            .ok_or(EvidenceError::Bounds)?;
        let recent_length =
            checked_product(meta.observation_count, SEGMENT_V1_OBSERVATION_ENTRY_LEN)?;
        let artifact_length = recent_offset
            .checked_add(recent_length)
            .and_then(|length| length.checked_add(SEGMENT_V1_CRC_LEN as u64))
            .ok_or(EvidenceError::Bounds)?;
        if artifact_length > MAX_SEGMENT_V1_BYTES {
            return Err(EvidenceError::Bounds);
        }
        Ok(Self {
            series_offset,
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

struct Preflight {
    meta: FixtureMeta,
    layout: Layout,
    appends: Vec<FrameMeta>,
    series_order: Vec<FrameMeta>,
    observations: Vec<ObservationWork>,
    source_stats: IoStats,
    max_frame_capacity: usize,
    max_reencode_capacity: usize,
    metadata_bytes: u64,
    _controlled: ControlledGuard,
}

impl Preflight {
    #[allow(clippy::too_many_lines)]
    fn run(meta: FixtureMeta, raw: File) -> Result<Self> {
        meta.validate()?;
        if meta.frame_count > MAX_FRAMES
            || meta.series_count > MAX_SERIES
            || meta.observation_count > MAX_OBSERVATIONS
        {
            return Err(EvidenceError::Bounds);
        }
        let controlled = ControlledGuard::acquire(crate::ledger::CONTROLLED_BASE_BYTES)?;
        let mut source = CountedFile::new(raw);
        if source.length()? != meta.source_length {
            return Err(EvidenceError::InvalidSource);
        }
        let mut header = [0_u8; JOURNAL_V1_HEADER_LEN];
        source.read_exact_source(&mut header)?;
        let decoded_header =
            JournalHeaderV1::decode(&header).map_err(|_| EvidenceError::InvalidSource)?;
        if decoded_header.store_id() != meta.store_id {
            return Err(EvidenceError::InvalidSource);
        }
        let mut source_crc = Crc32c::new();
        source_crc.update(&header);
        let mut appends = Vec::with_capacity(meta.frame_count);
        let mut observations = Vec::with_capacity(meta.observation_count);
        ensure_capacity(appends.capacity(), MAX_FRAMES)?;
        ensure_capacity(observations.capacity(), MAX_OBSERVATIONS)?;
        let mut previous = if meta.sequence_floor == 0 {
            None
        } else {
            Some(
                AppendSequenceV1::new(meta.sequence_floor)
                    .map_err(|_| EvidenceError::InvalidSource)?,
            )
        };
        let mut source_offset = JOURNAL_V1_HEADER_LEN as u64;
        let mut max_frame_capacity = 0_usize;
        let mut max_reencode_capacity = 0_usize;
        for append_index in 0..meta.frame_count {
            let mut prefix = [0_u8; JOURNAL_V1_FRAME_PREFIX_LEN];
            source.read_exact_source(&mut prefix)?;
            let frame_length = frame_length(&prefix)?;
            let frame_length_u64 =
                u64::try_from(frame_length).map_err(|_| EvidenceError::Bounds)?;
            let frame_end = source_offset
                .checked_add(frame_length_u64)
                .ok_or(EvidenceError::Bounds)?;
            if frame_end > meta.source_length {
                return Err(EvidenceError::InvalidSource);
            }
            let mut frame = vec![0_u8; frame_length];
            ensure_capacity(frame.capacity(), MAX_FRAME_BYTES)?;
            frame[..JOURNAL_V1_FRAME_PREFIX_LEN].copy_from_slice(&prefix);
            source.read_exact_source(&mut frame[JOURNAL_V1_FRAME_PREFIX_LEN..])?;
            source_crc.update(&frame);
            max_frame_capacity = max_frame_capacity.max(frame.capacity());
            let decoded = decode_admission_frame_v1(&frame, DecodeLimitsV1::maximum(), previous)
                .map_err(|_| EvidenceError::InvalidSource)?;
            let reencoded = encode_decoded_admission_frame_v1(&decoded)
                .map_err(|_| EvidenceError::InvalidSource)?;
            ensure_capacity(reencoded.capacity(), MAX_FRAME_BYTES)?;
            max_reencode_capacity = max_reencode_capacity.max(reencoded.capacity());
            if reencoded != frame
                || decoded.store_id() != meta.store_id
                || decoded.declaration().store_id() != meta.store_id
                || decoded.declaration().series_id() != decoded.envelope().series().series_id()
                || decoded.declaration().series_id() != decoded.retry().series_id()
            {
                return Err(EvidenceError::InvalidSource);
            }
            let series_id = *decoded.declaration().series_id().as_bytes();
            let observation_start = observations.len();
            for (ordinal, observation) in decoded.envelope().observations().iter().enumerate() {
                if observations.len() >= meta.observation_count {
                    return Err(EvidenceError::Bounds);
                }
                let key = observation.raw_order_key();
                observations.push(ObservationWork {
                    series_id,
                    observation_id: *key.observation_id().as_bytes(),
                    effective_seconds: key.effective().unix_seconds(),
                    receive_seconds: key.receive().unix_seconds(),
                    append_sequence: decoded.append_sequence(),
                    frame_offset: 0,
                    frame_length: frame_length_u64,
                    effective_nanos: key.effective().nanosecond(),
                    receive_nanos: key.receive().nanosecond(),
                    observation_ordinal: u32::try_from(ordinal)
                        .map_err(|_| EvidenceError::Bounds)?,
                    frame_ordinal: 0,
                    reserved: 0,
                });
                ensure_capacity(observations.capacity(), MAX_OBSERVATIONS)?;
            }
            let observation_count = observations
                .len()
                .checked_sub(observation_start)
                .ok_or(EvidenceError::Bounds)?;
            if observations.len() > meta.observation_count
                || observation_count > och_core::MAX_SOURCE_OBSERVATION_CONTEXTS
            {
                return Err(EvidenceError::Bounds);
            }
            appends.push(FrameMeta {
                series_id,
                append_sequence: decoded.append_sequence(),
                source_offset,
                frame_length: frame_length_u64,
                segment_offset: 0,
                append_index: u32::try_from(append_index).map_err(|_| EvidenceError::Bounds)?,
                frame_ordinal: 0,
                observation_start: u32::try_from(observation_start)
                    .map_err(|_| EvidenceError::Bounds)?,
                observation_count: u16::try_from(observation_count)
                    .map_err(|_| EvidenceError::Bounds)?,
                reserved: 0,
            });
            ensure_capacity(appends.capacity(), MAX_FRAMES)?;
            previous = Some(
                AppendSequenceV1::new(decoded.append_sequence())
                    .map_err(|_| EvidenceError::InvalidSource)?,
            );
            source_offset = frame_end;
        }
        if source_offset != meta.source_length
            || source_crc.finish() != meta.source_checksum
            || observations.len() != meta.observation_count
            || previous.map(AppendSequenceV1::get) != Some(meta.sequence_cutoff)
        {
            return Err(EvidenceError::InvalidSource);
        }
        let mut series_order = appends.clone();
        ensure_capacity(series_order.capacity(), MAX_FRAMES)?;
        series_order.sort_unstable_by(|left, right| {
            left.series_id
                .cmp(&right.series_id)
                .then_with(|| left.append_sequence.cmp(&right.append_sequence))
        });
        let actual_series = series_order
            .iter()
            .enumerate()
            .filter(|(index, frame)| {
                *index == 0 || series_order[*index - 1].series_id != frame.series_id
            })
            .count();
        if actual_series != meta.series_count {
            return Err(EvidenceError::InvalidSource);
        }
        let layout = Layout::new(&meta)?;
        let mut segment_offset = layout.blocks_offset;
        let mut prior_series = None;
        let mut frame_ordinal = 0_u32;
        for frame in &mut series_order {
            if prior_series != Some(frame.series_id) {
                frame_ordinal = 0;
                prior_series = Some(frame.series_id);
            }
            frame.segment_offset = segment_offset;
            frame.frame_ordinal = frame_ordinal;
            let append = appends
                .get_mut(usize::try_from(frame.append_index).map_err(|_| EvidenceError::Bounds)?)
                .ok_or(EvidenceError::InvalidSource)?;
            append.segment_offset = segment_offset;
            append.frame_ordinal = frame_ordinal;
            let start =
                usize::try_from(frame.observation_start).map_err(|_| EvidenceError::Bounds)?;
            let end = start
                .checked_add(usize::from(frame.observation_count))
                .ok_or(EvidenceError::Bounds)?;
            for observation in observations
                .get_mut(start..end)
                .ok_or(EvidenceError::InvalidSource)?
            {
                observation.frame_offset = segment_offset;
                observation.frame_ordinal = frame_ordinal;
            }
            segment_offset = segment_offset
                .checked_add(frame.frame_length)
                .ok_or(EvidenceError::Bounds)?;
            frame_ordinal = frame_ordinal.checked_add(1).ok_or(EvidenceError::Bounds)?;
        }
        if segment_offset != layout.append_offset {
            return Err(EvidenceError::InvalidSource);
        }
        // The comparator is total because series, append sequence, and ordinal
        // identify each source observation. Unstable sort therefore preserves
        // exact canonical bytes without allocating a hidden merge buffer.
        observations.sort_unstable_by(recent_order);
        let metadata_bytes = actual_metadata_bytes(
            appends.capacity(),
            series_order.capacity(),
            observations.capacity(),
        )?;
        Ok(Self {
            meta,
            layout,
            appends,
            series_order,
            observations,
            source_stats: source.stats,
            max_frame_capacity,
            max_reencode_capacity,
            metadata_bytes,
            _controlled: controlled,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OperationReport {
    pub(crate) identity: SegmentIdentity,
    pub(crate) source_stats: IoStats,
    pub(crate) segment_stats: IoStats,
    pub(crate) max_frame_buffer_bytes: usize,
    pub(crate) max_reencode_buffer_bytes: usize,
    pub(crate) metadata_ledger_bytes: u64,
    pub(crate) controlled_bytes_after: u64,
    pub(crate) external_workspace_bytes: u64,
}

impl OperationReport {
    pub(crate) fn print(self, operation: &str, meta: &FixtureMeta) {
        println!("schema=och-v2-evidence-operation-v1");
        println!("operation={operation}");
        println!("case={}", meta.case);
        println!("seed={}", meta.seed);
        println!("source_bytes={}", meta.source_length);
        println!("segment_bytes={}", self.identity.artifact_length);
        println!("frames={}", meta.frame_count);
        println!("series={}", meta.series_count);
        println!("observations={}", meta.observation_count);
        println!("source_read_bytes={}", self.source_stats.bytes_read);
        println!("segment_read_bytes={}", self.segment_stats.bytes_read);
        println!(
            "max_read_request_bytes={}",
            self.source_stats
                .max_read_request
                .max(self.segment_stats.max_read_request)
        );
        println!("max_frame_buffer_bytes={}", self.max_frame_buffer_bytes);
        println!(
            "max_reencode_buffer_bytes={}",
            self.max_reencode_buffer_bytes
        );
        println!("metadata_ledger_bytes={}", self.metadata_ledger_bytes);
        println!("controlled_bytes_after={}", self.controlled_bytes_after);
        println!(
            "external_sort_workspace_bytes={}",
            self.external_workspace_bytes
        );
        println!("segment_crc32c={:08x}", self.identity.artifact_checksum);
    }
}

pub(crate) fn build(
    root: &EvidenceRoot,
    case: &str,
    keep_on_failure: bool,
) -> Result<OperationReport> {
    root.ensure_layout()?;
    let files = root.pr03c_case(case)?;
    let meta = FixtureMeta::read(files.open_fixture_meta()?)?;
    if meta.case != case {
        return Err(EvidenceError::InvalidFixture);
    }
    let preflight = Preflight::run(meta.clone(), files.open_raw()?)?;
    let virtual_emission = emit(&preflight, files.open_raw()?, None)?;
    files.reset_segment()?;
    let result: Result<OperationReport> = (|| {
        let output = files.create_segment_partial()?;
        let emitted = emit(&preflight, files.open_raw()?, Some(output))?;
        if emitted.identity != virtual_emission.identity {
            return Err(EvidenceError::InvalidSegment);
        }
        files.publish_segment()?;
        files.write_segment_identity(emitted.identity.encode().as_bytes())?;
        let mut source_stats = preflight.source_stats;
        source_stats.add(virtual_emission.source_stats)?;
        source_stats.add(emitted.source_stats)?;
        Ok(OperationReport {
            identity: emitted.identity,
            source_stats,
            segment_stats: IoStats::default(),
            max_frame_buffer_bytes: preflight
                .max_frame_capacity
                .max(virtual_emission.max_frame_capacity)
                .max(emitted.max_frame_capacity),
            max_reencode_buffer_bytes: preflight.max_reencode_capacity,
            metadata_ledger_bytes: preflight.metadata_bytes,
            controlled_bytes_after: active_controlled_bytes(),
            external_workspace_bytes: 0,
        })
    })();
    if result.is_err() && !keep_on_failure {
        let _ = files.remove_segment_partial();
    }
    drop(preflight);
    let controlled_bytes_after = active_controlled_bytes();
    if controlled_bytes_after != 0 {
        return Err(EvidenceError::Bounds);
    }
    let mut report = result?;
    report.controlled_bytes_after = controlled_bytes_after;
    Ok(report)
}

struct Emission {
    identity: SegmentIdentity,
    source_stats: IoStats,
    max_frame_capacity: usize,
}

struct SegmentSink {
    output: Option<File>,
    checksum: Crc32c,
    body_length: u64,
}

impl SegmentSink {
    const fn new(output: Option<File>) -> Self {
        Self {
            output,
            checksum: Crc32c::new(),
            body_length: 0,
        }
    }

    fn write_body(&mut self, bytes: &[u8]) -> Result<()> {
        self.body_length = self
            .body_length
            .checked_add(u64::try_from(bytes.len()).map_err(|_| EvidenceError::Bounds)?)
            .ok_or(EvidenceError::Bounds)?;
        self.checksum.update(bytes);
        if let Some(output) = &mut self.output {
            output.write_all(bytes).map_err(|_| EvidenceError::Io)?;
        }
        Ok(())
    }

    fn finish(mut self, preflight: &Preflight) -> Result<SegmentIdentity> {
        let checksum = self.checksum.finish();
        if let Some(output) = &mut self.output {
            output
                .write_all(&checksum.to_be_bytes())
                .map_err(|_| EvidenceError::Io)?;
            output.sync_all().map_err(|_| EvidenceError::Io)?;
        }
        let artifact_length = self
            .body_length
            .checked_add(SEGMENT_V1_CRC_LEN as u64)
            .ok_or(EvidenceError::Bounds)?;
        if artifact_length != preflight.layout.artifact_length {
            return Err(EvidenceError::InvalidSegment);
        }
        Ok(SegmentIdentity {
            artifact_length,
            artifact_checksum: checksum,
            source_length: preflight.meta.source_length,
            source_checksum: preflight.meta.source_checksum,
        })
    }
}

fn emit(preflight: &Preflight, raw: File, output: Option<File>) -> Result<Emission> {
    let mut sink = SegmentSink::new(output);
    let header = encode_header(&preflight.meta, preflight.layout)?;
    sink.write_body(&header)?;
    emit_series_entries(preflight, |entry| sink.write_body(&entry))?;
    let mut source = CountedFile::new(raw);
    let mut frame_buffer = Vec::new();
    let mut max_frame_capacity = 0_usize;
    for frame in &preflight.series_order {
        let length = usize::try_from(frame.frame_length).map_err(|_| EvidenceError::Bounds)?;
        resize_frame_buffer(&mut frame_buffer, length)?;
        max_frame_capacity = max_frame_capacity.max(frame_buffer.capacity());
        source.seek(frame.source_offset)?;
        source.read_exact_source(&mut frame_buffer)?;
        sink.write_body(&frame_buffer)?;
    }
    for frame in &preflight.appends {
        sink.write_body(&encode_append_entry(*frame))?;
    }
    for observation in &preflight.observations {
        sink.write_body(&encode_observation_entry(*observation))?;
    }
    let identity = sink.finish(preflight)?;
    Ok(Emission {
        identity,
        source_stats: source.stats,
        max_frame_capacity,
    })
}

pub(crate) fn validate(root: &EvidenceRoot, case: &str) -> Result<OperationReport> {
    let files = root.pr03c_case(case)?;
    let meta = FixtureMeta::read(files.open_fixture_meta()?)?;
    if meta.case != case {
        return Err(EvidenceError::InvalidFixture);
    }
    let identity = SegmentIdentity::read(files.open_segment_identity()?)?;
    if identity.source_length != meta.source_length
        || identity.source_checksum != meta.source_checksum
    {
        return Err(EvidenceError::InvalidFixture);
    }
    let preflight = Preflight::run(meta.clone(), files.open_raw()?)?;
    let result: Result<OperationReport> = (|| {
        let validation = validate_pair(
            &preflight,
            files.open_raw()?,
            files.open_segment()?,
            identity,
        )?;
        let mut source_stats = preflight.source_stats;
        source_stats.add(validation.source_stats)?;
        Ok(OperationReport {
            identity,
            source_stats,
            segment_stats: validation.segment_stats,
            max_frame_buffer_bytes: preflight
                .max_frame_capacity
                .max(validation.max_frame_capacity),
            max_reencode_buffer_bytes: preflight
                .max_reencode_capacity
                .max(validation.max_reencode_capacity),
            metadata_ledger_bytes: preflight.metadata_bytes,
            controlled_bytes_after: active_controlled_bytes(),
            external_workspace_bytes: 0,
        })
    })();
    drop(preflight);
    let controlled_bytes_after = active_controlled_bytes();
    if controlled_bytes_after != 0 {
        return Err(EvidenceError::Bounds);
    }
    let mut report = result?;
    report.controlled_bytes_after = controlled_bytes_after;
    Ok(report)
}

struct Validation {
    source_stats: IoStats,
    segment_stats: IoStats,
    max_frame_capacity: usize,
    max_reencode_capacity: usize,
}

#[allow(clippy::too_many_lines)]
fn validate_pair(
    preflight: &Preflight,
    raw: File,
    segment: File,
    identity: SegmentIdentity,
) -> Result<Validation> {
    let mut segment = CountedFile::new(segment);
    if segment.length()? != preflight.layout.artifact_length
        || identity.artifact_length != preflight.layout.artifact_length
    {
        return Err(EvidenceError::InvalidSegment);
    }
    let mut crc = Crc32c::new();
    let mut header = [0_u8; SEGMENT_V1_HEADER_LEN];
    segment.read_exact(&mut header)?;
    crc.update(&header);
    if header != encode_header(&preflight.meta, preflight.layout)? {
        return Err(EvidenceError::InvalidSegment);
    }
    emit_series_entries(preflight, |expected| {
        let mut actual = [0_u8; SEGMENT_V1_SERIES_ENTRY_LEN];
        segment.read_exact(&mut actual)?;
        crc.update(&actual);
        if actual != expected {
            return Err(EvidenceError::InvalidSegment);
        }
        Ok(())
    })?;
    let mut raw = CountedFile::new(raw);
    let mut frame_buffer = Vec::new();
    let mut scratch = vec![0_u8; SCRATCH_BYTES];
    ensure_capacity(scratch.capacity(), SCRATCH_BYTES)?;
    let mut max_frame_capacity = 0_usize;
    let mut max_reencode_capacity = 0_usize;
    for frame in &preflight.series_order {
        let length = usize::try_from(frame.frame_length).map_err(|_| EvidenceError::Bounds)?;
        resize_frame_buffer(&mut frame_buffer, length)?;
        max_frame_capacity = max_frame_capacity.max(frame_buffer.capacity());
        segment.read_exact(&mut frame_buffer)?;
        crc.update(&frame_buffer);
        let decoded = decode_admission_frame_v1(&frame_buffer, DecodeLimitsV1::maximum(), None)
            .map_err(|_| EvidenceError::InvalidSegment)?;
        let reencoded = encode_decoded_admission_frame_v1(&decoded)
            .map_err(|_| EvidenceError::InvalidSegment)?;
        ensure_capacity(reencoded.capacity(), MAX_FRAME_BYTES)?;
        max_reencode_capacity = max_reencode_capacity.max(reencoded.capacity());
        if reencoded != frame_buffer
            || decoded.store_id() != preflight.meta.store_id
            || decoded.declaration().store_id() != preflight.meta.store_id
            || decoded.declaration().series_id().as_bytes() != &frame.series_id
            || decoded.envelope().series().series_id().as_bytes() != &frame.series_id
            || decoded.retry().series_id().as_bytes() != &frame.series_id
            || decoded.append_sequence() != frame.append_sequence
        {
            return Err(EvidenceError::InvalidSegment);
        }
        raw.seek(frame.source_offset)?;
        let mut compared = 0_usize;
        while compared < length {
            let amount = (length - compared).min(scratch.len());
            raw.read_exact_source(&mut scratch[..amount])?;
            if scratch[..amount] != frame_buffer[compared..compared + amount] {
                return Err(EvidenceError::InvalidSegment);
            }
            compared = compared.checked_add(amount).ok_or(EvidenceError::Bounds)?;
        }
    }
    for frame in &preflight.appends {
        let mut actual = [0_u8; SEGMENT_V1_APPEND_ENTRY_LEN];
        segment.read_exact(&mut actual)?;
        crc.update(&actual);
        if actual != encode_append_entry(*frame) {
            return Err(EvidenceError::InvalidSegment);
        }
    }
    for observation in &preflight.observations {
        let mut actual = [0_u8; SEGMENT_V1_OBSERVATION_ENTRY_LEN];
        segment.read_exact(&mut actual)?;
        crc.update(&actual);
        if actual != encode_observation_entry(*observation) {
            return Err(EvidenceError::InvalidSegment);
        }
    }
    let mut trailer = [0_u8; SEGMENT_V1_CRC_LEN];
    segment.read_exact(&mut trailer)?;
    let checksum = u32::from_be_bytes(trailer);
    if checksum != crc.finish() || checksum != identity.artifact_checksum {
        return Err(EvidenceError::InvalidSegment);
    }
    Ok(Validation {
        source_stats: raw.stats,
        segment_stats: segment.stats,
        max_frame_capacity,
        max_reencode_capacity,
    })
}

fn emit_series_entries(
    preflight: &Preflight,
    mut emit: impl FnMut([u8; SEGMENT_V1_SERIES_ENTRY_LEN]) -> Result<()>,
) -> Result<()> {
    let mut frame_index = 0_usize;
    let mut observation_index = 0_usize;
    let mut block_offset = preflight.layout.blocks_offset;
    let mut recent_offset = preflight.layout.recent_offset;
    let mut emitted = 0_usize;
    while frame_index < preflight.series_order.len() {
        let series_id = preflight.series_order[frame_index].series_id;
        let start = frame_index;
        let mut block_length = 0_u64;
        while frame_index < preflight.series_order.len()
            && preflight.series_order[frame_index].series_id == series_id
        {
            block_length = block_length
                .checked_add(preflight.series_order[frame_index].frame_length)
                .ok_or(EvidenceError::Bounds)?;
            frame_index += 1;
        }
        let observation_start = observation_index;
        while observation_index < preflight.observations.len()
            && preflight.observations[observation_index].series_id == series_id
        {
            observation_index += 1;
        }
        let frame_count = frame_index
            .checked_sub(start)
            .ok_or(EvidenceError::Bounds)?;
        let observation_count = observation_index
            .checked_sub(observation_start)
            .ok_or(EvidenceError::Bounds)?;
        let recent_length = checked_product(observation_count, SEGMENT_V1_OBSERVATION_ENTRY_LEN)?;
        let mut entry = [0_u8; SEGMENT_V1_SERIES_ENTRY_LEN];
        entry[..16].copy_from_slice(&series_id);
        put_u64(&mut entry, 16, block_offset);
        put_u64(&mut entry, 24, block_length);
        put_u32(&mut entry, 32, to_u32(frame_count)?);
        put_u32(&mut entry, 36, to_u32(observation_count)?);
        put_u64(&mut entry, 40, recent_offset);
        put_u64(&mut entry, 48, recent_length);
        emit(entry)?;
        block_offset = block_offset
            .checked_add(block_length)
            .ok_or(EvidenceError::Bounds)?;
        recent_offset = recent_offset
            .checked_add(recent_length)
            .ok_or(EvidenceError::Bounds)?;
        emitted += 1;
    }
    if emitted != preflight.meta.series_count
        || block_offset != preflight.layout.append_offset
        || recent_offset + SEGMENT_V1_CRC_LEN as u64 != preflight.layout.artifact_length
        || observation_index != preflight.observations.len()
    {
        return Err(EvidenceError::InvalidSource);
    }
    Ok(())
}

fn encode_header(meta: &FixtureMeta, layout: Layout) -> Result<[u8; SEGMENT_V1_HEADER_LEN]> {
    let mut bytes = [0_u8; SEGMENT_V1_HEADER_LEN];
    bytes[..8].copy_from_slice(&SEGMENT_V1_MAGIC);
    put_u16(&mut bytes, 8, SEGMENT_V1_VERSION);
    put_u16(
        &mut bytes,
        10,
        u16::try_from(SEGMENT_V1_HEADER_LEN).map_err(|_| EvidenceError::Bounds)?,
    );
    bytes[16..32].copy_from_slice(meta.store_id.as_bytes());
    put_u64(&mut bytes, 32, meta.journal_generation);
    put_u64(&mut bytes, 40, meta.sequence_floor);
    put_u64(&mut bytes, 48, meta.sequence_cutoff);
    put_u64(&mut bytes, 56, meta.registry_generation);
    put_u64(&mut bytes, 64, meta.source_length);
    put_u64(&mut bytes, 72, meta.source_length);
    put_u32(&mut bytes, 80, meta.source_checksum);
    put_u32(&mut bytes, 84, to_u32(meta.frame_count)?);
    put_u32(&mut bytes, 88, to_u32(meta.series_count)?);
    put_u32(&mut bytes, 92, to_u32(meta.observation_count)?);
    put_u64(&mut bytes, 96, layout.series_offset);
    put_u64(&mut bytes, 104, layout.series_length);
    put_u64(&mut bytes, 112, layout.blocks_offset);
    put_u64(&mut bytes, 120, layout.blocks_length);
    put_u64(&mut bytes, 128, layout.append_offset);
    put_u64(&mut bytes, 136, layout.append_length);
    put_u64(&mut bytes, 144, layout.recent_offset);
    put_u64(&mut bytes, 152, layout.recent_length);
    put_u64(&mut bytes, 160, layout.artifact_length);
    Ok(bytes)
}

fn encode_append_entry(frame: FrameMeta) -> [u8; SEGMENT_V1_APPEND_ENTRY_LEN] {
    let mut bytes = [0_u8; SEGMENT_V1_APPEND_ENTRY_LEN];
    put_u64(&mut bytes, 0, frame.append_sequence);
    bytes[8..24].copy_from_slice(&frame.series_id);
    put_u64(&mut bytes, 24, frame.segment_offset);
    put_u64(&mut bytes, 32, frame.frame_length);
    put_u32(&mut bytes, 40, frame.frame_ordinal);
    bytes
}

fn encode_observation_entry(
    observation: ObservationWork,
) -> [u8; SEGMENT_V1_OBSERVATION_ENTRY_LEN] {
    let mut bytes = [0_u8; SEGMENT_V1_OBSERVATION_ENTRY_LEN];
    bytes[..16].copy_from_slice(&observation.series_id);
    bytes[16..24].copy_from_slice(&observation.effective_seconds.to_be_bytes());
    put_u32(&mut bytes, 24, observation.effective_nanos);
    bytes[28..36].copy_from_slice(&observation.receive_seconds.to_be_bytes());
    put_u32(&mut bytes, 36, observation.receive_nanos);
    bytes[40..56].copy_from_slice(&observation.observation_id);
    put_u64(&mut bytes, 56, observation.append_sequence);
    put_u32(&mut bytes, 64, observation.observation_ordinal);
    put_u32(&mut bytes, 68, observation.frame_ordinal);
    put_u64(&mut bytes, 72, observation.frame_offset);
    put_u64(&mut bytes, 80, observation.frame_length);
    bytes
}

fn recent_order(left: &ObservationWork, right: &ObservationWork) -> Ordering {
    left.series_id.cmp(&right.series_id).then_with(|| {
        raw_key(right)
            .cmp(&raw_key(left))
            .then_with(|| right.append_sequence.cmp(&left.append_sequence))
            .then_with(|| left.observation_ordinal.cmp(&right.observation_ordinal))
    })
}

fn raw_key(observation: &ObservationWork) -> (i64, u32, i64, u32, [u8; 16]) {
    (
        observation.effective_seconds,
        observation.effective_nanos,
        observation.receive_seconds,
        observation.receive_nanos,
        observation.observation_id,
    )
}

fn frame_length(prefix: &[u8; JOURNAL_V1_FRAME_PREFIX_LEN]) -> Result<usize> {
    if prefix[..4] != JOURNAL_V1_FRAME_MAGIC
        || u16::from_be_bytes([prefix[4], prefix[5]]) != JOURNAL_V1_VERSION
        || prefix[6] != 1
        || prefix[7] != 0
        || u64::from_be_bytes(
            prefix[8..16]
                .try_into()
                .map_err(|_| EvidenceError::Bounds)?,
        ) == 0
    {
        return Err(EvidenceError::InvalidSource);
    }
    let payload = usize::try_from(u32::from_be_bytes(
        prefix[16..20]
            .try_into()
            .map_err(|_| EvidenceError::Bounds)?,
    ))
    .map_err(|_| EvidenceError::Bounds)?;
    let length = JOURNAL_V1_FRAME_PREFIX_LEN
        .checked_add(payload)
        .and_then(|value| value.checked_add(JOURNAL_V1_FRAME_CRC_LEN))
        .ok_or(EvidenceError::Bounds)?;
    if payload > MAX_ADMISSION_PAYLOAD_V1 || length > MAX_FRAME_BYTES {
        return Err(EvidenceError::Bounds);
    }
    Ok(length)
}

fn checked_product(count: usize, entry_length: usize) -> Result<u64> {
    count
        .checked_mul(entry_length)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(EvidenceError::Bounds)
}

fn resize_frame_buffer(buffer: &mut Vec<u8>, length: usize) -> Result<()> {
    if length > MAX_FRAME_BYTES {
        return Err(EvidenceError::Bounds);
    }
    if length > buffer.capacity() {
        *buffer = vec![0_u8; length];
    } else {
        buffer.resize(length, 0);
    }
    if buffer.capacity() > MAX_FRAME_BYTES {
        return Err(EvidenceError::Bounds);
    }
    Ok(())
}

fn ensure_capacity(actual: usize, maximum: usize) -> Result<()> {
    if actual > maximum {
        Err(EvidenceError::Bounds)
    } else {
        Ok(())
    }
}

fn to_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| EvidenceError::Bounds)
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
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn repair_segment_checksum(bytes: &mut [u8]) {
        let offset = bytes.len() - SEGMENT_V1_CRC_LEN;
        let mut crc = Crc32c::new();
        crc.update(&bytes[..offset]);
        bytes[offset..].copy_from_slice(&crc.finish().to_be_bytes());
    }
}
