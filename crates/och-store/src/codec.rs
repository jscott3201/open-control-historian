use crate::decoded::{
    DecodedAdmissionV1, DecodedDeclarationV1, DecodedEvidenceV1, DecodedObservationLineageV1,
    all_evidence_ids,
};
use crate::{
    JOURNAL_V1_FRAME_CRC_LEN, JOURNAL_V1_FRAME_MAGIC, JOURNAL_V1_FRAME_PREFIX_LEN,
    JOURNAL_V1_HEADER_LEN, JOURNAL_V1_HEADER_MAGIC, JOURNAL_V1_VERSION, JournalV1Error,
    MAX_ADMISSION_PAYLOAD_V1,
};
use och_core::{
    ArtifactId, ArtifactReference, CanonicalAdmission, CaptureLifecycle, CaptureRunEvidence,
    CollectionEnvelope, CollectionMode, ContentFormat, ContentIdentity, ContentVersion,
    DeclarationEvidence, DeclarationReference, DeclarationRevision, EvidenceId, EvidenceKind,
    ExactText, ExactValue, Gap, GapReason, NativeStatus, NativeStatusToken, NoChange,
    NormalizedRecordEvidence, Observation, ObservationId, ObservationTimes, ProducerEpoch,
    ProducerId, ProducerPosition, ProducerSequence, Quality, QualityFlags, QualityLevel,
    QuantityEvidence, RawRecordEvidence, RealBits, RetryKey, RetryQualification, SeriesBinding,
    SeriesDeclaration, SeriesDeclarationPayload, SeriesId, SeriesMetadata, SourceBatchMetadata,
    SourceEndpointEvidence, SourceGapEvidence, SourceGapReason, SourceIdempotency,
    SourceIntervalKind, SourceObservationEvidence, SourceObservationLineage, SourceProjection,
    SourceReference, SourceSchemaIdentity, SourceSchemaVersion, SourceSnapshotEvidence,
    SourceSystemEvidence, SourceTransport, StateClass, StateMember, StateValue, StoreId,
    TimeInterval, Timestamp, Unavailable, UnavailableReason, UnitEvidence, ValueFamily,
};
use std::fmt;

const HEADER_LENGTH_OFFSET: usize = 10;
const FRAME_KIND_ADMISSION: u8 = 1;
const FRAME_FLAGS_NONE: u8 = 0;
const FRAME_SEQUENCE_OFFSET: usize = 8;
const FRAME_PAYLOAD_LENGTH_OFFSET: usize = 16;

const MAX_DECLARATION_REFERENCE_BYTES: usize = 4 * 1_024;
const MAX_EXACT_TEXT_BYTES: usize = 4 * 4_096;
const MAX_PORTABLE_TOKEN_BYTES: usize = 256;
const MAX_CONTENT_FORMAT_BYTES: usize = 64;
const MAX_RETRY_KEY_BYTES: usize = 128;
const MAX_OBSERVATIONS: usize = 256;
const MAX_GAPS: usize = 64;
const MAX_NATIVE_STATUS_TOKENS: usize = 16;

/// Positive monotonically ordered Journal V1 append sequence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AppendSequenceV1(u64);

impl AppendSequenceV1 {
    /// Validates a positive append sequence.
    ///
    /// # Errors
    ///
    /// Returns [`JournalV1Error::InvalidAppendSequence`] for zero.
    pub const fn new(value: u64) -> Result<Self, JournalV1Error> {
        if value == 0 {
            return Err(JournalV1Error::InvalidAppendSequence);
        }
        Ok(Self(value))
    }

    /// Returns the exact numeric sequence.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    fn successor(self) -> Result<Self, JournalV1Error> {
        let next = self
            .0
            .checked_add(1)
            .ok_or(JournalV1Error::AppendSequenceOverflow)?;
        Ok(Self(next))
    }
}

/// Caller-selected decode bound below the fixed Journal V1 hard maximum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeLimitsV1 {
    max_payload_len: usize,
}

impl DecodeLimitsV1 {
    /// Validates a configured admission-payload byte maximum.
    ///
    /// Zero is valid and rejects every admission frame before payload parsing.
    ///
    /// # Errors
    ///
    /// Returns [`JournalV1Error::PayloadTooLarge`] above
    /// [`MAX_ADMISSION_PAYLOAD_V1`].
    pub const fn new(max_payload_len: usize) -> Result<Self, JournalV1Error> {
        if max_payload_len > MAX_ADMISSION_PAYLOAD_V1 {
            return Err(JournalV1Error::PayloadTooLarge);
        }
        Ok(Self { max_payload_len })
    }

    /// Returns the configured maximum admission payload length.
    #[must_use]
    pub const fn max_payload_len(self) -> usize {
        self.max_payload_len
    }

    /// Returns the Journal V1 hard maximum.
    #[must_use]
    pub const fn maximum() -> Self {
        Self {
            max_payload_len: MAX_ADMISSION_PAYLOAD_V1,
        }
    }
}

/// Fixed Journal V1 file header scoped to one exact store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalHeaderV1 {
    store_id: StoreId,
}

impl JournalHeaderV1 {
    /// Constructs the fixed header for `store_id`.
    #[must_use]
    pub const fn new(store_id: StoreId) -> Self {
        Self { store_id }
    }

    /// Returns the exact store identity carried by the header.
    #[must_use]
    pub const fn store_id(self) -> StoreId {
        self.store_id
    }

    /// Encodes the exact fixed-length Journal V1 header.
    #[must_use]
    pub fn encode(self) -> [u8; JOURNAL_V1_HEADER_LEN] {
        let mut bytes = [0_u8; JOURNAL_V1_HEADER_LEN];
        bytes[..8].copy_from_slice(&JOURNAL_V1_HEADER_MAGIC);
        bytes[8..10].copy_from_slice(&JOURNAL_V1_VERSION.to_be_bytes());
        bytes[HEADER_LENGTH_OFFSET..12].copy_from_slice(&28_u16.to_be_bytes());
        bytes[12..].copy_from_slice(self.store_id.as_bytes());
        bytes
    }

    /// Decodes exactly one fixed-length Journal V1 header.
    ///
    /// # Errors
    ///
    /// Rejects truncation, trailing bytes, unknown magic/version/length, or an
    /// invalid store identity.
    pub fn decode(bytes: &[u8]) -> Result<Self, JournalV1Error> {
        if bytes.len() < JOURNAL_V1_HEADER_LEN {
            return Err(JournalV1Error::Truncated);
        }
        if bytes.len() > JOURNAL_V1_HEADER_LEN {
            return Err(JournalV1Error::TrailingBytes);
        }
        if bytes[..8] != JOURNAL_V1_HEADER_MAGIC {
            return Err(JournalV1Error::InvalidHeaderMagic);
        }
        if u16::from_be_bytes([bytes[8], bytes[9]]) != JOURNAL_V1_VERSION {
            return Err(JournalV1Error::UnsupportedHeaderVersion);
        }
        if u16::from_be_bytes([bytes[10], bytes[11]])
            != u16::try_from(JOURNAL_V1_HEADER_LEN).unwrap_or_default()
        {
            return Err(JournalV1Error::InvalidHeaderLength);
        }
        let mut identity = [0_u8; 16];
        identity.copy_from_slice(&bytes[12..28]);
        let store_id =
            StoreId::from_bytes(identity).map_err(|_| JournalV1Error::InvalidIdentity)?;
        Ok(Self { store_id })
    }
}

/// Encodes one independent Journal V1 canonical-admission frame.
///
/// The resulting bytes contain the complete admission and do not imply append,
/// persistence, synchronization, durability, or receipt completion.
///
/// # Errors
///
/// Returns a sanitized refusal if the bounded canonical payload cannot be
/// represented within [`MAX_ADMISSION_PAYLOAD_V1`].
pub fn encode_admission_frame_v1(
    append_sequence: AppendSequenceV1,
    admission: &CanonicalAdmission,
) -> Result<Vec<u8>, JournalV1Error> {
    let mut encoder = Encoder::new();
    encode_canonical_payload(&mut encoder, admission)?;
    frame_bytes(append_sequence, encoder.finish())
}

/// Counts the exact Journal V1 frame bytes without allocating encoded storage.
///
/// # Errors
///
/// Returns the same bounded representation refusals as frame encoding.
pub fn admission_frame_len_v1(admission: &CanonicalAdmission) -> Result<usize, JournalV1Error> {
    let mut encoder = Encoder::counting();
    encode_canonical_payload(&mut encoder, admission)?;
    let payload_len = encoder.len();
    if payload_len > MAX_ADMISSION_PAYLOAD_V1 {
        return Err(JournalV1Error::PayloadTooLarge);
    }
    JOURNAL_V1_FRAME_PREFIX_LEN
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(JOURNAL_V1_FRAME_CRC_LEN))
        .ok_or(JournalV1Error::PayloadTooLarge)
}

/// An admission encoded only after exact byte reservation.
///
/// Append sequence remains absent until the sole writer constructs a frame.
pub struct PreparedAdmissionV1 {
    admission: CanonicalAdmission,
    payload: Vec<u8>,
    frame_len: usize,
}

impl PreparedAdmissionV1 {
    /// Encodes an owned admission after its non-allocating counting pass.
    ///
    /// # Errors
    ///
    /// Returns a recoverable error carrying the exact admission.
    pub fn new(admission: CanonicalAdmission) -> Result<Self, PrepareAdmissionError> {
        let frame_len = match admission_frame_len_v1(&admission) {
            Ok(length) => length,
            Err(error) => {
                return Err(PrepareAdmissionError {
                    error,
                    admission: Box::new(admission),
                });
            }
        };
        let mut encoder = Encoder::new();
        if let Err(error) = encode_canonical_payload(&mut encoder, &admission) {
            return Err(PrepareAdmissionError {
                error,
                admission: Box::new(admission),
            });
        }
        let payload = encoder.finish();
        let actual = JOURNAL_V1_FRAME_PREFIX_LEN + payload.len() + JOURNAL_V1_FRAME_CRC_LEN;
        if actual != frame_len {
            return Err(PrepareAdmissionError {
                error: JournalV1Error::InvalidCanonicalData,
                admission: Box::new(admission),
            });
        }
        Ok(Self {
            admission,
            payload,
            frame_len,
        })
    }

    /// Returns the exact future frame length.
    #[must_use]
    pub const fn frame_len(&self) -> usize {
        self.frame_len
    }

    /// Borrows the complete authorized admission.
    #[must_use]
    pub const fn admission(&self) -> &CanonicalAdmission {
        &self.admission
    }

    /// Recovers the complete admission before writer sequencing.
    #[must_use]
    pub fn into_admission(self) -> CanonicalAdmission {
        self.admission
    }

    /// Applies the writer-owned append sequence and constructs exact frame bytes.
    ///
    /// # Errors
    ///
    /// Returns a recoverable framing error carrying the exact admission.
    pub fn into_frame(
        self,
        append_sequence: AppendSequenceV1,
    ) -> Result<PreparedFrameV1, PrepareAdmissionError> {
        let Self {
            admission,
            payload,
            frame_len,
        } = self;
        match frame_bytes(append_sequence, payload) {
            Ok(bytes) if bytes.len() == frame_len => Ok(PreparedFrameV1 {
                admission,
                append_sequence,
                bytes,
            }),
            Ok(_) => Err(PrepareAdmissionError {
                error: JournalV1Error::InvalidCanonicalData,
                admission: Box::new(admission),
            }),
            Err(error) => Err(PrepareAdmissionError {
                error,
                admission: Box::new(admission),
            }),
        }
    }
}

impl fmt::Debug for PreparedAdmissionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedAdmissionV1")
            .field("frame_len", &self.frame_len)
            .finish_non_exhaustive()
    }
}

/// Recoverable failure to prepare or sequence an admission frame.
pub struct PrepareAdmissionError {
    error: JournalV1Error,
    admission: Box<CanonicalAdmission>,
}

impl PrepareAdmissionError {
    /// Returns the closed framing refusal.
    #[must_use]
    pub const fn error(&self) -> JournalV1Error {
        self.error
    }

    /// Recovers the exact authorized admission.
    #[must_use]
    pub fn into_admission(self) -> CanonicalAdmission {
        *self.admission
    }
}

impl fmt::Debug for PrepareAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrepareAdmissionError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PrepareAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for PrepareAdmissionError {}

/// One complete writer-sequenced frame with its authorized admission.
pub struct PreparedFrameV1 {
    admission: CanonicalAdmission,
    append_sequence: AppendSequenceV1,
    bytes: Vec<u8>,
}

impl PreparedFrameV1 {
    /// Returns the writer-assigned append sequence.
    #[must_use]
    pub const fn append_sequence(&self) -> AppendSequenceV1 {
        self.append_sequence
    }

    /// Borrows exact frame bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns exact frame length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Reports whether the frame is empty. Prepared frames are never empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Borrows the complete authorized admission.
    #[must_use]
    pub const fn admission(&self) -> &CanonicalAdmission {
        &self.admission
    }

    /// Recovers the complete authorized admission after append handling.
    #[must_use]
    pub fn into_admission(self) -> CanonicalAdmission {
        self.admission
    }
}

impl fmt::Debug for PreparedFrameV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedFrameV1")
            .field("append_sequence", &self.append_sequence)
            .field("length", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Deterministically re-encodes one decoded non-authorizing admission frame.
///
/// # Errors
///
/// Returns a sanitized refusal if the decoded bounded payload cannot be
/// represented within [`MAX_ADMISSION_PAYLOAD_V1`].
pub fn encode_decoded_admission_frame_v1(
    admission: &DecodedAdmissionV1,
) -> Result<Vec<u8>, JournalV1Error> {
    let mut encoder = Encoder::new();
    encode_decoded_payload(&mut encoder, admission)?;
    frame_bytes(
        AppendSequenceV1(admission.append_sequence()),
        encoder.finish(),
    )
}

/// Decodes one independent Journal V1 admission frame into inspection evidence.
///
/// When `previous_append_sequence` is supplied, the frame must carry its exact
/// successor. Declared payload length is checked against both bounds before any
/// decoded field allocation.
///
/// # Errors
///
/// Rejects invalid framing, sequence, bounds, CRC-32C, tags, lengths, counts,
/// primitive values, cross-field relationships, truncation, or trailing bytes.
pub fn decode_admission_frame_v1(
    bytes: &[u8],
    limits: DecodeLimitsV1,
    previous_append_sequence: Option<AppendSequenceV1>,
) -> Result<DecodedAdmissionV1, JournalV1Error> {
    if bytes.len() < JOURNAL_V1_FRAME_PREFIX_LEN + JOURNAL_V1_FRAME_CRC_LEN {
        return Err(JournalV1Error::Truncated);
    }
    if bytes[..4] != JOURNAL_V1_FRAME_MAGIC {
        return Err(JournalV1Error::InvalidFrameMagic);
    }
    if u16::from_be_bytes([bytes[4], bytes[5]]) != JOURNAL_V1_VERSION {
        return Err(JournalV1Error::UnsupportedFrameVersion);
    }
    if bytes[6] != FRAME_KIND_ADMISSION {
        return Err(JournalV1Error::UnsupportedFrameKind);
    }
    if bytes[7] != FRAME_FLAGS_NONE {
        return Err(JournalV1Error::InvalidFrameFlags);
    }
    let append_sequence = read_array::<8>(&bytes[FRAME_SEQUENCE_OFFSET..16])
        .map(u64::from_be_bytes)
        .ok_or(JournalV1Error::Truncated)
        .and_then(AppendSequenceV1::new)?;
    if let Some(previous) = previous_append_sequence
        && append_sequence != previous.successor()?
    {
        return Err(JournalV1Error::NonMonotonicAppendSequence);
    }
    let payload_len = read_array::<4>(&bytes[FRAME_PAYLOAD_LENGTH_OFFSET..20])
        .map(u32::from_be_bytes)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(JournalV1Error::InvalidLength)?;
    if payload_len > MAX_ADMISSION_PAYLOAD_V1 || payload_len > limits.max_payload_len {
        return Err(JournalV1Error::PayloadTooLarge);
    }
    let checksum_offset = JOURNAL_V1_FRAME_PREFIX_LEN
        .checked_add(payload_len)
        .ok_or(JournalV1Error::InvalidLength)?;
    let expected_len = checksum_offset
        .checked_add(JOURNAL_V1_FRAME_CRC_LEN)
        .ok_or(JournalV1Error::InvalidLength)?;
    if bytes.len() < expected_len {
        return Err(JournalV1Error::Truncated);
    }
    if bytes.len() > expected_len {
        return Err(JournalV1Error::TrailingBytes);
    }
    let expected_crc = read_array::<4>(&bytes[checksum_offset..expected_len])
        .map(u32::from_be_bytes)
        .ok_or(JournalV1Error::Truncated)?;
    if crc32c(&bytes[..checksum_offset]) != expected_crc {
        return Err(JournalV1Error::ChecksumMismatch);
    }

    let mut cursor = Cursor::new(&bytes[JOURNAL_V1_FRAME_PREFIX_LEN..checksum_offset]);
    let decoded = decode_payload(&mut cursor, append_sequence)?;
    cursor.finish()?;
    validate_decoded(&decoded)?;
    Ok(decoded)
}

fn frame_bytes(
    append_sequence: AppendSequenceV1,
    mut payload: Vec<u8>,
) -> Result<Vec<u8>, JournalV1Error> {
    if payload.len() > MAX_ADMISSION_PAYLOAD_V1 {
        return Err(JournalV1Error::PayloadTooLarge);
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| JournalV1Error::PayloadTooLarge)?;
    let capacity = JOURNAL_V1_FRAME_PREFIX_LEN
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(JOURNAL_V1_FRAME_CRC_LEN))
        .ok_or(JournalV1Error::PayloadTooLarge)?;
    let original_len = payload.len();
    payload.reserve_exact(capacity - original_len);
    payload.resize(capacity, 0);
    payload.copy_within(0..original_len, JOURNAL_V1_FRAME_PREFIX_LEN);
    payload[..4].copy_from_slice(&JOURNAL_V1_FRAME_MAGIC);
    payload[4..6].copy_from_slice(&JOURNAL_V1_VERSION.to_be_bytes());
    payload[6] = FRAME_KIND_ADMISSION;
    payload[7] = FRAME_FLAGS_NONE;
    payload[8..16].copy_from_slice(&append_sequence.get().to_be_bytes());
    payload[16..20].copy_from_slice(&payload_len.to_be_bytes());
    let checksum_offset = JOURNAL_V1_FRAME_PREFIX_LEN + original_len;
    let crc = crc32c(&payload[..checksum_offset]);
    payload[checksum_offset..].copy_from_slice(&crc.to_be_bytes());
    Ok(payload)
}

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
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

pub(crate) fn frame_len_from_prefix_v1(
    prefix: &[u8],
    limits: DecodeLimitsV1,
) -> Result<usize, JournalV1Error> {
    if prefix.len() != JOURNAL_V1_FRAME_PREFIX_LEN {
        return Err(JournalV1Error::Truncated);
    }
    if prefix[..4] != JOURNAL_V1_FRAME_MAGIC {
        return Err(JournalV1Error::InvalidFrameMagic);
    }
    if u16::from_be_bytes([prefix[4], prefix[5]]) != JOURNAL_V1_VERSION {
        return Err(JournalV1Error::UnsupportedFrameVersion);
    }
    if prefix[6] != FRAME_KIND_ADMISSION {
        return Err(JournalV1Error::UnsupportedFrameKind);
    }
    if prefix[7] != FRAME_FLAGS_NONE {
        return Err(JournalV1Error::InvalidFrameFlags);
    }
    let _sequence = AppendSequenceV1::new(u64::from_be_bytes(
        prefix[8..16]
            .try_into()
            .map_err(|_| JournalV1Error::Truncated)?,
    ))?;
    let payload_len = usize::try_from(u32::from_be_bytes(
        prefix[16..20]
            .try_into()
            .map_err(|_| JournalV1Error::Truncated)?,
    ))
    .map_err(|_| JournalV1Error::InvalidLength)?;
    if payload_len > MAX_ADMISSION_PAYLOAD_V1 || payload_len > limits.max_payload_len() {
        return Err(JournalV1Error::PayloadTooLarge);
    }
    JOURNAL_V1_FRAME_PREFIX_LEN
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(JOURNAL_V1_FRAME_CRC_LEN))
        .ok_or(JournalV1Error::InvalidLength)
}

fn read_array<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    bytes.try_into().ok()
}

enum EncoderOutput {
    Bytes(Vec<u8>),
    Count(usize),
}

struct Encoder {
    output: EncoderOutput,
}

impl Encoder {
    const fn new() -> Self {
        Self {
            output: EncoderOutput::Bytes(Vec::new()),
        }
    }

    const fn counting() -> Self {
        Self {
            output: EncoderOutput::Count(0),
        }
    }

    fn len(&self) -> usize {
        match &self.output {
            EncoderOutput::Bytes(bytes) => bytes.len(),
            EncoderOutput::Count(length) => *length,
        }
    }

    fn finish(self) -> Vec<u8> {
        match self.output {
            EncoderOutput::Bytes(bytes) => bytes,
            EncoderOutput::Count(_) => Vec::new(),
        }
    }

    fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    fn u32(&mut self, value: u32) {
        self.bytes(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_be_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes(&value.to_be_bytes());
    }

    fn bytes(&mut self, bytes: &[u8]) {
        match &mut self.output {
            EncoderOutput::Bytes(output) => output.extend_from_slice(bytes),
            EncoderOutput::Count(length) => *length = length.saturating_add(bytes.len()),
        }
    }

    fn string(&mut self, value: &str) -> Result<(), JournalV1Error> {
        let length = u32::try_from(value.len()).map_err(|_| JournalV1Error::InvalidLength)?;
        self.u32(length);
        self.bytes(value.as_bytes());
        Ok(())
    }

    fn count(&mut self, value: usize) -> Result<(), JournalV1Error> {
        self.u32(u32::try_from(value).map_err(|_| JournalV1Error::InvalidCount)?);
        Ok(())
    }
}

fn encode_canonical_payload(
    encoder: &mut Encoder,
    admission: &CanonicalAdmission,
) -> Result<(), JournalV1Error> {
    encode_store_id(encoder, admission.store_id());
    encode_declaration(encoder, admission.declaration())?;
    encode_envelope(encoder, admission.envelope())?;
    encode_retry(encoder, admission.retry())?;
    encode_batch(encoder, admission.batch())?;
    encode_lifecycle(encoder, admission.lifecycle())?;
    encode_source_interval_kind(encoder, admission.evidence_kind());
    match admission.evidence_kind() {
        SourceIntervalKind::Observed => {
            encoder.count(admission.observations().len())?;
            for lineage in admission.observations() {
                encode_lineage(encoder, lineage)?;
            }
            encoder.count(admission.gaps().len())?;
            for gap in admission.gaps() {
                encode_source_gap(encoder, gap);
            }
        }
        SourceIntervalKind::NoChange => {}
    }
    Ok(())
}

fn encode_decoded_payload(
    encoder: &mut Encoder,
    admission: &DecodedAdmissionV1,
) -> Result<(), JournalV1Error> {
    encode_store_id(encoder, admission.store_id());
    encode_decoded_declaration(encoder, admission.declaration())?;
    encode_envelope(encoder, admission.envelope())?;
    encode_retry(encoder, admission.retry())?;
    encode_batch(encoder, admission.batch())?;
    encode_lifecycle(encoder, admission.lifecycle())?;
    encode_source_interval_kind(encoder, admission.evidence_kind());
    match admission.evidence() {
        DecodedEvidenceV1::Observed { observations, gaps } => {
            encoder.count(observations.len())?;
            for lineage in observations {
                encode_decoded_lineage(encoder, lineage)?;
            }
            encoder.count(gaps.len())?;
            for gap in gaps {
                encode_source_gap(encoder, gap);
            }
        }
        DecodedEvidenceV1::NoChange => {}
    }
    Ok(())
}

fn encode_declaration(
    encoder: &mut Encoder,
    declaration: &SeriesDeclaration,
) -> Result<(), JournalV1Error> {
    encode_store_id(encoder, declaration.store_id());
    encode_series_id(encoder, declaration.series_id());
    encoder.u128(declaration.revision().get());
    encode_optional_revision(encoder, declaration.previous_revision());
    encode_binding(encoder, declaration.binding())?;
    encode_declaration_payload(encoder, declaration.payload())?;
    encode_declaration_evidence(encoder, declaration.evidence())
}

fn encode_decoded_declaration(
    encoder: &mut Encoder,
    declaration: &DecodedDeclarationV1,
) -> Result<(), JournalV1Error> {
    encode_store_id(encoder, declaration.store_id());
    encode_series_id(encoder, declaration.series_id());
    encoder.u128(declaration.revision().get());
    encode_optional_revision(encoder, declaration.previous_revision());
    encode_binding(encoder, declaration.binding())?;
    encode_declaration_payload(encoder, declaration.payload())?;
    encode_declaration_evidence(encoder, declaration.evidence())
}

fn encode_optional_revision(encoder: &mut Encoder, revision: Option<DeclarationRevision>) {
    match revision {
        Some(revision) => {
            encoder.u8(1);
            encoder.u128(revision.get());
        }
        None => encoder.u8(0),
    }
}

fn encode_binding(encoder: &mut Encoder, binding: &SeriesBinding) -> Result<(), JournalV1Error> {
    encode_source_reference(encoder, binding.source())
}

fn encode_source_reference(
    encoder: &mut Encoder,
    source: &SourceReference,
) -> Result<(), JournalV1Error> {
    encoder.string(source.provider().as_str())?;
    match source.projection() {
        Some(projection) => {
            encoder.u8(1);
            encoder.string(projection.as_str())?;
        }
        None => encoder.u8(0),
    }
    encoder.string(source.locator().as_str())
}

fn encode_declaration_payload(
    encoder: &mut Encoder,
    payload: &SeriesDeclarationPayload,
) -> Result<(), JournalV1Error> {
    encode_producer_id(encoder, payload.producer_id());
    encode_collection_mode(encoder, payload.collection_mode());
    encode_value_family(encoder, payload.value_family());
    encode_quantity(encoder, payload.quantity())?;
    encode_unit(encoder, payload.unit())?;
    encode_optional_declaration_reference(encoder, payload.application())
}

fn encode_declaration_evidence(
    encoder: &mut Encoder,
    evidence: &DeclarationEvidence,
) -> Result<(), JournalV1Error> {
    encode_timestamp(encoder, evidence.effective_at());
    encode_optional_artifact(encoder, evidence.artifact())
}

fn encode_envelope(
    encoder: &mut Encoder,
    envelope: &CollectionEnvelope,
) -> Result<(), JournalV1Error> {
    encode_series_metadata(encoder, envelope.series());
    match envelope.evidence_kind() {
        EvidenceKind::Observed => {
            encoder.u8(1);
            encoder.count(envelope.observations().len())?;
            for observation in envelope.observations() {
                encode_observation(encoder, observation)?;
            }
            encoder.count(envelope.gaps().len())?;
            for gap in envelope.gaps() {
                encode_gap(encoder, gap);
            }
        }
        EvidenceKind::NoChange => {
            encoder.u8(2);
            let no_change = envelope
                .no_change_evidence()
                .ok_or(JournalV1Error::InvalidCanonicalData)?;
            encode_time_interval(encoder, no_change.interval());
        }
    }
    Ok(())
}

fn encode_series_metadata(encoder: &mut Encoder, metadata: &SeriesMetadata) {
    encode_series_id(encoder, metadata.series_id());
    encode_producer_id(encoder, metadata.producer_id());
    encode_collection_mode(encoder, metadata.collection_mode());
}

fn encode_observation(
    encoder: &mut Encoder,
    observation: &Observation,
) -> Result<(), JournalV1Error> {
    encode_observation_id(encoder, observation.observation_id());
    encode_exact_value(encoder, observation.value())?;
    encode_observation_times(encoder, observation.times());
    encode_quality(encoder, observation.quality());
    encode_native_status(encoder, observation.native_status())?;
    encode_optional_position(encoder, observation.producer_position());
    encode_optional_interval(encoder, observation.interval());
    Ok(())
}

fn encode_exact_value(encoder: &mut Encoder, value: &ExactValue) -> Result<(), JournalV1Error> {
    match value {
        ExactValue::Real(value) => {
            encoder.u8(1);
            encoder.u64(value.to_bits());
        }
        ExactValue::Signed(value) => {
            encoder.u8(2);
            encoder.i64(*value);
        }
        ExactValue::Unsigned(value) => {
            encoder.u8(3);
            encoder.u64(*value);
        }
        ExactValue::Boolean(value) => {
            encoder.u8(4);
            encoder.u8(u8::from(*value));
        }
        ExactValue::State(value) => {
            encoder.u8(5);
            encoder.string(value.class().as_str())?;
            encoder.string(value.member().as_str())?;
        }
        ExactValue::Text(value) => {
            encoder.u8(6);
            encoder.string(value.as_str())?;
        }
        ExactValue::Artifact(value) => {
            encoder.u8(7);
            encode_artifact(encoder, value)?;
        }
        ExactValue::Unavailable(value) => {
            encoder.u8(8);
            match value.reason() {
                Some(reason) => {
                    encoder.u8(1);
                    encoder.string(reason.as_str())?;
                }
                None => encoder.u8(0),
            }
        }
    }
    Ok(())
}

fn encode_observation_times(encoder: &mut Encoder, times: ObservationTimes) {
    encode_optional_timestamp(encoder, times.source());
    encode_timestamp(encoder, times.receive());
    encode_timestamp(encoder, times.effective());
}

fn encode_quality(encoder: &mut Encoder, quality: Quality) {
    encoder.u8(match quality.level() {
        QualityLevel::Unknown => 1,
        QualityLevel::Good => 2,
        QualityLevel::Uncertain => 3,
        QualityLevel::Bad => 4,
        QualityLevel::NotEvaluated => 5,
    });
    let flags = quality.flags();
    let bits = u8::from(flags.stale())
        | (u8::from(flags.invalid()) << 1)
        | (u8::from(flags.substituted()) << 2)
        | (u8::from(flags.overridden()) << 3)
        | (u8::from(flags.out_of_service()) << 4)
        | (u8::from(flags.communication_failure()) << 5);
    encoder.u8(bits);
}

fn encode_native_status(
    encoder: &mut Encoder,
    status: &NativeStatus,
) -> Result<(), JournalV1Error> {
    encoder.count(status.tokens().len())?;
    for token in status.tokens() {
        encoder.string(token.as_str())?;
    }
    Ok(())
}

fn encode_gap(encoder: &mut Encoder, gap: &Gap) {
    encoder.u128(gap.epoch().get());
    encoder.u128(gap.start().get());
    encoder.u128(gap.end().get());
    encoder.u8(match gap.reason() {
        GapReason::Unknown => 1,
        GapReason::ProducerRestart => 2,
        GapReason::BufferOverflow => 3,
        GapReason::CommunicationFailure => 4,
        GapReason::SourceDataLoss => 5,
        GapReason::AdministrativeExclusion => 6,
    });
}

fn encode_retry(encoder: &mut Encoder, retry: &RetryQualification) -> Result<(), JournalV1Error> {
    encode_series_id(encoder, retry.series_id());
    encode_producer_id(encoder, retry.producer_id());
    encoder.string(retry.key().as_str())?;
    encode_content(encoder, retry.content())
}

fn encode_batch(encoder: &mut Encoder, batch: &SourceBatchMetadata) -> Result<(), JournalV1Error> {
    encoder.string(batch.schema().as_str())?;
    encoder.u128(batch.version().get());
    encode_source_interval_kind(encoder, batch.interval());
    Ok(())
}

fn encode_lifecycle(
    encoder: &mut Encoder,
    lifecycle: &CaptureLifecycle,
) -> Result<(), JournalV1Error> {
    let system = lifecycle.system();
    encode_evidence_id(encoder, system.evidence_id());
    encoder.string(system.provider().as_str())?;
    encoder.string(system.projection().as_str())?;

    let endpoint = lifecycle.endpoint();
    encode_evidence_id(encoder, endpoint.evidence_id());
    encode_evidence_id(encoder, endpoint.system_id());
    encoder.string(endpoint.locator().as_str())?;

    let run = lifecycle.run();
    encode_evidence_id(encoder, run.evidence_id());
    encode_evidence_id(encoder, run.endpoint_id());
    encode_timestamp(encoder, run.started_at());
    encode_optional_timestamp(encoder, run.completed_at());

    let snapshot = lifecycle.snapshot();
    encode_evidence_id(encoder, snapshot.evidence_id());
    encode_evidence_id(encoder, snapshot.run_id());
    encode_artifact(encoder, snapshot.artifact())
}

fn encode_lineage(
    encoder: &mut Encoder,
    lineage: &SourceObservationLineage,
) -> Result<(), JournalV1Error> {
    encoder.u8(lineage.ordinal());
    encode_observation_id(encoder, lineage.canonical_observation_id());
    encode_source_observation(encoder, lineage.observation())?;
    encode_raw_record(encoder, lineage.raw())?;
    encode_normalized_record(encoder, lineage.normalized())
}

fn encode_decoded_lineage(
    encoder: &mut Encoder,
    lineage: &DecodedObservationLineageV1,
) -> Result<(), JournalV1Error> {
    encoder.u8(lineage.ordinal());
    encode_observation_id(encoder, lineage.canonical_observation_id());
    encode_source_observation(encoder, lineage.observation())?;
    encode_raw_record(encoder, lineage.raw())?;
    encode_normalized_record(encoder, lineage.normalized())
}

fn encode_source_observation(
    encoder: &mut Encoder,
    evidence: &SourceObservationEvidence,
) -> Result<(), JournalV1Error> {
    encode_evidence_id(encoder, evidence.evidence_id());
    encode_optional_artifact(encoder, evidence.provenance_artifact())?;
    encoder.u8(match evidence.transport() {
        SourceTransport::New => 1,
        SourceTransport::Redelivered => 2,
    });
    encode_optional_idempotency(encoder, evidence.idempotency())
}

fn encode_raw_record(
    encoder: &mut Encoder,
    evidence: &RawRecordEvidence,
) -> Result<(), JournalV1Error> {
    encode_evidence_id(encoder, evidence.evidence_id());
    encode_evidence_id(encoder, evidence.snapshot_id());
    encode_artifact(encoder, evidence.artifact())?;
    encode_optional_idempotency(encoder, evidence.idempotency())
}

fn encode_normalized_record(
    encoder: &mut Encoder,
    evidence: &NormalizedRecordEvidence,
) -> Result<(), JournalV1Error> {
    encode_evidence_id(encoder, evidence.evidence_id());
    encode_evidence_id(encoder, evidence.raw_record_id());
    encode_content(encoder, evidence.content())?;
    encode_evidence_id(encoder, evidence.observation_evidence_id());
    Ok(())
}

fn encode_optional_idempotency(
    encoder: &mut Encoder,
    evidence: Option<&SourceIdempotency>,
) -> Result<(), JournalV1Error> {
    if let Some(evidence) = evidence {
        encoder.u8(1);
        encoder.string(evidence.key().as_str())?;
        encode_content(encoder, evidence.content())
    } else {
        encoder.u8(0);
        Ok(())
    }
}

fn encode_source_gap(encoder: &mut Encoder, gap: &SourceGapEvidence) {
    encoder.u128(gap.epoch().get());
    encoder.u128(gap.start().get());
    encoder.u128(gap.end().get());
    encoder.u8(match gap.reason() {
        SourceGapReason::CommunicationFailure => 1,
        SourceGapReason::SourceUnavailable => 2,
        SourceGapReason::ProducerReset => 3,
        SourceGapReason::Filtered => 4,
        SourceGapReason::Unknown => 5,
    });
}

fn encode_artifact(
    encoder: &mut Encoder,
    artifact: &ArtifactReference,
) -> Result<(), JournalV1Error> {
    encode_artifact_id(encoder, artifact.artifact_id());
    encode_content(encoder, artifact.content())
}

fn encode_optional_artifact(
    encoder: &mut Encoder,
    artifact: Option<&ArtifactReference>,
) -> Result<(), JournalV1Error> {
    if let Some(artifact) = artifact {
        encoder.u8(1);
        encode_artifact(encoder, artifact)
    } else {
        encoder.u8(0);
        Ok(())
    }
}

fn encode_content(encoder: &mut Encoder, content: &ContentIdentity) -> Result<(), JournalV1Error> {
    encoder.string(content.format().as_str())?;
    encoder.u128(content.version().get());
    encoder.bytes(content.sha256());
    Ok(())
}

fn encode_quantity(
    encoder: &mut Encoder,
    quantity: &QuantityEvidence,
) -> Result<(), JournalV1Error> {
    match quantity {
        QuantityEvidence::Absent => encoder.u8(0),
        QuantityEvidence::Resolved(reference) => {
            encoder.u8(1);
            encoder.string(reference.as_str())?;
        }
        QuantityEvidence::Unresolved(reference) => {
            encoder.u8(2);
            encoder.string(reference.as_str())?;
        }
    }
    Ok(())
}

fn encode_unit(encoder: &mut Encoder, unit: &UnitEvidence) -> Result<(), JournalV1Error> {
    match unit {
        UnitEvidence::Absent => encoder.u8(0),
        UnitEvidence::Resolved(reference) => {
            encoder.u8(1);
            encoder.string(reference.as_str())?;
        }
        UnitEvidence::Unresolved(reference) => {
            encoder.u8(2);
            encoder.string(reference.as_str())?;
        }
    }
    Ok(())
}

fn encode_optional_declaration_reference(
    encoder: &mut Encoder,
    reference: Option<&DeclarationReference>,
) -> Result<(), JournalV1Error> {
    match reference {
        Some(reference) => {
            encoder.u8(1);
            encoder.string(reference.as_str())?;
        }
        None => encoder.u8(0),
    }
    Ok(())
}

fn encode_optional_timestamp(encoder: &mut Encoder, timestamp: Option<Timestamp>) {
    match timestamp {
        Some(timestamp) => {
            encoder.u8(1);
            encode_timestamp(encoder, timestamp);
        }
        None => encoder.u8(0),
    }
}

fn encode_optional_position(encoder: &mut Encoder, position: Option<ProducerPosition>) {
    match position {
        Some(position) => {
            encoder.u8(1);
            encoder.u128(position.epoch().get());
            encoder.u128(position.sequence().get());
        }
        None => encoder.u8(0),
    }
}

fn encode_optional_interval(encoder: &mut Encoder, interval: Option<TimeInterval>) {
    match interval {
        Some(interval) => {
            encoder.u8(1);
            encode_time_interval(encoder, interval);
        }
        None => encoder.u8(0),
    }
}

fn encode_timestamp(encoder: &mut Encoder, timestamp: Timestamp) {
    encoder.i64(timestamp.unix_seconds());
    encoder.u32(timestamp.nanosecond());
}

fn encode_time_interval(encoder: &mut Encoder, interval: TimeInterval) {
    encode_timestamp(encoder, interval.start());
    encode_timestamp(encoder, interval.end());
}

fn encode_collection_mode(encoder: &mut Encoder, mode: CollectionMode) {
    encoder.u8(match mode {
        CollectionMode::Sampled => 1,
        CollectionMode::ChangeOnly => 2,
        CollectionMode::Cumulative => 3,
        CollectionMode::Interval => 4,
        CollectionMode::Event => 5,
    });
}

fn encode_value_family(encoder: &mut Encoder, family: ValueFamily) {
    encoder.u8(match family {
        ValueFamily::Real => 1,
        ValueFamily::Signed => 2,
        ValueFamily::Unsigned => 3,
        ValueFamily::Boolean => 4,
        ValueFamily::State => 5,
        ValueFamily::Text => 6,
        ValueFamily::Artifact => 7,
    });
}

fn encode_source_interval_kind(encoder: &mut Encoder, kind: SourceIntervalKind) {
    encoder.u8(match kind {
        SourceIntervalKind::Observed => 1,
        SourceIntervalKind::NoChange => 2,
    });
}

fn encode_store_id(encoder: &mut Encoder, id: StoreId) {
    encoder.bytes(id.as_bytes());
}

fn encode_series_id(encoder: &mut Encoder, id: SeriesId) {
    encoder.bytes(id.as_bytes());
}

fn encode_producer_id(encoder: &mut Encoder, id: ProducerId) {
    encoder.bytes(id.as_bytes());
}

fn encode_observation_id(encoder: &mut Encoder, id: ObservationId) {
    encoder.bytes(id.as_bytes());
}

fn encode_artifact_id(encoder: &mut Encoder, id: ArtifactId) {
    encoder.bytes(id.as_bytes());
}

fn encode_evidence_id(encoder: &mut Encoder, id: EvidenceId) {
    encoder.bytes(id.as_bytes());
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], JournalV1Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(JournalV1Error::InvalidLength)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(JournalV1Error::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], JournalV1Error> {
        self.take(N)?
            .try_into()
            .map_err(|_| JournalV1Error::Truncated)
    }

    fn u8(&mut self) -> Result<u8, JournalV1Error> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, JournalV1Error> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, JournalV1Error> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn u128(&mut self) -> Result<u128, JournalV1Error> {
        Ok(u128::from_be_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, JournalV1Error> {
        Ok(i64::from_be_bytes(self.array()?))
    }

    fn string(&mut self, maximum: usize) -> Result<String, JournalV1Error> {
        let length = usize::try_from(self.u32()?).map_err(|_| JournalV1Error::InvalidLength)?;
        if length > maximum {
            return Err(JournalV1Error::InvalidLength);
        }
        let bytes = self.take(length)?;
        let value = core::str::from_utf8(bytes).map_err(|_| JournalV1Error::InvalidUtf8)?;
        Ok(value.to_owned())
    }

    fn count(&mut self, maximum: usize) -> Result<usize, JournalV1Error> {
        let count = usize::try_from(self.u32()?).map_err(|_| JournalV1Error::InvalidCount)?;
        if count > maximum {
            return Err(JournalV1Error::InvalidCount);
        }
        Ok(count)
    }

    fn option(&mut self) -> Result<bool, JournalV1Error> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(JournalV1Error::UnknownTag),
        }
    }

    fn finish(self) -> Result<(), JournalV1Error> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(JournalV1Error::TrailingBytes)
        }
    }
}

fn decode_payload(
    cursor: &mut Cursor<'_>,
    append_sequence: AppendSequenceV1,
) -> Result<DecodedAdmissionV1, JournalV1Error> {
    let store_id = decode_store_id(cursor)?;
    let declaration = decode_declaration(cursor)?;
    let envelope = decode_envelope(cursor)?;
    let retry = decode_retry(cursor)?;
    let batch = decode_batch(cursor)?;
    let lifecycle = decode_lifecycle(cursor)?;
    let evidence = match decode_source_interval_kind(cursor)? {
        SourceIntervalKind::Observed => {
            let observation_count = cursor.count(MAX_OBSERVATIONS)?;
            let mut observations = Vec::with_capacity(observation_count);
            for _ in 0..observation_count {
                observations.push(decode_lineage(cursor)?);
            }
            let gap_count = cursor.count(MAX_GAPS)?;
            let mut gaps = Vec::with_capacity(gap_count);
            for _ in 0..gap_count {
                gaps.push(decode_source_gap(cursor)?);
            }
            DecodedEvidenceV1::Observed {
                observations: observations.into_boxed_slice(),
                gaps: gaps.into_boxed_slice(),
            }
        }
        SourceIntervalKind::NoChange => DecodedEvidenceV1::NoChange,
    };
    Ok(DecodedAdmissionV1 {
        append_sequence: append_sequence.get(),
        store_id,
        declaration,
        envelope,
        retry,
        batch,
        lifecycle,
        evidence,
    })
}

fn decode_declaration(cursor: &mut Cursor<'_>) -> Result<DecodedDeclarationV1, JournalV1Error> {
    let store_id = decode_store_id(cursor)?;
    let series_id = decode_series_id(cursor)?;
    let revision = DeclarationRevision::new(cursor.u128()?).map_err(invalid_model)?;
    let previous_revision = if cursor.option()? {
        Some(DeclarationRevision::new(cursor.u128()?).map_err(invalid_model)?)
    } else {
        None
    };
    let binding = decode_binding(cursor)?;
    let payload = decode_declaration_payload(cursor)?;
    let evidence = decode_declaration_evidence(cursor)?;
    Ok(DecodedDeclarationV1 {
        store_id,
        series_id,
        revision,
        previous_revision,
        binding,
        payload,
        evidence,
    })
}

fn decode_binding(cursor: &mut Cursor<'_>) -> Result<SeriesBinding, JournalV1Error> {
    Ok(SeriesBinding::new(decode_source_reference(cursor)?))
}

fn decode_source_reference(cursor: &mut Cursor<'_>) -> Result<SourceReference, JournalV1Error> {
    let provider = decode_declaration_reference(cursor)?;
    let projection = if cursor.option()? {
        Some(decode_source_projection(cursor)?)
    } else {
        None
    };
    let locator = decode_declaration_reference(cursor)?;
    Ok(match projection {
        Some(projection) => SourceReference::with_projection(provider, projection, locator),
        None => SourceReference::new(provider, locator),
    })
}

fn decode_declaration_payload(
    cursor: &mut Cursor<'_>,
) -> Result<SeriesDeclarationPayload, JournalV1Error> {
    Ok(SeriesDeclarationPayload::new(
        decode_producer_id(cursor)?,
        decode_collection_mode(cursor)?,
        decode_value_family(cursor)?,
        decode_quantity(cursor)?,
        decode_unit(cursor)?,
        if cursor.option()? {
            Some(decode_declaration_reference(cursor)?)
        } else {
            None
        },
    ))
}

fn decode_declaration_evidence(
    cursor: &mut Cursor<'_>,
) -> Result<DeclarationEvidence, JournalV1Error> {
    let effective_at = decode_timestamp(cursor)?;
    let artifact = decode_optional_artifact(cursor)?;
    Ok(DeclarationEvidence::new(effective_at, artifact))
}

fn invalid_model(_: och_core::ModelError) -> JournalV1Error {
    JournalV1Error::InvalidCanonicalData
}

fn decode_envelope(cursor: &mut Cursor<'_>) -> Result<CollectionEnvelope, JournalV1Error> {
    let metadata = decode_series_metadata(cursor)?;
    match cursor.u8()? {
        1 => {
            let observation_count = cursor.count(MAX_OBSERVATIONS)?;
            let mut observations = Vec::with_capacity(observation_count);
            for _ in 0..observation_count {
                observations.push(decode_observation(cursor)?);
            }
            let gap_count = cursor.count(MAX_GAPS)?;
            let mut gaps = Vec::with_capacity(gap_count);
            for _ in 0..gap_count {
                gaps.push(decode_gap(cursor)?);
            }
            CollectionEnvelope::observed(metadata, observations, gaps).map_err(invalid_model)
        }
        2 => CollectionEnvelope::no_change(metadata, NoChange::new(decode_time_interval(cursor)?))
            .map_err(invalid_model),
        _ => Err(JournalV1Error::UnknownTag),
    }
}

fn decode_series_metadata(cursor: &mut Cursor<'_>) -> Result<SeriesMetadata, JournalV1Error> {
    Ok(SeriesMetadata::new(
        decode_series_id(cursor)?,
        decode_producer_id(cursor)?,
        decode_collection_mode(cursor)?,
    ))
}

fn decode_observation(cursor: &mut Cursor<'_>) -> Result<Observation, JournalV1Error> {
    Ok(Observation::new(
        decode_observation_id(cursor)?,
        decode_exact_value(cursor)?,
        decode_observation_times(cursor)?,
        decode_quality(cursor)?,
        decode_native_status(cursor)?,
        decode_optional_position(cursor)?,
        decode_optional_interval(cursor)?,
    ))
}

fn decode_exact_value(cursor: &mut Cursor<'_>) -> Result<ExactValue, JournalV1Error> {
    match cursor.u8()? {
        1 => Ok(ExactValue::Real(RealBits::from_bits(cursor.u64()?))),
        2 => Ok(ExactValue::Signed(cursor.i64()?)),
        3 => Ok(ExactValue::Unsigned(cursor.u64()?)),
        4 => match cursor.u8()? {
            0 => Ok(ExactValue::Boolean(false)),
            1 => Ok(ExactValue::Boolean(true)),
            _ => Err(JournalV1Error::UnknownTag),
        },
        5 => Ok(ExactValue::State(StateValue::new(
            StateClass::new(cursor.string(MAX_PORTABLE_TOKEN_BYTES)?).map_err(invalid_model)?,
            StateMember::new(cursor.string(MAX_PORTABLE_TOKEN_BYTES)?).map_err(invalid_model)?,
        ))),
        6 => Ok(ExactValue::Text(
            ExactText::new(cursor.string(MAX_EXACT_TEXT_BYTES)?).map_err(invalid_model)?,
        )),
        7 => Ok(ExactValue::Artifact(decode_artifact(cursor)?)),
        8 => {
            let reason = if cursor.option()? {
                Some(
                    UnavailableReason::new(cursor.string(MAX_PORTABLE_TOKEN_BYTES)?)
                        .map_err(invalid_model)?,
                )
            } else {
                None
            };
            Ok(ExactValue::Unavailable(Unavailable::new(reason)))
        }
        _ => Err(JournalV1Error::UnknownTag),
    }
}

fn decode_observation_times(cursor: &mut Cursor<'_>) -> Result<ObservationTimes, JournalV1Error> {
    let source = decode_optional_timestamp(cursor)?;
    let receive = decode_timestamp(cursor)?;
    let effective = decode_timestamp(cursor)?;
    Ok(ObservationTimes::new(source, receive, effective))
}

fn decode_quality(cursor: &mut Cursor<'_>) -> Result<Quality, JournalV1Error> {
    let level = match cursor.u8()? {
        1 => QualityLevel::Unknown,
        2 => QualityLevel::Good,
        3 => QualityLevel::Uncertain,
        4 => QualityLevel::Bad,
        5 => QualityLevel::NotEvaluated,
        _ => return Err(JournalV1Error::UnknownTag),
    };
    let bits = cursor.u8()?;
    if bits & !0x3f != 0 {
        return Err(JournalV1Error::UnknownTag);
    }
    let flags = QualityFlags::none()
        .with_stale(bits & 1 != 0)
        .with_invalid(bits & 2 != 0)
        .with_substituted(bits & 4 != 0)
        .with_overridden(bits & 8 != 0)
        .with_out_of_service(bits & 16 != 0)
        .with_communication_failure(bits & 32 != 0);
    Ok(Quality::new(level, flags))
}

fn decode_native_status(cursor: &mut Cursor<'_>) -> Result<NativeStatus, JournalV1Error> {
    let count = cursor.count(MAX_NATIVE_STATUS_TOKENS)?;
    let mut tokens = Vec::with_capacity(count);
    for _ in 0..count {
        tokens.push(
            NativeStatusToken::new(cursor.string(MAX_PORTABLE_TOKEN_BYTES)?)
                .map_err(invalid_model)?,
        );
    }
    NativeStatus::new(tokens).map_err(invalid_model)
}

fn decode_gap(cursor: &mut Cursor<'_>) -> Result<Gap, JournalV1Error> {
    let epoch = ProducerEpoch::new(cursor.u128()?);
    let start = ProducerSequence::new(cursor.u128()?);
    let end = ProducerSequence::new(cursor.u128()?);
    let reason = match cursor.u8()? {
        1 => GapReason::Unknown,
        2 => GapReason::ProducerRestart,
        3 => GapReason::BufferOverflow,
        4 => GapReason::CommunicationFailure,
        5 => GapReason::SourceDataLoss,
        6 => GapReason::AdministrativeExclusion,
        _ => return Err(JournalV1Error::UnknownTag),
    };
    Gap::new(epoch, start, end, reason).map_err(invalid_model)
}

fn decode_retry(cursor: &mut Cursor<'_>) -> Result<RetryQualification, JournalV1Error> {
    Ok(RetryQualification::new(
        decode_series_id(cursor)?,
        decode_producer_id(cursor)?,
        RetryKey::new(cursor.string(MAX_RETRY_KEY_BYTES)?).map_err(invalid_model)?,
        decode_content(cursor)?,
    ))
}

fn decode_batch(cursor: &mut Cursor<'_>) -> Result<SourceBatchMetadata, JournalV1Error> {
    Ok(SourceBatchMetadata::new(
        SourceSchemaIdentity::new(cursor.string(MAX_DECLARATION_REFERENCE_BYTES)?)
            .map_err(invalid_model)?,
        SourceSchemaVersion::new(cursor.u128()?).map_err(invalid_model)?,
        decode_source_interval_kind(cursor)?,
    ))
}

fn decode_lifecycle(cursor: &mut Cursor<'_>) -> Result<CaptureLifecycle, JournalV1Error> {
    let system = SourceSystemEvidence::new(
        decode_evidence_id(cursor)?,
        decode_declaration_reference(cursor)?,
        decode_source_projection(cursor)?,
    );
    let endpoint = SourceEndpointEvidence::new(
        decode_evidence_id(cursor)?,
        decode_evidence_id(cursor)?,
        decode_declaration_reference(cursor)?,
    );
    let run = CaptureRunEvidence::new(
        decode_evidence_id(cursor)?,
        decode_evidence_id(cursor)?,
        decode_timestamp(cursor)?,
        decode_optional_timestamp(cursor)?,
    )
    .map_err(invalid_model)?;
    let snapshot = SourceSnapshotEvidence::new(
        decode_evidence_id(cursor)?,
        decode_evidence_id(cursor)?,
        decode_artifact(cursor)?,
    );
    CaptureLifecycle::new(system, endpoint, run, snapshot).map_err(invalid_model)
}

fn decode_lineage(cursor: &mut Cursor<'_>) -> Result<DecodedObservationLineageV1, JournalV1Error> {
    Ok(DecodedObservationLineageV1 {
        ordinal: cursor.u8()?,
        canonical_observation_id: decode_observation_id(cursor)?,
        observation: decode_source_observation(cursor)?,
        raw: decode_raw_record(cursor)?,
        normalized: decode_normalized_record(cursor)?,
    })
}

fn decode_source_observation(
    cursor: &mut Cursor<'_>,
) -> Result<SourceObservationEvidence, JournalV1Error> {
    Ok(SourceObservationEvidence::new(
        decode_evidence_id(cursor)?,
        decode_optional_artifact(cursor)?,
        match cursor.u8()? {
            1 => SourceTransport::New,
            2 => SourceTransport::Redelivered,
            _ => return Err(JournalV1Error::UnknownTag),
        },
        decode_optional_idempotency(cursor)?,
    ))
}

fn decode_raw_record(cursor: &mut Cursor<'_>) -> Result<RawRecordEvidence, JournalV1Error> {
    Ok(RawRecordEvidence::new(
        decode_evidence_id(cursor)?,
        decode_evidence_id(cursor)?,
        decode_artifact(cursor)?,
        decode_optional_idempotency(cursor)?,
    ))
}

fn decode_normalized_record(
    cursor: &mut Cursor<'_>,
) -> Result<NormalizedRecordEvidence, JournalV1Error> {
    Ok(NormalizedRecordEvidence::new(
        decode_evidence_id(cursor)?,
        decode_evidence_id(cursor)?,
        decode_content(cursor)?,
        decode_evidence_id(cursor)?,
    ))
}

fn decode_optional_idempotency(
    cursor: &mut Cursor<'_>,
) -> Result<Option<SourceIdempotency>, JournalV1Error> {
    if cursor.option()? {
        Ok(Some(SourceIdempotency::new(
            RetryKey::new(cursor.string(MAX_RETRY_KEY_BYTES)?).map_err(invalid_model)?,
            decode_content(cursor)?,
        )))
    } else {
        Ok(None)
    }
}

fn decode_source_gap(cursor: &mut Cursor<'_>) -> Result<SourceGapEvidence, JournalV1Error> {
    let epoch = ProducerEpoch::new(cursor.u128()?);
    let start = ProducerSequence::new(cursor.u128()?);
    let end = ProducerSequence::new(cursor.u128()?);
    let reason = match cursor.u8()? {
        1 => SourceGapReason::CommunicationFailure,
        2 => SourceGapReason::SourceUnavailable,
        3 => SourceGapReason::ProducerReset,
        4 => SourceGapReason::Filtered,
        5 => SourceGapReason::Unknown,
        _ => return Err(JournalV1Error::UnknownTag),
    };
    SourceGapEvidence::new(epoch, start, end, reason).map_err(invalid_model)
}

fn decode_artifact(cursor: &mut Cursor<'_>) -> Result<ArtifactReference, JournalV1Error> {
    Ok(ArtifactReference::new(
        decode_artifact_id(cursor)?,
        decode_content(cursor)?,
    ))
}

fn decode_optional_artifact(
    cursor: &mut Cursor<'_>,
) -> Result<Option<ArtifactReference>, JournalV1Error> {
    if cursor.option()? {
        Ok(Some(decode_artifact(cursor)?))
    } else {
        Ok(None)
    }
}

fn decode_content(cursor: &mut Cursor<'_>) -> Result<ContentIdentity, JournalV1Error> {
    Ok(ContentIdentity::new(
        ContentFormat::new(cursor.string(MAX_CONTENT_FORMAT_BYTES)?).map_err(invalid_model)?,
        ContentVersion::new(cursor.u128()?),
        cursor.array()?,
    ))
}

fn decode_quantity(cursor: &mut Cursor<'_>) -> Result<QuantityEvidence, JournalV1Error> {
    match cursor.u8()? {
        0 => Ok(QuantityEvidence::Absent),
        1 => Ok(QuantityEvidence::Resolved(decode_declaration_reference(
            cursor,
        )?)),
        2 => Ok(QuantityEvidence::Unresolved(decode_declaration_reference(
            cursor,
        )?)),
        _ => Err(JournalV1Error::UnknownTag),
    }
}

fn decode_unit(cursor: &mut Cursor<'_>) -> Result<UnitEvidence, JournalV1Error> {
    match cursor.u8()? {
        0 => Ok(UnitEvidence::Absent),
        1 => Ok(UnitEvidence::Resolved(decode_declaration_reference(
            cursor,
        )?)),
        2 => Ok(UnitEvidence::Unresolved(decode_declaration_reference(
            cursor,
        )?)),
        _ => Err(JournalV1Error::UnknownTag),
    }
}

fn decode_optional_timestamp(cursor: &mut Cursor<'_>) -> Result<Option<Timestamp>, JournalV1Error> {
    if cursor.option()? {
        Ok(Some(decode_timestamp(cursor)?))
    } else {
        Ok(None)
    }
}

fn decode_optional_position(
    cursor: &mut Cursor<'_>,
) -> Result<Option<ProducerPosition>, JournalV1Error> {
    if cursor.option()? {
        Ok(Some(ProducerPosition::new(
            ProducerEpoch::new(cursor.u128()?),
            ProducerSequence::new(cursor.u128()?),
        )))
    } else {
        Ok(None)
    }
}

fn decode_optional_interval(
    cursor: &mut Cursor<'_>,
) -> Result<Option<TimeInterval>, JournalV1Error> {
    if cursor.option()? {
        Ok(Some(decode_time_interval(cursor)?))
    } else {
        Ok(None)
    }
}

fn decode_timestamp(cursor: &mut Cursor<'_>) -> Result<Timestamp, JournalV1Error> {
    Timestamp::new(cursor.i64()?, cursor.u32()?).map_err(invalid_model)
}

fn decode_time_interval(cursor: &mut Cursor<'_>) -> Result<TimeInterval, JournalV1Error> {
    TimeInterval::new(decode_timestamp(cursor)?, decode_timestamp(cursor)?).map_err(invalid_model)
}

fn decode_collection_mode(cursor: &mut Cursor<'_>) -> Result<CollectionMode, JournalV1Error> {
    match cursor.u8()? {
        1 => Ok(CollectionMode::Sampled),
        2 => Ok(CollectionMode::ChangeOnly),
        3 => Ok(CollectionMode::Cumulative),
        4 => Ok(CollectionMode::Interval),
        5 => Ok(CollectionMode::Event),
        _ => Err(JournalV1Error::UnknownTag),
    }
}

fn decode_value_family(cursor: &mut Cursor<'_>) -> Result<ValueFamily, JournalV1Error> {
    match cursor.u8()? {
        1 => Ok(ValueFamily::Real),
        2 => Ok(ValueFamily::Signed),
        3 => Ok(ValueFamily::Unsigned),
        4 => Ok(ValueFamily::Boolean),
        5 => Ok(ValueFamily::State),
        6 => Ok(ValueFamily::Text),
        7 => Ok(ValueFamily::Artifact),
        _ => Err(JournalV1Error::UnknownTag),
    }
}

fn decode_source_interval_kind(
    cursor: &mut Cursor<'_>,
) -> Result<SourceIntervalKind, JournalV1Error> {
    match cursor.u8()? {
        1 => Ok(SourceIntervalKind::Observed),
        2 => Ok(SourceIntervalKind::NoChange),
        _ => Err(JournalV1Error::UnknownTag),
    }
}

fn decode_declaration_reference(
    cursor: &mut Cursor<'_>,
) -> Result<DeclarationReference, JournalV1Error> {
    DeclarationReference::new(cursor.string(MAX_DECLARATION_REFERENCE_BYTES)?)
        .map_err(invalid_model)
}

fn decode_source_projection(cursor: &mut Cursor<'_>) -> Result<SourceProjection, JournalV1Error> {
    SourceProjection::new(cursor.string(MAX_DECLARATION_REFERENCE_BYTES)?).map_err(invalid_model)
}

fn decode_identity_bytes(cursor: &mut Cursor<'_>) -> Result<[u8; 16], JournalV1Error> {
    cursor.array()
}

fn decode_store_id(cursor: &mut Cursor<'_>) -> Result<StoreId, JournalV1Error> {
    StoreId::from_bytes(decode_identity_bytes(cursor)?).map_err(|_| JournalV1Error::InvalidIdentity)
}

fn decode_series_id(cursor: &mut Cursor<'_>) -> Result<SeriesId, JournalV1Error> {
    SeriesId::from_bytes(decode_identity_bytes(cursor)?)
        .map_err(|_| JournalV1Error::InvalidIdentity)
}

fn decode_producer_id(cursor: &mut Cursor<'_>) -> Result<ProducerId, JournalV1Error> {
    ProducerId::from_bytes(decode_identity_bytes(cursor)?)
        .map_err(|_| JournalV1Error::InvalidIdentity)
}

fn decode_observation_id(cursor: &mut Cursor<'_>) -> Result<ObservationId, JournalV1Error> {
    ObservationId::from_bytes(decode_identity_bytes(cursor)?)
        .map_err(|_| JournalV1Error::InvalidIdentity)
}

fn decode_artifact_id(cursor: &mut Cursor<'_>) -> Result<ArtifactId, JournalV1Error> {
    ArtifactId::from_bytes(decode_identity_bytes(cursor)?)
        .map_err(|_| JournalV1Error::InvalidIdentity)
}

fn decode_evidence_id(cursor: &mut Cursor<'_>) -> Result<EvidenceId, JournalV1Error> {
    EvidenceId::from_bytes(decode_identity_bytes(cursor)?)
        .map_err(|_| JournalV1Error::InvalidIdentity)
}

fn validate_decoded(admission: &DecodedAdmissionV1) -> Result<(), JournalV1Error> {
    let declaration = admission.declaration();
    let envelope = admission.envelope();
    let metadata = envelope.series();
    let payload = declaration.payload();
    if admission.store_id() != declaration.store_id()
        || declaration.series_id() != metadata.series_id()
        || payload.producer_id() != metadata.producer_id()
        || payload.collection_mode() != metadata.collection_mode()
        || admission.retry().series_id() != metadata.series_id()
        || admission.retry().producer_id() != metadata.producer_id()
    {
        return Err(JournalV1Error::InvalidCanonicalData);
    }

    let revision = declaration.revision().get();
    let expected_previous = if revision == DeclarationRevision::FIRST.get() {
        None
    } else {
        Some(
            DeclarationRevision::new(revision - 1)
                .map_err(|_| JournalV1Error::InvalidCanonicalData)?,
        )
    };
    if declaration.previous_revision() != expected_previous {
        return Err(JournalV1Error::InvalidCanonicalData);
    }

    let expected_interval = match envelope.evidence_kind() {
        EvidenceKind::Observed => SourceIntervalKind::Observed,
        EvidenceKind::NoChange => SourceIntervalKind::NoChange,
    };
    if admission.batch().interval() != expected_interval
        || admission.evidence_kind() != expected_interval
    {
        return Err(JournalV1Error::InvalidCanonicalData);
    }

    let source = declaration.binding().source();
    let Some(projection) = source.projection() else {
        return Err(JournalV1Error::InvalidCanonicalData);
    };
    if admission.lifecycle().system().provider() != source.provider()
        || admission.lifecycle().system().projection() != projection
        || admission.lifecycle().endpoint().locator() != source.locator()
    {
        return Err(JournalV1Error::InvalidCanonicalData);
    }

    if envelope
        .observations()
        .iter()
        .any(|observation| !payload.value_family().admits(observation.value()))
    {
        return Err(JournalV1Error::InvalidCanonicalData);
    }

    match admission.evidence() {
        DecodedEvidenceV1::Observed { observations, gaps } => {
            if observations.len() != envelope.observations().len()
                || gaps.len() != envelope.gaps().len()
            {
                return Err(JournalV1Error::InvalidCanonicalData);
            }
            let mut previous_ordinal = None;
            for (lineage, canonical) in observations.iter().zip(envelope.observations()) {
                if lineage.canonical_observation_id() != canonical.observation_id()
                    || previous_ordinal.is_some_and(|previous| previous >= lineage.ordinal())
                    || lineage.raw().snapshot_id() != admission.lifecycle().snapshot().evidence_id()
                    || lineage.normalized().raw_record_id() != lineage.raw().evidence_id()
                    || lineage.normalized().observation_evidence_id()
                        != lineage.observation().evidence_id()
                    || lineage.raw().idempotency().is_some_and(|idempotency| {
                        idempotency.content() != lineage.raw().artifact().content()
                    })
                {
                    return Err(JournalV1Error::InvalidCanonicalData);
                }
                previous_ordinal = Some(lineage.ordinal());
            }
            if gaps.iter().zip(envelope.gaps()).any(|(source_gap, gap)| {
                source_gap.epoch() != gap.epoch()
                    || source_gap.start() != gap.start()
                    || source_gap.end() != gap.end()
            }) {
                return Err(JournalV1Error::InvalidCanonicalData);
            }
        }
        DecodedEvidenceV1::NoChange => {}
    }

    let evidence_ids = all_evidence_ids(admission);
    for (index, evidence_id) in evidence_ids.iter().enumerate() {
        if evidence_ids[..index].contains(evidence_id) {
            return Err(JournalV1Error::InvalidCanonicalData);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn crc32c_matches_castagnoli_known_vectors() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }
}
