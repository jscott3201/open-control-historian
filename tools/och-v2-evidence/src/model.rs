use crate::error::{EvidenceError, Result};
use crate::ledger::{MAX_FRAMES, MAX_OBSERVATIONS, MAX_SERIES};
use och_core::StoreId;
use std::fs::File;
use std::io::Read;

pub(crate) const FIXTURE_SCHEMA: &str = "och-v2-evidence-fixture-v1";
pub(crate) const IDENTITY_SCHEMA: &str = "och-v2-evidence-segment-identity-v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FixtureMeta {
    pub(crate) case: String,
    pub(crate) seed: u64,
    pub(crate) store_id: StoreId,
    pub(crate) journal_generation: u64,
    pub(crate) sequence_floor: u64,
    pub(crate) sequence_cutoff: u64,
    pub(crate) registry_generation: u64,
    pub(crate) source_length: u64,
    pub(crate) source_checksum: u32,
    pub(crate) frame_count: usize,
    pub(crate) series_count: usize,
    pub(crate) observation_count: usize,
}

impl FixtureMeta {
    pub(crate) fn validate(&self) -> Result<()> {
        let expected_frames = self
            .sequence_cutoff
            .checked_sub(self.sequence_floor)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or(EvidenceError::InvalidFixture)?;
        if !valid_case_name(&self.case)
            || self.journal_generation == 0
            || self.registry_generation == 0
            || self.frame_count == 0
            || self.frame_count > MAX_FRAMES
            || self.series_count == 0
            || self.series_count > MAX_SERIES
            || self.series_count > self.frame_count
            || self.observation_count > MAX_OBSERVATIONS
            || self.frame_count != expected_frames
            || self.source_length <= och_store::JOURNAL_V1_HEADER_LEN as u64
            || self.source_length > och_store::MAX_ACTIVE_JOURNAL_BYTES
        {
            return Err(EvidenceError::InvalidFixture);
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> String {
        format!(
            concat!(
                "schema={}\n",
                "case={}\n",
                "seed={}\n",
                "store_id={}\n",
                "journal_generation={}\n",
                "sequence_floor={}\n",
                "sequence_cutoff={}\n",
                "registry_generation={}\n",
                "source_length={}\n",
                "source_crc32c={:08x}\n",
                "frames={}\n",
                "series={}\n",
                "observations={}\n",
            ),
            FIXTURE_SCHEMA,
            self.case,
            self.seed,
            encode_hex(self.store_id.as_bytes()),
            self.journal_generation,
            self.sequence_floor,
            self.sequence_cutoff,
            self.registry_generation,
            self.source_length,
            self.source_checksum,
            self.frame_count,
            self.series_count,
            self.observation_count,
        )
    }

    pub(crate) fn read(file: File) -> Result<Self> {
        let text = read_bounded_text(file, 2_048, EvidenceError::InvalidFixture)?;
        let mut fields = Fields::new(&text)?;
        if fields.take("schema")? != FIXTURE_SCHEMA {
            return Err(EvidenceError::InvalidFixture);
        }
        let case = fields.take("case")?.to_owned();
        let seed = parse_u64(fields.take("seed")?)?;
        let store_id = StoreId::from_bytes(decode_hex_16(fields.take("store_id")?)?)
            .map_err(|_| EvidenceError::InvalidFixture)?;
        let meta = Self {
            case,
            seed,
            store_id,
            journal_generation: parse_u64(fields.take("journal_generation")?)?,
            sequence_floor: parse_u64(fields.take("sequence_floor")?)?,
            sequence_cutoff: parse_u64(fields.take("sequence_cutoff")?)?,
            registry_generation: parse_u64(fields.take("registry_generation")?)?,
            source_length: parse_u64(fields.take("source_length")?)?,
            source_checksum: parse_hex_u32(fields.take("source_crc32c")?)?,
            frame_count: parse_usize(fields.take("frames")?)?,
            series_count: parse_usize(fields.take("series")?)?,
            observation_count: parse_usize(fields.take("observations")?)?,
        };
        fields.finish()?;
        meta.validate()?;
        Ok(meta)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentIdentity {
    pub(crate) artifact_length: u64,
    pub(crate) artifact_checksum: u32,
    pub(crate) source_length: u64,
    pub(crate) source_checksum: u32,
}

impl SegmentIdentity {
    pub(crate) fn encode(self) -> String {
        format!(
            concat!(
                "schema={}\n",
                "artifact_length={}\n",
                "artifact_crc32c={:08x}\n",
                "source_length={}\n",
                "source_crc32c={:08x}\n",
            ),
            IDENTITY_SCHEMA,
            self.artifact_length,
            self.artifact_checksum,
            self.source_length,
            self.source_checksum,
        )
    }

    pub(crate) fn read(file: File) -> Result<Self> {
        let text = read_bounded_text(file, 512, EvidenceError::InvalidFixture)?;
        let mut fields = Fields::new(&text)?;
        if fields.take("schema")? != IDENTITY_SCHEMA {
            return Err(EvidenceError::InvalidFixture);
        }
        let identity = Self {
            artifact_length: parse_u64(fields.take("artifact_length")?)?,
            artifact_checksum: parse_hex_u32(fields.take("artifact_crc32c")?)?,
            source_length: parse_u64(fields.take("source_length")?)?,
            source_checksum: parse_hex_u32(fields.take("source_crc32c")?)?,
        };
        fields.finish()?;
        Ok(identity)
    }
}

pub(crate) fn read_bounded_text(
    mut file: File,
    limit: usize,
    excess_error: EvidenceError,
) -> Result<String> {
    let read_limit = limit.checked_add(1).ok_or(EvidenceError::Bounds)?;
    let mut bytes = vec![0_u8; read_limit];
    let mut filled = 0_usize;
    while filled < read_limit {
        let read = file
            .read(&mut bytes[filled..])
            .map_err(|_| EvidenceError::Io)?;
        if read == 0 {
            break;
        }
        filled = filled.checked_add(read).ok_or(EvidenceError::Bounds)?;
    }
    if filled > limit {
        return Err(excess_error);
    }
    bytes.truncate(filled);
    String::from_utf8(bytes).map_err(|_| EvidenceError::InvalidFixture)
}

pub(crate) fn valid_case_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn parse_u64(value: &str) -> Result<u64> {
    value.parse().map_err(|_| EvidenceError::InvalidFixture)
}

fn parse_usize(value: &str) -> Result<usize> {
    value.parse().map_err(|_| EvidenceError::InvalidFixture)
}

fn parse_hex_u32(value: &str) -> Result<u32> {
    if value.len() != 8 {
        return Err(EvidenceError::InvalidFixture);
    }
    u32::from_str_radix(value, 16).map_err(|_| EvidenceError::InvalidFixture)
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn decode_hex_16(value: &str) -> Result<[u8; 16]> {
    if value.len() != 32 {
        return Err(EvidenceError::InvalidFixture);
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&value[start..start + 2], 16)
            .map_err(|_| EvidenceError::InvalidFixture)?;
    }
    Ok(bytes)
}

struct Fields<'a> {
    values: std::collections::BTreeMap<&'a str, &'a str>,
}

impl<'a> Fields<'a> {
    fn new(text: &'a str) -> Result<Self> {
        let mut values = std::collections::BTreeMap::new();
        for line in text.lines() {
            let (key, value) = line.split_once('=').ok_or(EvidenceError::InvalidFixture)?;
            if key.is_empty() || values.insert(key, value).is_some() {
                return Err(EvidenceError::InvalidFixture);
            }
        }
        Ok(Self { values })
    }

    fn take(&mut self, key: &str) -> Result<&'a str> {
        self.values.remove(key).ok_or(EvidenceError::InvalidFixture)
    }

    fn finish(self) -> Result<()> {
        if self.values.is_empty() {
            Ok(())
        } else {
            Err(EvidenceError::InvalidFixture)
        }
    }
}
