//! Transient, crate-test-only typed-value block codec proof.

use crate::MAX_ADMISSION_PAYLOAD_V1;
use och_core::bounded::{MAX_CONTENT_FORMAT_BYTES, MAX_PORTABLE_TOKEN_BYTES, MAX_TEXT_SCALARS};
use och_core::{
    ArtifactId, ArtifactReference, ContentFormat, ContentIdentity, ContentVersion, ExactText,
    ExactValue, MAX_SOURCE_OBSERVATION_CONTEXTS, RealBits, StateClass, StateMember, StateValue,
    Unavailable, UnavailableReason, ValueFamily,
};
use std::str;

#[path = "../tests/support/typed_value_block_oracle.rs"]
mod oracle;

const MAGIC: [u8; 4] = *b"TVBP";
const INTERNAL_VERSION: u8 = 1;
const HEADER_LEN: usize = 16;
const PACKED_ORDER_LSB_FIRST: u8 = 1;
const RLE_RUN_WIDTH_U16: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlockError {
    InvalidBlock,
    Bounds,
    FamilyMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Codec {
    Raw,
    BitPack,
    Rle,
}

impl Codec {
    const fn code(self) -> u8 {
        match self {
            Self::Raw => 0,
            Self::BitPack => 1,
            Self::Rle => 2,
        }
    }

    const fn from_code(code: u8) -> Result<Self, BlockError> {
        match code {
            0 => Ok(Self::Raw),
            1 => Ok(Self::BitPack),
            2 => Ok(Self::Rle),
            _ => Err(BlockError::InvalidBlock),
        }
    }
}

fn encode(family: ValueFamily, values: &[ExactValue]) -> Result<Vec<u8>, BlockError> {
    validate_count(values.len())?;
    if !values.iter().all(|value| family.admits(value)) {
        return Err(BlockError::FamilyMismatch);
    }

    let raw_payload_len = raw_payload_len(family, values)?;
    let (codec, payload_len) = choose_codec(family, values, raw_payload_len)?;
    let total_len = checked_total_len(payload_len)?;
    let mut bytes = Vec::with_capacity(total_len);
    write_header(&mut bytes, family, codec, values.len(), payload_len)?;
    match codec {
        Codec::Raw => encode_raw(&mut bytes, family, values)?,
        Codec::BitPack => encode_packed(&mut bytes, values),
        Codec::Rle => encode_rle(&mut bytes, values)?,
    }
    if bytes.len() != total_len {
        return Err(BlockError::InvalidBlock);
    }
    Ok(bytes)
}

fn decode(expected_family: ValueFamily, bytes: &[u8]) -> Result<Vec<ExactValue>, BlockError> {
    if bytes.len() > MAX_ADMISSION_PAYLOAD_V1 {
        return Err(BlockError::Bounds);
    }
    if bytes.len() < HEADER_LEN || bytes[..4] != MAGIC {
        return Err(BlockError::InvalidBlock);
    }
    if bytes[4] != INTERNAL_VERSION || bytes[7] != 0 || bytes[10..12] != [0; 2] {
        return Err(BlockError::InvalidBlock);
    }

    let family = family_from_code(bytes[5])?;
    if family != expected_family {
        return Err(BlockError::FamilyMismatch);
    }
    let codec = Codec::from_code(bytes[6])?;
    if codec != Codec::Raw && family != ValueFamily::Boolean {
        return Err(BlockError::InvalidBlock);
    }
    let count = usize::from(u16::from_le_bytes([bytes[8], bytes[9]]));
    validate_count(count)?;
    let payload_len_u32 = u32::from_le_bytes(
        bytes[12..16]
            .try_into()
            .map_err(|_| BlockError::InvalidBlock)?,
    );
    let payload_len = usize::try_from(payload_len_u32).map_err(|_| BlockError::Bounds)?;
    let total_len = checked_total_len(payload_len)?;
    if total_len != bytes.len() {
        return Err(BlockError::InvalidBlock);
    }
    let payload = &bytes[HEADER_LEN..];

    let values = match codec {
        Codec::Raw => decode_raw(family, count, payload)?,
        Codec::BitPack => decode_packed(count, payload)?,
        Codec::Rle => decode_rle(count, payload)?,
    };
    let raw_payload_len = raw_payload_len(family, &values)?;
    let (winner, _) = choose_codec(family, &values, raw_payload_len)?;
    if winner != codec {
        return Err(BlockError::InvalidBlock);
    }
    Ok(values)
}

fn validate_count(count: usize) -> Result<(), BlockError> {
    if count == 0 || count > MAX_SOURCE_OBSERVATION_CONTEXTS {
        Err(BlockError::Bounds)
    } else {
        Ok(())
    }
}

fn checked_total_len(payload_len: usize) -> Result<usize, BlockError> {
    let total = HEADER_LEN
        .checked_add(payload_len)
        .ok_or(BlockError::Bounds)?;
    if total > MAX_ADMISSION_PAYLOAD_V1 {
        Err(BlockError::Bounds)
    } else {
        Ok(total)
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, BlockError> {
    left.checked_add(right).ok_or(BlockError::Bounds)
}

fn family_code(family: ValueFamily) -> u8 {
    match family {
        ValueFamily::Real => 0,
        ValueFamily::Signed => 1,
        ValueFamily::Unsigned => 2,
        ValueFamily::Boolean => 3,
        ValueFamily::State => 4,
        ValueFamily::Text => 5,
        ValueFamily::Artifact => 6,
    }
}

fn family_from_code(code: u8) -> Result<ValueFamily, BlockError> {
    match code {
        0 => Ok(ValueFamily::Real),
        1 => Ok(ValueFamily::Signed),
        2 => Ok(ValueFamily::Unsigned),
        3 => Ok(ValueFamily::Boolean),
        4 => Ok(ValueFamily::State),
        5 => Ok(ValueFamily::Text),
        6 => Ok(ValueFamily::Artifact),
        _ => Err(BlockError::InvalidBlock),
    }
}

fn write_header(
    bytes: &mut Vec<u8>,
    family: ValueFamily,
    codec: Codec,
    count: usize,
    payload_len: usize,
) -> Result<(), BlockError> {
    let count = u16::try_from(count).map_err(|_| BlockError::Bounds)?;
    let payload_len = u32::try_from(payload_len).map_err(|_| BlockError::Bounds)?;
    bytes.extend_from_slice(&MAGIC);
    bytes.push(INTERNAL_VERSION);
    bytes.push(family_code(family));
    bytes.push(codec.code());
    bytes.push(0);
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(&[0; 2]);
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    Ok(())
}

fn raw_payload_len(family: ValueFamily, values: &[ExactValue]) -> Result<usize, BlockError> {
    values.iter().try_fold(0, |total, value| {
        checked_add(total, raw_value_len(family, value)?)
    })
}

fn unavailable_len(value: &Unavailable) -> Result<usize, BlockError> {
    value
        .reason()
        .map_or(Ok(1), |reason| checked_add(3, reason.as_str().len()))
}

fn raw_value_len(family: ValueFamily, value: &ExactValue) -> Result<usize, BlockError> {
    if let ExactValue::Unavailable(value) = value {
        return unavailable_len(value);
    }
    match (family, value) {
        (ValueFamily::Real, ExactValue::Real(_))
        | (ValueFamily::Signed, ExactValue::Signed(_))
        | (ValueFamily::Unsigned, ExactValue::Unsigned(_)) => Ok(9),
        (ValueFamily::Boolean, ExactValue::Boolean(_)) => Ok(1),
        (ValueFamily::State, ExactValue::State(value)) => checked_add(
            checked_add(5, value.class().as_str().len())?,
            value.member().as_str().len(),
        ),
        (ValueFamily::Text, ExactValue::Text(value)) => checked_add(5, value.as_str().len()),
        (ValueFamily::Artifact, ExactValue::Artifact(value)) => {
            checked_add(67, value.content().format().as_str().len())
        }
        _ => Err(BlockError::FamilyMismatch),
    }
}

fn choose_codec(
    family: ValueFamily,
    values: &[ExactValue],
    raw_payload_len: usize,
) -> Result<(Codec, usize), BlockError> {
    let raw_total = checked_total_len(raw_payload_len)?;
    if family != ValueFamily::Boolean
        || !values
            .iter()
            .all(|value| matches!(value, ExactValue::Boolean(_)))
    {
        return Ok((Codec::Raw, raw_payload_len));
    }

    let packed_payload_len = checked_add(1, values.len().div_ceil(8))?;
    let run_bytes = boolean_run_count(values)?
        .checked_mul(3)
        .ok_or(BlockError::Bounds)?;
    let rle_payload_len = checked_add(1, run_bytes)?;
    let packed_total = checked_total_len(packed_payload_len)?;
    let rle_total = checked_total_len(rle_payload_len)?;

    if packed_total < raw_total && packed_total <= rle_total {
        Ok((Codec::BitPack, packed_payload_len))
    } else if rle_total < raw_total && rle_total < packed_total {
        Ok((Codec::Rle, rle_payload_len))
    } else {
        Ok((Codec::Raw, raw_payload_len))
    }
}

fn boolean_run_count(values: &[ExactValue]) -> Result<usize, BlockError> {
    let mut previous = None;
    let mut count = 0_usize;
    for value in values {
        let ExactValue::Boolean(value) = value else {
            return Err(BlockError::FamilyMismatch);
        };
        if previous != Some(*value) {
            previous = Some(*value);
            count = count.checked_add(1).ok_or(BlockError::Bounds)?;
        }
    }
    Ok(count)
}

fn encode_raw(
    bytes: &mut Vec<u8>,
    family: ValueFamily,
    values: &[ExactValue],
) -> Result<(), BlockError> {
    for value in values {
        if family == ValueFamily::Boolean {
            match value {
                ExactValue::Boolean(value) => bytes.push(u8::from(*value)),
                ExactValue::Unavailable(value) => encode_unavailable(bytes, value, 2, 3)?,
                _ => return Err(BlockError::FamilyMismatch),
            }
        } else if let ExactValue::Unavailable(value) = value {
            encode_unavailable(bytes, value, 1, 2)?;
        } else {
            bytes.push(0);
            encode_available(bytes, family, value)?;
        }
    }
    Ok(())
}

fn encode_unavailable(
    bytes: &mut Vec<u8>,
    value: &Unavailable,
    absent_marker: u8,
    reason_marker: u8,
) -> Result<(), BlockError> {
    if let Some(reason) = value.reason() {
        bytes.push(reason_marker);
        put_u16_bytes(bytes, reason.as_str().as_bytes())?;
    } else {
        bytes.push(absent_marker);
    }
    Ok(())
}

fn encode_available(
    bytes: &mut Vec<u8>,
    family: ValueFamily,
    value: &ExactValue,
) -> Result<(), BlockError> {
    match (family, value) {
        (ValueFamily::Real, ExactValue::Real(value)) => {
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        (ValueFamily::Signed, ExactValue::Signed(value)) => {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        (ValueFamily::Unsigned, ExactValue::Unsigned(value)) => {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        (ValueFamily::State, ExactValue::State(value)) => {
            put_u16_bytes(bytes, value.class().as_str().as_bytes())?;
            put_u16_bytes(bytes, value.member().as_str().as_bytes())?;
        }
        (ValueFamily::Text, ExactValue::Text(value)) => {
            put_u32_bytes(bytes, value.as_str().as_bytes())?;
        }
        (ValueFamily::Artifact, ExactValue::Artifact(value)) => {
            bytes.extend_from_slice(value.artifact_id().as_bytes());
            put_u16_bytes(bytes, value.content().format().as_str().as_bytes())?;
            bytes.extend_from_slice(&value.content().version().get().to_le_bytes());
            bytes.extend_from_slice(value.content().sha256());
        }
        _ => return Err(BlockError::FamilyMismatch),
    }
    Ok(())
}

fn put_u16_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), BlockError> {
    let length = u16::try_from(value.len()).map_err(|_| BlockError::Bounds)?;
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

fn put_u32_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), BlockError> {
    let length = u32::try_from(value.len()).map_err(|_| BlockError::Bounds)?;
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

fn encode_packed(bytes: &mut Vec<u8>, values: &[ExactValue]) {
    bytes.push(PACKED_ORDER_LSB_FIRST);
    let byte_count = values.len().div_ceil(8);
    let payload_start = bytes.len();
    bytes.resize(payload_start + byte_count, 0);
    for (index, value) in values.iter().enumerate() {
        if matches!(value, ExactValue::Boolean(true)) {
            bytes[payload_start + index / 8] |= 1 << (index % 8);
        }
    }
}

fn encode_rle(bytes: &mut Vec<u8>, values: &[ExactValue]) -> Result<(), BlockError> {
    bytes.push(RLE_RUN_WIDTH_U16);
    let mut start = 0;
    while start < values.len() {
        let ExactValue::Boolean(value) = values[start] else {
            return Err(BlockError::FamilyMismatch);
        };
        let mut end = start + 1;
        while end < values.len() && values[end] == ExactValue::Boolean(value) {
            end += 1;
        }
        let run = u16::try_from(end - start).map_err(|_| BlockError::Bounds)?;
        bytes.push(u8::from(value));
        bytes.extend_from_slice(&run.to_le_bytes());
        start = end;
    }
    Ok(())
}

fn decode_raw(
    family: ValueFamily,
    count: usize,
    payload: &[u8],
) -> Result<Vec<ExactValue>, BlockError> {
    if payload.len() < count {
        return Err(BlockError::InvalidBlock);
    }
    let mut reader = Reader::new(payload);
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let marker = reader.byte()?;
        let value = if family == ValueFamily::Boolean {
            match marker {
                0 => ExactValue::Boolean(false),
                1 => ExactValue::Boolean(true),
                2 => ExactValue::Unavailable(Unavailable::without_reason()),
                3 => ExactValue::Unavailable(Unavailable::new(Some(decode_reason(&mut reader)?))),
                _ => return Err(BlockError::InvalidBlock),
            }
        } else {
            match marker {
                0 => decode_available(family, &mut reader)?,
                1 => ExactValue::Unavailable(Unavailable::without_reason()),
                2 => ExactValue::Unavailable(Unavailable::new(Some(decode_reason(&mut reader)?))),
                _ => return Err(BlockError::InvalidBlock),
            }
        };
        values.push(value);
    }
    if !reader.finished() {
        return Err(BlockError::InvalidBlock);
    }
    Ok(values)
}

fn decode_reason(reader: &mut Reader<'_>) -> Result<UnavailableReason, BlockError> {
    let length = reader.u16_length()?;
    if length == 0 || length > MAX_PORTABLE_TOKEN_BYTES {
        return Err(BlockError::InvalidBlock);
    }
    let text = str::from_utf8(reader.take(length)?).map_err(|_| BlockError::InvalidBlock)?;
    UnavailableReason::new(text.to_owned()).map_err(|_| BlockError::InvalidBlock)
}

fn decode_available(
    family: ValueFamily,
    reader: &mut Reader<'_>,
) -> Result<ExactValue, BlockError> {
    match family {
        ValueFamily::Real => Ok(ExactValue::Real(RealBits::from_bits(reader.u64()?))),
        ValueFamily::Signed => Ok(ExactValue::Signed(reader.i64()?)),
        ValueFamily::Unsigned => Ok(ExactValue::Unsigned(reader.u64()?)),
        ValueFamily::Boolean => Err(BlockError::InvalidBlock),
        ValueFamily::State => decode_state(reader),
        ValueFamily::Text => decode_text(reader),
        ValueFamily::Artifact => decode_artifact(reader),
    }
}

fn decode_state(reader: &mut Reader<'_>) -> Result<ExactValue, BlockError> {
    let class_len = reader.u16_length()?;
    if class_len == 0 || class_len > MAX_PORTABLE_TOKEN_BYTES {
        return Err(BlockError::InvalidBlock);
    }
    let class = str::from_utf8(reader.take(class_len)?)
        .map_err(|_| BlockError::InvalidBlock)?
        .to_owned();
    let member_len = reader.u16_length()?;
    if member_len == 0 || member_len > MAX_PORTABLE_TOKEN_BYTES {
        return Err(BlockError::InvalidBlock);
    }
    let member = str::from_utf8(reader.take(member_len)?)
        .map_err(|_| BlockError::InvalidBlock)?
        .to_owned();
    let class = StateClass::new(class).map_err(|_| BlockError::InvalidBlock)?;
    let member = StateMember::new(member).map_err(|_| BlockError::InvalidBlock)?;
    Ok(ExactValue::State(StateValue::new(class, member)))
}

fn decode_text(reader: &mut Reader<'_>) -> Result<ExactValue, BlockError> {
    let length = reader.u32_length()?;
    let bytes = reader.take(length)?;
    let text = str::from_utf8(bytes).map_err(|_| BlockError::InvalidBlock)?;
    if text.chars().take(MAX_TEXT_SCALARS + 1).count() > MAX_TEXT_SCALARS {
        return Err(BlockError::InvalidBlock);
    }
    ExactText::new(text.to_owned())
        .map(ExactValue::Text)
        .map_err(|_| BlockError::InvalidBlock)
}

fn decode_artifact(reader: &mut Reader<'_>) -> Result<ExactValue, BlockError> {
    let artifact_id =
        ArtifactId::from_bytes(reader.array()?).map_err(|_| BlockError::InvalidBlock)?;
    let format_len = reader.u16_length()?;
    if format_len == 0 || format_len > MAX_CONTENT_FORMAT_BYTES {
        return Err(BlockError::InvalidBlock);
    }
    let format = str::from_utf8(reader.take(format_len)?)
        .map_err(|_| BlockError::InvalidBlock)?
        .to_owned();
    let format = ContentFormat::new(format).map_err(|_| BlockError::InvalidBlock)?;
    let version = ContentVersion::new(reader.u128()?);
    let digest = reader.array()?;
    Ok(ExactValue::Artifact(ArtifactReference::new(
        artifact_id,
        ContentIdentity::new(format, version, digest),
    )))
}

fn decode_packed(count: usize, payload: &[u8]) -> Result<Vec<ExactValue>, BlockError> {
    let byte_count = count.div_ceil(8);
    let expected_len = checked_add(1, byte_count)?;
    if payload.len() != expected_len || payload.first() != Some(&PACKED_ORDER_LSB_FIRST) {
        return Err(BlockError::InvalidBlock);
    }
    let used_bits = count % 8;
    if used_bits != 0 {
        let unused_mask = u8::MAX << used_bits;
        if payload[expected_len - 1] & unused_mask != 0 {
            return Err(BlockError::InvalidBlock);
        }
    }
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        values.push(ExactValue::Boolean(
            payload[1 + index / 8] & (1 << (index % 8)) != 0,
        ));
    }
    Ok(values)
}

fn decode_rle(count: usize, payload: &[u8]) -> Result<Vec<ExactValue>, BlockError> {
    if payload.first() != Some(&RLE_RUN_WIDTH_U16)
        || payload.len() < 4
        || !(payload.len() - 1).is_multiple_of(3)
    {
        return Err(BlockError::InvalidBlock);
    }
    let mut values = Vec::with_capacity(count);
    let mut previous = None;
    let (runs, remainder) = payload[1..].as_chunks::<3>();
    if !remainder.is_empty() {
        return Err(BlockError::InvalidBlock);
    }
    for run in runs {
        let value = match run[0] {
            0 => false,
            1 => true,
            _ => return Err(BlockError::InvalidBlock),
        };
        if previous == Some(value) {
            return Err(BlockError::InvalidBlock);
        }
        previous = Some(value);
        let length = usize::from(u16::from_le_bytes([run[1], run[2]]));
        if length == 0 {
            return Err(BlockError::InvalidBlock);
        }
        let new_len = values.len().checked_add(length).ok_or(BlockError::Bounds)?;
        if new_len > count {
            return Err(BlockError::InvalidBlock);
        }
        values.extend(std::iter::repeat_n(ExactValue::Boolean(value), length));
    }
    if values.len() != count {
        return Err(BlockError::InvalidBlock);
    }
    Ok(values)
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], BlockError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(BlockError::Bounds)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(BlockError::InvalidBlock)?;
        self.position = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, BlockError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], BlockError> {
        self.take(N)?
            .try_into()
            .map_err(|_| BlockError::InvalidBlock)
    }

    fn u16_length(&mut self) -> Result<usize, BlockError> {
        Ok(usize::from(u16::from_le_bytes(self.array()?)))
    }

    fn u32_length(&mut self) -> Result<usize, BlockError> {
        usize::try_from(u32::from_le_bytes(self.array()?)).map_err(|_| BlockError::Bounds)
    }

    fn u64(&mut self) -> Result<u64, BlockError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, BlockError> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn u128(&mut self) -> Result<u128, BlockError> {
        Ok(u128::from_le_bytes(self.array()?))
    }

    fn finished(&self) -> bool {
        self.position == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORACLE_SOURCE: &str = include_str!("../tests/support/typed_value_block_oracle.rs");
    const RAW_REAL_GOLDEN: &[u8] = &[
        b'T', b'V', b'B', b'P', 1, 0, 0, 0, 2, 0, 0, 0, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128,
        0, 66, 0, 0, 0, 0, 0, 248, 127,
    ];

    fn exact_text(value: &str) -> ExactValue {
        ExactValue::Text(ExactText::new(value.to_owned()).expect("bounded exact text"))
    }

    fn state(class: &str, member: &str) -> ExactValue {
        ExactValue::State(StateValue::new(
            StateClass::new(class.to_owned()).expect("bounded state class"),
            StateMember::new(member.to_owned()).expect("bounded state member"),
        ))
    }

    fn unavailable(reason: Option<&str>) -> ExactValue {
        ExactValue::Unavailable(Unavailable::new(reason.map(|reason| {
            UnavailableReason::new(reason.to_owned()).expect("bounded unavailable reason")
        })))
    }

    fn uuid_bytes() -> [u8; 16] {
        [
            0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x2a,
        ]
    }

    fn artifact() -> ExactValue {
        let digest = std::array::from_fn(|index| {
            u8::try_from(index).expect("digest index remains in one byte")
        });
        ExactValue::Artifact(ArtifactReference::new(
            ArtifactId::from_bytes(uuid_bytes()).expect("UUIDv7 artifact"),
            ContentIdentity::new(
                ContentFormat::new("application/vnd.test+bin".to_owned())
                    .expect("bounded content format"),
                ContentVersion::new(u128::MAX),
                digest,
            ),
        ))
    }

    fn booleans(values: &[bool]) -> Vec<ExactValue> {
        values.iter().copied().map(ExactValue::Boolean).collect()
    }

    fn repeated_boolean(value: bool, count: usize) -> Vec<ExactValue> {
        std::iter::repeat_n(ExactValue::Boolean(value), count).collect()
    }

    fn oracle_family(family: ValueFamily) -> oracle::Family {
        match family {
            ValueFamily::Real => oracle::Family::Real,
            ValueFamily::Signed => oracle::Family::Signed,
            ValueFamily::Unsigned => oracle::Family::Unsigned,
            ValueFamily::Boolean => oracle::Family::Boolean,
            ValueFamily::State => oracle::Family::State,
            ValueFamily::Text => oracle::Family::Text,
            ValueFamily::Artifact => oracle::Family::Artifact,
        }
    }

    fn oracle_value(value: &ExactValue) -> oracle::Value {
        match value {
            ExactValue::Real(value) => oracle::Value::Real(value.to_bits()),
            ExactValue::Signed(value) => oracle::Value::Signed(*value),
            ExactValue::Unsigned(value) => oracle::Value::Unsigned(*value),
            ExactValue::Boolean(value) => oracle::Value::Boolean(*value),
            ExactValue::State(value) => oracle::Value::State {
                class: value.class().as_str().as_bytes().to_vec(),
                member: value.member().as_str().as_bytes().to_vec(),
            },
            ExactValue::Text(value) => oracle::Value::Text(value.as_str().as_bytes().to_vec()),
            ExactValue::Artifact(value) => oracle::Value::Artifact {
                id: value.artifact_id().into_bytes(),
                format: value.content().format().as_str().as_bytes().to_vec(),
                version: value.content().version().get(),
                digest: *value.content().sha256(),
            },
            ExactValue::Unavailable(value) => oracle::Value::Unavailable(
                value
                    .reason()
                    .map(|reason| reason.as_str().as_bytes().to_vec()),
            ),
        }
    }

    fn assert_oracle_round_trip(family: ValueFamily, values: &[ExactValue]) -> Vec<u8> {
        let bytes = encode(family, values).expect("bounded proof block");
        assert_eq!(
            decode(family, &bytes).expect("canonical proof block"),
            values
        );
        let primitive: Vec<_> = values.iter().map(oracle_value).collect();
        assert_eq!(
            bytes,
            oracle::encode(oracle_family(family), &primitive),
            "product bytes must match the independent primitive oracle"
        );
        assert_eq!(
            oracle::decode(&bytes)
                .expect("oracle accepts product bytes")
                .values,
            primitive,
            "oracle reconstructs exact primitive values"
        );
        bytes
    }

    fn assert_raw_round_trip(family: ValueFamily, values: &[ExactValue]) {
        let bytes = assert_oracle_round_trip(family, values);
        assert_eq!(bytes[6], Codec::Raw.code());
    }

    #[test]
    fn typed_value_block_raw_round_trips_every_exact_family_domain() {
        let real = [
            0_u64,
            0x8000_0000_0000_0000,
            1,
            0x000f_ffff_ffff_ffff,
            0x7ff0_0000_0000_0000,
            0xfff0_0000_0000_0000,
            0x7ff0_0000_0000_0042,
            0x7ff8_0000_0000_0042,
            0xfff8_0000_0000_1234,
        ]
        .map(|bits| ExactValue::Real(RealBits::from_bits(bits)));
        assert_raw_round_trip(ValueFamily::Real, &real);
        assert_raw_round_trip(
            ValueFamily::Signed,
            &[
                ExactValue::Signed(i64::MIN),
                ExactValue::Signed(0),
                ExactValue::Signed(i64::MAX),
            ],
        );
        assert_raw_round_trip(
            ValueFamily::Unsigned,
            &[ExactValue::Unsigned(0), ExactValue::Unsigned(u64::MAX)],
        );
        assert_raw_round_trip(ValueFamily::Boolean, &booleans(&[false, true]));
        assert_raw_round_trip(
            ValueFamily::State,
            &[state("opaque vocabulary", "member:~42")],
        );
        assert_raw_round_trip(
            ValueFamily::Text,
            &[
                exact_text(""),
                exact_text("é"),
                exact_text("e\u{301}"),
                exact_text("日本語🦀"),
            ],
        );
        assert_raw_round_trip(ValueFamily::Artifact, &[artifact()]);
    }

    #[test]
    fn typed_value_block_unavailable_is_exact_and_admitted_under_every_family() {
        let values = [unavailable(None), unavailable(Some("opaque reason: 42"))];
        for family in [
            ValueFamily::Real,
            ValueFamily::Signed,
            ValueFamily::Unsigned,
            ValueFamily::Boolean,
            ValueFamily::State,
            ValueFamily::Text,
            ValueFamily::Artifact,
        ] {
            assert_raw_round_trip(family, &values);
        }
    }

    #[test]
    fn typed_value_block_fixed_real_golden_preserves_signed_zero_and_nan_payload() {
        let values = [
            ExactValue::Real(RealBits::from_bits(0x8000_0000_0000_0000)),
            ExactValue::Real(RealBits::from_bits(0x7ff8_0000_0000_0042)),
        ];
        let bytes = assert_oracle_round_trip(ValueFamily::Real, &values);
        assert_eq!(bytes, RAW_REAL_GOLDEN);
    }

    #[test]
    fn typed_value_block_oracle_source_imports_no_product_crate_or_helper() {
        for forbidden in ["och_core", "och_store", "typed_value_block::"] {
            assert!(
                !ORACLE_SOURCE.contains(forbidden),
                "primitive oracle must not contain {forbidden}"
            );
        }
    }

    #[test]
    fn typed_value_block_boolean_selection_proves_raw_packed_rle_and_ties() {
        let raw_expansion = assert_oracle_round_trip(ValueFamily::Boolean, &booleans(&[true]));
        assert_eq!(raw_expansion[6], Codec::Raw.code());

        let raw_tie = assert_oracle_round_trip(ValueFamily::Boolean, &booleans(&[true, false]));
        assert_eq!(raw_tie[6], Codec::Raw.code());

        let packed = assert_oracle_round_trip(
            ValueFamily::Boolean,
            &booleans(&[true, false, true, true, false, false, true, false, true]),
        );
        assert_eq!(packed[6], Codec::BitPack.code());

        let compact_tie =
            assert_oracle_round_trip(ValueFamily::Boolean, &repeated_boolean(true, 17));
        assert_eq!(compact_tie[6], Codec::BitPack.code());

        let rle = assert_oracle_round_trip(ValueFamily::Boolean, &repeated_boolean(false, 25));
        assert_eq!(rle[6], Codec::Rle.code());
    }

    #[test]
    fn typed_value_block_boolean_unavailable_forces_raw_and_bytes_repeat() {
        let mut values = repeated_boolean(true, 24);
        values.push(unavailable(Some("not observed")));
        let expected = assert_oracle_round_trip(ValueFamily::Boolean, &values);
        assert_eq!(expected[6], Codec::Raw.code());
        for _ in 0..16 {
            assert_eq!(encode(ValueFamily::Boolean, &values), Ok(expected.clone()));
            assert_eq!(decode(ValueFamily::Boolean, &expected), Ok(values.clone()));
        }
    }

    fn framed(family: ValueFamily, codec: Codec, count: usize, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
        write_header(&mut bytes, family, codec, count, payload.len()).expect("bounded fixture");
        bytes.extend_from_slice(payload);
        bytes
    }

    fn with_mutation(bytes: &[u8], offset: usize, value: u8) -> Vec<u8> {
        let mut mutated = bytes.to_vec();
        mutated[offset] = value;
        mutated
    }

    #[test]
    fn typed_value_block_refuses_hostile_fixed_header_metadata() {
        let canonical =
            encode(ValueFamily::Boolean, &booleans(&[true, false, true])).expect("packed fixture");
        for (offset, value) in [(0, b'X'), (4, 2), (5, 7), (6, 9), (7, 1), (10, 1)] {
            assert_eq!(
                decode(
                    ValueFamily::Boolean,
                    &with_mutation(&canonical, offset, value)
                ),
                Err(BlockError::InvalidBlock)
            );
        }
        assert_eq!(
            decode(ValueFamily::Signed, &canonical),
            Err(BlockError::FamilyMismatch)
        );

        let empty = framed(ValueFamily::Boolean, Codec::Raw, 0, &[]);
        assert_eq!(
            decode(ValueFamily::Boolean, &empty),
            Err(BlockError::Bounds)
        );
        let too_many = framed(ValueFamily::Boolean, Codec::Raw, 257, &[0; 257]);
        assert_eq!(
            decode(ValueFamily::Boolean, &too_many),
            Err(BlockError::Bounds)
        );

        let mut wrong_length = canonical.clone();
        let declared = u32::from_le_bytes(wrong_length[12..16].try_into().expect("payload length"));
        wrong_length[12..16].copy_from_slice(&(declared + 1).to_le_bytes());
        assert_eq!(
            decode(ValueFamily::Boolean, &wrong_length),
            Err(BlockError::InvalidBlock)
        );
    }

    #[test]
    fn typed_value_block_refuses_truncation_trailing_and_compact_payload_metadata() {
        let canonical =
            encode(ValueFamily::Boolean, &booleans(&[true, false, true])).expect("packed fixture");
        assert_eq!(
            decode(ValueFamily::Boolean, &canonical[..canonical.len() - 1]),
            Err(BlockError::InvalidBlock)
        );
        let mut trailing = canonical.clone();
        trailing.push(0);
        assert_eq!(
            decode(ValueFamily::Boolean, &trailing),
            Err(BlockError::InvalidBlock)
        );
        assert_eq!(
            decode(
                ValueFamily::Boolean,
                &with_mutation(&canonical, HEADER_LEN, 0)
            ),
            Err(BlockError::InvalidBlock)
        );

        let rle = encode(ValueFamily::Boolean, &repeated_boolean(true, 25)).expect("RLE fixture");
        assert_eq!(
            decode(ValueFamily::Boolean, &with_mutation(&rle, HEADER_LEN, 0)),
            Err(BlockError::InvalidBlock)
        );
    }

    #[test]
    fn typed_value_block_refuses_invalid_boolean_raw_and_packed_padding() {
        let invalid_raw = framed(ValueFamily::Boolean, Codec::Raw, 1, &[4]);
        assert_eq!(
            decode(ValueFamily::Boolean, &invalid_raw),
            Err(BlockError::InvalidBlock)
        );

        let packed =
            encode(ValueFamily::Boolean, &booleans(&[true, false, true])).expect("packed fixture");
        let payload_byte = HEADER_LEN + 1;
        assert_eq!(
            decode(
                ValueFamily::Boolean,
                &with_mutation(&packed, payload_byte, packed[payload_byte] | 0x80)
            ),
            Err(BlockError::InvalidBlock)
        );
    }

    #[test]
    fn typed_value_block_refuses_invalid_rle_runs() {
        let canonical =
            encode(ValueFamily::Boolean, &repeated_boolean(true, 25)).expect("RLE fixture");
        assert_eq!(canonical[6], Codec::Rle.code());
        for (offset, value) in [(HEADER_LEN + 1, 2), (HEADER_LEN + 2, 0)] {
            assert_eq!(
                decode(
                    ValueFamily::Boolean,
                    &with_mutation(&canonical, offset, value)
                ),
                Err(BlockError::InvalidBlock)
            );
        }
        let overflowing = with_mutation(&canonical, HEADER_LEN + 3, u8::MAX);
        assert_eq!(
            decode(ValueFamily::Boolean, &overflowing),
            Err(BlockError::InvalidBlock)
        );

        let adjacent = framed(
            ValueFamily::Boolean,
            Codec::Rle,
            25,
            &[RLE_RUN_WIDTH_U16, 1, 10, 0, 1, 15, 0],
        );
        assert_eq!(
            decode(ValueFamily::Boolean, &adjacent),
            Err(BlockError::InvalidBlock)
        );
        let mismatch = framed(
            ValueFamily::Boolean,
            Codec::Rle,
            25,
            &[RLE_RUN_WIDTH_U16, 1, 24, 0],
        );
        assert_eq!(
            decode(ValueFamily::Boolean, &mismatch),
            Err(BlockError::InvalidBlock)
        );
        let truncated_run = framed(
            ValueFamily::Boolean,
            Codec::Rle,
            25,
            &[RLE_RUN_WIDTH_U16, 1, 25],
        );
        assert_eq!(
            decode(ValueFamily::Boolean, &truncated_run),
            Err(BlockError::InvalidBlock)
        );
        let trailing_run_byte = framed(
            ValueFamily::Boolean,
            Codec::Rle,
            25,
            &[RLE_RUN_WIDTH_U16, 1, 25, 0, 0],
        );
        assert_eq!(
            decode(ValueFamily::Boolean, &trailing_run_byte),
            Err(BlockError::InvalidBlock)
        );
    }

    #[test]
    fn typed_value_block_refuses_syntactic_nonwinning_codecs() {
        let packed_tied_with_raw = framed(
            ValueFamily::Boolean,
            Codec::BitPack,
            2,
            &[PACKED_ORDER_LSB_FIRST, 0b0000_0001],
        );
        assert_eq!(
            decode(ValueFamily::Boolean, &packed_tied_with_raw),
            Err(BlockError::InvalidBlock)
        );
        let raw_larger_than_packed = framed(ValueFamily::Boolean, Codec::Raw, 3, &[1, 0, 1]);
        assert_eq!(
            decode(ValueFamily::Boolean, &raw_larger_than_packed),
            Err(BlockError::InvalidBlock)
        );
        let packed_larger_than_rle = framed(
            ValueFamily::Boolean,
            Codec::BitPack,
            25,
            &[PACKED_ORDER_LSB_FIRST, 0xff, 0xff, 0xff, 0x01],
        );
        assert_eq!(
            decode(ValueFamily::Boolean, &packed_larger_than_rle),
            Err(BlockError::InvalidBlock)
        );
    }

    #[test]
    fn typed_value_block_refuses_invalid_utf8_and_model_bounds() {
        let invalid_utf8 = framed(ValueFamily::Text, Codec::Raw, 1, &[0, 1, 0, 0, 0, 0xff]);
        assert_eq!(
            decode(ValueFamily::Text, &invalid_utf8),
            Err(BlockError::InvalidBlock)
        );

        let mut too_many_scalars = vec![0];
        too_many_scalars.extend_from_slice(&4097_u32.to_le_bytes());
        too_many_scalars.extend(std::iter::repeat_n(b'x', 4097));
        let too_many_scalars = framed(ValueFamily::Text, Codec::Raw, 1, &too_many_scalars);
        assert_eq!(
            decode(ValueFamily::Text, &too_many_scalars),
            Err(BlockError::InvalidBlock)
        );

        let empty_state_class = framed(ValueFamily::State, Codec::Raw, 1, &[0, 0, 0, 1, 0, b'm']);
        assert_eq!(
            decode(ValueFamily::State, &empty_state_class),
            Err(BlockError::InvalidBlock)
        );
        let empty_reason = framed(ValueFamily::Signed, Codec::Raw, 1, &[2, 0, 0]);
        assert_eq!(
            decode(ValueFamily::Signed, &empty_reason),
            Err(BlockError::InvalidBlock)
        );
    }

    #[test]
    fn typed_value_block_refuses_invalid_artifact_identity_and_format() {
        let mut payload = vec![0];
        payload.extend_from_slice(&[0; 16]);
        payload.extend_from_slice(&3_u16.to_le_bytes());
        payload.extend_from_slice(b"bin");
        payload.extend_from_slice(&0_u128.to_le_bytes());
        payload.extend_from_slice(&[0; 32]);
        let invalid_id = framed(ValueFamily::Artifact, Codec::Raw, 1, &payload);
        assert_eq!(
            decode(ValueFamily::Artifact, &invalid_id),
            Err(BlockError::InvalidBlock)
        );

        payload[1..17].copy_from_slice(&uuid_bytes());
        payload[19] = b'B';
        let invalid_format = framed(ValueFamily::Artifact, Codec::Raw, 1, &payload);
        assert_eq!(
            decode(ValueFamily::Artifact, &invalid_format),
            Err(BlockError::InvalidBlock)
        );
    }

    #[test]
    fn typed_value_block_family_and_count_bounds_fail_closed() {
        assert_eq!(encode(ValueFamily::Boolean, &[]), Err(BlockError::Bounds));
        assert_eq!(
            encode(
                ValueFamily::Boolean,
                &repeated_boolean(false, MAX_SOURCE_OBSERVATION_CONTEXTS + 1)
            ),
            Err(BlockError::Bounds)
        );
        assert_eq!(
            encode(ValueFamily::Signed, &[ExactValue::Unsigned(1)]),
            Err(BlockError::FamilyMismatch)
        );

        let maximum = repeated_boolean(false, MAX_SOURCE_OBSERVATION_CONTEXTS);
        let bytes = assert_oracle_round_trip(ValueFamily::Boolean, &maximum);
        assert_eq!(decode(ValueFamily::Boolean, &bytes), Ok(maximum));
    }

    #[test]
    fn typed_value_block_checked_arithmetic_and_encoded_cap_are_exact() {
        assert_eq!(checked_total_len(usize::MAX), Err(BlockError::Bounds));
        assert_eq!(
            checked_total_len(MAX_ADMISSION_PAYLOAD_V1 - HEADER_LEN),
            Ok(MAX_ADMISSION_PAYLOAD_V1)
        );
        assert_eq!(
            checked_total_len(MAX_ADMISSION_PAYLOAD_V1 - HEADER_LEN + 1),
            Err(BlockError::Bounds)
        );

        let mut declared_over_cap = framed(ValueFamily::Boolean, Codec::Raw, 1, &[]);
        declared_over_cap[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            decode(ValueFamily::Boolean, &declared_over_cap),
            Err(BlockError::Bounds)
        );
        let over_cap = vec![0; MAX_ADMISSION_PAYLOAD_V1 + 1];
        assert_eq!(
            decode(ValueFamily::Boolean, &over_cap),
            Err(BlockError::Bounds)
        );
    }
}
