//! Primitive-only oracle for the transient typed-value block proof.
//!
//! This test module deliberately depends only on standard-library primitives.

const MAGIC: [u8; 4] = *b"TVBP";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 16;
const PACKED_ORDER_LSB_FIRST: u8 = 1;
const RLE_RUN_WIDTH_U16: u8 = 2;

/// Primitive family understood by the independent proof oracle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Family {
    /// Exact binary64 bits.
    Real,
    /// Signed 64-bit integer.
    Signed,
    /// Unsigned 64-bit integer.
    Unsigned,
    /// Boolean.
    Boolean,
    /// Opaque class/member state.
    State,
    /// Exact UTF-8 bytes.
    Text,
    /// Nominal artifact plus supplied content identity.
    Artifact,
}

/// Primitive value reconstructed by the independent proof oracle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Value {
    /// Exact binary64 bits.
    Real(u64),
    /// Signed 64-bit integer.
    Signed(i64),
    /// Unsigned 64-bit integer.
    Unsigned(u64),
    /// Boolean.
    Boolean(bool),
    /// Exact state token bytes.
    State {
        /// Opaque class bytes.
        class: Vec<u8>,
        /// Opaque member bytes.
        member: Vec<u8>,
    },
    /// Exact UTF-8 bytes.
    Text(Vec<u8>),
    /// Complete supplied artifact identity.
    Artifact {
        /// Network-order nominal UUID bytes.
        id: [u8; 16],
        /// Exact supplied content-format bytes.
        format: Vec<u8>,
        /// Full supplied content version.
        version: u128,
        /// Exact supplied digest bytes.
        digest: [u8; 32],
    },
    /// Explicit unavailable value and optional reason bytes.
    Unavailable(Option<Vec<u8>>),
}

/// Primitive codec classification decoded by the oracle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Codec {
    /// Exact family-specific raw records.
    Raw,
    /// Boolean least-significant-bit-first packing.
    BitPack,
    /// Boolean value/u16 run records.
    Rle,
}

/// Independently decoded transient block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Block {
    /// Header family.
    pub family: Family,
    /// Header codec.
    pub codec: Codec,
    /// Exact primitive values.
    pub values: Vec<Value>,
}

/// Independently encodes the canonical proof bytes for bounded fixtures.
#[must_use]
pub fn encode(family: Family, values: &[Value]) -> Vec<u8> {
    assert!(!values.is_empty() && values.len() <= 256);
    assert!(values.iter().all(|value| family_admits(family, value)));

    let raw_len = raw_payload_len(family, values);
    let (codec, payload_len) = choose_codec(family, values, raw_len);
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload_len);
    bytes.extend_from_slice(&MAGIC);
    bytes.push(VERSION);
    bytes.push(family_code(family));
    bytes.push(codec_code(codec));
    bytes.push(0);
    let count = u16::try_from(values.len()).expect("oracle count is at most 256");
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(&[0; 2]);
    let payload_len_u32 = u32::try_from(payload_len).expect("oracle fixture payload is bounded");
    bytes.extend_from_slice(&payload_len_u32.to_le_bytes());

    match codec {
        Codec::Raw => encode_raw(&mut bytes, family, values),
        Codec::BitPack => encode_packed(&mut bytes, values),
        Codec::Rle => encode_rle(&mut bytes, values),
    }
    assert_eq!(bytes.len(), HEADER_LEN + payload_len);
    bytes
}

/// Independently decodes product proof bytes into primitive values.
pub fn decode(bytes: &[u8]) -> Result<Block, ()> {
    if bytes.len() < HEADER_LEN || bytes[..4] != MAGIC {
        return Err(());
    }
    if bytes[4] != VERSION || bytes[7] != 0 || bytes[10..12] != [0; 2] {
        return Err(());
    }
    let family = decode_family(bytes[5])?;
    let codec = decode_codec(bytes[6])?;
    let count = usize::from(u16::from_le_bytes([bytes[8], bytes[9]]));
    let payload_len = u32::from_le_bytes(bytes[12..16].try_into().map_err(|_| ())?) as usize;
    if count == 0 || count > 256 || HEADER_LEN + payload_len != bytes.len() {
        return Err(());
    }
    let payload = &bytes[HEADER_LEN..];
    let values = match codec {
        Codec::Raw => decode_raw(family, count, payload)?,
        Codec::BitPack => decode_packed(family, count, payload)?,
        Codec::Rle => decode_rle(family, count, payload)?,
    };
    Ok(Block {
        family,
        codec,
        values,
    })
}

fn family_admits(family: Family, value: &Value) -> bool {
    matches!(value, Value::Unavailable(_))
        || matches!(
            (family, value),
            (Family::Real, Value::Real(_))
                | (Family::Signed, Value::Signed(_))
                | (Family::Unsigned, Value::Unsigned(_))
                | (Family::Boolean, Value::Boolean(_))
                | (Family::State, Value::State { .. })
                | (Family::Text, Value::Text(_))
                | (Family::Artifact, Value::Artifact { .. })
        )
}

fn family_code(family: Family) -> u8 {
    match family {
        Family::Real => 0,
        Family::Signed => 1,
        Family::Unsigned => 2,
        Family::Boolean => 3,
        Family::State => 4,
        Family::Text => 5,
        Family::Artifact => 6,
    }
}

fn decode_family(code: u8) -> Result<Family, ()> {
    match code {
        0 => Ok(Family::Real),
        1 => Ok(Family::Signed),
        2 => Ok(Family::Unsigned),
        3 => Ok(Family::Boolean),
        4 => Ok(Family::State),
        5 => Ok(Family::Text),
        6 => Ok(Family::Artifact),
        _ => Err(()),
    }
}

fn codec_code(codec: Codec) -> u8 {
    match codec {
        Codec::Raw => 0,
        Codec::BitPack => 1,
        Codec::Rle => 2,
    }
}

fn decode_codec(code: u8) -> Result<Codec, ()> {
    match code {
        0 => Ok(Codec::Raw),
        1 => Ok(Codec::BitPack),
        2 => Ok(Codec::Rle),
        _ => Err(()),
    }
}

fn unavailable_len(reason: Option<&[u8]>) -> usize {
    reason.map_or(1, |reason| 3 + reason.len())
}

fn raw_payload_len(family: Family, values: &[Value]) -> usize {
    values
        .iter()
        .map(|value| match value {
            Value::Unavailable(reason) => unavailable_len(reason.as_deref()),
            Value::Boolean(_) if family == Family::Boolean => 1,
            Value::Real(_) | Value::Signed(_) | Value::Unsigned(_) => 9,
            Value::State { class, member } => 5 + class.len() + member.len(),
            Value::Text(text) => 5 + text.len(),
            Value::Artifact { format, .. } => 67 + format.len(),
            Value::Boolean(_) => unreachable!("family checked by oracle"),
        })
        .sum()
}

fn choose_codec(family: Family, values: &[Value], raw_len: usize) -> (Codec, usize) {
    if family != Family::Boolean
        || !values
            .iter()
            .all(|value| matches!(value, Value::Boolean(_)))
    {
        return (Codec::Raw, raw_len);
    }
    let packed_len = 1 + values.len().div_ceil(8);
    let rle_len = 1 + 3 * run_count(values);
    if packed_len < raw_len && packed_len <= rle_len {
        (Codec::BitPack, packed_len)
    } else if rle_len < raw_len && rle_len < packed_len {
        (Codec::Rle, rle_len)
    } else {
        (Codec::Raw, raw_len)
    }
}

fn run_count(values: &[Value]) -> usize {
    let mut previous = None;
    let mut count = 0;
    for value in values {
        let Value::Boolean(value) = value else {
            unreachable!("compact oracle input is Boolean");
        };
        if previous != Some(*value) {
            previous = Some(*value);
            count += 1;
        }
    }
    count
}

fn encode_raw(bytes: &mut Vec<u8>, family: Family, values: &[Value]) {
    for value in values {
        match value {
            Value::Unavailable(None) => bytes.push(if family == Family::Boolean { 2 } else { 1 }),
            Value::Unavailable(Some(reason)) => {
                bytes.push(if family == Family::Boolean { 3 } else { 2 });
                put_u16_bytes(bytes, reason);
            }
            Value::Boolean(value) if family == Family::Boolean => bytes.push(u8::from(*value)),
            Value::Real(bits) => {
                bytes.push(0);
                bytes.extend_from_slice(&bits.to_le_bytes());
            }
            Value::Signed(value) => {
                bytes.push(0);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::Unsigned(value) => {
                bytes.push(0);
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Value::State { class, member } => {
                bytes.push(0);
                put_u16_bytes(bytes, class);
                put_u16_bytes(bytes, member);
            }
            Value::Text(text) => {
                bytes.push(0);
                let length = u32::try_from(text.len()).expect("oracle text fixture is bounded");
                bytes.extend_from_slice(&length.to_le_bytes());
                bytes.extend_from_slice(text);
            }
            Value::Artifact {
                id,
                format,
                version,
                digest,
            } => {
                bytes.push(0);
                bytes.extend_from_slice(id);
                put_u16_bytes(bytes, format);
                bytes.extend_from_slice(&version.to_le_bytes());
                bytes.extend_from_slice(digest);
            }
            Value::Boolean(_) => unreachable!("family checked by oracle"),
        }
    }
}

fn put_u16_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    let length = u16::try_from(value.len()).expect("oracle token fixture is bounded");
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value);
}

fn encode_packed(bytes: &mut Vec<u8>, values: &[Value]) {
    bytes.push(PACKED_ORDER_LSB_FIRST);
    bytes.resize(bytes.len() + values.len().div_ceil(8), 0);
    let payload_start = bytes.len() - values.len().div_ceil(8);
    for (index, value) in values.iter().enumerate() {
        let Value::Boolean(value) = value else {
            unreachable!("compact oracle input is Boolean");
        };
        if *value {
            bytes[payload_start + index / 8] |= 1 << (index % 8);
        }
    }
}

fn encode_rle(bytes: &mut Vec<u8>, values: &[Value]) {
    bytes.push(RLE_RUN_WIDTH_U16);
    let mut start = 0;
    while start < values.len() {
        let Value::Boolean(value) = values[start] else {
            unreachable!("compact oracle input is Boolean");
        };
        let mut end = start + 1;
        while end < values.len() && values[end] == Value::Boolean(value) {
            end += 1;
        }
        bytes.push(u8::from(value));
        let run = u16::try_from(end - start).expect("oracle count is at most 256");
        bytes.extend_from_slice(&run.to_le_bytes());
        start = end;
    }
}

fn decode_raw(family: Family, count: usize, payload: &[u8]) -> Result<Vec<Value>, ()> {
    let mut reader = Reader::new(payload);
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let marker = reader.byte()?;
        let value = if family == Family::Boolean {
            match marker {
                0 => Value::Boolean(false),
                1 => Value::Boolean(true),
                2 => Value::Unavailable(None),
                3 => Value::Unavailable(Some(reader.u16_bytes()?)),
                _ => return Err(()),
            }
        } else {
            match marker {
                0 => decode_available(family, &mut reader)?,
                1 => Value::Unavailable(None),
                2 => Value::Unavailable(Some(reader.u16_bytes()?)),
                _ => return Err(()),
            }
        };
        values.push(value);
    }
    if reader.finished() {
        Ok(values)
    } else {
        Err(())
    }
}

fn decode_available(family: Family, reader: &mut Reader<'_>) -> Result<Value, ()> {
    match family {
        Family::Real => Ok(Value::Real(reader.u64()?)),
        Family::Signed => Ok(Value::Signed(reader.i64()?)),
        Family::Unsigned => Ok(Value::Unsigned(reader.u64()?)),
        Family::State => Ok(Value::State {
            class: reader.u16_bytes()?,
            member: reader.u16_bytes()?,
        }),
        Family::Text => Ok(Value::Text(reader.u32_bytes()?)),
        Family::Artifact => Ok(Value::Artifact {
            id: reader.array()?,
            format: reader.u16_bytes()?,
            version: reader.u128()?,
            digest: reader.array()?,
        }),
        Family::Boolean => Err(()),
    }
}

fn decode_packed(family: Family, count: usize, payload: &[u8]) -> Result<Vec<Value>, ()> {
    if family != Family::Boolean
        || payload.len() != 1 + count.div_ceil(8)
        || payload.first() != Some(&PACKED_ORDER_LSB_FIRST)
    {
        return Err(());
    }
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        values.push(Value::Boolean(
            payload[1 + index / 8] & (1 << (index % 8)) != 0,
        ));
    }
    Ok(values)
}

fn decode_rle(family: Family, count: usize, payload: &[u8]) -> Result<Vec<Value>, ()> {
    if family != Family::Boolean
        || payload.first() != Some(&RLE_RUN_WIDTH_U16)
        || !(payload.len() - 1).is_multiple_of(3)
    {
        return Err(());
    }
    let mut values = Vec::with_capacity(count);
    let (runs, remainder) = payload[1..].as_chunks::<3>();
    if !remainder.is_empty() {
        return Err(());
    }
    for run in runs {
        let value = match run[0] {
            0 => false,
            1 => true,
            _ => return Err(()),
        };
        let length = usize::from(u16::from_le_bytes([run[1], run[2]]));
        values.extend(std::iter::repeat_n(Value::Boolean(value), length));
    }
    if values.len() == count {
        Ok(values)
    } else {
        Err(())
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ()> {
        let end = self.position.checked_add(length).ok_or(())?;
        let value = self.bytes.get(self.position..end).ok_or(())?;
        self.position = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, ()> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ()> {
        self.take(N)?.try_into().map_err(|_| ())
    }

    fn u16_bytes(&mut self) -> Result<Vec<u8>, ()> {
        let length = usize::from(u16::from_le_bytes(self.array()?));
        Ok(self.take(length)?.to_vec())
    }

    fn u32_bytes(&mut self) -> Result<Vec<u8>, ()> {
        let length = u32::from_le_bytes(self.array()?) as usize;
        Ok(self.take(length)?.to_vec())
    }

    fn u64(&mut self) -> Result<u64, ()> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, ()> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn u128(&mut self) -> Result<u128, ()> {
        Ok(u128::from_le_bytes(self.array()?))
    }

    fn finished(&self) -> bool {
        self.position == self.bytes.len()
    }
}
