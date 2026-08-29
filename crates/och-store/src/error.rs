use core::fmt;
use std::error::Error;

/// Closed sanitized Journal V1 encode/decode refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalV1Error {
    /// File-header magic does not identify Journal V1.
    InvalidHeaderMagic,
    /// File-header version is not supported.
    UnsupportedHeaderVersion,
    /// File-header length field is not the fixed V1 length.
    InvalidHeaderLength,
    /// Frame magic does not identify a Journal V1 frame.
    InvalidFrameMagic,
    /// Frame version is not supported.
    UnsupportedFrameVersion,
    /// Frame kind is not the V1 canonical-admission kind.
    UnsupportedFrameKind,
    /// Reserved frame flags are non-zero.
    InvalidFrameFlags,
    /// Append sequence zero is invalid.
    InvalidAppendSequence,
    /// Append sequence does not exactly follow the supplied predecessor.
    NonMonotonicAppendSequence,
    /// Append sequence cannot advance beyond its maximum value.
    AppendSequenceOverflow,
    /// Configured or declared payload exceeds the Journal V1 hard bound.
    PayloadTooLarge,
    /// Input ended before the declared structure was complete.
    Truncated,
    /// Input contains bytes after one complete header or frame.
    TrailingBytes,
    /// Frame CRC-32C does not match its prefix and payload.
    ChecksumMismatch,
    /// A closed semantic tag is unknown.
    UnknownTag,
    /// A collection count exceeds its exact semantic bound.
    InvalidCount,
    /// A length is impossible or exceeds its exact field bound.
    InvalidLength,
    /// Text bytes are not valid UTF-8.
    InvalidUtf8,
    /// Identity bytes are not a valid nominal `UUIDv7`.
    InvalidIdentity,
    /// Decoded primitives or cross-field structure violate canonical invariants.
    InvalidCanonicalData,
}

impl fmt::Display for JournalV1Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidHeaderMagic => "invalid Journal V1 header magic",
            Self::UnsupportedHeaderVersion => "unsupported Journal header version",
            Self::InvalidHeaderLength => "invalid Journal V1 header length",
            Self::InvalidFrameMagic => "invalid Journal V1 frame magic",
            Self::UnsupportedFrameVersion => "unsupported Journal frame version",
            Self::UnsupportedFrameKind => "unsupported Journal V1 frame kind",
            Self::InvalidFrameFlags => "invalid Journal V1 frame flags",
            Self::InvalidAppendSequence => "invalid Journal V1 append sequence",
            Self::NonMonotonicAppendSequence => "non-monotonic Journal V1 append sequence",
            Self::AppendSequenceOverflow => "Journal V1 append sequence overflow",
            Self::PayloadTooLarge => "Journal V1 payload exceeds its bound",
            Self::Truncated => "truncated Journal V1 bytes",
            Self::TrailingBytes => "trailing Journal V1 bytes",
            Self::ChecksumMismatch => "Journal V1 checksum mismatch",
            Self::UnknownTag => "unknown Journal V1 semantic tag",
            Self::InvalidCount => "invalid Journal V1 collection count",
            Self::InvalidLength => "invalid Journal V1 field length",
            Self::InvalidUtf8 => "invalid Journal V1 UTF-8",
            Self::InvalidIdentity => "invalid Journal V1 identity",
            Self::InvalidCanonicalData => "invalid Journal V1 canonical structure",
        })
    }
}

impl Error for JournalV1Error {}
