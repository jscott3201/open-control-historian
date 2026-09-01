use std::error::Error;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EvidenceError {
    Usage,
    UnsafeEvidenceRoot,
    InvalidFixture,
    InvalidSource,
    InvalidSegment,
    InvalidHarness,
    UnsafeInventory,
    Replan,
    Bounds,
    Io,
}

impl fmt::Display for EvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Usage => "invalid evidence command",
            Self::UnsafeEvidenceRoot => "evidence root overlaps a recognized store namespace",
            Self::InvalidFixture => "invalid evidence fixture metadata",
            Self::InvalidSource => "invalid evidence raw Journal V1 source",
            Self::InvalidSegment => "invalid evidence Native Segment V1 bytes",
            Self::InvalidHarness => "invalid private native harness structure",
            Self::UnsafeInventory => "unsafe or unsupported disposable inventory",
            Self::Replan => "native evidence collection requires replanning",
            Self::Bounds => "evidence hard bound exceeded",
            Self::Io => "sanitized evidence I/O failure",
        })
    }
}

impl Error for EvidenceError {}

impl From<std::io::Error> for EvidenceError {
    fn from(_: std::io::Error) -> Self {
        Self::Io
    }
}

pub(crate) type Result<T> = std::result::Result<T, EvidenceError>;
