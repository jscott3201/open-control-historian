#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! The dependency-free canonical model for `OpenControl` Historian.
//!
//! This crate defines exact, platform-independent contracts for identity, values,
//! time, quality, producer ordering, collection evidence, and retry comparison.
//! It deliberately contains no runtime, persistence, storage, query, wire-format,
//! hashing, ID-generation, or adapter behavior.

pub mod bounded;
pub mod collection;
pub mod error;
pub mod identity;
pub mod observation;
pub mod position;
pub mod quality;
pub mod retry;
pub mod time;
pub mod value;

pub use bounded::{
    ContentFormat, ExactText, NativeStatusToken, RetryKey, StateClass, StateMember,
    UnavailableReason,
};
pub use collection::{CollectionEnvelope, EvidenceKind, Gap, GapReason, NoChange};
pub use error::ModelError;
pub use identity::{ArtifactId, ObservationId, ProducerId, SeriesId};
pub use observation::{CollectionMode, Observation, RawObservationOrderKey, SeriesMetadata};
pub use position::{ProducerEpoch, ProducerPosition, ProducerSequence};
pub use quality::{NativeStatus, Quality, QualityFlags, QualityLevel};
pub use retry::{RetryClassification, RetryQualification};
pub use time::{ObservationTimes, TimeInterval, Timestamp};
pub use value::{
    ArtifactReference, ContentIdentity, ContentVersion, ExactValue, RealBits, StateValue,
    Unavailable,
};
