//! Content-qualified retry comparison without durability or transport policy.

use crate::{ContentIdentity, ProducerId, RetryKey, SeriesId};

/// The result of comparing two retry qualifications.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RetryClassification {
    /// Scope, key, and externally supplied content identity all match.
    Equivalent,
    /// Scope and key match, but content identity differs.
    Conflict,
    /// Series scope, producer scope, or retry key differs.
    Distinct,
}

/// Explicit series/producer scope, opaque key, and external content identity.
///
/// This model does not derive keys or HMACs, hash content, establish a durable
/// retry horizon, or equate transport redelivery with idempotency.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RetryQualification {
    series_id: SeriesId,
    producer_id: ProducerId,
    key: RetryKey,
    content: ContentIdentity,
}

impl RetryQualification {
    /// Constructs a complete retry qualification.
    #[must_use]
    pub const fn new(
        series_id: SeriesId,
        producer_id: ProducerId,
        key: RetryKey,
        content: ContentIdentity,
    ) -> Self {
        Self {
            series_id,
            producer_id,
            key,
            content,
        }
    }

    /// Returns the nominal series scope.
    #[must_use]
    pub const fn series_id(&self) -> SeriesId {
        self.series_id
    }

    /// Returns the nominal producer scope.
    #[must_use]
    pub const fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    /// Returns the opaque retry key.
    #[must_use]
    pub const fn key(&self) -> &RetryKey {
        &self.key
    }

    /// Returns the externally supplied content identity.
    #[must_use]
    pub const fn content(&self) -> &ContentIdentity {
        &self.content
    }

    /// Classifies another retry qualification by exact scope, key, and content.
    #[must_use]
    pub fn classify(&self, other: &Self) -> RetryClassification {
        if self.series_id != other.series_id
            || self.producer_id != other.producer_id
            || self.key != other.key
        {
            RetryClassification::Distinct
        } else if self.content == other.content {
            RetryClassification::Equivalent
        } else {
            RetryClassification::Conflict
        }
    }
}
