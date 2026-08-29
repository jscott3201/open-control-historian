//! Primitive bounded series-lifecycle fixtures and independent state oracle.

use super::fixtures::RawError;
use std::collections::BTreeMap;

pub const SERIES_ERROR_INVENTORY: [RawError; 11] = [
    RawError::InvalidDeclarationReference,
    RawError::InvalidDeclarationRevision,
    RawError::RegistrySeriesCapacityExceeded,
    RawError::RegistryRevisionCapacityExceeded,
    RawError::SeriesAlreadyRegistered,
    RawError::SeriesNotFound,
    RawError::SeriesRetired,
    RawError::StaleDeclarationRevision,
    RawError::DeclarationUnchanged,
    RawError::SeriesMetadataMismatch,
    RawError::ObservationValueFamilyMismatch,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawPayload {
    pub producer: u8,
    pub mode: u8,
    pub family: u8,
    pub metadata: u8,
}

impl RawPayload {
    pub const fn new(producer: u8, mode: u8, family: u8, metadata: u8) -> Self {
        Self {
            producer,
            mode,
            family,
            metadata,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawValue {
    Family(u8),
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawRequest {
    Register {
        series: u8,
        binding: u8,
        payload: RawPayload,
        evidence: u8,
    },
    Revise {
        series: u8,
        expected: u128,
        payload: RawPayload,
        evidence: u8,
    },
    Retire {
        series: u8,
        expected: u128,
        evidence: u8,
    },
    Bind {
        series: u8,
        producer: u8,
        mode: u8,
        value: RawValue,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawOutcome {
    Declaration(u128),
    Retirement(u128),
    Binding(u128),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RawDeclaration {
    revision: u128,
    previous: Option<u128>,
    payload: RawPayload,
    evidence: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawHistory {
    binding: u8,
    declarations: Vec<RawDeclaration>,
    retirement: Option<(u128, u8)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawHistorySnapshot {
    pub series: u8,
    pub binding: u8,
    pub revisions: Vec<(u128, Option<u128>, RawPayload, u8)>,
    pub retirement: Option<(u128, u8)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawSnapshot {
    pub revision_count: usize,
    pub series: Vec<RawHistorySnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRegistry {
    max_series: usize,
    max_revisions: usize,
    revision_count: usize,
    series: BTreeMap<u8, RawHistory>,
}

impl RawRegistry {
    pub const fn new(max_series: usize, max_revisions: usize) -> Self {
        Self {
            max_series,
            max_revisions,
            revision_count: 0,
            series: BTreeMap::new(),
        }
    }

    pub fn apply(&mut self, request: RawRequest) -> Result<RawOutcome, RawError> {
        match request {
            RawRequest::Register {
                series,
                binding,
                payload,
                evidence,
            } => self.register(series, binding, payload, evidence),
            RawRequest::Revise {
                series,
                expected,
                payload,
                evidence,
            } => self.revise(series, expected, payload, evidence),
            RawRequest::Retire {
                series,
                expected,
                evidence,
            } => self.retire(series, expected, evidence),
            RawRequest::Bind {
                series,
                producer,
                mode,
                value,
            } => self.bind(series, producer, mode, value),
        }
    }

    pub fn snapshot(&self) -> RawSnapshot {
        RawSnapshot {
            revision_count: self.revision_count,
            series: self
                .series
                .iter()
                .map(|(series, history)| RawHistorySnapshot {
                    series: *series,
                    binding: history.binding,
                    revisions: history
                        .declarations
                        .iter()
                        .map(|declaration| {
                            (
                                declaration.revision,
                                declaration.previous,
                                declaration.payload,
                                declaration.evidence,
                            )
                        })
                        .collect(),
                    retirement: history.retirement,
                })
                .collect(),
        }
    }

    fn register(
        &mut self,
        series: u8,
        binding: u8,
        payload: RawPayload,
        evidence: u8,
    ) -> Result<RawOutcome, RawError> {
        if let Some(history) = self.series.get(&series) {
            if history.retirement.is_some() {
                return Err(RawError::SeriesRetired);
            }
            let initial = history.declarations[0];
            if history.declarations.len() == 1
                && history.binding == binding
                && initial.payload == payload
                && initial.evidence == evidence
            {
                return Ok(RawOutcome::Declaration(1));
            }
            return Err(RawError::SeriesAlreadyRegistered);
        }
        if self.series.len() >= self.max_series {
            return Err(RawError::RegistrySeriesCapacityExceeded);
        }
        if self.revision_count >= self.max_revisions {
            return Err(RawError::RegistryRevisionCapacityExceeded);
        }
        self.series.insert(
            series,
            RawHistory {
                binding,
                declarations: vec![RawDeclaration {
                    revision: 1,
                    previous: None,
                    payload,
                    evidence,
                }],
                retirement: None,
            },
        );
        self.revision_count += 1;
        Ok(RawOutcome::Declaration(1))
    }

    fn revise(
        &mut self,
        series: u8,
        expected: u128,
        payload: RawPayload,
        evidence: u8,
    ) -> Result<RawOutcome, RawError> {
        let history = self.series.get(&series).ok_or(RawError::SeriesNotFound)?;
        if history.retirement.is_some() {
            return Err(RawError::SeriesRetired);
        }
        let current = *history.declarations.last().expect("registered history");
        if current.previous == Some(expected)
            && current.payload == payload
            && current.evidence == evidence
        {
            return Ok(RawOutcome::Declaration(current.revision));
        }
        if current.revision != expected {
            return Err(RawError::StaleDeclarationRevision);
        }
        if current.payload == payload {
            return Err(RawError::DeclarationUnchanged);
        }
        if self.revision_count >= self.max_revisions {
            return Err(RawError::RegistryRevisionCapacityExceeded);
        }
        let next = current.revision + 1;
        self.series
            .get_mut(&series)
            .expect("series resolved before mutation")
            .declarations
            .push(RawDeclaration {
                revision: next,
                previous: Some(expected),
                payload,
                evidence,
            });
        self.revision_count += 1;
        Ok(RawOutcome::Declaration(next))
    }

    fn retire(&mut self, series: u8, expected: u128, evidence: u8) -> Result<RawOutcome, RawError> {
        let history = self.series.get(&series).ok_or(RawError::SeriesNotFound)?;
        if let Some(retirement) = history.retirement {
            if retirement == (expected, evidence) {
                return Ok(RawOutcome::Retirement(expected));
            }
            return Err(RawError::SeriesRetired);
        }
        if history
            .declarations
            .last()
            .expect("registered history")
            .revision
            != expected
        {
            return Err(RawError::StaleDeclarationRevision);
        }
        self.series
            .get_mut(&series)
            .expect("series resolved before mutation")
            .retirement = Some((expected, evidence));
        Ok(RawOutcome::Retirement(expected))
    }

    fn bind(
        &self,
        series: u8,
        producer: u8,
        mode: u8,
        value: RawValue,
    ) -> Result<RawOutcome, RawError> {
        let history = self.series.get(&series).ok_or(RawError::SeriesNotFound)?;
        if history.retirement.is_some() {
            return Err(RawError::SeriesRetired);
        }
        let current = history.declarations.last().expect("registered history");
        if (current.payload.producer, current.payload.mode) != (producer, mode) {
            return Err(RawError::SeriesMetadataMismatch);
        }
        if matches!(value, RawValue::Family(family) if family != current.payload.family) {
            return Err(RawError::ObservationValueFamilyMismatch);
        }
        Ok(RawOutcome::Binding(current.revision))
    }
}

pub fn lifecycle_script() -> Vec<RawRequest> {
    let initial = RawPayload::new(10, 1, 1, 1);
    let corrected = RawPayload::new(11, 2, 2, 2);
    vec![
        RawRequest::Register {
            series: 2,
            binding: 2,
            payload: initial,
            evidence: 1,
        },
        RawRequest::Register {
            series: 1,
            binding: 1,
            payload: initial,
            evidence: 1,
        },
        RawRequest::Register {
            series: 1,
            binding: 1,
            payload: initial,
            evidence: 1,
        },
        RawRequest::Revise {
            series: 1,
            expected: 1,
            payload: corrected,
            evidence: 2,
        },
        RawRequest::Revise {
            series: 1,
            expected: 1,
            payload: corrected,
            evidence: 2,
        },
        RawRequest::Revise {
            series: 1,
            expected: 1,
            payload: RawPayload::new(12, 3, 3, 3),
            evidence: 3,
        },
        RawRequest::Bind {
            series: 1,
            producer: 11,
            mode: 2,
            value: RawValue::Unavailable,
        },
        RawRequest::Bind {
            series: 1,
            producer: 11,
            mode: 2,
            value: RawValue::Family(1),
        },
        RawRequest::Retire {
            series: 2,
            expected: 1,
            evidence: 4,
        },
        RawRequest::Retire {
            series: 2,
            expected: 1,
            evidence: 4,
        },
        RawRequest::Bind {
            series: 2,
            producer: 10,
            mode: 1,
            value: RawValue::Family(1),
        },
        RawRequest::Register {
            series: 3,
            binding: 3,
            payload: initial,
            evidence: 5,
        },
    ]
}
