use crate::error::{EvidenceError, Result};

pub(crate) const FOUNDATION_SCHEMA: &str = "m03-pr03g1-v1";

macro_rules! closed_enum {
    ($vis:vis enum $name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        $vis enum $name { $($variant),+ }

        impl $name {
            $vis const ALL: &'static [Self] = &[$(Self::$variant),+];

            $vis const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $value),+ }
            }

            $vis fn parse(value: &str) -> Result<Self> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(EvidenceError::InvalidHarness),
                }
            }
        }
    };
}

closed_enum! {
    pub(crate) enum PhaseId {
        Preflight => "V2TX-P0-PREFLIGHT",
        Intent => "V2TX-P1-INTENT",
        Raw => "V2TX-P2-RAW",
        Segment => "V2TX-P3-SEGMENT",
        Successor => "V2TX-P4-SUCCESSOR",
        Catalog => "V2TX-P5-CATALOG",
        Manifest => "V2TX-P6-MANIFEST",
        AdoptClean => "V2TX-P7-ADOPT-CLEAN",
        Rollback => "V2TX-PRECOMMIT-ROLLBACK",
        EagerOpen => "V2TX-EAGER-OPEN"
    }
}

closed_enum! {
    pub(crate) enum FaultMode {
        None => "NONE",
        PreOperationError => "PRE_OPERATION_ERROR",
        ShortPartialWrite => "SHORT_PARTIAL_WRITE",
        ChildCrashAfterSuccess => "CHILD_CRASH_AFTER_SUCCESS"
    }
}

closed_enum! {
    pub(crate) enum PressureKind {
        None => "NONE",
        StorageFull => "STORAGE_FULL",
        QuotaExceeded => "QUOTA_EXCEEDED"
    }
}

closed_enum! {
    pub(crate) enum RootClassification {
        Prior => "PRIOR_ROOT",
        Committed => "COMMITTED_ROOT",
        UnchangedRefusal => "UNCHANGED_REFUSAL"
    }
}

pub(crate) fn validate_closed_schema() -> Result<()> {
    for value in PhaseId::ALL {
        if PhaseId::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in FaultMode::ALL {
        if FaultMode::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in PressureKind::ALL {
        if PressureKind::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in RootClassification::ALL {
        if RootClassification::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn g1_schema_is_closed_without_collection_or_report_status() {
        validate_closed_schema().expect("closed g1 schema");
        assert!(PhaseId::parse("V2TX-P8").is_err());
        assert!(FaultMode::parse("WILDCARD").is_err());
        assert!(PressureKind::parse("OTHER").is_err());
    }
}
