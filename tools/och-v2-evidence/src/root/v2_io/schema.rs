use crate::error::{EvidenceError, Result};

pub(super) const FOUNDATION_SCHEMA: &str = "m03-pr03g1-v1";
pub(super) const REPORT_SCHEMA: &str = "m03-pr03e-v1";

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
    pub(super) enum PhaseId {
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
    pub(super) enum FaultMode {
        None => "NONE",
        PreOperationError => "PRE_OPERATION_ERROR",
        ShortPartialWrite => "SHORT_PARTIAL_WRITE",
        ChildCrashAfterSuccess => "CHILD_CRASH_AFTER_SUCCESS"
    }
}

closed_enum! {
    pub(super) enum PressureKind {
        None => "NONE",
        StorageFull => "StorageFull",
        QuotaExceeded => "QuotaExceeded"
    }
}

closed_enum! {
    pub(super) enum ReportClassification {
        StructuralSynthetic => "STRUCTURAL_SYNTHETIC",
        AcceptanceCandidate => "ACCEPTANCE_CANDIDATE"
    }
}

closed_enum! {
    pub(super) enum SampleClass {
        Success => "SUCCESS",
        Fault => "FAULT",
        Pressure => "PRESSURE",
        NonSuccess => "NON_SUCCESS"
    }
}

closed_enum! {
    pub(super) enum SampleOutcome {
        Success => "SUCCESS",
        Refused => "REFUSED",
        Terminal => "TERMINAL",
        ReopenRequired => "REOPEN_REQUIRED",
        Error => "ERROR",
        Crashed => "CRASHED",
        OtherNonSuccess => "OTHER_NON_SUCCESS"
    }
}

closed_enum! {
    pub(super) enum TraceStatus {
        CompleteSuccess => "COMPLETE_SUCCESS",
        CompleteNonSuccess => "COMPLETE_NON_SUCCESS",
        IncompleteFaultWitness => "INCOMPLETE_FAULT_WITNESS",
        IncompleteNonSuccessWitness => "INCOMPLETE_NON_SUCCESS_WITNESS"
    }
}

closed_enum! {
    pub(super) enum RotationTriggerPath {
        PreAppend => "PRE_APPEND",
        PostPublication => "POST_PUBLICATION",
        NotApplicable => "NOT_APPLICABLE"
    }
}

closed_enum! {
    pub(super) enum ProcessMode {
        Fresh => "FRESH_PROCESS",
        Retained => "RETAINED_PROCESS",
        ParentSupervisor => "PARENT_SUPERVISOR"
    }
}

closed_enum! {
    pub(super) enum StoreMode {
        New => "NEW",
        Reused => "REUSED"
    }
}

closed_enum! {
    pub(super) enum CacheState {
        Cold => "COLD",
        Warm => "WARM",
        Unknown => "UNKNOWN"
    }
}

closed_enum! {
    pub(super) enum EventId {
        ReceiptHandledDurable => "V2TIME-RECEIPT-HANDLED-DURABLE",
        WriterRotationDelay => "V2TIME-WRITER-ROTATION-DELAY",
        RotationMutationCritical => "V2TIME-ROTATION-MUTATION-CRITICAL",
        EagerOpen => "V2TIME-EAGER-OPEN",
        OrdinaryJournalSync => "V2TIME-ORD-JOURNAL-SYNC",
        OrdinaryCheckpointWrite => "V2TIME-ORD-CHECKPOINT-WRITE",
        OrdinaryCheckpointSync => "V2TIME-ORD-CHECKPOINT-SYNC",
        OrdinaryCheckpointAdopt => "V2TIME-ORD-CHECKPOINT-ADOPT",
        OrdinaryRetryPublish => "V2TIME-ORD-RETRY-PUBLISH",
        OrdinaryManifestPrepare => "V2TIME-ORD-MANIFEST-PUBLICATION-PREPARE",
        OrdinaryManifestRename => "V2TIME-ORD-MANIFEST-RENAME-COMMIT",
        OrdinaryManifestPostcommit => "V2TIME-ORD-MANIFEST-POSTCOMMIT-VALIDATE",
        OrdinaryManifestAdopt => "V2TIME-ORD-MANIFEST-ADOPT",
        OrdinaryInspectionUpdate => "V2TIME-ORD-INSPECTION-UPDATE",
        OrdinaryReceiptResolve => "V2TIME-ORD-RECEIPT-RESOLVE",
        OrdinaryNoopBarrier => "V2TIME-ORD-NOOP-BARRIER",
        Preflight => "V2TIME-P0-PREFLIGHT",
        Intent => "V2TIME-P1-INTENT",
        Raw => "V2TIME-P2-RAW",
        Segment => "V2TIME-P3-SEGMENT",
        Successor => "V2TIME-P4-SUCCESSOR",
        Catalog => "V2TIME-P5-CATALOG",
        Manifest => "V2TIME-P6-MANIFEST",
        ManifestPrepare => "V2TIME-P6-MANIFEST-PUBLICATION-PREPARE",
        ManifestRename => "V2TIME-P6-MANIFEST-RENAME-COMMIT",
        ManifestPostcommit => "V2TIME-P6-MANIFEST-POSTCOMMIT-VALIDATE",
        Adoption => "V2TIME-P7-ADOPTION",
        Cleanup => "V2TIME-P7-CLEANUP",
        OpenPairValidation => "V2TIME-OPEN-PAIR-VALIDATION"
    }
}

closed_enum! {
    pub(super) enum RootClassification {
        Prior => "PRIOR_ROOT",
        Committed => "COMMITTED_ROOT",
        UnchangedRefusal => "UNCHANGED_REFUSAL"
    }
}

pub(super) fn validate_closed_schema() -> Result<()> {
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
    for value in ReportClassification::ALL {
        if ReportClassification::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in SampleClass::ALL {
        if SampleClass::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in SampleOutcome::ALL {
        if SampleOutcome::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in TraceStatus::ALL {
        if TraceStatus::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in RotationTriggerPath::ALL {
        if RotationTriggerPath::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in ProcessMode::ALL {
        if ProcessMode::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in StoreMode::ALL {
        if StoreMode::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in CacheState::ALL {
        if CacheState::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    for value in EventId::ALL {
        if EventId::parse(value.as_str())? != *value {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn g1_and_g2_schemas_are_closed() {
        validate_closed_schema().expect("closed g1 schema");
        assert!(PhaseId::parse("V2TX-P8").is_err());
        assert!(FaultMode::parse("WILDCARD").is_err());
        assert!(PressureKind::parse("OTHER").is_err());
        assert!(EventId::parse("V2TIME-*").is_err());
        assert!(SampleOutcome::parse("UNKNOWN").is_err());
        assert!(ReportClassification::parse("MEASURED").is_err());
    }
}
