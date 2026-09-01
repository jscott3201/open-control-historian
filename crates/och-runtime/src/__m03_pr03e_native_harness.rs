//! Unsupported temporary runtime facade for later reviewed native evidence tooling.
//!
//! The facade is disabled by default, rustdoc-hidden, non-product, and temporary.
//! It cannot open a store, access registries or filesystem handles, emit V2
//! behavior, or install callbacks.

use std::time::Duration;

pub use och_store::__m03_pr03e_native_harness::{
    BoundaryDescriptor, BoundaryEvent, BoundaryId, BoundaryLookupError, BoundaryOperation,
    BoundaryOutcome, BoundaryOwner, BoundarySource, CrashReady, EvidenceFailure,
    EvidenceSessionError, EvidenceSnapshot, EvidenceStatus, FaultAction, FaultPlan, FaultPlanError,
    InjectedErrorKind, MAX_NATIVE_EVIDENCE_EVENTS, TraceValidationError, boundary_registry,
};

/// One runtime-owned process-local, fixed-capacity native evidence session.
///
/// This cloneable unsupported feature-only value is the sole future tooling
/// facade. It records no payload/path strings and performs no control/report I/O.
#[derive(Clone, Debug)]
pub struct NativeEvidenceSession {
    inner: och_store::__m03_pr03e_native_harness::NativeEvidenceSession,
}

impl NativeEvidenceSession {
    /// Validates and preallocates one fixed-capacity session before runtime open.
    ///
    /// # Errors
    ///
    /// Refuses zero/excessive capacity or exact preallocation failure.
    pub fn new(capacity: usize, plan: FaultPlan) -> Result<Self, EvidenceSessionError> {
        och_store::__m03_pr03e_native_harness::NativeEvidenceSession::new(capacity, plan)
            .map(|inner| Self { inner })
    }

    /// Copies the bounded recorder state outside the measured event path.
    #[must_use]
    pub fn snapshot(&self) -> EvidenceSnapshot {
        self.inner.snapshot()
    }

    /// Waits for in-memory crash readiness without native control/report file I/O.
    #[must_use]
    pub fn wait_for_crash_ready(&self, timeout: Duration) -> Option<CrashReady> {
        self.inner.wait_for_crash_ready(timeout)
    }

    pub(crate) fn into_store_session(
        self,
    ) -> och_store::__m03_pr03e_native_harness::NativeEvidenceSession {
        self.inner
    }
}
