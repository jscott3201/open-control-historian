//! Shared volatile write-custody state and pressure classification.

use std::io::ErrorKind;

/// Volatile write custody for one open store handle.
///
/// A handle in [`Self::ReopenRequired`] or [`Self::Faulted`] cannot return to
/// [`Self::Writable`]. Dropping it and performing a fully validated reopen is
/// the only recovery path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreWriteState {
    /// Mutating and authorizing operations may proceed.
    Writable,
    /// A store-owned mutating boundary observed normalized storage pressure.
    ReopenRequired,
    /// A non-pressure mutation failure terminally faulted the live authority.
    Faulted,
}

pub(crate) const fn is_storage_pressure(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::StorageFull | ErrorKind::QuotaExceeded)
}
