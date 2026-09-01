use crate::error::{EvidenceError, Result};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Opaque ownership of the fixed `PR03c` files for one validated case name.
///
/// The paths remain private to this owner. Callers can only open or mutate the
/// exact fixture/artifact identities represented by the capability.
pub(crate) struct Pr03cCase {
    raw: PathBuf,
    raw_partial: PathBuf,
    fixture_meta: PathBuf,
    segment: PathBuf,
    segment_partial: PathBuf,
    segment_identity: PathBuf,
}

impl Pr03cCase {
    pub(super) fn new(root: &Path, case: &str) -> Self {
        let fixtures = root.join("fixtures");
        let artifacts = root.join("artifacts");
        Self {
            raw: fixtures.join(format!("{case}.raw-journal-v1-evidence")),
            raw_partial: fixtures.join(format!("{case}.raw-journal-v1-evidence.partial")),
            fixture_meta: fixtures.join(format!("{case}.fixture-meta")),
            segment: artifacts.join(format!("{case}.ochseg01-evidence")),
            segment_partial: artifacts.join(format!("{case}.ochseg01-evidence.partial")),
            segment_identity: artifacts.join(format!("{case}.segment-identity")),
        }
    }

    pub(crate) fn reset_fixture(&self) -> Result<()> {
        remove_if_present(&self.raw_partial)?;
        remove_if_present(&self.raw)?;
        remove_if_present(&self.fixture_meta)
    }

    pub(crate) fn create_raw_partial(&self) -> Result<File> {
        create_new(&self.raw_partial)
    }

    pub(crate) fn publish_raw(&self) -> Result<()> {
        fs::rename(&self.raw_partial, &self.raw).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn remove_raw_partial(&self) -> Result<()> {
        remove_if_present(&self.raw_partial)
    }

    pub(crate) fn write_fixture_meta(&self, bytes: &[u8]) -> Result<()> {
        fs::write(&self.fixture_meta, bytes).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn open_fixture_meta(&self) -> Result<File> {
        File::open(&self.fixture_meta).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn open_raw(&self) -> Result<File> {
        File::open(&self.raw).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn reset_segment(&self) -> Result<()> {
        remove_if_present(&self.segment)?;
        remove_if_present(&self.segment_identity)?;
        remove_if_present(&self.segment_partial)
    }

    pub(crate) fn create_segment_partial(&self) -> Result<File> {
        create_new(&self.segment_partial)
    }

    pub(crate) fn publish_segment(&self) -> Result<()> {
        fs::rename(&self.segment_partial, &self.segment).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn remove_segment_partial(&self) -> Result<()> {
        remove_if_present(&self.segment_partial)
    }

    pub(crate) fn write_segment_identity(&self, bytes: &[u8]) -> Result<()> {
        fs::write(&self.segment_identity, bytes).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn open_segment_identity(&self) -> Result<File> {
        File::open(&self.segment_identity).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn open_segment(&self) -> Result<File> {
        File::open(&self.segment).map_err(|_| EvidenceError::Io)
    }

    #[cfg(test)]
    pub(crate) fn replace_raw_from(&self, source: &mut File) -> Result<()> {
        let mut destination = File::create(&self.raw).map_err(|_| EvidenceError::Io)?;
        io::copy(source, &mut destination).map_err(|_| EvidenceError::Io)?;
        io::Write::flush(&mut destination).map_err(|_| EvidenceError::Io)
    }

    #[cfg(test)]
    pub(crate) fn replace_segment(&self, bytes: &[u8]) -> Result<()> {
        fs::write(&self.segment, bytes).map_err(|_| EvidenceError::Io)
    }
}

/// Opaque ownership of one fixed, validated fixture-set identity.
pub(crate) struct Pr03cSet {
    path: PathBuf,
}

impl Pr03cSet {
    pub(super) fn new(root: &Path, set: &str) -> Self {
        Self {
            path: root.join("fixtures").join(format!("{set}.fixture-set")),
        }
    }

    pub(crate) fn write(&self, bytes: &[u8]) -> Result<()> {
        fs::write(&self.path, bytes).map_err(|_| EvidenceError::Io)
    }

    pub(crate) fn open(&self) -> Result<File> {
        File::open(&self.path).map_err(|_| EvidenceError::Io)
    }
}

fn create_new(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|_| EvidenceError::Io)
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(EvidenceError::Io),
    }
}
