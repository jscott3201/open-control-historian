#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! The native dependency boundary anchor for `OpenControl` Historian.
//!
//! This crate intentionally exposes no observation model or runtime API. Its
//! current purpose is to make the native product closure concrete so dependency
//! direction, build cost, and binary size can be measured before semantic work
//! begins in M00-PR02.
