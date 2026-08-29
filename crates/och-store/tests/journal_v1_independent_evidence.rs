#![forbid(unsafe_code)]
//! Independent primitive oracle comparison for Journal V1.

#[path = "support/journal_oracle.rs"]
mod journal_oracle;
mod support;

use och_core::{ExactValue, ValueFamily};
use och_store::{AppendSequenceV1, encode_admission_frame_v1};

const ORACLE_SOURCE: &str = include_str!("support/journal_oracle.rs");

#[test]
fn rich_public_admission_matches_primitive_only_byte_oracle_exactly() {
    let admission = support::observed_admission(
        vec![ExactValue::Boolean(true)],
        ValueFamily::Boolean,
        1,
        true,
    );
    let actual = encode_admission_frame_v1(
        AppendSequenceV1::new(9).expect("positive append sequence"),
        &admission,
    )
    .expect("bounded public admission encoding");
    assert_eq!(actual, journal_oracle::expected_rich_observed_frame());
}

#[test]
fn primitive_oracle_does_not_import_product_crates_or_helpers() {
    let forbidden = ["och_store", "och_core", "encode_admission_frame_v1"];
    for symbol in forbidden {
        assert!(
            !ORACLE_SOURCE.contains(symbol),
            "primitive oracle must not contain {symbol}"
        );
    }
}
