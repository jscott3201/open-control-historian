#![forbid(unsafe_code)]
//! Native Segment V1 public query-bound evidence.

use och_store::{
    MAX_SEGMENT_QUERY_RESULTS_V1, SegmentObservationQueryV1, SegmentObservationQueryV1Error,
};

#[test]
fn public_query_bound_refuses_zero_and_seventeen() {
    let series = och_core::SeriesId::from_bytes([
        0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, 0, 0, 0, 0, 0, 2,
    ])
    .expect("UUIDv7 query series");
    for invalid in [0, MAX_SEGMENT_QUERY_RESULTS_V1 + 1] {
        assert_eq!(
            SegmentObservationQueryV1::new(series, None, invalid)
                .expect_err("invalid public result limit"),
            SegmentObservationQueryV1Error::InvalidLimit
        );
    }
}
