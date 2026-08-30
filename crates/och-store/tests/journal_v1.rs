#![forbid(unsafe_code)]
//! Focused deterministic Journal V1 format and hostile-parser evidence.

mod support;

use och_core::{
    ExactText, ExactValue, RealBits, StateClass, StateMember, StateValue, Unavailable,
    UnavailableReason, ValueFamily,
};
use och_store::{
    AppendSequenceV1, DecodeLimitsV1, JOURNAL_V1_FRAME_CRC_LEN, JOURNAL_V1_FRAME_PREFIX_LEN,
    JOURNAL_V1_HEADER_LEN, JournalHeaderV1, JournalV1Error, MAX_ADMISSION_PAYLOAD_V1,
    decode_admission_frame_v1, encode_admission_frame_v1, encode_decoded_admission_frame_v1,
};

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

fn rewrite_crc(frame: &mut [u8]) {
    let checksum_offset = frame.len() - JOURNAL_V1_FRAME_CRC_LEN;
    let checksum = crc32c(&frame[..checksum_offset]).to_be_bytes();
    frame[checksum_offset..].copy_from_slice(&checksum);
}

fn sequence(value: u64) -> AppendSequenceV1 {
    AppendSequenceV1::new(value).expect("positive append sequence")
}

fn payload_len(frame: &[u8]) -> usize {
    usize::try_from(u32::from_be_bytes(
        frame[16..20].try_into().expect("length bytes"),
    ))
    .expect("u32 fits usize")
}

fn nth_subslice(haystack: &[u8], needle: &[u8], occurrence: usize) -> usize {
    haystack
        .windows(needle.len())
        .enumerate()
        .filter_map(|(index, window)| (window == needle).then_some(index))
        .nth(occurrence)
        .expect("fixture subsequence occurrence")
}

#[test]
fn header_has_exact_fixed_bytes_and_rejects_hostile_layouts() {
    let header = JournalHeaderV1::new(support::store_id(1));
    let bytes = header.encode();
    let expected = [
        b'O', b'C', b'H', b'J', b'N', b'L', b'0', b'1', 0, 1, 0, 28, 0x01, 0x94, 0x1f, 0x29, 0x7c,
        0x00, 0x70, 0x00, 0x80, 0x00, 0, 0, 0, 0, 0, 1,
    ];
    assert_eq!(bytes, expected);
    assert_eq!(bytes.len(), JOURNAL_V1_HEADER_LEN);
    assert_eq!(JournalHeaderV1::decode(&bytes), Ok(header));

    assert_eq!(
        JournalHeaderV1::decode(&bytes[..bytes.len() - 1]),
        Err(JournalV1Error::Truncated)
    );
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert_eq!(
        JournalHeaderV1::decode(&trailing),
        Err(JournalV1Error::TrailingBytes)
    );
    for (offset, expected_error) in [
        (0, JournalV1Error::InvalidHeaderMagic),
        (9, JournalV1Error::UnsupportedHeaderVersion),
        (11, JournalV1Error::InvalidHeaderLength),
        (18, JournalV1Error::InvalidIdentity),
    ] {
        let mut hostile = bytes;
        hostile[offset] ^= 0xff;
        assert_eq!(JournalHeaderV1::decode(&hostile), Err(expected_error));
    }
}

#[test]
fn rich_observed_frame_round_trips_every_retained_authority_surface() {
    let input = support::observed_admission(
        vec![ExactValue::Boolean(true)],
        ValueFamily::Boolean,
        1,
        true,
    );
    let frame = encode_admission_frame_v1(sequence(9), &input).expect("bounded encoding");
    let decoded =
        decode_admission_frame_v1(&frame, DecodeLimitsV1::maximum(), None).expect("valid frame");

    assert_eq!(decoded.append_sequence(), 9);
    assert_eq!(decoded.store_id(), input.store_id());
    assert_eq!(
        decoded.declaration().store_id(),
        input.declaration().store_id()
    );
    assert_eq!(
        decoded.declaration().series_id(),
        input.declaration().series_id()
    );
    assert_eq!(
        decoded.declaration().revision(),
        input.declaration().revision()
    );
    assert_eq!(
        decoded.declaration().previous_revision(),
        input.declaration().previous_revision()
    );
    assert_eq!(
        decoded.declaration().binding(),
        input.declaration().binding()
    );
    assert_eq!(
        decoded.declaration().payload(),
        input.declaration().payload()
    );
    assert_eq!(
        decoded.declaration().evidence(),
        input.declaration().evidence()
    );
    assert_eq!(decoded.envelope(), input.envelope());
    assert_eq!(decoded.retry(), input.retry());
    assert_eq!(decoded.batch(), input.batch());
    assert_eq!(decoded.lifecycle(), input.lifecycle());
    assert_eq!(decoded.evidence_kind(), input.evidence_kind());
    assert_eq!(decoded.observations().len(), input.observations().len());
    assert_eq!(decoded.gaps(), input.gaps());
    let actual = &decoded.observations()[0];
    let expected = &input.observations()[0];
    assert_eq!(actual.ordinal(), expected.ordinal());
    assert_eq!(
        actual.canonical_observation_id(),
        expected.canonical_observation_id()
    );
    assert_eq!(actual.observation(), expected.observation());
    assert_eq!(actual.raw(), expected.raw());
    assert_eq!(actual.normalized(), expected.normalized());
    assert_eq!(
        encode_decoded_admission_frame_v1(&decoded),
        Ok(frame.clone())
    );
    assert_eq!(encode_admission_frame_v1(sequence(9), &input), Ok(frame));
}

#[test]
fn every_value_family_and_unavailable_variant_round_trips_exactly() {
    let cases = [
        (
            ExactValue::Real(RealBits::from_bits(0x7ff8_1234_5678_9abc)),
            ValueFamily::Real,
        ),
        (ExactValue::Signed(i64::MIN), ValueFamily::Signed),
        (ExactValue::Unsigned(u64::MAX), ValueFamily::Unsigned),
        (ExactValue::Boolean(false), ValueFamily::Boolean),
        (
            ExactValue::State(StateValue::new(
                StateClass::new("class".to_owned()).expect("portable class"),
                StateMember::new("member".to_owned()).expect("portable member"),
            )),
            ValueFamily::State,
        ),
        (
            ExactValue::Text(ExactText::new("e\u{301}xact".to_owned()).expect("bounded text")),
            ValueFamily::Text,
        ),
        (
            ExactValue::Artifact(support::artifact(600, 60)),
            ValueFamily::Artifact,
        ),
        (
            ExactValue::Unavailable(Unavailable::without_reason()),
            ValueFamily::Real,
        ),
        (
            ExactValue::Unavailable(Unavailable::new(Some(
                UnavailableReason::new("sensor-offline".to_owned()).expect("portable reason"),
            ))),
            ValueFamily::Unsigned,
        ),
    ];
    for (index, (value, family)) in cases.into_iter().enumerate() {
        let admission = support::observed_admission(vec![value.clone()], family, 0, false);
        let frame = encode_admission_frame_v1(sequence(index as u64 + 1), &admission)
            .expect("bounded encoding");
        let decoded = decode_admission_frame_v1(&frame, DecodeLimitsV1::maximum(), None)
            .expect("valid frame");
        assert_eq!(decoded.envelope().observations()[0].value(), &value);
        assert_eq!(encode_decoded_admission_frame_v1(&decoded), Ok(frame));
    }
}

#[test]
fn gap_only_no_change_and_maximum_observation_bounds_are_explicit() {
    let gap_only = support::observed_admission(Vec::new(), ValueFamily::Boolean, 64, false);
    let gap_frame = encode_admission_frame_v1(sequence(1), &gap_only).expect("bounded gap frame");
    let decoded_gap = decode_admission_frame_v1(&gap_frame, DecodeLimitsV1::maximum(), None)
        .expect("valid gap frame");
    assert!(decoded_gap.observations().is_empty());
    assert_eq!(decoded_gap.gaps().len(), 64);

    let no_change = support::no_change_admission();
    let no_change_frame =
        encode_admission_frame_v1(sequence(2), &no_change).expect("bounded no-change frame");
    let decoded_no_change = decode_admission_frame_v1(
        &no_change_frame,
        DecodeLimitsV1::maximum(),
        Some(sequence(1)),
    )
    .expect("valid consecutive frame");
    assert!(decoded_no_change.observations().is_empty());
    assert!(decoded_no_change.gaps().is_empty());
    assert_eq!(
        decoded_no_change.envelope().no_change_evidence(),
        no_change.envelope().no_change_evidence()
    );

    let maximum = support::observed_admission(
        vec![ExactValue::Boolean(true); 256],
        ValueFamily::Boolean,
        0,
        false,
    );
    let maximum_frame =
        encode_admission_frame_v1(sequence(3), &maximum).expect("maximum observed frame");
    let decoded_maximum =
        decode_admission_frame_v1(&maximum_frame, DecodeLimitsV1::maximum(), None)
            .expect("maximum observed decode");
    assert_eq!(decoded_maximum.observations().len(), 256);
    assert_eq!(decoded_maximum.envelope().observations().len(), 256);
}

#[test]
fn framing_refuses_invalid_sequence_magic_version_kind_flags_crc_and_layout() {
    assert_eq!(
        AppendSequenceV1::new(0),
        Err(JournalV1Error::InvalidAppendSequence)
    );
    let admission = support::no_change_admission();
    let frame = encode_admission_frame_v1(sequence(7), &admission).expect("bounded frame");
    assert_eq!(
        decode_admission_frame_v1(&frame, DecodeLimitsV1::maximum(), Some(sequence(5))),
        Err(JournalV1Error::NonMonotonicAppendSequence)
    );
    assert_eq!(
        decode_admission_frame_v1(&frame, DecodeLimitsV1::maximum(), Some(sequence(u64::MAX))),
        Err(JournalV1Error::AppendSequenceOverflow)
    );

    for (offset, expected_error) in [
        (0, JournalV1Error::InvalidFrameMagic),
        (5, JournalV1Error::UnsupportedFrameVersion),
        (6, JournalV1Error::UnsupportedFrameKind),
        (7, JournalV1Error::InvalidFrameFlags),
    ] {
        let mut hostile = frame.clone();
        hostile[offset] ^= 0xff;
        assert_eq!(
            decode_admission_frame_v1(&hostile, DecodeLimitsV1::maximum(), None),
            Err(expected_error)
        );
    }

    let mut zero_sequence = frame.clone();
    zero_sequence[8..16].fill(0);
    assert_eq!(
        decode_admission_frame_v1(&zero_sequence, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::InvalidAppendSequence)
    );
    let mut checksum = frame.clone();
    checksum[JOURNAL_V1_FRAME_PREFIX_LEN] ^= 1;
    assert_eq!(
        decode_admission_frame_v1(&checksum, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::ChecksumMismatch)
    );
    let mut trailing = frame.clone();
    trailing.push(0);
    assert_eq!(
        decode_admission_frame_v1(&trailing, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::TrailingBytes)
    );
    for cut in 0..frame.len() {
        assert_eq!(
            decode_admission_frame_v1(&frame[..cut], DecodeLimitsV1::maximum(), None),
            Err(JournalV1Error::Truncated),
            "cut={cut}"
        );
    }
}

#[test]
fn declared_payload_bounds_are_checked_before_field_allocation() {
    let admission = support::no_change_admission();
    let frame = encode_admission_frame_v1(sequence(1), &admission).expect("bounded frame");
    let payload_len = payload_len(&frame);
    assert!(
        decode_admission_frame_v1(
            &frame,
            DecodeLimitsV1::new(payload_len).expect("exact configured limit"),
            None,
        )
        .is_ok()
    );
    assert_eq!(
        decode_admission_frame_v1(
            &frame,
            DecodeLimitsV1::new(payload_len - 1).expect("one-below configured limit"),
            None,
        ),
        Err(JournalV1Error::PayloadTooLarge)
    );
    assert_eq!(
        decode_admission_frame_v1(
            &frame,
            DecodeLimitsV1::new(0).expect("zero configured limit"),
            None,
        ),
        Err(JournalV1Error::PayloadTooLarge)
    );
    assert_eq!(
        DecodeLimitsV1::new(MAX_ADMISSION_PAYLOAD_V1 + 1),
        Err(JournalV1Error::PayloadTooLarge)
    );

    let mut declared = vec![0_u8; JOURNAL_V1_FRAME_PREFIX_LEN + JOURNAL_V1_FRAME_CRC_LEN];
    declared[..4].copy_from_slice(b"OCHF");
    declared[4..6].copy_from_slice(&1_u16.to_be_bytes());
    declared[6] = 1;
    declared[8..16].copy_from_slice(&1_u64.to_be_bytes());
    declared[16..20].copy_from_slice(
        &u32::try_from(MAX_ADMISSION_PAYLOAD_V1 + 1)
            .expect("hard maximum fits u32")
            .to_be_bytes(),
    );
    assert_eq!(
        decode_admission_frame_v1(&declared, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::PayloadTooLarge)
    );
    declared[16..20].copy_from_slice(
        &u32::try_from(MAX_ADMISSION_PAYLOAD_V1)
            .expect("hard maximum fits u32")
            .to_be_bytes(),
    );
    assert_eq!(
        decode_admission_frame_v1(&declared, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::Truncated)
    );
}

#[test]
fn hostile_payload_tags_lengths_identities_and_duplicate_evidence_are_closed() {
    let admission = support::no_change_admission();
    let frame = encode_admission_frame_v1(sequence(1), &admission).expect("bounded frame");

    let mut invalid_identity = frame.clone();
    invalid_identity[JOURNAL_V1_FRAME_PREFIX_LEN + 6] = 0x60;
    rewrite_crc(&mut invalid_identity);
    assert_eq!(
        decode_admission_frame_v1(&invalid_identity, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::InvalidIdentity)
    );

    let mut invalid_length = frame.clone();
    let first_provider_length = JOURNAL_V1_FRAME_PREFIX_LEN + 65;
    invalid_length[first_provider_length..first_provider_length + 4]
        .copy_from_slice(&u32::MAX.to_be_bytes());
    rewrite_crc(&mut invalid_length);
    assert_eq!(
        decode_admission_frame_v1(&invalid_length, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::InvalidLength)
    );

    let mut invalid_utf8 = frame.clone();
    invalid_utf8[first_provider_length + 4] = 0xff;
    rewrite_crc(&mut invalid_utf8);
    assert_eq!(
        decode_admission_frame_v1(&invalid_utf8, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::InvalidUtf8)
    );

    let mut unknown_tag = frame.clone();
    let checksum_offset = unknown_tag.len() - JOURNAL_V1_FRAME_CRC_LEN;
    unknown_tag[checksum_offset - 1] = 0xff;
    rewrite_crc(&mut unknown_tag);
    assert_eq!(
        decode_admission_frame_v1(&unknown_tag, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::UnknownTag)
    );

    let observed = support::observed_admission(
        vec![ExactValue::Boolean(true)],
        ValueFamily::Boolean,
        0,
        false,
    );
    let mut duplicate =
        encode_admission_frame_v1(sequence(2), &observed).expect("bounded observed frame");
    let needle = support::uuid_bytes(1_000);
    let position = duplicate
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("unique source observation evidence identity");
    duplicate[position..position + needle.len()].copy_from_slice(&support::uuid_bytes(100));
    rewrite_crc(&mut duplicate);
    assert_eq!(
        decode_admission_frame_v1(&duplicate, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::InvalidCanonicalData)
    );

    let maximum = support::observed_admission(
        vec![ExactValue::Boolean(true); 256],
        ValueFamily::Boolean,
        0,
        false,
    );
    let mut too_many =
        encode_admission_frame_v1(sequence(3), &maximum).expect("maximum observed frame");
    let envelope_series = nth_subslice(&too_many, &support::uuid_bytes(2), 1);
    let observation_count = envelope_series + 16 + 16 + 1 + 1;
    too_many[observation_count..observation_count + 4].copy_from_slice(&257_u32.to_be_bytes());
    rewrite_crc(&mut too_many);
    assert_eq!(
        decode_admission_frame_v1(&too_many, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::InvalidCount)
    );

    let maximum_gaps = support::observed_admission(Vec::new(), ValueFamily::Boolean, 64, false);
    let mut too_many_gaps =
        encode_admission_frame_v1(sequence(4), &maximum_gaps).expect("maximum gap frame");
    let envelope_series = nth_subslice(&too_many_gaps, &support::uuid_bytes(2), 1);
    let gap_count = envelope_series + 16 + 16 + 1 + 1 + 4;
    too_many_gaps[gap_count..gap_count + 4].copy_from_slice(&65_u32.to_be_bytes());
    rewrite_crc(&mut too_many_gaps);
    assert_eq!(
        decode_admission_frame_v1(&too_many_gaps, DecodeLimitsV1::maximum(), None),
        Err(JournalV1Error::InvalidCount)
    );
}
