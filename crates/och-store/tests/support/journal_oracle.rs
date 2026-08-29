//! Primitive-only independent Journal V1 byte oracle.
//!
//! This module deliberately imports no product crate and calls no product
//! encoding, validation, or CRC helper.

fn uuid(number: u64) -> [u8; 16] {
    let suffix = number.to_be_bytes();
    [
        0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, suffix[2], suffix[3],
        suffix[4], suffix[5], suffix[6], suffix[7],
    ]
}

fn put_string(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .expect("primitive fixture length")
            .to_be_bytes(),
    );
    bytes.extend_from_slice(value.as_bytes());
}

fn put_timestamp(bytes: &mut Vec<u8>, seconds: i64, nanos: u32) {
    bytes.extend_from_slice(&seconds.to_be_bytes());
    bytes.extend_from_slice(&nanos.to_be_bytes());
}

fn put_content(bytes: &mut Vec<u8>, seed: u8) {
    put_string(bytes, "application/octet-stream");
    bytes.extend_from_slice(&u128::from(seed).to_be_bytes());
    bytes.extend_from_slice(&[seed; 32]);
}

fn put_artifact(bytes: &mut Vec<u8>, id: u64, seed: u8) {
    bytes.extend_from_slice(&uuid(id));
    put_content(bytes, seed);
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let low_bit_mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82f6_3b78 & low_bit_mask);
        }
    }
    !crc
}

/// Returns the independently specified rich observed fixture frame.
#[allow(clippy::too_many_lines)]
pub fn expected_rich_observed_frame() -> Vec<u8> {
    let mut payload = Vec::new();

    // Admission store, revision-two declaration, immutable binding and payload.
    payload.extend_from_slice(&uuid(1));
    payload.extend_from_slice(&uuid(1));
    payload.extend_from_slice(&uuid(2));
    payload.extend_from_slice(&2_u128.to_be_bytes());
    payload.push(1);
    payload.extend_from_slice(&1_u128.to_be_bytes());
    put_string(&mut payload, "provider:acme");
    payload.push(1);
    put_string(&mut payload, "Mqtt");
    put_string(&mut payload, "locator:device-1");
    payload.extend_from_slice(&uuid(3));
    payload.push(1); // sampled
    payload.push(4); // Boolean
    payload.push(1); // resolved quantity
    put_string(&mut payload, "quantity:temperature");
    payload.push(2); // unresolved unit
    put_string(&mut payload, "native-unit:degC");
    payload.push(1);
    put_string(&mut payload, "application:ahu-1:revised");
    put_timestamp(&mut payload, -1, 8);
    payload.push(1);
    put_artifact(&mut payload, 202, 23);

    // Atomically validated observed envelope.
    payload.extend_from_slice(&uuid(2));
    payload.extend_from_slice(&uuid(3));
    payload.push(1); // sampled
    payload.push(1); // observed
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.extend_from_slice(&uuid(10_000));
    payload.push(4); // Boolean
    payload.push(1); // true
    payload.push(1); // source time present
    put_timestamp(&mut payload, -1, 999_999_999);
    put_timestamp(&mut payload, 10, 11);
    put_timestamp(&mut payload, 9, 12);
    payload.push(3); // uncertain
    payload.push(37); // stale, substituted, communication failure
    payload.extend_from_slice(&2_u32.to_be_bytes());
    put_string(&mut payload, "source-ok");
    put_string(&mut payload, "vendor:42");
    payload.push(1); // producer position present
    payload.extend_from_slice(&9_u128.to_be_bytes());
    payload.extend_from_slice(&10_000_u128.to_be_bytes());
    payload.push(0); // no observation interval
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.extend_from_slice(&10_u128.to_be_bytes());
    payload.extend_from_slice(&20_000_u128.to_be_bytes());
    payload.extend_from_slice(&20_001_u128.to_be_bytes());
    payload.push(5); // source data loss

    // Request retry, batch metadata, and complete capture lifecycle.
    payload.extend_from_slice(&uuid(2));
    payload.extend_from_slice(&uuid(3));
    put_string(&mut payload, "historian-request");
    put_content(&mut payload, 21);
    put_string(&mut payload, "studio.source-batch");
    payload.extend_from_slice(&7_u128.to_be_bytes());
    payload.push(1); // observed
    payload.extend_from_slice(&uuid(100));
    put_string(&mut payload, "provider:acme");
    put_string(&mut payload, "Mqtt");
    payload.extend_from_slice(&uuid(101));
    payload.extend_from_slice(&uuid(100));
    put_string(&mut payload, "locator:device-1");
    payload.extend_from_slice(&uuid(102));
    payload.extend_from_slice(&uuid(101));
    put_timestamp(&mut payload, -2, 999_000_000);
    payload.push(1);
    put_timestamp(&mut payload, 3, 4);
    payload.extend_from_slice(&uuid(103));
    payload.extend_from_slice(&uuid(102));
    put_artifact(&mut payload, 200, 20);

    // Retained observed lineage and exact source gap reason.
    payload.push(1); // observed
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.push(0); // original ordinal
    payload.extend_from_slice(&uuid(10_000));
    payload.extend_from_slice(&uuid(1_000));
    payload.push(1);
    put_artifact(&mut payload, 300, 30);
    payload.push(1); // source transport new
    payload.push(1);
    put_string(&mut payload, "source-observation-key");
    put_content(&mut payload, 31);
    payload.extend_from_slice(&uuid(1_001));
    payload.extend_from_slice(&uuid(103));
    put_artifact(&mut payload, 400, 40);
    payload.push(1);
    put_string(&mut payload, "raw-record-0");
    put_content(&mut payload, 40);
    payload.extend_from_slice(&uuid(1_002));
    payload.extend_from_slice(&uuid(1_001));
    put_content(&mut payload, 80);
    payload.extend_from_slice(&uuid(1_000));
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.extend_from_slice(&10_u128.to_be_bytes());
    payload.extend_from_slice(&20_000_u128.to_be_bytes());
    payload.extend_from_slice(&20_001_u128.to_be_bytes());
    payload.push(2); // source unavailable

    let mut frame = Vec::new();
    frame.extend_from_slice(b"OCHF");
    frame.extend_from_slice(&1_u16.to_be_bytes());
    frame.push(1); // canonical admission
    frame.push(0); // flags
    frame.extend_from_slice(&9_u64.to_be_bytes());
    frame.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("primitive payload length")
            .to_be_bytes(),
    );
    frame.extend_from_slice(&payload);
    let checksum = crc32c(&frame);
    frame.extend_from_slice(&checksum.to_be_bytes());
    frame
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn primitive_crc_matches_published_castagnoli_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }
}
