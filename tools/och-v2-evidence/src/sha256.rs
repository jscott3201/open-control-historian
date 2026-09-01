use crate::error::{EvidenceError, Result};

pub(crate) const DIGEST_BYTES: usize = 32;
pub(crate) const HEX_BYTES: usize = DIGEST_BYTES * 2;
const BLOCK_BYTES: usize = 64;

const INITIAL: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

const ROUND: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

#[derive(Clone)]
pub(crate) struct Sha256 {
    state: [u32; 8],
    block: [u8; BLOCK_BYTES],
    filled: usize,
    length: u64,
}

impl Sha256 {
    pub(crate) const fn new() -> Self {
        Self {
            state: INITIAL,
            block: [0; BLOCK_BYTES],
            filled: 0,
            length: 0,
        }
    }

    pub(crate) fn update(&mut self, mut bytes: &[u8]) -> Result<()> {
        self.length = self
            .length
            .checked_add(u64::try_from(bytes.len()).map_err(|_| EvidenceError::Bounds)?)
            .ok_or(EvidenceError::Bounds)?;
        if self.filled != 0 {
            let take = (BLOCK_BYTES - self.filled).min(bytes.len());
            self.block[self.filled..self.filled + take].copy_from_slice(&bytes[..take]);
            self.filled += take;
            bytes = &bytes[take..];
            if self.filled == BLOCK_BYTES {
                compress(&mut self.state, &self.block);
                self.filled = 0;
            }
        }
        let (chunks, remainder) = bytes.as_chunks::<BLOCK_BYTES>();
        for chunk in chunks {
            compress(&mut self.state, chunk);
        }
        self.block[..remainder.len()].copy_from_slice(remainder);
        self.filled = remainder.len();
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<[u8; DIGEST_BYTES]> {
        let bit_length = self.length.checked_mul(8).ok_or(EvidenceError::Bounds)?;
        self.block[self.filled] = 0x80;
        self.filled += 1;
        if self.filled > 56 {
            self.block[self.filled..].fill(0);
            compress(&mut self.state, &self.block);
            self.block.fill(0);
        } else {
            self.block[self.filled..56].fill(0);
        }
        self.block[56..].copy_from_slice(&bit_length.to_be_bytes());
        compress(&mut self.state, &self.block);
        let mut output = [0_u8; DIGEST_BYTES];
        for (chunk, word) in output.as_chunks_mut::<4>().0.iter_mut().zip(self.state) {
            chunk.copy_from_slice(&word.to_be_bytes());
        }
        Ok(output)
    }
}

pub(crate) fn digest(bytes: &[u8]) -> Result<[u8; DIGEST_BYTES]> {
    let mut hash = Sha256::new();
    hash.update(bytes)?;
    hash.finish()
}

pub(crate) fn hex(digest: &[u8; DIGEST_BYTES]) -> String {
    let mut output = String::with_capacity(HEX_BYTES);
    for byte in digest {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub(crate) fn parse_hex(value: &str) -> Result<[u8; DIGEST_BYTES]> {
    if value.len() != HEX_BYTES || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(EvidenceError::InvalidHarness);
    }
    let mut output = [0_u8; DIGEST_BYTES];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| EvidenceError::InvalidHarness)?;
    }
    Ok(output)
}

fn compress(state: &mut [u32; 8], block: &[u8; BLOCK_BYTES]) {
    let mut words = [0_u32; 64];
    for (word, bytes) in words[..16].iter_mut().zip(block.as_chunks::<4>().0) {
        *word = u32::from_be_bytes(*bytes);
    }
    for index in 16..64 {
        let s0 = words[index - 15].rotate_right(7)
            ^ words[index - 15].rotate_right(18)
            ^ (words[index - 15] >> 3);
        let s1 = words[index - 2].rotate_right(17)
            ^ words[index - 2].rotate_right(19)
            ^ (words[index - 2] >> 10);
        words[index] = words[index - 16]
            .wrapping_add(s0)
            .wrapping_add(words[index - 7])
            .wrapping_add(s1);
    }
    let mut working = *state;
    for (constant, word) in ROUND.into_iter().zip(words) {
        let choose = (working[4] & working[5]) ^ (!working[4] & working[6]);
        let majority =
            (working[0] & working[1]) ^ (working[0] & working[2]) ^ (working[1] & working[2]);
        let upper =
            working[4].rotate_right(6) ^ working[4].rotate_right(11) ^ working[4].rotate_right(25);
        let lower =
            working[0].rotate_right(2) ^ working[0].rotate_right(13) ^ working[0].rotate_right(22);
        let first = working[7]
            .wrapping_add(upper)
            .wrapping_add(choose)
            .wrapping_add(constant)
            .wrapping_add(word);
        let second = lower.wrapping_add(majority);
        working.copy_within(0..7, 1);
        working[4] = working[4].wrapping_add(first);
        working[0] = first.wrapping_add(second);
    }
    for (value, addend) in state.iter_mut().zip(working) {
        *value = value.wrapping_add(addend);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_standard_vectors_match() {
        for (input, expected) in [
            (
                b"".as_slice(),
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                b"abc".as_slice(),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq".as_slice(),
                "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
            ),
        ] {
            assert_eq!(hex(&digest(input).expect("bounded digest")), expected);
        }
    }

    #[test]
    fn chunking_does_not_change_digest() {
        let bytes = vec![0xa5; 1_000_003];
        let expected = digest(&bytes).expect("one-shot digest");
        let mut hash = Sha256::new();
        for chunk in bytes.chunks(997) {
            hash.update(chunk).expect("stream update");
        }
        assert_eq!(hash.finish().expect("stream digest"), expected);
    }
}
