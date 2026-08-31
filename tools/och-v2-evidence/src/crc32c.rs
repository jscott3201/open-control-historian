#[derive(Clone, Copy)]
pub(crate) struct Crc32c {
    register: u32,
}

impl Crc32c {
    pub(crate) const fn new() -> Self {
        Self { register: u32::MAX }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.register ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = 0_u32.wrapping_sub(self.register & 1);
                self.register = (self.register >> 1) ^ (0x82f6_3b78 & mask);
            }
        }
    }

    pub(crate) const fn finish(self) -> u32 {
        !self.register
    }
}

#[cfg(test)]
pub(crate) fn checksum(bytes: &[u8]) -> u32 {
    let mut crc = Crc32c::new();
    crc.update(bytes);
    crc.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_check_value_is_exact() {
        assert_eq!(checksum(b"123456789"), 0xe306_9283);
    }
}
