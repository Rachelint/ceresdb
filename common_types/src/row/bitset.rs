// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Simple BitSet implementation.

#![allow(dead_code)]

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const UNSET_BIT_MASK: [u8; 8] = [
    255 - 1,
    255 - 2,
    255 - 4,
    255 - 8,
    255 - 16,
    255 - 32,
    255 - 64,
    255 - 128,
];

/// A bit set representing at most 8 bits with a underlying u8.
pub struct OneByteBitSet(pub u8);

impl OneByteBitSet {
    /// Create from a given boolean slice.
    ///
    /// The values in the `bits` whose index is greater than 8 will be ignored.
    pub fn from_slice(bits: &[bool]) -> Self {
        let mut v = 0u8;
        for (idx, set) in bits.iter().take(8).map(|v| *v as u8).enumerate() {
            let (_, bit_idx) = RoBitSet::compute_byte_bit_index(idx);
            v |= set << bit_idx
        }

        Self(v)
    }
}

/// A basic implementation supporting read/write.
#[derive(Debug, Default, Clone)]
pub struct BitSet {
    /// The bits are stored as bytes in the least significant bit order.
    buffer: Vec<u8>,
    /// The number of real bits in the `buffer`
    num_bits: usize,
}

impl BitSet {
    /// Initialize a unset [`BitSet`].
    pub fn new(num_bits: usize) -> Self {
        Self {
            buffer: vec![0; Self::num_bytes(num_bits)],
            num_bits,
        }
    }

    /// Initialize a [`BitSet`] with all bits set.
    pub fn all_set(num_bits: usize) -> Self {
        Self {
            buffer: vec![0xFF; Self::num_bytes(num_bits)],
            num_bits,
        }
    }

    #[inline]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    #[inline]
    pub fn num_bytes(num_bits: usize) -> usize {
        (num_bits + 7) >> 3
    }

    /// Initialize directly from a buffer.
    ///
    /// None will be returned if the buffer's length is not enough to cover the
    /// bits of `num_bits`.
    pub fn try_from_raw(buffer: Vec<u8>, num_bits: usize) -> Option<Self> {
        if buffer.len() < Self::num_bytes(num_bits) {
            None
        } else {
            Some(Self { buffer, num_bits })
        }
    }

    /// Set the bit at the `index`.
    ///
    /// Return false if the index is outside the range.
    pub fn set(&mut self, index: usize) -> bool {
        if index >= self.num_bits {
            return false;
        }
        let (byte_index, bit_index) = RoBitSet::compute_byte_bit_index(index);
        self.buffer[byte_index] |= BIT_MASK[bit_index];
        true
    }

    /// Set the bit at the `index`.
    ///
    /// Return false if the index is outside the range.
    pub fn unset(&mut self, index: usize) -> bool {
        if index >= self.num_bits {
            return false;
        }
        let (byte_index, bit_index) = RoBitSet::compute_byte_bit_index(index);
        self.buffer[byte_index] &= UNSET_BIT_MASK[bit_index];
        true
    }

    /// Tells whether the bit at the `index` is set.
    pub fn is_set(&self, index: usize) -> Option<bool> {
        let ro = RoBitSet {
            buffer: &self.buffer,
            num_bits: self.num_bits,
        };
        ro.is_set(index)
    }

    /// Tells whether the bit at the `index` is unset.
    pub fn is_unset(&self, index: usize) -> Option<bool> {
        let ro = RoBitSet {
            buffer: &self.buffer,
            num_bits: self.num_bits,
        };
        ro.is_unset(index)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }
}

/// A readonly version of [BitSet], only supports read.
pub struct RoBitSet<'a> {
    /// The bits are stored as bytes in the least significant bit order.
    buffer: &'a [u8],
    /// The number of real bits in the `buffer`
    num_bits: usize,
}

impl<'a> RoBitSet<'a> {
    pub fn try_new(buffer: &'a [u8], num_bits: usize) -> Option<Self> {
        if buffer.len() < BitSet::num_bytes(num_bits) {
            None
        } else {
            Some(Self { buffer, num_bits })
        }
    }

    /// Tells whether the bit at the `index` is set.
    pub fn is_set(&self, index: usize) -> Option<bool> {
        if index >= self.num_bits {
            return None;
        }
        let (byte_index, bit_index) = Self::compute_byte_bit_index(index);
        let set = (self.buffer[byte_index] & (1 << bit_index)) != 0;
        Some(set)
    }

    /// Tells whether the bit at the `index` is set.
    #[inline]
    pub fn is_unset(&self, index: usize) -> Option<bool> {
        self.is_set(index).map(|v| !v)
    }

    #[inline]
    fn compute_byte_bit_index(index: usize) -> (usize, usize) {
        (index >> 3, index & 7)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use super::BitSet;
    use crate::row::bitset::OneByteBitSet;

    #[test]
    fn test_set_op() {
        let mut bit_set = BitSet::new(50);

        assert!(bit_set.set(1));
        assert!(bit_set.is_set(1).unwrap());

        assert!(bit_set.set(20));
        assert!(bit_set.is_set(20).unwrap());
        assert!(bit_set.set(49));
        assert!(bit_set.is_set(49).unwrap());
        assert!(bit_set.unset(49));
        assert!(bit_set.is_unset(49).unwrap());

        assert!(!bit_set.set(100));
        assert!(bit_set.is_set(100).is_none());

        assert_eq!(
            bit_set.into_bytes(),
            vec![
                0b00000010, 0b00000000, 0b00010000, 0b00000000, 0b00000000, 0b00000000, 0b00000000
            ]
        );
    }

    #[test]
    fn test_unset() {
        let mut bit_set = BitSet::all_set(50);

        assert!(bit_set.unset(1));
        assert!(bit_set.is_unset(1).unwrap());

        assert!(bit_set.unset(20));
        assert!(bit_set.is_unset(20).unwrap());
        assert!(bit_set.unset(49));
        assert!(bit_set.is_unset(49).unwrap());

        assert!(!bit_set.unset(100));
        assert!(bit_set.is_unset(100).is_none());

        assert_eq!(
            bit_set.into_bytes(),
            vec![
                0b11111101, 0b11111111, 0b11101111, 0b11111111, 0b11111111, 0b11111111, 0b11111101
            ]
        );
    }

    #[test]
    fn test_try_from_raw() {
        let raw_bytes: Vec<u8> = vec![0b11111111, 0b11110000, 0b00001111, 0b00001100, 0b00001001];
        assert!(BitSet::try_from_raw(raw_bytes.clone(), 50).is_none());
        assert!(BitSet::try_from_raw(raw_bytes.clone(), 40).is_some());
        assert!(BitSet::try_from_raw(raw_bytes, 1).is_some());
    }

    #[test]
    fn test_one_byte() {
        let bits = [false, false, false, false, false, false];
        assert_eq!(0, OneByteBitSet::from_slice(&bits).0);

        let bits = [true, false, false, false, false, false];
        assert_eq!(1, OneByteBitSet::from_slice(&bits).0);

        let bits = [false, false, false, true, false, false, true, true];
        assert_eq!(128 + 64 + 8, OneByteBitSet::from_slice(&bits).0);

        let bits = [
            false, false, false, false, false, false, true, true, true, true,
        ];
        assert_eq!(128 + 64, OneByteBitSet::from_slice(&bits).0);
    }
}
