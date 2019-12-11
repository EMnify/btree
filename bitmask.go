package btree

import (
	"math/bits"
)

type bitmask uint64
type bit uint64

const bitmaskSize = 64

func (m bitmask) bitAt(i int) bit {
	return bit(bitmask(1) & (m >> uint(i)))
}

func (m bitmask) bitsAt(i int) bitmask {
	return m >> uint(i)
}

func (m *bitmask) setBitAt(i int, bit bit) {
	*m = *m&^(bitmask(1)<<uint(i)) | (bitmask(bit) << uint(i))
}

func (m *bitmask) setBitsAt(i int, bits bitmask) {
	*m = *m&^(^bitmask(0)<<uint(i)) | (bits << uint(i))
}

func (m *bitmask) insertBitAt(i int, bit bit) {
	mask := ^bitmask(0) << uint(i)
	*m = *m&^mask | (bitmask(bit) << uint(i)) | (*m & mask << 1)
}

func (m *bitmask) removeBitAt(i int) bit {
	out := m.bitAt(i)
	mask := ^bitmask(0) << uint(i)
	*m = *m&^mask | ((*m & (mask << 1)) >> 1)
	return out
}

func (m bitmask) firstClearBitIndex(at int) int {
	return bits.TrailingZeros64(uint64((^bitmask(0) << uint(at)) &^ m))
}
