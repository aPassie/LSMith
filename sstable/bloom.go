package sstable

import (
	"encoding/binary"
	"math"
)

// Bloom filter implementation

func buildBloomFilter(keys [][]byte, bitsPerKey int) []byte {
	if len(keys) == 0 {
		return []byte{0}
	}

	numProbes := int(math.Round(float64(bitsPerKey) * math.Ln2))
	if numProbes < 1 {
		numProbes = 1
	}
	if numProbes > 30 {
		numProbes = 30
	}

	nBits := len(keys) * bitsPerKey
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8

	filter := make([]byte, nBytes+1)

	for _, k := range keys {
		h := bloomHash(k)
		delta := (h >> 17) | (h << 15)
		for j := 0; j < numProbes; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}

	filter[nBytes] = byte(numProbes)

	return filter
}

func bloomCheck(filter []byte, userKey []byte) bool {
	if len(filter) < 2 {
		return false
	}

	numProbes := int(filter[len(filter)-1])
	if numProbes == 0 {
		return false
	}

	nBits := uint32((len(filter) - 1) * 8)
	h := bloomHash(userKey)
	delta := (h >> 17) | (h << 15)
	for j := 0; j < numProbes; j++ {
		bitPos := h % nBits
		if filter[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func bloomHash(data []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(data))*m

	for len(data) >= 4 {
		h += binary.LittleEndian.Uint32(data[:4])
		h *= m
		h ^= h >> 16
		data = data[4:]
	}

	switch len(data) {
	case 3:
		h += uint32(data[2]) << 16
		fallthrough
	case 2:
		h += uint32(data[1]) << 8
		fallthrough
	case 1:
		h += uint32(data[0])
		h *= m
		h ^= h >> 24
	}

	return h
}
