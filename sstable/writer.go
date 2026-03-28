// Package sstable implements the Sorted String Table - the on-disk file format or LSMith's persistent storage
package sstable

import (
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/shanks/lsmith/internal/key"
)

const (
	blockSize        = 4096
	restartInterval  = 16
	footerSize       = 48
	magicNumber      = 0x88E241B785F4CFF7
	bloomBitsPerKey  = 10
)

type Writer struct {
	f *os.File

	blockBuf     []byte
	restarts     []uint32
	entryCount   int
	lastKey      []byte
	blockEntries int

	indexEntries []indexEntry

	bloomKeys [][]byte

	offset uint64

	smallestKey key.InternalKey
	largestKey  key.InternalKey
	totalCount  uint64
}

type indexEntry struct {
	lastKey key.InternalKey
	offset  uint64
	size    uint64
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{
		f:        f,
		restarts: []uint32{0},
	}, nil
}

func (w *Writer) Add(ikey key.InternalKey, value []byte) error {
	if w.totalCount == 0 {
		w.smallestKey = append(key.InternalKey{}, ikey...)
	}
	w.largestKey = append(w.largestKey[:0], ikey...)
	w.totalCount++

	userKey := ikey.UserKey()
	bloomKey := make([]byte, len(userKey))
	copy(bloomKey, userKey)
	w.bloomKeys = append(w.bloomKeys, bloomKey)

	shared := 0
	if w.entryCount == 0 {
		shared = 0
	} else {
		limit := len(w.lastKey)
		if len(ikey) < limit {
			limit = len(ikey)
		}
		for shared < limit && w.lastKey[shared] == ikey[shared] {
			shared++
		}
	}
	unshared := len(ikey) - shared

	var entry []byte
	entry = appendUvarint(entry, uint64(shared))
	entry = appendUvarint(entry, uint64(unshared))
	entry = appendUvarint(entry, uint64(len(value)))
	entry = append(entry, ikey[shared:]...)
	entry = append(entry, value...)

	w.blockBuf = append(w.blockBuf, entry...)
	w.blockEntries++
	w.entryCount++

	w.lastKey = append(w.lastKey[:0], ikey...)

	if w.entryCount >= restartInterval {
		w.entryCount = 0
		w.restarts = append(w.restarts, uint32(len(w.blockBuf)))
	}

	if len(w.blockBuf) >= blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) flushBlock() error {
	if w.blockEntries == 0 {
		return nil
	}

	block := make([]byte, 0, len(w.blockBuf)+len(w.restarts)*4+4)
	block = append(block, w.blockBuf...)
	for _, r := range w.restarts {
		block = binary.LittleEndian.AppendUint32(block, r)
	}
	block = binary.LittleEndian.AppendUint32(block, uint32(len(w.restarts)))

	checksum := crc32.ChecksumIEEE(block)
	block = binary.LittleEndian.AppendUint32(block, checksum)

	n, err := w.f.Write(block)
	if err != nil {
		return err
	}

	lastKey := make(key.InternalKey, len(w.lastKey))
	copy(lastKey, w.lastKey)
	w.indexEntries = append(w.indexEntries, indexEntry{
		lastKey: lastKey,
		offset:  w.offset,
		size:    uint64(n),
	})

	w.offset += uint64(n)

	w.blockBuf = w.blockBuf[:0]
	w.restarts = w.restarts[:0]
	w.restarts = append(w.restarts, 0)
	w.entryCount = 0
	w.blockEntries = 0
	w.lastKey = w.lastKey[:0]

	return nil
}

func (w *Writer) Finish() error {
	if err := w.flushBlock(); err != nil {
		return err
	}

	bloomData := buildBloomFilter(w.bloomKeys, bloomBitsPerKey)
	bloomOffset := w.offset
	n, err := w.f.Write(bloomData)
	if err != nil {
		return err
	}
	bloomSize := uint64(n)
	w.offset += bloomSize

	indexOffset := w.offset
	indexData := encodeIndexBlock(w.indexEntries)
	n, err = w.f.Write(indexData)
	if err != nil {
		return err
	}
	indexSize := uint64(n)
	w.offset += indexSize

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], bloomOffset)
	binary.LittleEndian.PutUint64(footer[8:16], bloomSize)
	binary.LittleEndian.PutUint64(footer[16:24], indexOffset)
	binary.LittleEndian.PutUint64(footer[24:32], indexSize)
	binary.LittleEndian.PutUint64(footer[32:40], magicNumber)
	_, err = w.f.Write(footer)
	if err != nil {
		return err
	}

	return w.f.Close()
}

func (w *Writer) SmallestKey() key.InternalKey { return w.smallestKey }

func (w *Writer) LargestKey() key.InternalKey { return w.largestKey }

func (w *Writer) EntryCount() uint64 { return w.totalCount }

func encodeIndexBlock(entries []indexEntry) []byte {
	var buf []byte
	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.lastKey)))
		buf = append(buf, e.lastKey...)
		buf = binary.LittleEndian.AppendUint64(buf, e.offset)
		buf = binary.LittleEndian.AppendUint64(buf, e.size)
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))
	return buf
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], x)
	return append(dst, buf[:n]...)
}
