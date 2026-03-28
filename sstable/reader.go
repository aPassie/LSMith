package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"

	"github.com/shanks/lsmith/internal/key"
)

var (
	ErrBadMagic   = errors.New("sstable: invalid magic number")
	ErrBadBlock   = errors.New("sstable: block CRC mismatch")
	ErrNotFound   = errors.New("sstable: key not found")
)


type Reader struct {
	f     *os.File
	size  int64
	index []indexEntry
	bloom []byte

	SmallestKey key.InternalKey
	LargestKey  key.InternalKey
}


func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	r := &Reader{f: f, size: info.Size()}

	if err := r.readFooter(); err != nil {
		f.Close()
		return nil, err
	}

	return r, nil
}

func (r *Reader) readFooter() error {
	if r.size < footerSize {
		return ErrBadMagic
	}

	footer := make([]byte, footerSize)
	_, err := r.f.ReadAt(footer, r.size-footerSize)
	if err != nil {
		return err
	}

	bloomOffset := binary.LittleEndian.Uint64(footer[0:8])
	bloomSize := binary.LittleEndian.Uint64(footer[8:16])
	indexOffset := binary.LittleEndian.Uint64(footer[16:24])
	indexSize := binary.LittleEndian.Uint64(footer[24:32])
	magic := binary.LittleEndian.Uint64(footer[32:40])

	if magic != magicNumber {
		return ErrBadMagic
	}

	r.bloom = make([]byte, bloomSize)
	if _, err := r.f.ReadAt(r.bloom, int64(bloomOffset)); err != nil {
		return err
	}

	indexData := make([]byte, indexSize)
	if _, err := r.f.ReadAt(indexData, int64(indexOffset)); err != nil {
		return err
	}
	r.index, err = decodeIndexBlock(indexData)
	if err != nil {
		return err
	}

	if len(r.index) > 0 {
		firstBlock, err := r.readBlock(r.index[0].offset, r.index[0].size)
		if err != nil {
			return err
		}
		iter := newBlockIterator(firstBlock)
		if iter.Next() {
			r.SmallestKey = append(key.InternalKey{}, iter.Key()...)
		}
		r.LargestKey = append(key.InternalKey{}, r.index[len(r.index)-1].lastKey...)
	}

	return nil
}

func (r *Reader) Get(userKey []byte, seq uint64) ([]byte, key.ValueType, bool) {
	if !bloomCheck(r.bloom, userKey) {
		return nil, 0, false
	}

	lookupKey := key.MakeInternalKey(userKey, seq, key.TypeValue)

	blockIdx := 0
	lo, hi := 0, len(r.index)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if key.Compare(r.index[mid].lastKey, lookupKey) < 0 {
			lo = mid + 1
		} else {
			blockIdx = mid
			hi = mid - 1
		}
	}
	if lo > len(r.index)-1 {
		return nil, 0, false
	}
	blockIdx = lo

	block, err := r.readBlock(r.index[blockIdx].offset, r.index[blockIdx].size)
	if err != nil {
		return nil, 0, false
	}

	iter := newBlockIterator(block)
	iter.Seek(lookupKey)
	if !iter.Valid() {
		return nil, 0, false
	}

	foundKey := iter.Key()
	if key.UserKeyCompare(key.InternalKey(foundKey).UserKey(), userKey) != 0 {
		return nil, 0, false
	}

	if key.InternalKey(foundKey).Sequence() > seq {
		return nil, 0, false
	}

	return iter.Value(), key.InternalKey(foundKey).Kind(), true
}

func (r *Reader) readBlock(offset, size uint64) ([]byte, error) {
	data := make([]byte, size)
	_, err := r.f.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}

	if len(data) < 4 {
		return nil, ErrBadBlock
	}
	payload := data[:len(data)-4]
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(payload) != storedCRC {
		return nil, ErrBadBlock
	}

	return payload, nil
}

func (r *Reader) Close() error {
	return r.f.Close()
}

func (r *Reader) Path() string {
	return r.f.Name()
}

func (r *Reader) BloomFilter() []byte {
	return r.bloom
}

func (r *Reader) IndexEntryCount() int {
	return len(r.index)
}

func decodeIndexBlock(data []byte) ([]indexEntry, error) {
	if len(data) < 4 {
		return nil, errors.New("sstable: index block too short")
	}

	count := binary.LittleEndian.Uint32(data[len(data)-4:])
	data = data[:len(data)-4]

	entries := make([]indexEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(data) < 4 {
			return nil, errors.New("sstable: truncated index entry")
		}
		keyLen := binary.LittleEndian.Uint32(data[0:4])
		data = data[4:]

		if len(data) < int(keyLen)+16 {
			return nil, errors.New("sstable: truncated index entry")
		}
		k := make(key.InternalKey, keyLen)
		copy(k, data[:keyLen])
		data = data[keyLen:]

		offset := binary.LittleEndian.Uint64(data[0:8])
		size := binary.LittleEndian.Uint64(data[8:16])
		data = data[16:]

		entries = append(entries, indexEntry{lastKey: k, offset: offset, size: size})
	}

	return entries, nil
}

type blockIterator struct {
	data     []byte
	restarts []uint32
	nRestart int

	offset     int
	currentKey []byte
	currentVal []byte
	valid      bool
}

func newBlockIterator(data []byte) *blockIterator {
	if len(data) < 4 {
		return &blockIterator{}
	}

	nRestart := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartStart := len(data) - 4 - nRestart*4

	restarts := make([]uint32, nRestart)
	for i := 0; i < nRestart; i++ {
		restarts[i] = binary.LittleEndian.Uint32(data[restartStart+i*4:])
	}

	return &blockIterator{
		data:     data[:restartStart],
		restarts: restarts,
		nRestart: nRestart,
	}
}

func (it *blockIterator) Next() bool {
	if it.offset >= len(it.data) {
		it.valid = false
		return false
	}
	return it.decodeEntry()
}

func (it *blockIterator) Seek(target key.InternalKey) {
	lo, hi := 0, it.nRestart-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		it.offset = int(it.restarts[mid])
		it.currentKey = it.currentKey[:0]
		if !it.decodeEntry() {
			hi = mid - 1
			continue
		}
		if key.Compare(key.InternalKey(it.currentKey), target) < 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	it.offset = int(it.restarts[lo])
	it.currentKey = it.currentKey[:0]
	it.valid = false

	for it.offset < len(it.data) {
		savedOffset := it.offset
		savedKey := append([]byte{}, it.currentKey...)
		if !it.decodeEntry() {
			break
		}
		if key.Compare(key.InternalKey(it.currentKey), target) >= 0 {
			it.valid = true
			return
		}
		_ = savedOffset
		_ = savedKey
	}
	it.valid = false
}

func (it *blockIterator) Valid() bool { return it.valid }

func (it *blockIterator) Key() []byte { return it.currentKey }

func (it *blockIterator) Value() []byte { return it.currentVal }

func (it *blockIterator) decodeEntry() bool {
	if it.offset >= len(it.data) {
		it.valid = false
		return false
	}

	shared, n := binary.Uvarint(it.data[it.offset:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.offset += n

	unshared, n := binary.Uvarint(it.data[it.offset:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.offset += n

	valLen, n := binary.Uvarint(it.data[it.offset:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.offset += n

	if int(shared) > len(it.currentKey) {
		it.valid = false
		return false
	}
	newKey := make([]byte, int(shared)+int(unshared))
	copy(newKey[:shared], it.currentKey[:shared])
	copy(newKey[shared:], it.data[it.offset:it.offset+int(unshared)])
	it.currentKey = newKey
	it.offset += int(unshared)

	it.currentVal = make([]byte, valLen)
	copy(it.currentVal, it.data[it.offset:it.offset+int(valLen)])
	it.offset += int(valLen)

	it.valid = true
	return true
}

type TableIterator struct {
	reader   *Reader
	blockIdx int
	blockIt  *blockIterator
	valid    bool
}

func (r *Reader) NewIterator() *TableIterator {
	return &TableIterator{reader: r, blockIdx: -1}
}

func (it *TableIterator) Next() bool {
	if it.blockIt != nil && it.blockIt.Next() {
		it.valid = true
		return true
	}

	it.blockIdx++
	if it.blockIdx >= len(it.reader.index) {
		it.valid = false
		return false
	}

	entry := it.reader.index[it.blockIdx]
	block, err := it.reader.readBlock(entry.offset, entry.size)
	if err != nil {
		it.valid = false
		return false
	}
	it.blockIt = newBlockIterator(block)

	if it.blockIt.Next() {
		it.valid = true
		return true
	}

	it.valid = false
	return false
}

func (it *TableIterator) Valid() bool { return it.valid }

func (it *TableIterator) Key() key.InternalKey { return key.InternalKey(it.blockIt.Key()) }

func (it *TableIterator) Value() []byte { return it.blockIt.Value() }
