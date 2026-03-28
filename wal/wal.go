// Package wal implements a Write-Ahead Log for crash recovery.

package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type RecordType byte

const (
	RecordPut    RecordType = 1
	RecordDelete RecordType = 2
)

type SyncMode int

const (
	SyncEveryWrite SyncMode = iota
	NoSync
)

const headerSize = 9

type Writer struct {
	f        *os.File
	syncMode SyncMode
	mu       sync.Mutex
}

func NewWriter(path string, mode SyncMode) (*Writer, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &Writer{f: f, syncMode: mode}, nil
}


func (w *Writer) Append(rt RecordType, key, value []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var payload []byte
	switch rt {
	case RecordPut:
		payload = make([]byte, 4+len(key)+4+len(value))
		binary.LittleEndian.PutUint32(payload[0:4], uint32(len(key)))
		copy(payload[4:4+len(key)], key)
		binary.LittleEndian.PutUint32(payload[4+len(key):8+len(key)], uint32(len(value)))
		copy(payload[8+len(key):], value)
	case RecordDelete:
		payload = make([]byte, 4+len(key))
		binary.LittleEndian.PutUint32(payload[0:4], uint32(len(key)))
		copy(payload[4:], key)
	}


	crcData := make([]byte, 1+len(payload))
	crcData[0] = byte(rt)
	copy(crcData[1:], payload)
	checksum := crc32.ChecksumIEEE(crcData)

	length := uint32(1 + len(payload))

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint32(header[4:8], length)
	header[8] = byte(rt)

	record := append(header, payload...)
	n, err := w.f.Write(record)
	if err != nil {
		return n, err
	}

	if w.syncMode == SyncEveryWrite {
		if err := w.f.Sync(); err != nil {
			return n, err
		}
	}

	return n, nil
}

func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

func (w *Writer) Path() string {
	return w.f.Name()
}

type Record struct {
	Type  RecordType
	Key   []byte
	Value []byte
}

type Reader struct {
	f *os.File
}

func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{f: f}, nil
}

func (r *Reader) ReadAll() ([]Record, error) {
	var records []Record

	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(r.f, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return records, err
		}

		storedCRC := binary.LittleEndian.Uint32(header[0:4])
		length := binary.LittleEndian.Uint32(header[4:8])
		recordType := RecordType(header[8])

		payloadLen := int(length) - 1
		if payloadLen < 0 {
			break
		}

		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			_, err = io.ReadFull(r.f, payload)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			if err != nil {
				return records, err
			}
		}

		crcData := make([]byte, 1+payloadLen)
		crcData[0] = byte(recordType)
		copy(crcData[1:], payload)
		if crc32.ChecksumIEEE(crcData) != storedCRC {
			break
		}

		rec := Record{Type: recordType}
		switch recordType {
		case RecordPut:
			if len(payload) < 4 {
				break
			}
			keyLen := binary.LittleEndian.Uint32(payload[0:4])
			if len(payload) < int(4+keyLen+4) {
				break
			}
			rec.Key = make([]byte, keyLen)
			copy(rec.Key, payload[4:4+keyLen])
			valLen := binary.LittleEndian.Uint32(payload[4+keyLen : 8+keyLen])
			rec.Value = make([]byte, valLen)
			copy(rec.Value, payload[8+keyLen:8+keyLen+valLen])

		case RecordDelete:
			if len(payload) < 4 {
				break
			}
			keyLen := binary.LittleEndian.Uint32(payload[0:4])
			rec.Key = make([]byte, keyLen)
			copy(rec.Key, payload[4:4+keyLen])
		}

		records = append(records, rec)
	}

	return records, nil
}

func (r *Reader) Close() error {
	return r.f.Close()
}
