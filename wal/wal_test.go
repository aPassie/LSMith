package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func TestWriteAndReadBack(t *testing.T) {
	path := tempPath(t)

	w, err := NewWriter(path, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}

	w.Append(RecordPut, []byte("name"), []byte("alice"))
	w.Append(RecordPut, []byte("age"), []byte("30"))
	w.Append(RecordDelete, []byte("name"), nil)
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	if records[0].Type != RecordPut {
		t.Errorf("record 0: expected PUT, got %d", records[0].Type)
	}
	if string(records[0].Key) != "name" || string(records[0].Value) != "alice" {
		t.Errorf("record 0: expected name=alice, got %s=%s", records[0].Key, records[0].Value)
	}

	if string(records[1].Key) != "age" || string(records[1].Value) != "30" {
		t.Errorf("record 1: expected age=30, got %s=%s", records[1].Key, records[1].Value)
	}

	if records[2].Type != RecordDelete {
		t.Errorf("record 2: expected DELETE, got %d", records[2].Type)
	}
	if string(records[2].Key) != "name" {
		t.Errorf("record 2: expected key 'name', got %s", records[2].Key)
	}
	if records[2].Value != nil {
		t.Errorf("record 2: expected nil value for DELETE, got %v", records[2].Value)
	}
}

func TestEmptyWAL(t *testing.T) {
	path := tempPath(t)

	w, err := NewWriter(path, NoSync)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records from empty WAL, got %d", len(records))
	}
}

func TestTornWrite_TruncatedHeader(t *testing.T) {
	path := tempPath(t)

	w, err := NewWriter(path, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(RecordPut, []byte("key1"), []byte("val1"))
	w.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	f.Write([]byte{0x01, 0x02, 0x03})
	f.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 valid record, got %d", len(records))
	}
	if string(records[0].Key) != "key1" {
		t.Errorf("expected key1, got %s", records[0].Key)
	}
}

func TestTornWrite_TruncatedPayload(t *testing.T) {
	path := tempPath(t)

	w, err := NewWriter(path, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(RecordPut, []byte("key1"), []byte("val1"))
	w.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], 0xDEADBEEF)
	binary.LittleEndian.PutUint32(header[4:8], 50)
	header[8] = byte(RecordPut)
	f.Write(header)
	f.Write([]byte{0x01, 0x02})
	f.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 valid record, got %d", len(records))
	}
}

func TestCorruptedCRC(t *testing.T) {
	path := tempPath(t)

	w, err := NewWriter(path, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(RecordPut, []byte("key1"), []byte("val1"))
	w.Append(RecordPut, []byte("key2"), []byte("val2"))
	w.Close()

	data, _ := os.ReadFile(path)
	firstRecordSize := headerSize + 4 + len("key1") + 4 + len("val1")
	data[firstRecordSize] ^= 0xFF
	os.WriteFile(path, data, 0644)

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 valid record (second corrupted), got %d", len(records))
	}
	if string(records[0].Key) != "key1" {
		t.Errorf("expected key1, got %s", records[0].Key)
	}
}

func TestLargeValues(t *testing.T) {
	path := tempPath(t)

	bigKey := make([]byte, 1024)
	bigVal := make([]byte, 1<<20)
	for i := range bigKey {
		bigKey[i] = byte(i % 256)
	}
	for i := range bigVal {
		bigVal[i] = byte(i % 256)
	}

	w, err := NewWriter(path, NoSync)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(RecordPut, bigKey, bigVal)
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	if len(records[0].Key) != 1024 {
		t.Errorf("expected key length 1024, got %d", len(records[0].Key))
	}
	if len(records[0].Value) != 1<<20 {
		t.Errorf("expected value length %d, got %d", 1<<20, len(records[0].Value))
	}

	for i := range bigKey {
		if records[0].Key[i] != bigKey[i] {
			t.Fatalf("key mismatch at byte %d", i)
		}
	}
	for i := range bigVal {
		if records[0].Value[i] != bigVal[i] {
			t.Fatalf("value mismatch at byte %d", i)
		}
	}
}

func TestMultipleRecords_ManyWrites(t *testing.T) {
	path := tempPath(t)

	w, err := NewWriter(path, NoSync)
	if err != nil {
		t.Fatal(err)
	}

	n := 10000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("val-%06d", i))
		if _, err := w.Append(RecordPut, key, val); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}
	w.Close()

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != n {
		t.Fatalf("expected %d records, got %d", n, len(records))
	}

	if string(records[0].Key) != "key-000000" {
		t.Errorf("first key: %s", records[0].Key)
	}
	if string(records[n-1].Key) != fmt.Sprintf("key-%06d", n-1) {
		t.Errorf("last key: %s", records[n-1].Key)
	}
}
