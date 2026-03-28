package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/shanks/lsmith/internal/key"
)

func sstPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.sst")
}

func TestWriteAndRead_Basic(t *testing.T) {
	path := sstPath(t)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	entries := []struct {
		key string
		seq uint64
		val string
	}{
		{"apple", 1, "red"},
		{"banana", 2, "yellow"},
		{"cherry", 3, "dark red"},
		{"date", 4, "brown"},
		{"elderberry", 5, "purple"},
	}

	for _, e := range entries {
		ik := key.MakeInternalKey([]byte(e.key), e.seq, key.TypeValue)
		if err := w.Add(ik, []byte(e.val)); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, e := range entries {
		val, kind, found := r.Get([]byte(e.key), e.seq)
		if !found {
			t.Errorf("key %q not found", e.key)
			continue
		}
		if kind != key.TypeValue {
			t.Errorf("key %q: expected PUT, got %d", e.key, kind)
		}
		if string(val) != e.val {
			t.Errorf("key %q: expected %q, got %q", e.key, e.val, val)
		}
	}

	_, _, found := r.Get([]byte("fig"), 10)
	if found {
		t.Error("expected 'fig' not found")
	}
}

func TestWriteAndRead_MultipleVersions(t *testing.T) {
	path := sstPath(t)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	w.Add(key.MakeInternalKey([]byte("key"), 10, key.TypeValue), []byte("v10"))
	w.Add(key.MakeInternalKey([]byte("key"), 5, key.TypeValue), []byte("v5"))
	w.Add(key.MakeInternalKey([]byte("key"), 1, key.TypeValue), []byte("v1"))
	w.Finish()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	val, _, found := r.Get([]byte("key"), 10)
	if !found || string(val) != "v10" {
		t.Errorf("seq 10: expected v10, got %q (found=%v)", val, found)
	}

	val, _, found = r.Get([]byte("key"), 7)
	if !found || string(val) != "v5" {
		t.Errorf("seq 7: expected v5, got %q (found=%v)", val, found)
	}

	val, _, found = r.Get([]byte("key"), 3)
	if !found || string(val) != "v1" {
		t.Errorf("seq 3: expected v1, got %q (found=%v)", val, found)
	}

	_, _, found = r.Get([]byte("key"), 0)
	if found {
		t.Error("seq 0: expected not found")
	}
}

func TestWriteAndRead_Tombstone(t *testing.T) {
	path := sstPath(t)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	w.Add(key.MakeInternalKey([]byte("key"), 5, key.TypeDeletion), nil)
	w.Add(key.MakeInternalKey([]byte("key"), 1, key.TypeValue), []byte("old"))
	w.Finish()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	val, kind, found := r.Get([]byte("key"), 5)
	if !found {
		t.Fatal("expected to find tombstone")
	}
	if kind != key.TypeDeletion {
		t.Errorf("expected DELETE, got %d", kind)
	}
	if len(val) != 0 {
		t.Errorf("expected empty value for tombstone, got %q", val)
	}

	val, kind, found = r.Get([]byte("key"), 3)
	if !found || kind != key.TypeValue || string(val) != "old" {
		t.Errorf("seq 3: expected 'old', got %q (kind=%d, found=%v)", val, kind, found)
	}
}

func TestWriteAndRead_ManyEntries(t *testing.T) {
	path := sstPath(t)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	n := 10000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("val-%06d", i)
		ik := key.MakeInternalKey([]byte(k), uint64(i+1), key.TypeValue)
		w.Add(ik, []byte(v))
	}
	w.Finish()

	if w.EntryCount() != uint64(n) {
		t.Errorf("expected %d entries, got %d", n, w.EntryCount())
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.IndexEntryCount() < 2 {
		t.Errorf("expected multiple blocks, got %d", r.IndexEntryCount())
	}

	for _, i := range []int{0, 100, 999, 5000, 9999} {
		k := fmt.Sprintf("key-%06d", i)
		v := fmt.Sprintf("val-%06d", i)
		val, _, found := r.Get([]byte(k), uint64(i+1))
		if !found {
			t.Errorf("key %q not found", k)
			continue
		}
		if string(val) != v {
			t.Errorf("key %q: expected %q, got %q", k, v, val)
		}
	}

	_, _, found := r.Get([]byte("key-999999"), 100000)
	if found {
		t.Error("expected missing key not found")
	}
}

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	path := sstPath(t)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	n := 1000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("bloom-key-%06d", i)
		ik := key.MakeInternalKey([]byte(k), uint64(i+1), key.TypeValue)
		w.Add(ik, []byte("v"))
	}
	w.Finish()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("bloom-key-%06d", i)
		if !bloomCheck(r.BloomFilter(), []byte(k)) {
			t.Errorf("bloom filter false negative for key %q", k)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("present-%06d", i))
	}

	filter := buildBloomFilter(keys, bloomBitsPerKey)

	falsePositives := 0
	checks := 10000
	for i := 0; i < checks; i++ {
		absent := []byte(fmt.Sprintf("absent-%06d", i))
		if bloomCheck(filter, absent) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(checks)
	t.Logf("Bloom filter FP rate: %.2f%% (%d/%d)", fpRate*100, falsePositives, checks)

	if fpRate > 0.03 {
		t.Errorf("FP rate %.2f%% exceeds 3%% threshold", fpRate*100)
	}
}

func TestTableIterator(t *testing.T) {
	path := sstPath(t)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	n := 500
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("iter-%06d", i)
		v := fmt.Sprintf("val-%06d", i)
		ik := key.MakeInternalKey([]byte(k), uint64(i+1), key.TypeValue)
		w.Add(ik, []byte(v))
	}
	w.Finish()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()
	count := 0
	var lastKey key.InternalKey
	for it.Next() {
		if lastKey != nil {
			if key.Compare(lastKey, it.Key()) >= 0 {
				t.Fatalf("keys not in sorted order at entry %d", count)
			}
		}
		lastKey = append(key.InternalKey{}, it.Key()...)
		count++
	}

	if count != n {
		t.Errorf("expected %d entries, got %d", n, count)
	}
}

func TestSmallestLargestKey(t *testing.T) {
	path := sstPath(t)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	w.Add(key.MakeInternalKey([]byte("aaa"), 3, key.TypeValue), []byte("1"))
	w.Add(key.MakeInternalKey([]byte("mmm"), 2, key.TypeValue), []byte("2"))
	w.Add(key.MakeInternalKey([]byte("zzz"), 1, key.TypeValue), []byte("3"))
	w.Finish()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if string(r.SmallestKey.UserKey()) != "aaa" {
		t.Errorf("smallest key: expected 'aaa', got %q", r.SmallestKey.UserKey())
	}
	if string(r.LargestKey.UserKey()) != "zzz" {
		t.Errorf("largest key: expected 'zzz', got %q", r.LargestKey.UserKey())
	}
}
