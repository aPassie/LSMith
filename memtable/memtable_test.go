package memtable

import (
	"fmt"
	"testing"

	"github.com/shanks/lsmith/internal/key"
)

func TestMemtable_PutAndGet(t *testing.T) {
	m := New(4 * 1024 * 1024) // 4MB

	m.Put([]byte("name"), 1, []byte("alice"))
	m.Put([]byte("age"), 2, []byte("30"))

	val, found := m.Get([]byte("name"), 2)
	if !found || string(val) != "alice" {
		t.Errorf("expected 'alice', got %q (found=%v)", val, found)
	}

	val, found = m.Get([]byte("age"), 2)
	if !found || string(val) != "30" {
		t.Errorf("expected '30', got %q (found=%v)", val, found)
	}
}

func TestMemtable_GetNotFound(t *testing.T) {
	m := New(4 * 1024 * 1024)

	m.Put([]byte("exists"), 1, []byte("yes"))

	_, found := m.Get([]byte("missing"), 1)
	if found {
		t.Error("expected not found for missing key")
	}
}

func TestMemtable_Delete(t *testing.T) {
	m := New(4 * 1024 * 1024)

	m.Put([]byte("key"), 1, []byte("value"))
	m.Delete([]byte("key"), 2)

	val, found := m.Get([]byte("key"), 2)
	if !found {
		t.Error("expected found=true (tombstone should be found to stop further lookup)")
	}
	if val != nil {
		t.Errorf("expected nil value for tombstone, got %q", val)
	}

	val, found = m.Get([]byte("key"), 1)
	if !found || string(val) != "value" {
		t.Errorf("expected 'value' at seq 1, got %q (found=%v)", val, found)
	}
}

func TestMemtable_MVCC_SequenceVisibility(t *testing.T) {
	m := New(4 * 1024 * 1024)

	m.Put([]byte("key"), 1, []byte("v1"))
	m.Put([]byte("key"), 5, []byte("v5"))
	m.Put([]byte("key"), 10, []byte("v10"))

	val, found := m.Get([]byte("key"), 10)
	if !found || string(val) != "v10" {
		t.Errorf("seq 10: expected 'v10', got %q", val)
	}

	val, found = m.Get([]byte("key"), 7)
	if !found || string(val) != "v5" {
		t.Errorf("seq 7: expected 'v5', got %q", val)
	}

	val, found = m.Get([]byte("key"), 3)
	if !found || string(val) != "v1" {
		t.Errorf("seq 3: expected 'v1', got %q", val)
	}

	_, found = m.Get([]byte("key"), 0)
	if found {
		t.Error("seq 0: expected not found")
	}
}

func TestMemtable_OverwriteSameSequence(t *testing.T) {
	m := New(4 * 1024 * 1024)

	m.Put([]byte("key"), 5, []byte("first"))
	m.Put([]byte("key"), 5, []byte("second"))

	val, found := m.Get([]byte("key"), 5)
	if !found || string(val) != "second" {
		t.Errorf("expected 'second', got %q", val)
	}
}

func TestMemtable_ShouldFlush(t *testing.T) {
	m := New(1024)

	if m.ShouldFlush() {
		t.Error("empty memtable should not need flushing")
	}

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%05d", i)
		v := fmt.Sprintf("value-%05d", i)
		m.Put([]byte(k), uint64(i+1), []byte(v))
	}

	if !m.ShouldFlush() {
		t.Errorf("memtable with %d bytes should need flushing (limit=1024)", m.ApproxSize())
	}
}

func TestMemtable_Iterator(t *testing.T) {
	m := New(4 * 1024 * 1024)

	m.Put([]byte("cherry"), 1, []byte("c"))
	m.Put([]byte("apple"), 2, []byte("a"))
	m.Put([]byte("banana"), 3, []byte("b"))

	it := m.NewIterator()

	var keys []string
	for it.Next() {
		p := key.Parse(it.Key())
		keys = append(keys, string(p.UserKey))
	}

	expected := []string{"apple", "banana", "cherry"}
	if len(keys) != len(expected) {
		t.Fatalf("expected %d entries, got %d: %v", len(expected), len(keys), keys)
	}
	for i, k := range keys {
		if k != expected[i] {
			t.Errorf("position %d: expected %q, got %q", i, expected[i], k)
		}
	}
}

func TestMemtable_Iterator_MultipleVersions(t *testing.T) {
	m := New(4 * 1024 * 1024)

	m.Put([]byte("key"), 1, []byte("v1"))
	m.Put([]byte("key"), 3, []byte("v3"))
	m.Put([]byte("key"), 2, []byte("v2"))

	it := m.NewIterator()

	var seqs []uint64
	for it.Next() {
		p := key.Parse(it.Key())
		seqs = append(seqs, p.Sequence)
	}

	if len(seqs) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(seqs))
	}
	if seqs[0] != 3 || seqs[1] != 2 || seqs[2] != 1 {
		t.Errorf("expected sequences [3,2,1], got %v", seqs)
	}
}

func TestMemtable_Iterator_Seek(t *testing.T) {
	m := New(4 * 1024 * 1024)

	m.Put([]byte("a"), 1, []byte("1"))
	m.Put([]byte("c"), 2, []byte("2"))
	m.Put([]byte("e"), 3, []byte("3"))
	m.Put([]byte("g"), 4, []byte("4"))

	it := m.NewIterator()

	seekKey := key.MakeInternalKey([]byte("c"), key.MaxSequenceNumber, key.TypeValue)
	it.Seek(seekKey)
	if !it.Next() {
		t.Fatal("expected entry after seek")
	}

	p := key.Parse(it.Key())
	if string(p.UserKey) != "c" {
		t.Errorf("expected 'c' after seek, got %q", p.UserKey)
	}

	seekKey = key.MakeInternalKey([]byte("d"), key.MaxSequenceNumber, key.TypeValue)
	it.Seek(seekKey)
	if !it.Next() {
		t.Fatal("expected entry after seek to 'd'")
	}

	p = key.Parse(it.Key())
	if string(p.UserKey) != "e" {
		t.Errorf("expected 'e' after seek to 'd', got %q", p.UserKey)
	}
}

func TestSkipList_ConcurrentReads(t *testing.T) {
	sl := NewSkipList()

	for i := 0; i < 1000; i++ {
		k := key.MakeInternalKey([]byte(fmt.Sprintf("key-%05d", i)), uint64(i+1), key.TypeValue)
		sl.Put(k, []byte(fmt.Sprintf("val-%05d", i)))
	}

	done := make(chan bool, 10)
	for g := 0; g < 10; g++ {
		go func() {
			for i := 0; i < 1000; i++ {
				_, _, _ = sl.Get([]byte(fmt.Sprintf("key-%05d", i)), uint64(i+1))
			}
			done <- true
		}()
	}
	for g := 0; g < 10; g++ {
		<-done
	}
}
