package key

import (
	"testing"
)

func TestMakeAndParse(t *testing.T) {
	ik := MakeInternalKey([]byte("hello"), 42, TypeValue)
	p := Parse(ik)

	if string(p.UserKey) != "hello" {
		t.Errorf("expected user key 'hello', got %q", p.UserKey)
	}
	if p.Sequence != 42 {
		t.Errorf("expected sequence 42, got %d", p.Sequence)
	}
	if p.Kind != TypeValue {
		t.Errorf("expected TypeValue, got %d", p.Kind)
	}
}

func TestCompare_DifferentUserKeys(t *testing.T) {
	a := MakeInternalKey([]byte("apple"), 10, TypeValue)
	b := MakeInternalKey([]byte("banana"), 10, TypeValue)

	if c := Compare(a, b); c >= 0 {
		t.Errorf("expected 'apple' < 'banana', got %d", c)
	}
	if c := Compare(b, a); c <= 0 {
		t.Errorf("expected 'banana' > 'apple', got %d", c)
	}
}

func TestCompare_SameUserKey_HigherSequenceFirst(t *testing.T) {
	old := MakeInternalKey([]byte("key"), 10, TypeValue)
	new := MakeInternalKey([]byte("key"), 20, TypeValue)

	if c := Compare(new, old); c >= 0 {
		t.Errorf("expected seq 20 < seq 10 (newer first), got %d", c)
	}
	if c := Compare(old, new); c <= 0 {
		t.Errorf("expected seq 10 > seq 20 (older later), got %d", c)
	}
}

func TestCompare_SameKeyAndSeq_TypeOrdering(t *testing.T) {
	put := MakeInternalKey([]byte("key"), 10, TypeValue)
	del := MakeInternalKey([]byte("key"), 10, TypeDeletion)

	if c := Compare(put, del); c >= 0 {
		t.Errorf("expected PUT < DEL at same seq (PUT first), got %d", c)
	}
}

func TestCompare_Equal(t *testing.T) {
	a := MakeInternalKey([]byte("key"), 10, TypeValue)
	b := MakeInternalKey([]byte("key"), 10, TypeValue)

	if c := Compare(a, b); c != 0 {
		t.Errorf("expected equal, got %d", c)
	}
}

func TestCompare_EmptyUserKey(t *testing.T) {
	a := MakeInternalKey([]byte(""), 5, TypeValue)
	b := MakeInternalKey([]byte("a"), 5, TypeValue)

	if c := Compare(a, b); c >= 0 {
		t.Errorf("expected empty key < 'a', got %d", c)
	}
}

func TestMaxSequenceNumber(t *testing.T) {
	ik := MakeInternalKey([]byte("key"), MaxSequenceNumber, TypeValue)
	p := Parse(ik)

	if p.Sequence != MaxSequenceNumber {
		t.Errorf("expected max sequence %d, got %d", MaxSequenceNumber, p.Sequence)
	}
}

func TestSortOrder_MultipleEntries(t *testing.T) {

	entries := []InternalKey{
		MakeInternalKey([]byte("b"), 10, TypeValue),
		MakeInternalKey([]byte("a"), 5, TypeValue),
		MakeInternalKey([]byte("b"), 20, TypeValue),
		MakeInternalKey([]byte("c"), 1, TypeValue),
		MakeInternalKey([]byte("b"), 15, TypeDeletion),
	}

	expected := []struct {
		key string
		seq uint64
	}{
		{"a", 5},
		{"b", 20},
		{"b", 15},
		{"b", 10},
		{"c", 1},
	}

	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if Compare(entries[i], entries[j]) > 0 {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	for i, ik := range entries {
		p := Parse(ik)
		if string(p.UserKey) != expected[i].key || p.Sequence != expected[i].seq {
			t.Errorf("position %d: expected (%s, seq=%d), got (%s, seq=%d)",
				i, expected[i].key, expected[i].seq, string(p.UserKey), p.Sequence)
		}
	}
}

func TestParsedInternalKey_String(t *testing.T) {
	p := ParsedInternalKey{
		UserKey:  []byte("hello"),
		Sequence: 42,
		Kind:     TypeValue,
	}
	s := p.String()
	if s != `"hello"@42(PUT)` {
		t.Errorf("unexpected string: %s", s)
	}

	p.Kind = TypeDeletion
	s = p.String()
	if s != `"hello"@42(DEL)` {
		t.Errorf("unexpected string: %s", s)
	}
}
