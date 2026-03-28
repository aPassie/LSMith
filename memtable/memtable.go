// Package memtable wraps the skip list with the Memtable API.

package memtable

import (
	"github.com/shanks/lsmith/internal/key"
)

// Memtable is an in-memory sorted buffer backed by a skip list.
type Memtable struct {
	list      *SkipList
	sizeLimit int64
}

func New(sizeLimit int64) *Memtable {
	return &Memtable{
		list:      NewSkipList(),
		sizeLimit: sizeLimit,
	}
}

func (m *Memtable) Put(userKey []byte, seq uint64, value []byte) {
	ikey := key.MakeInternalKey(userKey, seq, key.TypeValue)
	m.list.Put(ikey, value)
}

func (m *Memtable) Delete(userKey []byte, seq uint64) {
	ikey := key.MakeInternalKey(userKey, seq, key.TypeDeletion)
	m.list.Put(ikey, nil)
}

func (m *Memtable) Get(userKey []byte, seq uint64) ([]byte, bool) {
	val, kind, found := m.list.Get(userKey, seq)
	if !found {
		return nil, false
	}
	if kind == key.TypeDeletion {
		return nil, true
	}
	return val, true
}

func (m *Memtable) ShouldFlush() bool {
	return m.list.ApproxSize() >= m.sizeLimit
}

func (m *Memtable) ApproxSize() int64 {
	return m.list.ApproxSize()
}

func (m *Memtable) Count() int64 {
	return m.list.Count()
}


func (m *Memtable) NewIterator() *Iterator {
	return m.list.NewIterator()
}
