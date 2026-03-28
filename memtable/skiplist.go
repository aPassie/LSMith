// Package memtable implements an in-memory sorted buffer for recent writes.

package memtable

import (
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/shanks/lsmith/internal/key"
)

const (
	maxHeight    = 12    // Maximum skip list height. 12 levels supports ~4^12 = 16M entries efficiently.
	branchFactor = 4     // 1/4 probability of increasing height. Higher = flatter list = less memory.
)

type node struct {
	key   key.InternalKey
	value []byte

	tower [maxHeight]atomic.Pointer[node]

	height int
}

type SkipList struct {
	head   *node
	height atomic.Int32
	size   atomic.Int64
	count  atomic.Int64
	rng    *rand.Rand
	mu     sync.Mutex
}

func NewSkipList() *SkipList {
	sl := &SkipList{
		head: &node{height: maxHeight},
		rng:  rand.New(rand.NewSource(0xBEDC0DE)),
	}
	sl.height.Store(1)
	return sl
}

func (sl *SkipList) randomHeight() int {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	h := 1
	for h < maxHeight && sl.rng.Intn(branchFactor) == 0 {
		h++
	}
	return h
}

func (sl *SkipList) Put(ikey key.InternalKey, value []byte) {
	var prev [maxHeight]*node
	x := sl.head
	for i := int(sl.height.Load()) - 1; i >= 0; i-- {
		for {
			next := x.tower[i].Load()
			if next == nil || key.Compare(ikey, next.key) <= 0 {
				break
			}
			x = next
		}
		prev[i] = x
	}

	next := x.tower[0].Load()
	if next != nil && key.Compare(ikey, next.key) == 0 {
		next.value = value
		return
	}

	h := sl.randomHeight()
	nd := &node{
		key:    ikey,
		value:  make([]byte, len(value)),
		height: h,
	}
	copy(nd.value, value)

	listHeight := int(sl.height.Load())
	if h > listHeight {
		for i := listHeight; i < h; i++ {
			prev[i] = sl.head
		}
		sl.height.Store(int32(h))
	}

	for i := 0; i < h; i++ {
		nd.tower[i].Store(prev[i].tower[i].Load())
		prev[i].tower[i].Store(nd)
	}

	sl.count.Add(1)
	sl.size.Add(int64(len(ikey) + len(value) + 128))
}

func (sl *SkipList) Get(userKey []byte, seq uint64) ([]byte, key.ValueType, bool) {
	lookupKey := key.MakeInternalKey(userKey, seq, key.TypeValue)

	x := sl.head
	for i := int(sl.height.Load()) - 1; i >= 0; i-- {
		for {
			next := x.tower[i].Load()
			if next == nil || key.Compare(lookupKey, next.key) <= 0 {
				break
			}
			x = next
		}
	}

	candidate := x.tower[0].Load()
	if candidate == nil {
		return nil, 0, false
	}

	parsed := key.Parse(candidate.key)
	if key.UserKeyCompare(parsed.UserKey, userKey) != 0 {
		return nil, 0, false
	}

	return candidate.value, parsed.Kind, true
}

type Iterator struct {
	list    *SkipList
	current *node
}

func (sl *SkipList) NewIterator() *Iterator {
	return &Iterator{list: sl, current: sl.head}
}

func (it *Iterator) Seek(target key.InternalKey) {
	x := it.list.head
	for i := int(it.list.height.Load()) - 1; i >= 0; i-- {
		for {
			next := x.tower[i].Load()
			if next == nil || key.Compare(target, next.key) <= 0 {
				break
			}
			x = next
		}
	}
	it.current = x
}

func (it *Iterator) Next() bool {
	next := it.current.tower[0].Load()
	if next == nil {
		it.current = nil
		return false
	}
	it.current = next
	return true
}

func (it *Iterator) Valid() bool {
	return it.current != nil && it.current != it.list.head
}

func (it *Iterator) Key() key.InternalKey {
	return it.current.key
}

func (it *Iterator) Value() []byte {
	return it.current.value
}

func (sl *SkipList) Count() int64 {
	return sl.count.Load()
}

func (sl *SkipList) ApproxSize() int64 {
	return sl.size.Load()
}
