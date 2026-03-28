// Package key defines the internal key format used throughout the storage engine
package key

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type ValueType byte

const (
	TypeDeletion ValueType = 0
	TypeValue    ValueType = 1
)

// We use 7 bytes for sequence (56 bits) and 1 byte for type, packed into a uint64 trailer
const MaxSequenceNumber uint64 = (1 << 56) - 1


type InternalKey []byte

func MakeInternalKey(userKey []byte, seq uint64, vt ValueType) InternalKey {
	if seq > MaxSequenceNumber {
		panic(fmt.Sprintf("sequence number %d exceeds max %d", seq, MaxSequenceNumber))
	}
	ik := make([]byte, len(userKey)+8)
	copy(ik, userKey)
	trailer := (seq << 8) | uint64(vt)
	binary.LittleEndian.PutUint64(ik[len(userKey):], trailer)
	return InternalKey(ik)
}

func (ik InternalKey) UserKey() []byte {
	if len(ik) < 8 {
		return nil
	}
	return ik[:len(ik)-8]
}

func (ik InternalKey) Trailer() uint64 {
	if len(ik) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(ik[len(ik)-8:])
}

func (ik InternalKey) Sequence() uint64 {
	return ik.Trailer() >> 8
}

func (ik InternalKey) Kind() ValueType {
	return ValueType(ik.Trailer() & 0xff)
}


func Compare(a, b InternalKey) int {
	ua, ub := a.UserKey(), b.UserKey()
	if c := bytes.Compare(ua, ub); c != 0 {
		return c
	}

	ta, tb := a.Trailer(), b.Trailer()
	if ta > tb {
		return -1
	}
	if ta < tb {
		return +1
	}
	return 0
}

func UserKeyCompare(a, b []byte) int {
	return bytes.Compare(a, b)
}

type ParsedInternalKey struct {
	UserKey  []byte
	Sequence uint64
	Kind     ValueType
}

func Parse(ik InternalKey) ParsedInternalKey {
	return ParsedInternalKey{
		UserKey:  ik.UserKey(),
		Sequence: ik.Sequence(),
		Kind:     ik.Kind(),
	}
}

func (p ParsedInternalKey) String() string {
	kindStr := "PUT"
	if p.Kind == TypeDeletion {
		kindStr = "DEL"
	}
	return fmt.Sprintf("%q@%d(%s)", p.UserKey, p.Sequence, kindStr)
}
