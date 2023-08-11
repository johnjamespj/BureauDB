package localstore

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack"
)

// These are not legal sort keys, but are used to represent the largest and smallest possible sort keys.
var (
	LargestSortKey  = []byte{0x0}
	SmallestSortKey = []byte{}
)

type RowType int

const (
	Put RowType = iota
	Delete
	Merge
)

type Row struct {
	Sequence  int64
	Timestamp int64
	KeyHash   DataSlice
	RowHash   DataSlice
	RowType   RowType

	Key     DataSlice
	SortKey DataSlice
	Value   DataSlice
}

func RowFromBytes(b []byte) (*Row, error) {
	reader := bytes.NewReader(b)
	dec := msgpack.NewDecoder(reader)

	var keyHash DataSlice
	var rowHash DataSlice
	var key DataSlice
	var sortKey DataSlice
	var value DataSlice

	sequence, err := dec.DecodeInt64()
	if err != nil {
		return nil, err
	}

	timestamp, err := dec.DecodeInt64()
	if err != nil {
		return nil, err
	}

	keyHash, err = dec.DecodeBytes()
	if err != nil {
		return nil, err
	}

	rowHash, err = dec.DecodeBytes()
	if err != nil {
		return nil, err
	}

	key, err = dec.DecodeBytes()
	if err != nil {
		return nil, err
	}

	sortKey, err = dec.DecodeBytes()
	if err != nil {
		return nil, err
	}

	value, err = dec.DecodeBytes()
	if err != nil {
		return nil, err
	}

	return &Row{
		Sequence:  sequence,
		Timestamp: timestamp,
		KeyHash:   keyHash,
		RowHash:   rowHash,
		Key:       key,
		SortKey:   sortKey,
		Value:     value,
	}, nil
}

func (r *Row) Size() int {
	return len(r.Key) + len(r.SortKey) + len(r.Value) + 2*8 + 2*16
}

func (r *Row) ToBytes() []byte {
	var buf bytes.Buffer
	writer := io.Writer(&buf)
	enc := msgpack.NewEncoder(writer)

	enc.EncodeInt64(r.Sequence)
	enc.EncodeInt64(r.Timestamp)
	enc.EncodeBytes(r.KeyHash)
	enc.EncodeBytes(r.RowHash)
	enc.EncodeBytes(r.Key)
	enc.EncodeBytes(r.SortKey)
	enc.EncodeBytes(r.Value)

	return buf.Bytes()
}

func (r *Row) ToString() string {
	return fmt.Sprintf("Row{Sequence: %d, Timestamp: %d, KeyHash: %s, RowHash: %s, size: %d, pk: %s, sk: %s}", r.Sequence, r.Timestamp, hex.EncodeToString(r.KeyHash), hex.EncodeToString(r.RowHash), r.Size(), r.Key, r.SortKey)
}

func (r *Row) CompareTo(other *Row) int {
	keyCmp := bytes.Compare(r.KeyHash, other.KeyHash)
	if keyCmp != 0 {
		return keyCmp
	}

	if bytes.Equal(other.SortKey, LargestSortKey) {
		return -1
	} else if bytes.Equal(other.SortKey, SmallestSortKey) {
		return 1
	}

	skCmp := bytes.Compare(r.SortKey, other.SortKey)
	if skCmp != 0 {
		return skCmp
	}

	return int(other.Sequence) - int(r.Sequence)
}

type DataSlice []byte

func (d *DataSlice) Hash() DataSlice {
	hash := md5.Sum(*d)
	return hash[:]
}
