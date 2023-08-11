package localstore

import (
	"bytes"
	"sync"
	"time"

	"github.com/johnjamespj/BureauDB/pkg/util"
)

type BucketConfig struct {
	LsmConfig   *LSMTreeConfig
	TaskManager util.TaskManager
}

type Bucket struct {
	tree               *LSMTree
	lock               sync.Mutex
	sequenceNumCounter int64
}

func NewBucket(config *BucketConfig) (*Bucket, error) {
	tree, err := NewLSMTree(config.LsmConfig)
	if err != nil {
		return nil, err
	}
	return &Bucket{tree: tree}, nil
}

func (b *Bucket) Put(sequenceNum int64, key DataSlice, sortKey DataSlice, value DataSlice) {
	b.append(Put, sequenceNum, key, sortKey, value)
}

func (b *Bucket) Delete(sequenceNum int64, key DataSlice, sortKey DataSlice) {
	b.append(Delete, sequenceNum, key, sortKey, nil)
}

func (b *Bucket) Close() error {
	return b.tree.Close()
}

func (b *Bucket) append(rowType RowType, sequenceNum int64, key DataSlice, sortKey DataSlice, value DataSlice) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.sequenceNumCounter < sequenceNum {
		b.sequenceNumCounter = sequenceNum
	} else {
		b.sequenceNumCounter++
	}

	row := &Row{
		Sequence:  b.sequenceNumCounter,
		Timestamp: time.Now().UnixNano(),
		KeyHash:   util.HashBytes(key),
		RowHash: util.HashBytes(bytes.Join([][]byte{
			key,
			sortKey,
			value,
		}, []byte{})),
		RowType: rowType,
		Key:     key,
		SortKey: sortKey,
		Value:   value,
	}

	b.tree.GetMemtable().Append(row)

	if int(b.tree.GetMemtable().GetTableSize()) >= b.tree.config.MaxMemtableSize {
		b.tree.SwapMemtable()
	}
}
