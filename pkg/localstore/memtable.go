package localstore

import (
	"bytes"

	"github.com/johnjamespj/BureauDB/pkg/iterator"
	"github.com/johnjamespj/BureauDB/pkg/util"
)

type Memtable struct {
	wal       *WAL
	list      *Skiplist[*Row]
	tableSize int64

	minSequence         int64
	maxPartitionKeyHash DataSlice
	minPartitionKeyHash DataSlice
	maxSortKey          DataSlice
	minSortKey          DataSlice
	size                int64
	filter              *util.Bloomfilter
}

func NewMemtable(wal *WAL) *Memtable {
	return &Memtable{
		wal:       wal,
		list:      NewSkiplist[*Row](100000),
		size:      0,
		tableSize: 0,
		filter:    util.NewBloomfilter(100000, 12),
	}
}

func (m *Memtable) Size() int64 {
	return m.size
}

func (m *Memtable) Add(row *Row) {
	rowCopy := *row
	rowCopy.RowType = Put
	m.wal.Write(&rowCopy)
	m.list.Put(&rowCopy)
	m.size += 1
	m.tableSize += int64(rowCopy.Size())

	m.filter.Add(row.KeyHash)
	m.filter.Add(bytes.Join([][]byte{
		row.KeyHash,
		row.SortKey,
	}, []byte{}))
}

func (m *Memtable) Head(partitionKeyHash DataSlice, sortKey DataSlice) iterator.Iterable[*Row] {
	row := &Row{
		Sequence: 0x7fffffffffffffff,
		KeyHash:  partitionKeyHash,
		SortKey:  sortKey,
	}

	return m.list.Search(true, func(v *Row) bool {
		return v.CompareTo(row) >= 0
	})
}

func (m *Memtable) Tail(partitionKeyHash DataSlice, sortKey DataSlice) iterator.Iterable[*Row] {
	row := &Row{
		Sequence: 0x7fffffffffffffff,
		KeyHash:  partitionKeyHash,
		SortKey:  sortKey,
	}

	return m.list.Search(false, func(v *Row) bool {
		return v.CompareTo(row) >= 0
	}).Skip(1)
}

func (m *Memtable) IsInRange(partitionKeyHash DataSlice, sortKey DataSlice, sequenceNumber int64) bool {
	if m.minSequence < sequenceNumber {
		return false
	}

	if bytes.Compare(m.minPartitionKeyHash, partitionKeyHash) < 0 ||
		bytes.Compare(m.maxPartitionKeyHash, partitionKeyHash) > 0 {
		return false
	}

	if bytes.Equal(m.maxPartitionKeyHash, partitionKeyHash) &&
		bytes.Compare(m.maxSortKey, sortKey) < 0 {
		return false
	}

	if bytes.Equal(m.minPartitionKeyHash, partitionKeyHash) &&
		bytes.Compare(m.minSortKey, sortKey) > 0 {
		return false
	}

	return true
}

func (m *Memtable) MightContain(partitionKeyHash DataSlice, sortKey DataSlice) bool {
	return m.filter.Contains(bytes.Join([][]byte{
		partitionKeyHash,
		sortKey,
	}, []byte{}))
}
