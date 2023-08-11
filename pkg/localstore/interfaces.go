package localstore

import "github.com/johnjamespj/BureauDB/pkg/iterator"

type NavigableTable interface {
	Head(partitionKeyHash DataSlice, sortKey DataSlice) iterator.Iterable[*Row]

	Tail(partitionKeyHash DataSlice, sortKey DataSlice) iterator.Iterable[*Row]

	Size() int64

	IsInRange(partitionKeyHash DataSlice, sortKey DataSlice, sequenceNumber int64) bool

	MightContain(partitionKeyHash DataSlice, sortKey DataSlice) bool
}
