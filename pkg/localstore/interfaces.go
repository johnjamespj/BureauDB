package localstore

import "github.com/johnjamespj/BureauDB/pkg/iterator"

type NavigableTable interface {
	Head(partitionKey DataSlice, sortKey DataSlice) iterator.Iterable[*Row]

	Tail(partitionKey DataSlice, sortKey DataSlice) iterator.Iterable[*Row]

	Size() int64

	IsInRange(partitionKey DataSlice, sortKey DataSlice) bool

	MightContain(partitionKey DataSlice, sortKey DataSlice) bool
}
