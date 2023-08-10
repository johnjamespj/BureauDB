package iterator

import "github.com/johnjamespj/BureauDB/pkg/util"

func Map[T, U any](it Iterable[T], f func(T) U) Iterable[U] {
	return &BaseIterable[U]{
		builder: func() Iterator[U] {
			return &MapIterator[T, U]{
				i: it.Itr(),
				f: f,
			}
		},
	}
}

type MapIterator[T, U any] struct {
	i Iterator[T]
	f func(T) U
}

func (i *MapIterator[T, U]) Move() (U, bool) {
	if v, ok := i.i.Move(); ok {
		return i.f(v), true
	}

	return *new(U), false
}

type GroupByRecord[T util.Comparable[T], U any] struct {
	Key   T
	Value []U
}

func GroupBy[T util.Comparable[T], U any](it Iterator[T], f func(T) U) Iterable[GroupByRecord[T, U]] {
	res := []GroupByRecord[T, U]{}

	for v, ok := it.Move(); ok; v, ok = it.Move() {
		found := false

		for i := range res {
			if res[i].Key.CompareTo(v) == 0 {
				res[i].Value = append(res[i].Value, f(v))
				found = true
				break
			}
		}

		if !found {
			res = append(res, GroupByRecord[T, U]{Key: v, Value: []U{f(v)}})
		}
	}

	return NewSliceIterable[GroupByRecord[T, U]](res)
}

func FlatMap[V any](it Iterable[Iterable[V]]) Iterable[V] {
	return &BaseIterable[V]{
		builder: func() Iterator[V] {
			return &FlatMapIterator[V]{
				i: it.Itr(),
			}
		},
	}
}

type FlatMapIterator[V any] struct {
	i Iterator[Iterable[V]]
	j Iterator[V]
}

func (i *FlatMapIterator[V]) Move() (V, bool) {
	if i.j == nil {
		if v, ok := i.i.Move(); ok {
			i.j = v.Itr()
		} else {
			return *new(V), false
		}
	}

	if v, ok := i.j.Move(); ok {
		return v, true
	}

	i.j = nil

	return i.Move()
}
