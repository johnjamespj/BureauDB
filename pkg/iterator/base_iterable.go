package iterator

import "container/list"

type BaseIterable[V any] struct {
	builder func() Iterator[V]
}

func BaseIterableFrom[V any](builder func() Iterator[V]) *BaseIterable[V] {
	return &BaseIterable[V]{
		builder: builder,
	}
}

func (i *BaseIterable[V]) Itr() Iterator[V] {
	return i.builder()
}

func (i *BaseIterable[V]) Take(n int) Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &TakeNIterator[V]{
			Iterable: i.Itr(),
			N:        n,
			idx:      0,
		}
	})
}

func (i *BaseIterable[V]) Skip(n int) Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &SkipNIterator[V]{
			Iterable: i.Itr(),
			N:        n,
			idx:      0,
		}
	})
}

func (i *BaseIterable[V]) SkipWhile(pred func(V) bool) Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &SkipWhileIterator[V]{
			Iterator:  i.Itr(),
			Predicate: pred,
		}
	})
}

func (i *BaseIterable[V]) TakeWhile(pred func(V) bool) Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &TakeWhileIterator[V]{
			Iterable:  i.Itr(),
			Predicate: pred,
		}
	})
}

func (i *BaseIterable[V]) Where(pred func(V) bool) Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &WhereIterator[V]{
			Iterable:  i.Itr(),
			Predicate: pred,
		}
	})
}

func (i *BaseIterable[V]) FollowedBy(itr ...Iterable[V]) Iterable[V] {
	var iterators []Iterator[V]
	iterators = append(iterators, i.Itr())
	for _, it := range itr {
		iterators = append(iterators, it.Itr())
	}

	return BaseIterableFrom(func() Iterator[V] {
		return &ChainIterator[V]{
			Iterables: iterators,
			idx:       0,
		}
	})
}

func (i *BaseIterable[V]) Reversed() Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &ReversedIterator[V]{
			Iterable: i.Itr(),
		}
	})
}

func (i *BaseIterable[V]) Any(pred func(V) bool) bool {
	itr := i.Itr()
	for v, ok := itr.Move(); ok; v, ok = itr.Move() {
		if pred(v) {
			return true
		}
	}
	return false
}

func (i *BaseIterable[V]) Every(pred func(V) bool) bool {
	itr := i.Itr()
	for v, ok := itr.Move(); ok; v, ok = itr.Move() {
		if !pred(v) {
			return false
		}
	}
	return true
}

func (i *BaseIterable[V]) ForEach(f func(V)) {
	itr := i.Itr()
	for v, ok := itr.Move(); ok; v, ok = itr.Move() {
		f(v)
	}
}

func (i *BaseIterable[V]) ToList() []V {
	itr := i.Itr()
	var list []V

	for v, ok := itr.Move(); ok; v, ok = itr.Move() {
		list = append(list, v)
	}

	return list
}

type TakeNIterator[V any] struct {
	Iterable Iterator[V]
	N        int
	idx      int
}

func (i *TakeNIterator[V]) Move() (V, bool) {
	if i.idx >= i.N {
		return *new(V), false
	}

	i.idx++
	return i.Iterable.Move()
}

type SkipNIterator[V any] struct {
	Iterable Iterator[V]
	N        int
	idx      int
}

func (i *SkipNIterator[V]) Move() (V, bool) {
	for i.idx < i.N {
		i.Iterable.Move()
		i.idx++
	}

	return i.Iterable.Move()
}

type TakeWhileIterator[V any] struct {
	Iterable  Iterator[V]
	Predicate func(V) bool
}

func (i *TakeWhileIterator[V]) Move() (V, bool) {
	if v, ok := i.Iterable.Move(); ok && i.Predicate(v) {
		return v, true
	}

	return *new(V), false
}

type WhereIterator[V any] struct {
	Iterable  Iterator[V]
	Predicate func(V) bool
}

func (i *WhereIterator[V]) Move() (V, bool) {
	for v, ok := i.Iterable.Move(); ok; v, ok = i.Iterable.Move() {
		if i.Predicate(v) {
			return v, true
		}
	}

	return *new(V), false
}

type ChainIterator[V any] struct {
	Iterables []Iterator[V]
	idx       int
}

func (i *ChainIterator[V]) Move() (V, bool) {
	for i.idx < len(i.Iterables) {
		if v, ok := i.Iterables[i.idx].Move(); ok {
			return v, true
		}

		i.idx++
	}

	return *new(V), false
}

type ReversedIterator[V any] struct {
	Iterable Iterator[V]
	stack    list.List
}

func (i *ReversedIterator[V]) Move() (V, bool) {
	if i.stack.Len() > 0 {
		v := i.stack.Back()
		i.stack.Remove(v)
		return v.Value.(V), true
	}

	for v, ok := i.Iterable.Move(); ok; v, ok = i.Iterable.Move() {
		i.stack.PushBack(v)
	}

	if i.stack.Len() > 0 {
		v := i.stack.Back()
		i.stack.Remove(v)
		return v.Value.(V), true
	}

	return *new(V), false
}

type SkipWhileIterator[V any] struct {
	Iterator  Iterator[V]
	Predicate func(V) bool
	Start     bool
}

func (i *SkipWhileIterator[V]) Move() (V, bool) {
	if !i.Start {
		for v, ok := i.Iterator.Move(); ok; v, ok = i.Iterator.Move() {
			if !i.Predicate(v) {
				i.Start = true
				return v, true
			}
		}
	}

	return i.Iterator.Move()
}
