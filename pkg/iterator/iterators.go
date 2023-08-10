package iterator

func NewEmptyIterable[V any]() Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &EmptyIterator[V]{}
	})
}

func NewGeneratorIterable[V any](generator func(idx int) V, length int) Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &GeneratorIterator[V]{
			Generator: generator,
			Length:    length,
			idx:       0,
		}
	})
}

func NewSliceIterable[V any](slice []V) Iterable[V] {
	return BaseIterableFrom(func() Iterator[V] {
		return &SliceIterator[V]{
			Slice: slice,
			idx:   0,
		}
	})
}

type EmptyIterator[V any] struct{}

func (*EmptyIterator[V]) Move() (V, bool) {
	return *new(V), false
}

func (*EmptyIterator[V]) HasNext() bool {
	return false
}

func (*EmptyIterator[V]) Current() V {
	return *new(V)
}

type GeneratorIterator[V any] struct {
	Generator func(idx int) V
	idx       int
	Length    int
	current   V
}

func (g *GeneratorIterator[V]) Move() (V, bool) {
	if g.idx < g.Length {
		g.current = g.Generator(g.idx)
		g.idx++
		return g.current, true
	}
	g.current = *new(V)
	return *new(V), false
}

func (g *GeneratorIterator[V]) HasNext() bool {
	return g.idx < g.Length
}

func (g *GeneratorIterator[V]) Current() V {
	return g.current
}

type SliceIterator[V any] struct {
	Slice []V
	idx   int
}

func (s *SliceIterator[V]) Move() (V, bool) {
	if s.idx < len(s.Slice) {
		v := s.Slice[s.idx]
		s.idx++
		return v, true
	}
	return *new(V), false
}

func (s *SliceIterator[V]) HasNext() bool {
	return s.idx < len(s.Slice)
}

func (s *SliceIterator[V]) Current() V {
	if s.idx >= len(s.Slice) {
		return *new(V)
	}

	return s.Slice[s.idx]
}
