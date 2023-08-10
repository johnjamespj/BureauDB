package iterator

type Iterable[V any] interface {
	Itr() Iterator[V]

	Take(n int) Iterable[V]

	Skip(n int) Iterable[V]

	TakeWhile(pred func(V) bool) Iterable[V]

	Where(pred func(V) bool) Iterable[V]

	FollowedBy(itr ...Iterable[V]) Iterable[V]

	Reversed() Iterable[V]

	Any(pred func(V) bool) bool

	Every(pred func(V) bool) bool

	ForEach(f func(V))

	ToList() []V
}

type Iterator[V any] interface {
	Move() (V, bool)
}
