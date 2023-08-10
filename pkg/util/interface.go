package util

type Comparable[T any] interface {
	CompareTo(T) int
}
