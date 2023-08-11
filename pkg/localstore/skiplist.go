package localstore

import (
	"math"
	"math/rand"
	"sync/atomic"

	"github.com/johnjamespj/BureauDB/pkg/iterator"
	"github.com/johnjamespj/BureauDB/pkg/util"
)

type SkiplistNode[V util.Comparable[V]] struct {
	Prev atomic.Pointer[SkiplistNode[V]]

	Levels []atomic.Pointer[SkiplistNode[V]]

	Value V
}

type Skiplist[V util.Comparable[V]] struct {
	Head      atomic.Pointer[SkiplistNode[V]]
	Tail      atomic.Pointer[SkiplistNode[V]]
	MaxHeight int
	Size      int
}

func NewSkiplist[V util.Comparable[V]](estimateSize int) *Skiplist[V] {
	height := int(math.Ceil(math.Log(float64(estimateSize)) / math.Log(2)))
	sl := &Skiplist[V]{
		MaxHeight: height,
	}
	sl.Head.Store(nil)
	return sl
}

func (sl *Skiplist[V]) Put(value V) {
	head := sl.Head.Load()
	sl.Size++
	if head == nil {
		sl.createNewHead(value)
		return
	}

	if head.Value.CompareTo(value) > 0 {
		sl.updateHead(value)
		return
	}

	// find the location to insert
	stack := sl.getPathStack(value)

	// insert the node
	sl.insertNode(value, stack)
}

func (sl *Skiplist[V]) createNewHead(value V) {
	levels := make([]atomic.Pointer[SkiplistNode[V]], sl.MaxHeight)
	for i := 0; i < sl.MaxHeight; i++ {
		levels[i].Store(nil)
	}

	node := &SkiplistNode[V]{
		Levels: levels,
		Value:  value,
	}
	node.Prev.Store(nil)
	sl.Head.Store(node)
	sl.Tail.Store(node)
}

func (sl *Skiplist[V]) updateHead(value V) {
	oldHead := sl.Head.Load()

	// insert before head
	newHead := &SkiplistNode[V]{
		Levels: make([]atomic.Pointer[SkiplistNode[V]], sl.MaxHeight),
		Value:  value,
	}
	newHead.Prev.Store(nil)
	newHead.Levels[0].Store(oldHead)
	for i := 1; i < sl.MaxHeight; i++ {
		newHead.Levels[i].Store(oldHead.Levels[i].Load())
	}
	sl.Head.Store(newHead)

	// Add levels to the old head
	height := calculateRandomHeight(sl.MaxHeight)
	newNode := &SkiplistNode[V]{
		Levels: make([]atomic.Pointer[SkiplistNode[V]], height),
		Value:  oldHead.Value,
	}
	newNode.Prev.Store(newHead)
	next := oldHead.Levels[0].Load()
	newNode.Levels[0].Store(next)
	if next != nil {
		oldHead.Levels[0].Load().Prev.Store(newNode)
	}
	newHead.Levels[0].Store(newNode)
	for i := 1; i < height; i++ {
		newNode.Levels[i].Store(newHead.Levels[i].Load())
		newHead.Levels[i].Store(newNode)
	}
}

func (sl *Skiplist[V]) getPathStack(value V) []*SkiplistNode[V] {
	stack := make([]*SkiplistNode[V], sl.MaxHeight)
	currentNode := sl.Head.Load()
	level := sl.MaxHeight - 1
	for {
		nextNode := currentNode.Levels[level].Load()
		if nextNode == nil || nextNode.Value.CompareTo(value) > 0 {
			stack[level] = currentNode
			if level == 0 {
				break
			}
			level--
			continue
		}
		currentNode = nextNode
	}
	return stack
}

func (sl *Skiplist[V]) insertNode(value V, stack []*SkiplistNode[V]) {
	height := calculateRandomHeight(sl.MaxHeight)
	node := &SkiplistNode[V]{
		Levels: make([]atomic.Pointer[SkiplistNode[V]], height),
		Value:  value,
	}
	node.Prev.Store(stack[0])

	for i := 0; i < height; i++ {
		node.Levels[i].Store(stack[i].Levels[i].Load())
		stack[i].Levels[i].Store(node)
	}

	if node.Levels[0].Load() == nil {
		sl.Tail.Store(node)
	} else {
		node.Levels[0].Load().Prev.Store(node)
	}
}

func (sl *Skiplist[V]) search(f func(V) bool) *SkiplistNode[V] {
	currentNode := sl.Head.Load()
	level := sl.MaxHeight - 1
	for {
		nextNode := currentNode.Levels[level].Load()
		if nextNode == nil || f(nextNode.Value) {
			if level == 0 {
				if currentNode == nil || f(currentNode.Value) {
					return currentNode
				} else {
					currentNode = nextNode
					continue
				}
			}
			level--
			continue
		}
		currentNode = nextNode
	}
}

// returns the node with gives f(node.Value) == true
func (sl *Skiplist[V]) Search(isBackward bool, f func(V) bool) iterator.Iterable[V] {
	node := sl.search(f)
	if node == nil {
		return iterator.NewEmptyIterable[V]()
	}

	if isBackward {
		return iterator.BaseIterableFrom[V](func() iterator.Iterator[V] {
			return &SkiplistForwardIterator[V]{
				CurrentNode: node,
			}
		})
	}
	return iterator.BaseIterableFrom[V](func() iterator.Iterator[V] {
		return &SkiplistReverseIterator[V]{
			CurrentNode: node,
		}
	})
}

func (sl *Skiplist[V]) Forward() iterator.Iterable[V] {
	return iterator.BaseIterableFrom[V](func() iterator.Iterator[V] {
		return &SkiplistForwardIterator[V]{
			CurrentNode: sl.Head.Load(),
		}
	})
}

func (sl *Skiplist[V]) Backward() iterator.Iterable[V] {
	return iterator.BaseIterableFrom[V](func() iterator.Iterator[V] {
		return &SkiplistReverseIterator[V]{
			CurrentNode: sl.Tail.Load(),
		}
	})
}

func calculateRandomHeight(maxHeight int) int {
	num := rand.Intn(1 << 30)
	height := 1

	for (num&1) != 0 && height < maxHeight {
		height++
		num >>= 1
	}

	return height
}

type SkiplistForwardIterator[V util.Comparable[V]] struct {
	CurrentNode *SkiplistNode[V]
}

func (it *SkiplistForwardIterator[V]) Move() (V, bool) {
	if it.CurrentNode == nil {
		return *new(V), false
	}
	value := it.CurrentNode.Value
	it.CurrentNode = it.CurrentNode.Levels[0].Load()
	return value, true
}

type SkiplistReverseIterator[V util.Comparable[V]] struct {
	CurrentNode *SkiplistNode[V]
}

func (it *SkiplistReverseIterator[V]) Move() (V, bool) {
	if it.CurrentNode == nil {
		return *new(V), false
	}
	value := it.CurrentNode.Value
	it.CurrentNode = it.CurrentNode.Prev.Load()
	return value, true
}
