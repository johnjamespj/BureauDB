package localstore

import (
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
)

type NavigableTableRef struct {
	NavigableTable
	ID    string
	locks atomic.Int32
}

func (n *NavigableTableRef) Lock() {
	n.locks.Add(1)
}

func (n *NavigableTableRef) Unlock() {
	n.locks.Add(-1)
}

type CompressionAlgorithm struct {
	compressor   Compressor
	decompressor Decompressor
}

var (
	None = &CompressionAlgorithm{
		compressor:   &NoCompressionWriter{},
		decompressor: &NoCompressionReader{},
	}
	Lz4 = &CompressionAlgorithm{
		compressor:   &Lz4CompressionWriter{},
		decompressor: &Lz4CompressionReader{},
	}
)

type LSMTreeConfig struct {
	Dir                  string
	MaxSSTableBlockSize  int
	MaxFileOpenCount     int
	MaxBlockCacheSize    int
	MaxMemtableSize      int
	CompressionAlgorithm *CompressionAlgorithm
}

type LSMTree struct {
	Memtable atomic.Pointer[Memtable]

	MemtableRef *NavigableTableRef
	Immutable   []*NavigableTableRef
	LevelZero   []*NavigableTableRef
	Levels      [][]*NavigableTableRef
	Locked      []*NavigableTableRef
	Snapshots   []int64
	lock        sync.RWMutex
	manifest    *Manifest

	config          *LSMTreeConfig
	blockCache      *lru.TwoQueueCache[string, []byte]
	fileHandleCache *lru.Cache[string, *os.File]
}

func NewLSMTree(config *LSMTreeConfig) (*LSMTree, error) {
	err := os.MkdirAll(path.Join(config.Dir, "wal"), 0755)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(path.Join(config.Dir, "tables"), 0755)
	if err != nil {
		return nil, err
	}

	manifest, err := NewManifest(config.Dir)
	if err != nil {
		return nil, err
	}

	fileHandleCache, err := lru.NewWithEvict[string, *os.File](config.MaxFileOpenCount, func(key string, value *os.File) {
		value.Close()
	})
	if err != nil {
		return nil, err
	}

	blockCacheItemCount := int(config.MaxBlockCacheSize / config.MaxSSTableBlockSize)
	blockCache, err := lru.New2Q[string, []byte](blockCacheItemCount)
	if err != nil {
		return nil, err
	}

	tree := &LSMTree{
		config:          config,
		manifest:        manifest,
		fileHandleCache: fileHandleCache,
		blockCache:      blockCache,
	}

	// err = tree.loadAllWALFiles()
	// if err != nil {
	// 	return nil, err
	// }

	// id, memtable := tree.newMemtable()
	// tree.MemtableRef = &NavigableTableRef{
	// 	NavigableTable: memtable,
	// 	ID:             id,
	// }
	// tree.MemtableRef.locks.Store(0)
	// tree.Memtable.Store(memtable)
	// tree.manifest.AddWAL(id)
	// tree.manifest.Save()

	return tree, nil
}

func (l *LSMTree) SwapMemtable() {
	l.lock.Lock()
	defer l.lock.Unlock()

	id, memtable := l.newMemtable()
	l.Immutable = append(l.Immutable, l.MemtableRef)
	l.Memtable.Swap(memtable)
	l.MemtableRef = &NavigableTableRef{NavigableTable: memtable, ID: id}
	l.MemtableRef.locks.Store(0)
	l.manifest.AddWAL(id)
	l.manifest.Save()
}

func (l *LSMTree) RemoveMemtable(table string) {
	l.lock.Lock()
	defer l.lock.Unlock()

	for i, ref := range l.Immutable {
		if ref.ID == table {
			if ref.locks.Load() > 0 {
				l.Locked = append(l.Locked, ref)
			}

			l.Immutable = append(l.Immutable[:i], l.Immutable[i+1:]...)
			l.manifest.RemoveWAL(table)
			break
		}
	}

	l.manifest.Save()
}

func (l *LSMTree) AddLevelZeroFile(table string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.LevelZero = append(l.LevelZero, l.openSSTable(table))
	l.manifest.AddSSTable(table)
	l.manifest.Save()
}

func (l *LSMTree) AddAndRemoveLevelNFiles(levelToAdd int, tablesToAdd []string, levelToRemove int, tablesToRemove []string) {
	l.lock.Lock()
	defer l.lock.Unlock()

	for _, table := range tablesToAdd {
		ref := l.openSSTable(table)
		l.Levels[levelToAdd] = append(l.Levels[levelToAdd], ref)
		l.manifest.AddSSTable(table)
	}

	for _, table := range tablesToRemove {
		for i, ref := range l.Levels[levelToRemove] {
			if ref.ID == table {
				if ref.locks.Load() > 0 {
					l.Locked = append(l.Locked, ref)
				}

				l.Levels[levelToRemove] = append(l.Levels[levelToRemove][:i], l.Levels[levelToRemove][i+1:]...)
				l.manifest.RemoveSSTable(table)
				break
			}
		}
	}

	l.manifest.Save()
}

func (l *LSMTree) GetFilesInRange(partitionKeyHash DataSlice, sortkey DataSlice, sequenceNo int64) []*NavigableTableRef {
	l.lock.RLock()
	defer l.lock.RUnlock()

	list := make([]*NavigableTableRef, 0, 10)
	if l.Memtable.Load() != nil && l.Memtable.Load().IsInRange(partitionKeyHash, sortkey, sequenceNo) {
		list = append(list, l.MemtableRef)
		l.MemtableRef.locks.Add(1)
	}

	for _, ref := range l.LevelZero {
		if ref.IsInRange(partitionKeyHash, sortkey, sequenceNo) {
			list = append(list, ref)
		}
	}

	for _, level := range l.Levels {
		for _, ref := range level {
			if ref.IsInRange(partitionKeyHash, sortkey, sequenceNo) {
				list = append(list, ref)
				break
			}
		}
	}

	return list
}

func (l *LSMTree) GetBlockCache() *lru.TwoQueueCache[string, []byte] {
	return l.blockCache
}

func (l *LSMTree) GetFileHandleCache() *lru.Cache[string, *os.File] {
	return l.fileHandleCache
}

func (l *LSMTree) GetManifest() *Manifest {
	return l.manifest
}

func (l *LSMTree) GetMemtable() *Memtable {
	return l.Memtable.Load()
}

func (l *LSMTree) Close() error {
	l.MemtableRef.Close()
	for _, ref := range l.Immutable {
		ref.Close()
	}
	for _, ref := range l.LevelZero {
		ref.Close()
	}
	for _, level := range l.Levels {
		for _, ref := range level {
			ref.Close()
		}
	}

	// err := l.loadAllWALFiles()
	// if err != nil {
	// 	return err
	// }
	l.manifest.Close()

	return nil
}

func (l *LSMTree) newMemtable() (string, *Memtable) {
	id := uuid.New().String()
	filepath := path.Join(l.config.Dir, "wal", fmt.Sprintf("wal.%s.log", id))
	wal, err := NewWAL(filepath)
	if err != nil {
		panic(err)
	}

	return id, NewMemtable(wal)
}

func (l *LSMTree) openSSTable(id string) *NavigableTableRef {
	filepath := path.Join(l.config.Dir, "tables", fmt.Sprintf("table.%s.sst", id))
	reader, err := NewSSTableReader(filepath, l.config.CompressionAlgorithm.decompressor, l.blockCache, l.fileHandleCache)
	if err != nil {
		panic(err)
	}

	ref := &NavigableTableRef{NavigableTable: reader, ID: id}
	ref.locks.Store(0)

	return ref
}
