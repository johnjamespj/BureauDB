package localstore

import (
	"encoding/json"
	"os"
	"path"
	"sync"
)

type FileType int8

const (
	WALFile FileType = iota
	SSTabelFile
)

type FileRef struct {
	ID   int64
	Type FileType
}

type Manifest struct {
	dirPath  string
	rwlock   sync.RWMutex
	sstables map[int64]*FileRef
	wals     map[int64]*FileRef
}

func NewManifest(dirPath string) (*Manifest, error) {
	manifest := &Manifest{
		dirPath:  dirPath,
		sstables: make(map[int64]*FileRef),
		wals:     make(map[int64]*FileRef),
	}

	err := manifest.readFromFile()
	if err != nil {
		return nil, err
	}

	return manifest, nil
}

func (m *Manifest) AddSSTable(id int64) {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	m.sstables[id] = &FileRef{
		ID:   id,
		Type: SSTabelFile,
	}
}

func (m *Manifest) AddWAL(id int64) {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	m.wals[id] = &FileRef{
		ID:   id,
		Type: WALFile,
	}
}

func (m *Manifest) RemoveSSTable(id int64) {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	delete(m.sstables, id)
}

func (m *Manifest) RemoveWAL(id int64) {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	delete(m.wals, id)
}

func (m *Manifest) GetSSTables() []*FileRef {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()
	var sstables []*FileRef
	for _, sstable := range m.sstables {
		sstables = append(sstables, sstable)
	}
	return sstables
}

func (m *Manifest) GetWALs() []*FileRef {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()
	var wals []*FileRef
	for _, wal := range m.wals {
		wals = append(wals, wal)
	}
	return wals
}

func (m *Manifest) saveToTempFile() error {
	file, err := os.Create(path.Join(m.dirPath, "manifest.temp"))
	if err != nil {
		return err
	}
	defer file.Close()

	sstableIds := []int64{}
	for id := range m.sstables {
		sstableIds = append(sstableIds, id)
	}

	walIds := []int64{}
	for id := range m.wals {
		walIds = append(walIds, id)
	}

	bytes, err := json.Marshal(map[string]any{
		"sstables": sstableIds,
		"wals":     walIds,
	})
	if err != nil {
		return err
	}

	_, err = file.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manifest) atomicSwap() error {
	err := m.saveToTempFile()
	if err != nil {
		return err
	}

	return os.Rename(path.Join(m.dirPath, "manifest.temp"), path.Join(m.dirPath, "manifest.json"))
}

func (m *Manifest) Save() error {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	return m.atomicSwap()
}

func (m *Manifest) readFromFile() error {
	// TODO: check if file exists
	if _, err := os.Stat(path.Join(m.dirPath, "manifest.json")); os.IsNotExist(err) {
		return m.Save()
	}

	file, err := os.Open(path.Join(m.dirPath, "manifest.json"))
	if err != nil {
		return err
	}

	var data map[string]any
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return err
	}

	sstableIds := data["sstables"].([]any)
	for _, id := range sstableIds {
		m.sstables[int64(id.(float64))] = &FileRef{
			ID:   int64(id.(float64)),
			Type: SSTabelFile,
		}
	}

	walIds := data["wals"].([]any)
	for _, id := range walIds {
		m.wals[int64(id.(float64))] = &FileRef{
			ID:   int64(id.(float64)),
			Type: WALFile,
		}
	}

	return nil
}

func (m *Manifest) Close() error {
	return m.Save()
}
