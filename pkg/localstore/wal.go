package localstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/johnjamespj/BureauDB/pkg/iterator"
	"github.com/johnjamespj/BureauDB/pkg/util"
)

var (
	ErrWALExists  = fmt.Errorf("WAL already exists")
	ErrWALCorrupt = fmt.Errorf("WAL is corrupt")
)

type WAL struct {
	filename string
	file     *os.File
}

func NewWAL(filename string) (*WAL, error) {
	if _, err := os.Stat(filename); !errors.Is(err, os.ErrNotExist) {
		return nil, ErrWALExists
	}

	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return &WAL{
		filename: filename,
		file:     file,
	}, nil
}

func (w *WAL) Write(row *Row) error {
	b := row.ToBytes()
	hash := util.HashBytes(b)
	size := util.Int64ToBytes(int64(len(b)))
	_, err := w.file.Write(hash[:])
	if err != nil {
		return err
	}
	_, err = w.file.Write(size)
	if err != nil {
		return err
	}
	_, err = w.file.Write(b)
	return err
}

func (w *WAL) Close() error {
	return w.file.Close()
}

type WALIterator struct {
	file *os.File
}

func NewWalIterable(filename string) (iterator.Iterable[*Row], error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return iterator.BaseIterableFrom(func() iterator.Iterator[*Row] {
		return &WALIterator{
			file: file,
		}
	}), nil
}

func (w *WALIterator) Move() (*Row, bool) {
	hash := make([]byte, 16)
	_, err := w.file.Read(hash)
	if err != nil {
		return nil, false
	}

	sizeBin := make([]byte, 8)
	_, err = w.file.Read(sizeBin)
	if err != nil {
		return nil, false
	}
	size := util.BytesToInt64(sizeBin, 0)

	b := make([]byte, size)
	l, err := w.file.Read(b)
	if err != nil {
		return nil, false
	}
	if l != int(size) {
		return nil, false
	}

	thisHash := util.HashBytes(b)
	if !bytes.Equal(thisHash[:], hash) {
		return nil, false
	}

	row, err := RowFromBytes(b)
	if err != nil {
		return nil, false
	}

	return row, true
}
