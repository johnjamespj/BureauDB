package localstore

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/johnjamespj/BureauDB/pkg/iterator"
	"github.com/johnjamespj/BureauDB/pkg/util"
	"github.com/vmihailenco/msgpack"
)

var (
	ErrSSTableExists = fmt.Errorf("SSTable already exists")
)

type BlockIndexEntry struct {
	Offset      int64
	Size        int
	StartRowRef *Row
}

type SSTableWriter struct {
	compressor   Compressor
	sortedRows   []*Row
	file         *os.File
	maxBlockSize int

	maxTimestamp        int64
	minTimestamp        int64
	maxSequence         int64
	minSequence         int64
	maxPartitionKeyHash DataSlice
	minPartitionKeyHash DataSlice
	maxSortKey          DataSlice
	minSortKey          DataSlice
	size                int64

	blockIndex []*BlockIndexEntry
	filter     *util.Bloomfilter
}

func NewSSTableWriter(filename string, compressor Compressor, maxBlockSize int, rows []*Row) (*SSTableWriter, error) {
	if _, err := os.Stat(filename); !errors.Is(err, os.ErrNotExist) {
		return nil, ErrSSTableExists
	}

	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return &SSTableWriter{
		maxBlockSize: maxBlockSize,
		compressor:   compressor,
		sortedRows:   rows,
		file:         file,
		filter:       util.NewBloomfilter(len(rows)*2, 12),
	}, nil
}

func (s *SSTableWriter) Write() error {
	err := s.WriteBlocks()
	if err != nil {
		return err
	}

	err = s.WriteMetadata()
	if err != nil {
		return err
	}

	return nil
}

func (s *SSTableWriter) WriteBlocks() error {
	buf := bufio.NewWriter(s.file)

	var fileOffset int64 = 0

	var maxSequence int64
	var minSequence int64
	var maxTimestamp int64
	var minTimestamp int64
	var size int64

	var minPartitionKeyHash DataSlice = s.sortedRows[0].KeyHash
	var maxPartitionKeyHash DataSlice = s.sortedRows[len(s.sortedRows)-1].KeyHash

	var minSortKey DataSlice = s.sortedRows[0].SortKey
	var maxSortKey DataSlice = s.sortedRows[len(s.sortedRows)-1].SortKey

	var currentBlockSize int
	var currentBlock [][]byte
	var startRow *Row

	for _, row := range s.sortedRows {
		rowSize := row.Size()
		currentBlockSize += rowSize
		size += int64(rowSize)

		// partition key hash and composite key hash
		s.filter.Add(row.KeyHash)
		s.filter.Add(bytes.Join([][]byte{
			row.KeyHash,
			row.SortKey,
		}, []byte{}))

		if startRow == nil {
			startRow = row
		}

		maxSequence = int64(math.Max(float64(maxSequence), float64(row.Sequence)))
		minSequence = int64(math.Min(float64(minSequence), float64(row.Sequence)))
		maxTimestamp = int64(math.Max(float64(maxTimestamp), float64(row.Timestamp)))
		minTimestamp = int64(math.Min(float64(minTimestamp), float64(row.Timestamp)))

		if currentBlockSize > s.maxBlockSize {
			copyRow := *startRow
			copyRow.Value = nil
			currentBlockSize -= rowSize
			blockCompressedSize, err := s.WriteBlock(currentBlock, buf)
			if err != nil {
				return err
			}

			s.blockIndex = append(s.blockIndex, &BlockIndexEntry{
				Size:        blockCompressedSize,
				Offset:      fileOffset,
				StartRowRef: &copyRow,
			})
			fileOffset += int64(blockCompressedSize)

			currentBlockSize = rowSize
			startRow = row
			currentBlock = [][]byte{
				row.ToBytes(),
			}
		} else {
			currentBlock = append(currentBlock, row.ToBytes())
		}
	}

	if currentBlockSize > 0 {
		copyRow := *startRow
		copyRow.Value = nil
		size, err := s.WriteBlock(currentBlock, buf)
		s.blockIndex = append(s.blockIndex, &BlockIndexEntry{
			Size:        size,
			Offset:      fileOffset,
			StartRowRef: &copyRow,
		})
		if err != nil {
			return err
		}
	}

	s.maxPartitionKeyHash = maxPartitionKeyHash
	s.minPartitionKeyHash = minPartitionKeyHash
	s.maxSortKey = maxSortKey
	s.minSortKey = minSortKey
	s.maxSequence = maxSequence
	s.minSequence = minSequence
	s.maxTimestamp = maxTimestamp
	s.minTimestamp = minTimestamp
	s.size = size

	buf.Flush()

	return nil
}

func (s *SSTableWriter) WriteMetadata() error {
	// read current file cursor position
	pos, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	buf := bufio.NewWriter(s.file)
	enc := msgpack.NewEncoder(buf)

	err = enc.Encode(s.maxPartitionKeyHash)
	if err != nil {
		return err
	}

	err = enc.Encode(s.minPartitionKeyHash)
	if err != nil {
		return err
	}

	err = enc.Encode(s.maxSortKey)
	if err != nil {
		return err
	}

	err = enc.Encode(s.minSortKey)
	if err != nil {
		return err
	}

	err = enc.Encode(s.maxSequence)
	if err != nil {
		return err
	}

	err = enc.Encode(s.minSequence)
	if err != nil {
		return err
	}

	err = enc.Encode(s.maxTimestamp)
	if err != nil {
		return err
	}

	err = enc.Encode(s.minTimestamp)
	if err != nil {
		return err
	}

	err = enc.Encode(s.size)
	if err != nil {
		return err
	}

	err = enc.EncodeArrayLen(len(s.blockIndex))
	if err != nil {
		return err
	}
	for _, entry := range s.blockIndex {
		err = enc.Encode(entry.Offset)
		if err != nil {
			return err
		}
		err = enc.Encode(entry.Size)
		if err != nil {
			return err
		}
		err = enc.Encode(entry.StartRowRef.ToBytes())
		if err != nil {
			return err
		}
	}

	err = enc.Encode(s.filter.ToBytes())
	if err != nil {
		return err
	}

	buf.Flush()

	// write pos to the end of the file
	position := util.Int64ToBytes(pos)
	_, err = s.file.Write(position)
	if err != nil {
		return err
	}

	return nil
}

func (s *SSTableWriter) WriteBlock(rows [][]byte, w *bufio.Writer) (int, error) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	for _, b := range rows {
		hash := util.HashBytes(b)
		size := util.Int64ToBytes(int64(len(b)))

		if _, err := writer.Write(hash[:]); err != nil {
			return -1, err
		}

		if _, err := writer.Write(size); err != nil {
			return -1, err
		}

		if _, err := writer.Write(b); err != nil {
			return -1, err
		}
	}
	writer.Flush()

	b, err := s.compressor.Compress(buf.Bytes())
	if err != nil {
		return -1, err
	}

	w.Write(b)
	return len(b), nil
}

func (s *SSTableWriter) Close() error {
	return s.file.Close()
}

type SSTableReader struct {
	file         *os.File
	decompressor Decompressor
	tableId      string

	maxTimestamp        int64
	minTimestamp        int64
	maxSequence         int64
	minSequence         int64
	maxPartitionKeyHash DataSlice
	minPartitionKeyHash DataSlice
	maxSortKey          DataSlice
	minSortKey          DataSlice
	size                int64

	blockCache *lru.TwoQueueCache[string, []byte]
	blockIndex []*BlockIndexEntry
	filter     *util.Bloomfilter
}

func NewSSTableReader(tableId string, file *os.File, decompressor Decompressor, cache *lru.TwoQueueCache[string, []byte]) (*SSTableReader, error) {
	return &SSTableReader{
		tableId:      tableId,
		file:         file,
		decompressor: decompressor,
		blockCache:   cache,
	}, nil
}

func (s *SSTableReader) ReadMetadata() error {
	_, err := s.file.Seek(-8, io.SeekEnd)
	if err != nil {
		return err
	}

	metadataBlockOffsetBytes := make([]byte, 8)
	_, err = s.file.Read(metadataBlockOffsetBytes)
	if err != nil {
		return err
	}

	metadataBlockOffset := util.BytesToInt64(metadataBlockOffsetBytes, 0)

	_, err = s.file.Seek(metadataBlockOffset, io.SeekStart)
	if err != nil {
		return err
	}

	buf := bufio.NewReader(s.file)
	dec := msgpack.NewDecoder(buf)

	err = dec.Decode(&s.maxPartitionKeyHash)
	if err != nil {
		return err
	}

	err = dec.Decode(&s.minPartitionKeyHash)
	if err != nil {
		return err
	}

	err = dec.Decode(&s.maxSortKey)
	if err != nil {
		return err
	}

	err = dec.Decode(&s.minSortKey)
	if err != nil {
		return err
	}

	err = dec.Decode(&s.maxSequence)
	if err != nil {
		return err
	}

	err = dec.Decode(&s.minSequence)
	if err != nil {
		return err
	}

	err = dec.Decode(&s.maxTimestamp)
	if err != nil {
		return err
	}

	err = dec.Decode(&s.minTimestamp)
	if err != nil {
		return err
	}

	err = dec.Decode(&s.size)
	if err != nil {
		return err
	}

	blockIndexLen, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}

	s.blockIndex = make([]*BlockIndexEntry, blockIndexLen)
	for i := 0; i < blockIndexLen; i++ {
		offset, err := dec.DecodeInt64()
		if err != nil {
			return err
		}
		size, err := dec.DecodeInt64()
		if err != nil {
			return err
		}
		startRowRefBytes, err := dec.DecodeBytes()
		if err != nil {
			return err
		}
		startRowRef, err := RowFromBytes(startRowRefBytes)
		if err != nil {
			return err
		}
		s.blockIndex[i] = &BlockIndexEntry{
			Offset:      offset,
			Size:        int(size),
			StartRowRef: startRowRef,
		}
	}

	filterBytes, err := dec.DecodeBytes()
	if err != nil {
		return err
	}

	s.filter = util.NewBloomfilterFromBytes(filterBytes, 12)
	return nil
}

func (s *SSTableReader) MetadataToString() string {
	return fmt.Sprintf("maxPartitionKeyHash: %s, minPartitionKeyHash: %s, maxSortKey: %s, minSortKey: %s, maxSequence: %d, minSequence: %d, maxTimestamp: %d, minTimestamp: %d, size: %d",
		hex.EncodeToString(s.maxPartitionKeyHash), hex.EncodeToString(s.minPartitionKeyHash), s.maxSortKey, s.minSortKey, s.maxSequence, s.minSequence, s.maxTimestamp, s.minTimestamp, s.size)
}

func (s *SSTableReader) BlockIndexToString() string {
	var result string
	for _, entry := range s.blockIndex {
		result += fmt.Sprintf("offset: %d, size: %d, startRowRef: %s\n", entry.Offset, entry.Size, entry.StartRowRef.ToString())
	}
	return result
}

func (s *SSTableReader) Forward() iterator.Iterable[*Row] {
	return iterator.FlatMap(iterator.NewGeneratorIterable(
		func(idx int) iterator.Iterable[*Row] {
			return s.ReadBlock(idx)
		},
		len(s.blockIndex),
	))
}

func (s *SSTableReader) Backward() iterator.Iterable[*Row] {
	return iterator.FlatMap(iterator.NewGeneratorIterable(
		func(idx int) iterator.Iterable[*Row] {
			return s.ReadBlock(len(s.blockIndex) - idx - 1).Reversed()
		},
		len(s.blockIndex),
	))
}

func (s *SSTableReader) MightContain(partitionKeyHash DataSlice, sortKey DataSlice) bool {
	return s.filter.Contains(bytes.Join([][]byte{
		partitionKeyHash,
		sortKey,
	}, []byte{}))
}

func (s *SSTableReader) IsInRange(partitionKeyHash DataSlice, sortKey DataSlice, sequenceNumber int64) bool {
	if s.minSequence < sequenceNumber {
		return false
	}

	if bytes.Compare(s.minPartitionKeyHash, partitionKeyHash) < 0 ||
		bytes.Compare(s.maxPartitionKeyHash, partitionKeyHash) > 0 {
		return false
	}

	if bytes.Equal(s.maxPartitionKeyHash, partitionKeyHash) &&
		bytes.Compare(s.maxSortKey, sortKey) < 0 {
		return false
	}

	if bytes.Equal(s.minPartitionKeyHash, partitionKeyHash) &&
		bytes.Compare(s.minSortKey, sortKey) > 0 {
		return false
	}

	return true
}

func (s *SSTableReader) Head(partitionKeyHash DataSlice, sortKey DataSlice, sequenceNumber int64) iterator.Iterable[*Row] {
	if s.IsInRange(partitionKeyHash, sortKey, 0) {
		return iterator.NewEmptyIterable[*Row]()
	}

	l := len(s.blockIndex)
	row := &Row{
		KeyHash: partitionKeyHash,
		SortKey: sortKey,
	}
	idx := sort.Search(l, func(i int) bool {
		return s.blockIndex[i].StartRowRef.CompareTo(row) >= 0
	})
	if idx == l {
		idx--
	}

	blockRef := s.blockIndex[idx].StartRowRef
	if bytes.Equal(blockRef.KeyHash, partitionKeyHash) && bytes.Compare(blockRef.SortKey, sortKey) >= 0 {
		idx--
	}

	return iterator.FlatMap(iterator.NewGeneratorIterable(func(i int) iterator.Iterable[*Row] {
		return s.ReadBlock(idx + i)
	}, l-idx)).SkipWhile(func(r *Row) bool {
		return r.CompareTo(row) < 0
	})
}

func (s *SSTableReader) Tail(partitionKeyHash DataSlice, sortKey DataSlice, sequenceNumber int64) iterator.Iterable[*Row] {
	if s.IsInRange(partitionKeyHash, sortKey, 0) {
		return iterator.NewEmptyIterable[*Row]()
	}

	l := len(s.blockIndex)
	row := &Row{
		KeyHash: partitionKeyHash,
		SortKey: sortKey,
	}
	idx := sort.Search(l, func(i int) bool {
		return s.blockIndex[i].StartRowRef.CompareTo(row) >= 0
	})
	if idx == l {
		idx--
	}

	return iterator.FlatMap(iterator.NewGeneratorIterable(func(i int) iterator.Iterable[*Row] {
		return s.ReadBlock(i).Reversed()
	}, idx).Reversed()).SkipWhile(
		func(r *Row) bool {
			return r.CompareTo(row) >= 0
		},
	)
}

func (s *SSTableReader) Size() int64 {
	return s.size
}

func (s *SSTableReader) ReadBlock(idx int) iterator.Iterable[*Row] {
	return iterator.BaseIterableFrom(func() iterator.Iterator[*Row] {
		cacheKey := fmt.Sprintf("%s.%d", s.tableId, idx)

		var b []byte
		if s.blockCache.Contains(cacheKey) {
			b, _ = s.blockCache.Get(cacheKey)
		} else {
			entry := s.blockIndex[idx]
			_, err := s.file.Seek(entry.Offset, io.SeekStart)
			if err != nil {
				return iterator.NewEmptyIterable[*Row]().Itr()
			}

			b = make([]byte, entry.Size)
			_, err = s.file.Read(b)
			if err != nil {
				return iterator.NewEmptyIterable[*Row]().Itr()
			}

			b, err = s.decompressor.Decompress(b)
			if err != nil {
				fmt.Println(err)
			}

			s.blockCache.Add(cacheKey, b)
		}

		reader := bytes.NewReader(b)
		return &BlockIterator{
			reader: reader,
		}
	})
}

type BlockIterator struct {
	reader *bytes.Reader
}

func (b *BlockIterator) Move() (*Row, bool) {
	// read hash
	var hash [16]byte
	_, err := io.ReadFull(b.reader, hash[:])
	if err != nil {
		return nil, false
	}

	// read size
	sizeBytes := make([]byte, 8)
	_, err = io.ReadFull(b.reader, sizeBytes)
	if err != nil {
		return nil, false
	}
	size := util.BytesToInt64(sizeBytes, 0)

	// read data
	data := make([]byte, size)
	_, err = io.ReadFull(b.reader, data)
	if err != nil {
		return nil, false
	}

	row, err := RowFromBytes(data)
	if err != nil {
		return nil, false
	}

	return row, true
}
