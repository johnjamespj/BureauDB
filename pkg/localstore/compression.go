package localstore

import (
	"bufio"
	"bytes"
	"io"

	"github.com/pierrec/lz4"
)

type Compressor interface {
	Compress([]byte) ([]byte, error)
}

type Decompressor interface {
	Decompress([]byte) ([]byte, error)
}

type NoCompressionWriter struct {
}

func NewNoCompressionWriter() *NoCompressionWriter {
	return &NoCompressionWriter{}
}

func (w *NoCompressionWriter) Compress(p []byte) ([]byte, error) {
	return p, nil
}

type NoCompressionReader struct {
}

func NewNoCompressionReader() *NoCompressionReader {
	return &NoCompressionReader{}
}

func (r *NoCompressionReader) Decompress(p []byte) ([]byte, error) {
	return p, nil
}

type Lz4CompressionWriter struct{}

func NewLz4CompressionWriter() *Lz4CompressionWriter {
	return &Lz4CompressionWriter{}
}

func (w *Lz4CompressionWriter) Compress(p []byte) ([]byte, error) {
	var b bytes.Buffer
	buffer := bufio.NewWriter(&b)
	writer := lz4.NewWriter(buffer)
	_, err := writer.Write(p)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	err = buffer.Flush()
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

type Lz4CompressionReader struct{}

func NewLz4CompressionReader() *Lz4CompressionReader {
	return &Lz4CompressionReader{}
}

func (r *Lz4CompressionReader) Decompress(p []byte) ([]byte, error) {
	var b bytes.Buffer
	buffer := bufio.NewWriter(&b)
	reader := lz4.NewReader(bytes.NewReader(p))
	_, err := io.Copy(buffer, reader)
	if err != nil {
		return nil, err
	}

	err = buffer.Flush()
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}
