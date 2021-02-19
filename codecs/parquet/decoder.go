package parquet

import (
	"io"
	"reflect"
	"sync"

	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	"github.com/popbones/bundlr-go/bundlr"
)

type DecoderMaker struct {
	bundle    *bundlr.Bundle
	np        int64
	prototype interface{}
}

func NewDecoderMaker(b *bundlr.Bundle) *DecoderMaker {
	return &DecoderMaker{
		bundle: b,
		np:     4,
	}
}

func (m *DecoderMaker) WithPrototype(proto interface{}) *DecoderMaker {
	m.prototype = proto
	return m
}

func (m *DecoderMaker) Make(f bundlr.File) (bundlr.Decoder, error) {
	pf := NewParquetFile(m.bundle.DataFS(), f)
	pr, err := reader.NewParquetReader(pf, m.prototype, m.np)
	if err != nil {
		return nil, err
	}
	return &Decoder{
		f:  pf,
		pr: pr,
		rc: pr.GetNumRows(),
	}, nil
}

type Decoder struct {
	mu    sync.Mutex
	f     source.ParquetFile
	pr    *reader.ParquetReader
	count int64
	rc    int64
}

func (e *Decoder) Decode(record interface{}) error {
	e.mu.Lock()
	if e.count == e.rc {
		e.mu.Unlock()
		return io.EOF
	}
	err := e.pr.Read(record)
	// parquet-go Read expect a slice and Read does not return io.EOF at the end of the file
	// to correctly determine the read count we use reflection as a hack here.
	e.count += int64(reflect.ValueOf(record).Elem().Len())
	e.mu.Unlock()
	return err
}

func (e *Decoder) Close() error {
	e.pr.ReadStop()
	return e.f.Close()
}
