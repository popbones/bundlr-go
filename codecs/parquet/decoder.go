package parquet

import (
	"io"
	"sync"

	"github.com/spf13/afero"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	"github.com/popbones/bundlr-go/bundlr"
)

type DecoderMaker struct {
	fs        afero.Fs
	np        int64
	prototype interface{}
}

func NewDecoderMaker(fs afero.Fs) *DecoderMaker {
	return &DecoderMaker{
		fs: fs,
		np: 4,
	}
}

func (m *DecoderMaker) WithPrototype(proto interface{}) *DecoderMaker {
	m.prototype = proto
	return m
}

func (m *DecoderMaker) Make(f bundlr.File) (bundlr.Decoder, error) {
	pf := NewParquetFile(m.fs, f)
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

type Student struct {
	Name string `parquet:"name=name, type=UTF8"`
	Age  int32  `parquet:"name=age, type=INT32"`
}

func (e *Decoder) Decode(record interface{}) error {
	e.mu.Lock()
	if e.count == e.rc {
		e.mu.Unlock()
		return io.EOF
	}
	err := e.pr.Read(record)
	e.count++
	e.mu.Unlock()
	return err
}

func (e *Decoder) Close() error {
	e.pr.ReadStop()
	return e.f.Close()
}
