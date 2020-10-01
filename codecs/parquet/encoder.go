package parquet

import (
	"github.com/spf13/afero"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/popbones/bundlr-go/bundlr"
)

type EncoderMaker struct {
	fs        afero.Fs
	np        int64
	prototype interface{}
}

func NewEncoderMaker(fs afero.Fs) *EncoderMaker {
	return &EncoderMaker{
		fs: fs,
		np: 4,
	}
}

func (m *EncoderMaker) WithPrototype(proto interface{}) *EncoderMaker {
	m.prototype = proto
	return m
}

func (m *EncoderMaker) Make(f bundlr.File) (bundlr.Encoder, error) {
	pf := NewParquetFile(m.fs, f)
	pw, err := writer.NewParquetWriter(pf, m.prototype, m.np)
	if err != nil {
		return nil, err
	}
	return &Encoder{
		f:  pf,
		pw: pw,
	}, nil
}

type Encoder struct {
	f  source.ParquetFile
	pw *writer.ParquetWriter
}

func (e *Encoder) Encode(record interface{}) error {
	return e.pw.Write(record)
}

func (e *Encoder) Close() error {
	if err := e.pw.WriteStop(); err != nil {
		_ = e.f.Close()
		return err
	}
	return e.f.Close()
}
