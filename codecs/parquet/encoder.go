package parquet

import (
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/popbones/bundlr-go/bundlr"
)

type EncoderMaker struct {
	bundle    *bundlr.Bundle
	np        int64
	prototype interface{}
}

func NewEncoderMaker(b *bundlr.Bundle) *EncoderMaker {
	return &EncoderMaker{
		bundle: b,
		np:     4,
	}
}

func (m *EncoderMaker) WithPrototype(proto interface{}) *EncoderMaker {
	m.prototype = proto
	return m
}

func (m *EncoderMaker) Make(f bundlr.File) (bundlr.Encoder, error) {
	pf := NewParquetFile(m.bundle.FS(), f)
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
