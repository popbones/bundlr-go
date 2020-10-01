package parquet

import (
	"github.com/popbones/bundlr-go/bundlr"
)

// ConfigBundle configs an existing bundle instance for handling parquet
func ConfigBundle(b *bundlr.Bundle, prototype interface{}) *bundlr.Bundle {
	return b.WithFileExtension("parquet").
		WithEncoderMaker(NewEncoderMaker(b).WithPrototype(prototype)).
		WithDecoderMaker(NewDecoderMaker(b).WithPrototype(prototype))
}
