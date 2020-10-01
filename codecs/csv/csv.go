package csv

import (
	"encoding/csv"
	"errors"

	"github.com/popbones/bundlr-go/bundlr"
)

var EncoderMaker = (bundlr.EncoderMakerFunc)(func(f bundlr.File) (bundlr.Encoder, error) {
	w := csv.NewWriter(f)

	return &bundlr.EncoderClosure{
		EncodeFunc: func(record interface{}) error {
			switch v := record.(type) {
			case []string:
				return w.Write(v)
			default:
				return errors.New("unexpected input")
			}
		},
		CloseFunc: func() error {
			w.Flush()
			if err := w.Error(); err != nil {
				_ = f.Close()
				return err
			}
			return f.Close()
		},
	}, nil
})

var DecoderMaker = (bundlr.DecoderMakerFunc)(func(f bundlr.File) (bundlr.Decoder, error) {
	r := csv.NewReader(f)

	return &bundlr.DecoderClosure{
		DecodeFunc: func(record interface{}) error {
			var err error
			switch v := record.(type) {
			case *[]string:
				*v, err = r.Read()
				return err
			default:
				return errors.New("unexpected dest")
			}
		},
		CloseFunc: func() error {
			return f.Close()
		},
	}, nil
})
