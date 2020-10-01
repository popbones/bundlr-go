package bundlr

import (
	"bufio"
	"errors"
	"io"
)

var plainTextEncoderMaker = (EncoderMakerFunc)(func(f File) (Encoder, error) {
	return &EncoderClosure{
		EncodeFunc: func(record interface{}) error {
			switch v := record.(type) {
			case []byte:
				_, err := f.Write(v)
				return err
			case string:
				_, err := f.Write([]byte(v))
				return err
			default:
				return errors.New("unexpected input")
			}
		},
		CloseFunc: func() error {
			return f.Close()
		},
	}, nil
})

var plainTextDecoderMaker = (DecoderMakerFunc)(func(f File) (Decoder, error) {
	scanner := bufio.NewScanner(f)
	return &DecoderClosure{
		DecodeFunc: func(record interface{}) error {
			if scanner.Scan() {
				switch v := record.(type) {
				case *[]byte:
					*v = scanner.Bytes()
				case *string:
					*v = scanner.Text()
				default:
					return errors.New("unexpected input")
				}
				return nil
			}
			return io.EOF
		},
		CloseFunc: func() error {
			return f.Close()
		},
	}, nil
})
