package bundlr

type Decoder interface {
	Decode(record interface{}) error
	Close() error
}

type DecoderClosure struct {
	DecodeFunc func(record interface{}) error
	CloseFunc  func() error
}

func (c *DecoderClosure) Decode(record interface{}) error {
	return c.DecodeFunc(record)
}

func (c *DecoderClosure) Close() error {
	return c.CloseFunc()
}

type DecoderMaker interface {
	Make(File) (Decoder, error)
}

type DecoderMakerFunc func(f File) (Decoder, error)

func (m DecoderMakerFunc) Make(f File) (Decoder, error) {
	return m(f)
}
