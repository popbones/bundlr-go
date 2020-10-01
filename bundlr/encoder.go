package bundlr

type Encoder interface {
	Encode(record interface{}) error
	Close() error
}

type EncoderClosure struct {
	EncodeFunc func(record interface{}) error
	CloseFunc  func() error
}

func (c *EncoderClosure) Encode(record interface{}) error {
	return c.EncodeFunc(record)
}

func (c *EncoderClosure) Close() error {
	return c.CloseFunc()
}

type EncoderMaker interface {
	Make(File) (Encoder, error)
}

type EncoderMakerFunc func(f File) (Encoder, error)

func (m EncoderMakerFunc) Make(f File) (Encoder, error) {
	return m(f)
}
