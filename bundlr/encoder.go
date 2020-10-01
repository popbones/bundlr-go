package bundlr

type Encoder interface {
	Extension() string
	Encode(record interface{}) error
	Close() error
}

type EncoderClosure struct {
	ExtensionString string
	EncodeFunc      func(record interface{}) error
	CloseFunc       func() error
}

func (c *EncoderClosure) Extension() string {
	if c.ExtensionString == "" {
		return DefaultDataFileExt
	}
	return c.ExtensionString
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
