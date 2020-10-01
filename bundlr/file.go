package bundlr

import "io"

type File interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}
