package bundlr

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type Reader struct {
	bundle    *Bundle
	decoder   Decoder
	totalRead uint64
	partRead  uint64
	files     []os.FileInfo
	fileIndex int
	closed    bool
	mu        sync.RWMutex
}

func NewReader(b *Bundle) (*Reader, error) {
	if b == nil {
		return nil, ErrNilBundle
	}

	b.rw.Lock()

	files, err := b.lsDataDirSorted()
	if err != nil {
		files, err = b.lsDataDir()
		if err != nil {
			return nil, err
		}
	}

	r := &Reader{
		bundle:    b,
		files:     files,
		fileIndex: -1,
	}

	return r, nil
}

func (r *Reader) Read(dst interface{}) error {
	r.mu.Lock()
	if err := r.getNextDecoderIfNeeded(); err != nil {
		r.mu.Unlock()
		return err
	}

	err := r.decoder.Decode(dst)
	if err == nil {
		r.mu.Unlock()
		atomic.AddUint64(&r.totalRead, 1)
		atomic.AddUint64(&r.partRead, 1)
		return nil
	}

	if err != io.EOF {
		r.mu.Unlock()
		return err
	}

	// We got EOF from the previous read, we need to try to read again
	// from the next file
	if err := r.decoder.Close(); err != nil {
		r.mu.Unlock()
		return err
	}
	r.decoder = nil
	if err := r.getNextDecoderIfNeeded(); err != nil {
		r.mu.Unlock()
		return err
	}

	if err = r.decoder.Decode(dst); err != nil {
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()

	atomic.AddUint64(&r.totalRead, 1)
	atomic.AddUint64(&r.partRead, 1)
	return nil
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer func() {
		r.closed = true
		r.bundle.rw.Unlock()
		r.mu.Unlock()
	}()
	if r.decoder != nil {
		return r.decoder.Close()
	}
	return nil
}

func (r *Reader) getNextDecoderIfNeeded() error {
	if r.decoder == nil {
		r.fileIndex++

		if len(r.files) == 0 || r.fileIndex >= len(r.files) {
			return io.EOF
		}

		f, err := r.bundle.DataFS().Open(r.files[r.fileIndex].Name())
		if err != nil {
			return err
		}

		decoder, err := r.bundle.decoderMaker.Make(f)
		if err != nil {
			if err := f.Close(); err != nil {
				return err
			}
			return err
		}
		r.decoder = decoder
	}
	return nil
}

func (r *Reader) reset() error {
	// TODO: implement this
	return nil
}
