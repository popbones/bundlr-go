package bundlr

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrWriterClosed = errors.New("writer closed")

type Writer struct {
	bundle       *Bundle
	encoder      Encoder
	totalWritten uint64
	partWritten  uint64
	partSize     uint64
	partIndex    int
	closed       bool
	mu           sync.RWMutex
}

func NewWriter(b *Bundle, n uint64) (*Writer, error) {
	if b == nil {
		return nil, ErrNilBundle
	}
	// Lock bundle until the the writer is closed
	b.rw.Lock()

	_ = b.ensureDataDir()

	// Check whats the next file to write to
	files, err := b.lsDataDirSorted()
	if err != nil {
		//b.rw.Unlock()
		//return nil, err
	}

	w := &Writer{
		bundle:    b,
		partSize:  n,
		partIndex: -1,
	}

	if len(files) != 0 {
		w.partIndex = parsePartIndexFromFileName(files[len(files)-1].Name())
	}

	if err := w.getNewEncoderIfNeeded(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Writer) Write(record interface{}) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrWriterClosed
	}

	if err := w.getNewEncoderIfNeeded(); err != nil {
		w.mu.Unlock()
		return err
	}

	if err := w.encoder.Encode(record); err != nil {
		w.mu.Unlock()
		return err
	}
	w.mu.Unlock()

	atomic.AddUint64(&w.totalWritten, 1)
	atomic.AddUint64(&w.partWritten, 1)
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer func() {
		w.closed = true
		w.bundle.rw.Unlock()
		w.mu.Unlock()
	}()
	if w.encoder != nil {
		return w.encoder.Close()
	}
	return w.reset()
}

// getNewEncoderIfNeeded rotates the encoder field. The caller must handle any locking
func (w *Writer) getNewEncoderIfNeeded() error {
	if w.encoder == nil || w.partWritten >= w.partSize {
		w.partWritten = 0
		if w.encoder != nil {
			if err := w.encoder.Close(); err != nil {
				return err
			}
		}

		// TODO: initialise a new encoder
		f, err := w.nextFile()
		if err != nil {
			return err
		}

		encoder, err := w.bundle.encoderMaker.Make(f)
		if err != nil {
			if err := f.Close(); err != nil {
				return err
			}
			return err
		}
		w.encoder = encoder
	}
	return nil
}

func (w *Writer) nextFileName() string {
	w.partIndex++
	return w.currentFileName()
}

func (w *Writer) currentFileName() string {
	return fmt.Sprintf(DefaultDataFileNameFormat, w.partIndex, w.bundle.fileExtension)
}

func (w *Writer) nextFile() (File, error) {
	f, err := w.bundle.DataFS().Create(w.nextFileName())
	return f, err
}

func (w *Writer) reset() error {
	// TODO: implement this
	return nil
}
