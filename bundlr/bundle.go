package bundlr

import (
	"os"
	"sort"
	"sync"

	"github.com/spf13/afero"
)

const (
	DataDir                   = "Data"
	DefaultDataFileNameFormat = "part-%05d.%s"
	DefaultDataFileExt        = "dat"
	DefaultPartSize           = 1000 * 1000
)

type Bundle struct {
	rw            sync.RWMutex
	fs            afero.Fs
	dataFs        afero.Fs
	encoderMaker  EncoderMaker
	decoderMaker  DecoderMaker
	fileExtension string // Default file extension to use when writing
	partSize      uint64 // Default partition size when writing
}

func OpenBundle(fs afero.Fs, path string) (*Bundle, error) {
	fs = afero.NewBasePathFs(fs, path)
	return &Bundle{
		fs:            fs,
		dataFs:        afero.NewBasePathFs(fs, DataDir),
		encoderMaker:  plainTextEncoderMaker,
		decoderMaker:  plainTextDecoderMaker,
		fileExtension: "txt",
		partSize:      DefaultPartSize,
	}, nil
}

func (b *Bundle) WithEncoderMaker(maker EncoderMaker) *Bundle {
	b.rw.Lock()
	b.encoderMaker = maker
	b.rw.Unlock()
	return b
}

func (b *Bundle) WithDecoderMaker(maker DecoderMaker) *Bundle {
	b.rw.Lock()
	b.decoderMaker = maker
	b.rw.Unlock()
	return b
}

func (b *Bundle) WithPartSize(n uint64) *Bundle {
	b.rw.Lock()
	b.partSize = n
	b.rw.Unlock()
	return b
}

func (b *Bundle) WithFileExtension(ext string) *Bundle {
	b.rw.Lock()
	b.fileExtension = ext
	b.rw.Unlock()
	return b
}

func (b *Bundle) Writer() (*Writer, error) {
	return b.WriterWithPartSize(b.partSize)
}

func (b *Bundle) WriterWithPartSize(n uint64) (*Writer, error) {
	return NewWriter(b, n)
}

func (b *Bundle) Reader() (*Reader, error) {
	return NewReader(b)
}

func (b *Bundle) Delete() error {
	b.rw.Lock()
	defer b.rw.Unlock()
	return b.fs.RemoveAll("")
}

func (b *Bundle) lsDataDir() ([]os.FileInfo, error) {
	return afero.ReadDir(b.fs, DataDir)
}

func (b *Bundle) lsDataDirSorted() ([]os.FileInfo, error) {
	fileInfos, err := b.lsDataDir()
	if err != nil {
		return nil, err
	}
	sortableFileInfos := sortableDataFileInfos(fileInfos)
	sort.Stable(sortableFileInfos)
	return fileInfos, nil
}

func (b *Bundle) ensureDataDir() error {
	return b.fs.MkdirAll(DataDir, 0700)
}
