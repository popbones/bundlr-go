package bundlr

import (
	"os"
	"sort"
	"sync"

	"github.com/spf13/afero"
)

const DataDir = "Data"
const DefaultDataFileNameFormat = "part-%05d.%s"
const DefaultDataFileExt = "dat"

type Bundle struct {
	rw           sync.RWMutex
	fs           afero.Fs
	dataFs       afero.Fs
	Path         string
	EncoderMaker EncoderMaker
	DecoderMaker DecoderMaker
}

func NewBundle(path string) (*Bundle, error) {
	// TODO: parse the path and figure out the source fs
	fs := afero.NewBasePathFs(afero.NewOsFs(), path)
	return &Bundle{
		fs:           fs,
		dataFs:       afero.NewBasePathFs(fs, DataDir),
		Path:         path,
		EncoderMaker: plainTextEncoderMaker,
		DecoderMaker: plainTextDecoderMaker,
	}, nil
}

func (b *Bundle) Writer() (*Writer, error) {
	return b.WriterWithPartSize(DefaultPartSize)
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
