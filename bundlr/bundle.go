package bundlr

import (
	"encoding/json"
	"os"
	"sort"
	"sync"

	"github.com/spf13/afero"
)

const (
	DataDir                   = "data"
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
	manifest      *Manifest
	filter        SourceFilter
}

func OpenBundle(fs afero.Fs, path string) (*Bundle, error) {
	fs = afero.NewBasePathFs(fs, path)
	return &Bundle{
		fs:            fs,
		dataFs:        afero.NewBasePathFs(fs, DataDir),
		encoderMaker:  plainTextEncoderMaker,
		decoderMaker:  plainTextDecoderMaker,
		fileExtension: DefaultDataFileExt,
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
	return b.FS().RemoveAll("")
}

func (b *Bundle) FS() afero.Fs {
	return b.fs
}

func (b *Bundle) DataFS() afero.Fs {
	return b.dataFs
}

func (b *Bundle) lsDataDir() ([]os.FileInfo, error) {
	files, err := afero.ReadDir(b.DataFS(), "")
	if err != nil {
		return files, err
	}

	if b.filter != nil {
		files = b.filter.Filter(files)
	}

	return files, nil
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
	return b.FS().MkdirAll(DataDir, 0700)
}

func (b *Bundle) Manifest() (*Manifest, error) {
	if b.manifest != nil {
		return b.manifest, nil
	}

	manifestExists, err := afero.Exists(b.FS(), manifestFilename)
	if err != nil {
		return nil, err
	}
	if !manifestExists {
		return &Manifest{}, nil
	}

	var manifest Manifest
	rawManifest, err := afero.ReadFile(b.FS(), manifestFilename)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rawManifest, &manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

func (b *Bundle) WriteManifest(manifest *Manifest) error {
	rawManifest, err := json.Marshal(manifest)
	if err != nil {
		return err
	}

	if err := afero.WriteFile(b.FS(), manifestFilename, rawManifest, 600); err != nil {
		return err
	}

	b.manifest = manifest

	return nil
}

func (b *Bundle) WithFilter(allowList []string, denyList []string) *Bundle {
	b.rw.Lock()
	b.filter = NewFilter(allowList, denyList)
	b.rw.Unlock()
	return b
}
