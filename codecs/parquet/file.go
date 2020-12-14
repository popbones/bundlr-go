package parquet

import (
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
	"github.com/xitongsys/parquet-go/source"

	"github.com/popbones/bundlr-go/bundlr"
)

type parquetFile struct {
	bundlr.File
	afero.Fs
}

func NewParquetFile(fs afero.Fs, file bundlr.File) source.ParquetFile {
	return &parquetFile{
		File: file,
		Fs:   fs,
	}
}

func (f *parquetFile) Open(name string) (source.ParquetFile, error) {
	if name == "" {
		_, name = filepath.Split(f.File.Name())
	}

	nf, err := f.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return &parquetFile{
		File: nf,
		Fs:   f.Fs,
	}, nil
}

func (f *parquetFile) Create(name string) (source.ParquetFile, error) {
	nf, err := f.Fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &parquetFile{
		File: nf,
		Fs:   f.Fs,
	}, nil
}

func (f *parquetFile) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekEnd {
		if offset == 0 {
			return f.File.Seek(offset, whence)
		}
		// We need to determine how f.File.Seek actually implement the Seek function as io.Seeker does not really
		// specify a behavior for negative offsets. We do this by seek twice and compare the result.
		var seek1, seek2 int64
		var err error
		seek1, err = f.File.Seek(offset, whence)
		if err != nil {
			return seek1, err
		}
		seek2, err = f.File.Seek(-offset, whence)
		if err != nil {
			return seek2, err
		}

		if seek2 <= seek1 {
			return seek2, err
		} else {
			return f.File.Seek(offset, whence)
		}
	}
	return f.File.Seek(offset, whence)
}
