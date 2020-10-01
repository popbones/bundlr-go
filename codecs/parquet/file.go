package parquet

import (
	"os"

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
		name = f.File.Name()
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
