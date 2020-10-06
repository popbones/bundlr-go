package parquet

import (
	"fmt"
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/popbones/bundlr-go/bundlr"
)

type Student struct {
	Name string `parquet:"name=name, type=UTF8"`
	Age  int32  `parquet:"name=age, type=INT32"`
}

func TestWriteReadDelete(t *testing.T) {
	const numOfRecords = 10 * 2

	fs := afero.NewOsFs()
	bundle, err := bundlr.OpenBundle(fs, "parquetTest.bundle")
	assert.NoError(t, err, "OpenBundle(...)")
	bundle = ConfigBundle(bundle, new(Student))

	// Write
	writer, err := bundle.WriterWithPartSize(2)
	assert.NoError(t, err, "bundle.Writer()")

	for i := 0; i < numOfRecords; i++ {
		st := Student{
			Name: fmt.Sprintf("student%d", i),
			Age:  int32(i % 18),
		}
		assert.NoError(t, writer.Write(st), "writer.Write(...)")
	}
	assert.NoError(t, writer.Close(), "writer.Close()")

	// Read
	reader, err := bundle.Reader()
	assert.NoError(t, err, "bundle.Reader()")

	count := 0
	for {
		st := make([]Student, 1)
		err := reader.Read(&st)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err, "reader.Read(&s)")

		s := st[0]
		assert.Equal(t, int32(count%18), s.Age)
		assert.Equal(t, fmt.Sprintf("student%d", count), s.Name)
		count++
	}
	assert.Equal(t, numOfRecords, count, "%d records written, but %d records read", numOfRecords, count)
	assert.NoError(t, reader.Close(), "reader.Close()")

	// Delete
	assert.NoError(t, bundle.Delete(), "bundle.Delete()")
}
