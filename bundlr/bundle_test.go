package bundlr

import (
	"fmt"
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestBundle_WriteReadDelete(t *testing.T) {
	const numOfRecords = 10 * 2

	bundle, err := OpenBundle(afero.NewOsFs(), "test.bundle")
	assert.NoError(t, err, "OpenBundle(...)")
	bundle = bundle.WithFileExtension("txt").WithPartSize(10)

	// Write
	writer, err := bundle.WriterWithPartSize(2)
	assert.NoError(t, err, "bundle.Writer()")

	for i := 0; i < numOfRecords; i++ {
		assert.NoError(t, writer.Write(fmt.Sprintf("testdata_%05d", i)), "writer.Write(...)")
	}
	assert.NoError(t, writer.Close(), "writer.Close()")

	// Read
	reader, err := bundle.Reader()
	assert.NoError(t, err, "bundle.Reader()")

	count := 0
	for {
		var s string
		err := reader.Read(&s)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err, "reader.Read(&s)")
		assert.Equal(t, fmt.Sprintf("testdata_%05d", count), s)
		count++
	}
	assert.Equal(t, numOfRecords, count, "%d records written, but %d records read", numOfRecords, count)
	assert.NoError(t, reader.Close(), "reader.Close()")

	// Delete
	assert.NoError(t, bundle.Delete(), "bundle.Delete()")
}
