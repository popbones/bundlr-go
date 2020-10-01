package csv

import (
	"fmt"
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/popbones/bundlr-go/bundlr"
)

func TestWriteReadDelete(t *testing.T) {
	const numOfRecords = 10 * 2

	bundle, err := bundlr.OpenBundle(afero.NewOsFs(), "test.bundle")
	assert.NoError(t, err, "OpenBundle(...)")
	bundle = bundle.WithFileExtension("csv").
		WithEncoderMaker(EncoderMaker).WithDecoderMaker(DecoderMaker)

	// Write
	writer, err := bundle.WriterWithPartSize(2)
	assert.NoError(t, err, "bundle.Writer()")

	for i := 0; i < numOfRecords; i++ {
		assert.NoError(t, writer.Write([]string{"foo", "bar", fmt.Sprintf("%05d", i)}), "writer.Write(...)")
	}
	assert.NoError(t, writer.Close(), "writer.Close()")

	// Read
	reader, err := bundle.Reader()
	assert.NoError(t, err, "bundle.Reader()")

	count := 0
	for {
		var s []string
		if err := reader.Read(&s); err == io.EOF {
			break
		}
		assert.NoError(t, err, "reader.Read(&s)")
		assert.Len(t, s, 3)
		assert.Equal(t, []string{"foo", "bar", fmt.Sprintf("%05d", count)}, s)
		count++
	}
	assert.Equal(t, numOfRecords, count, "%d records written, but %d records read", numOfRecords, count)
	assert.NoError(t, reader.Close(), "reader.Close()")

	// Delete
	assert.NoError(t, bundle.Delete(), "bundle.Delete()")
}
