package bundlr

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBundle_WriteReadDelete(t *testing.T) {
	const numOfRecords = 10 * 2

	bundle, err := NewBundle("test.bundle")
	assert.NoError(t, err, "NewBundle(...)")

	// Write
	writer, err := bundle.WriterWithPartSize(2)
	assert.NoError(t, err, "bundle.Writer()")

	for i := 0; i < numOfRecords; i++ {
		assert.NoError(t, writer.Write(fmt.Sprintf("testdata_%05d\n", i)), "writer.Write(...)")
	}
	assert.NoError(t, writer.Close(), "writer.Close()")

	// Read
	reader, err := bundle.Reader()
	assert.NoError(t, err, "bundle.Reader()")

	count := 0
	for {
		var s string
		if err := reader.Read(&s); err == io.EOF {
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
