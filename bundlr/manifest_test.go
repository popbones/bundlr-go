package bundlr

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestBundle_Manifest(t *testing.T) {
	const version = "1.0"

	bundle, err := OpenBundle(afero.NewMemMapFs(), "/tmp/")
	if err != nil {
		t.Fatal(err)
	}

	manifest, err := bundle.Manifest()
	assert.Nil(t, err)
	assert.Equal(t, &Manifest{}, manifest)

	manifest.Version = version
	manifest.UserData = UserData{
		"codec": "csv",
	}

	assert.NoError(t, bundle.WriteManifest(manifest))

	manifest, err = bundle.Manifest()
	assert.Nil(t, err)
	assert.NotNil(t, manifest)
	assert.Equal(t, version, manifest.Version)
	assert.Equal(t, "csv", manifest.UserData["codec"])
}
