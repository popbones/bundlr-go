package bundlr

const (
	manifestFilename = "manifest.json"

	// Manifest schema version
	ManifestVersion = "1.0"
)

// UserData allows to store any KV information
type UserData map[string]interface{}

// Manifest stores bundle metadata
// It is written as a json file at the root level of the bundle
type Manifest struct {
	Version  string   `json:"version"`
	UserData UserData `json:"userdata"`
}
