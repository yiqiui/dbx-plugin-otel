package main

import (
	"encoding/json"
	"os"
	"testing"
)

// The release version lives in three places: manifest.json (what DBX loads and
// shows in the plugin center), package.json (the npm-side project metadata) and
// pluginVersion above, which is the identity the sidecar reports in its
// handshake. DBX refuses to initialize a plugin whose backend identity does not
// match the manifest it loaded, so a version bump that misses main.go ships a
// package that opens with
// "Plugin backend identity 'x/0.2.1' does not match manifest 'x/0.2.2'".
func TestReleaseVersionStaysInSync(t *testing.T) {
	for _, file := range []string{"../manifest.json", "../package.json"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		var doc struct {
			Version string `json:"version"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		if doc.Version != pluginVersion {
			t.Errorf("%s version %q does not match backend pluginVersion %q; bump all three together",
				file, doc.Version, pluginVersion)
		}
	}
}
