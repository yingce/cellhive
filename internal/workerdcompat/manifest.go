// Package workerdcompat exposes generated compatibility metadata for the exact
// stock workerd release pinned by CellHive.
package workerdcompat

import (
	_ "embed"
	"encoding/json"
)

//go:embed manifest.json
var manifestJSON []byte

type Data struct {
	WorkerdVersion       string `json:"workerd_version"`
	UpstreamRevision     string `json:"upstream_revision"`
	SourceSHA256         string `json:"source_sha256"`
	MaxCompatibilityDate string `json:"max_compatibility_date"`
	Flags                []Flag `json:"flags"`
}

type Flag struct {
	Name            string `json:"name"`
	Experimental    bool   `json:"experimental"`
	CellHiveAllowed bool   `json:"cellhive_allowed"`
}

var (
	manifest = mustManifest()
	allowed  = allowedFlags(manifest.Flags)
)

func mustManifest() Data {
	var data Data
	if err := json.Unmarshal(manifestJSON, &data); err != nil {
		panic("workerdcompat: invalid embedded manifest: " + err.Error())
	}
	return data
}

func allowedFlags(flags []Flag) map[string]bool {
	out := make(map[string]bool, len(flags))
	for _, flag := range flags {
		if flag.CellHiveAllowed && !flag.Experimental {
			out[flag.Name] = true
		}
	}
	return out
}

func Manifest() Data {
	data := manifest
	data.Flags = append([]Flag(nil), manifest.Flags...)
	return data
}

func Allowed(flag string) bool { return allowed[flag] }

func MaxCompatibilityDate() string { return manifest.MaxCompatibilityDate }
