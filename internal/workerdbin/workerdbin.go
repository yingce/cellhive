// Package workerdbin locates the pinned workerd binary for the platform
// runtimes (user-runtime, do-runtime).
package workerdbin

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// PinnedVersion is the stock workerd release CellHive is built and tested
// against (ADR-001). The compatibility flags/date in wranglercompat pair with
// it, and tests assert the two stay in sync (ADR-153).
const PinnedVersion = "1.20260615.1"

// Find locates the workerd binary: CELLHIVE_WORKERD, PATH, then the local dev
// package store (PinnedVersion preferred).
func Find() (string, error) {
	if p := os.Getenv("CELLHIVE_WORKERD"); p != "" {
		return p, nil
	}
	if p, err := exec.LookPath("workerd"); err == nil {
		return p, nil
	}
	matches, _ := filepath.Glob("/opt/vwork/node_modules/.pnpm/@cloudflare+workerd-linux-64@*/node_modules/@cloudflare/workerd-linux-64/bin/workerd")
	if len(matches) > 0 {
		sort.Strings(matches)
		for _, m := range matches {
			if strings.Contains(m, PinnedVersion) {
				return m, nil
			}
		}
		return matches[len(matches)-1], nil
	}
	return "", errors.New("workerdbin: workerd not found (set CELLHIVE_WORKERD)")
}
