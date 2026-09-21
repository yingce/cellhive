// Package runtimeenv builds the complete, intentionally small environment for
// a workerd child process. It must never be combined with os.Environ().
package runtimeenv

import (
	"fmt"
	"sort"
	"strings"
)

// Build validates values and returns a fresh, deterministically sorted list of
// KEY=value entries. Empty values remain present because workerd's
// fromEnvironment bindings require the variable to exist.
func Build(values map[string]string) ([]string, error) {
	keys := make([]string, 0, len(values))
	for key, value := range values {
		if !validName(key) {
			return nil, fmt.Errorf("runtimeenv: invalid environment name %q", key)
		}
		if strings.IndexByte(value, 0) >= 0 {
			return nil, fmt.Errorf("runtimeenv: value for %s contains NUL", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, key+"="+values[key])
	}
	return env, nil
}

// Require verifies that env is a well-formed explicit environment and contains
// every fromEnvironment variable named by a runtime config. Values may be empty
// when the corresponding feature is optional, but the key must be intentional.
func Require(env []string, names ...string) error {
	present := make(map[string]struct{}, len(env))
	for _, entry := range env {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || !validName(name) {
			return fmt.Errorf("runtimeenv: invalid environment entry %q", entry)
		}
		if _, duplicate := present[name]; duplicate {
			return fmt.Errorf("runtimeenv: duplicate environment name %q", name)
		}
		present[name] = struct{}{}
	}
	for _, name := range names {
		if _, ok := present[name]; !ok {
			return fmt.Errorf("runtimeenv: required environment %s is missing", name)
		}
	}
	return nil
}

func validName(name string) bool {
	if name == "" || !upperOrUnderscore(name[0]) {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !upperOrUnderscore(c) && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func upperOrUnderscore(c byte) bool {
	return c == '_' || c >= 'A' && c <= 'Z'
}
