package bearertoken

import (
	"errors"
	"strings"
)

// ValidatePattern permits exact names, a single trailing *, or the universal *.
func ValidatePattern(pattern string) error {
	if pattern == "*" {
		return nil
	}
	s := strings.TrimSuffix(pattern, "*")
	if s == "" || s == "." || strings.HasPrefix(s, "__") || strings.Contains(s, "..") {
		return errors.New("invalid namespace pattern")
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return errors.New("invalid namespace character")
		}
	}
	return nil
}

func exact(ns string) bool { return !strings.Contains(ns, "*") && ValidatePattern(ns) == nil }

// Allows checks an exact namespace; invalid patterns fail closed.
func Allows(patterns []string, ns string) bool {
	if !exact(ns) {
		return false
	}
	allowed := false
	for _, p := range patterns {
		if ValidatePattern(p) != nil {
			return false
		}
		if p == ns || strings.HasSuffix(p, "*") && strings.HasPrefix(ns, strings.TrimSuffix(p, "*")) {
			allowed = true
		}
	}
	return allowed
}

// Contains checks structural set containment, not whether pattern text matches.
func Contains(ceiling, requested []string) bool {
	for _, p := range ceiling {
		if ValidatePattern(p) != nil {
			return false
		}
	}
	for _, r := range requested {
		if ValidatePattern(r) != nil {
			return false
		}
		covered := false
		for _, p := range ceiling {
			if p == r || strings.HasSuffix(p, "*") && strings.HasPrefix(strings.TrimSuffix(r, "*"), strings.TrimSuffix(p, "*")) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}
