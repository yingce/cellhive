// Package cell defines the cell identity and protocol value types.
package cell

import (
	"fmt"
	"strings"
)

// Scope is the stable, persistent identity of a cell:
//
//	<namespace>/<class>/<id>
//
// A Durable Object is a cell whose class is the user class name; KV/D1/Queue/
// Workflows/Cron use reserved classes (e.g. "__kv__").
type Scope struct {
	Namespace string
	Class     string
	ID        string
}

// ErrInvalidScope is returned by ParseScope for malformed input.
var ErrInvalidScope = fmt.Errorf("invalid cell scope")

// reservedClassPrefix marks platform-owned classes.
const reservedClassPrefix = "__"

// IsReservedClass reports whether the class is platform-owned.
func (s Scope) IsReservedClass() bool {
	return strings.HasPrefix(s.Class, reservedClassPrefix)
}

// String renders the canonical "<ns>/<class>/<id>" form.
func (s Scope) String() string {
	return s.Namespace + "/" + s.Class + "/" + s.ID
}

// Key returns the bucket key prefix for the cell.
func (s Scope) Key() string {
	return "cells/" + s.String()
}

// ParseScope parses "<ns>/<class>/<id>".
func ParseScope(s string) (Scope, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 3 {
		return Scope{}, fmt.Errorf("%w: %q", ErrInvalidScope, s)
	}
	sc := Scope{Namespace: parts[0], Class: parts[1], ID: parts[2]}
	if err := sc.Validate(); err != nil {
		return Scope{}, err
	}
	return sc, nil
}

// Validate checks non-empty, safe components.
func (s Scope) Validate() error {
	for _, part := range []string{s.Namespace, s.Class, s.ID} {
		// Reject path traversal: scope components become directory names.
		if part == "." || part == ".." || strings.Contains(part, "..") {
			return fmt.Errorf("cell: invalid scope component %q", part)
		}
	}
	for _, p := range []string{s.Namespace, s.Class, s.ID} {
		if p == "" {
			return fmt.Errorf("%w: empty component", ErrInvalidScope)
		}
		if strings.ContainsAny(p, "/\\") {
			return fmt.Errorf("%w: component contains separator", ErrInvalidScope)
		}
	}
	return nil
}
