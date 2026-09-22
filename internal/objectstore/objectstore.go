// Package objectstore is the generic, prefix-scoped view over an object bucket.
// It is intentionally service-agnostic: any feature that keeps files under a
// bucket prefix (bundles, assets, capture manifests, caches, exports) builds on
// Objects instead of touching bucket.Bucket directly, so key-safety rules
// (prefix allow-list + traversal defence) live in exactly one place.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"cellhive/internal/bucket"
)

// ErrBadKey means a key is unsafe: outside the configured prefix, absolute, or
// containing traversal/backslash segments.
var ErrBadKey = errors.New("objectstore: unsafe key")

// Reserved top-level prefixes in the shared bucket. The bucket is a single
// namespace shared by every subsystem, so each prefix is owned by exactly one
// component. Create a store over a reserved prefix only via NewOwned with the
// matching owner; NewObjects refuses reserved prefixes outright.
const (
	PrefixCells      = "cells/"        // cell replication segments/owner records (replica)
	PrefixNodes      = "nodes/"        // node-log recovery records
	PrefixFleet      = "fleet/"        // waker election
	PrefixBundles    = "bundles/"      // artifacts: content-addressed worker bundles
	PrefixAssets     = "assets/"       // artifacts: versioned static assets
	PrefixSupervisor = "dosupervisor/" // do-supervisor capture manifest + sidecars
	PrefixWake       = "wake/"         // due-timer wake index (waker)
	PrefixR2         = "r2/"           // R2 binding objects
	PrefixR2Meta     = "r2meta/"       // R2 binding object metadata sidecars
)

// Owners of the reserved prefixes.
const (
	OwnerReplica    = "replica"
	OwnerNodeLog    = "node-log"
	OwnerWaker      = "waker"
	OwnerArtifacts  = "artifacts"
	OwnerSupervisor = "do-supervisor"
	OwnerR2         = "r2"
)

var reserved = []struct{ Prefix, Owner string }{
	{PrefixCells, OwnerReplica},
	{PrefixNodes, OwnerNodeLog},
	{PrefixFleet, OwnerWaker},
	{PrefixBundles, OwnerArtifacts},
	{PrefixAssets, OwnerArtifacts},
	{PrefixSupervisor, OwnerSupervisor},
	{PrefixWake, OwnerWaker},
	{PrefixR2, OwnerR2},
	{PrefixR2Meta, OwnerR2},
}

// normalizePrefix guarantees a single trailing slash so a prefix cannot match a
// sibling (e.g. "fleet/" vs "fleet-old/").
func normalizePrefix(prefix string) string {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

// OwnerOf returns the owner subsystem of a reserved prefix, or "" if the prefix
// is not reserved.
func OwnerOf(prefix string) string {
	p := normalizePrefix(prefix)
	for _, r := range reserved {
		if r.Prefix == p {
			return r.Owner
		}
	}
	return ""
}

// ReservedPrefixes returns every reserved prefix (copy).
func ReservedPrefixes() []string {
	out := make([]string, 0, len(reserved))
	for _, r := range reserved {
		out = append(out, r.Prefix)
	}
	return out
}

// Objects is a prefix-scoped view over a bucket. Get/Put/Delete/PresignGet/List
// reject any key that is not under Prefix.
type Objects struct {
	B      bucket.Bucket
	Prefix string

	owner string
	err   error
}

// NewObjects creates a prefix-scoped object view for a NON-reserved prefix. A
// reserved prefix (cells/, nodes/, fleet/, bundles/, assets/, dosupervisor/)
// must be opened via NewOwned by its owning subsystem; the returned store then
// fails every operation with a clear error.
func NewObjects(b bucket.Bucket, prefix string) *Objects {
	o := &Objects{B: b, Prefix: normalizePrefix(prefix)}
	if owner := OwnerOf(o.Prefix); owner != "" {
		o.err = fmt.Errorf("objectstore: prefix %q is reserved for %q; use NewOwned(b, %q, %q)", o.Prefix, owner, o.Prefix, owner)
	}
	return o
}

// NewOwned creates a store over a reserved prefix, but only for its owner.
func NewOwned(b bucket.Bucket, prefix, owner string) *Objects {
	o := &Objects{B: b, Prefix: normalizePrefix(prefix), owner: owner}
	if want := OwnerOf(o.Prefix); want != "" && want != owner {
		o.err = fmt.Errorf("objectstore: prefix %q is owned by %q, not %q", o.Prefix, want, owner)
	}
	return o
}

func (o *Objects) key(key string) (string, error) {
	if o.err != nil {
		return "", o.err
	}
	if o.B == nil || o.Prefix == "" || !strings.HasPrefix(key, o.Prefix) {
		return "", ErrBadKey
	}
	if strings.Contains(key, "..") || strings.Contains(key, "\\") || strings.HasPrefix(key, "/") {
		return "", ErrBadKey
	}
	return key, nil
}

// Get returns an object's bytes.
func (o *Objects) Get(ctx context.Context, key string) ([]byte, error) {
	k, err := o.key(key)
	if err != nil {
		return nil, err
	}
	data, _, err := o.B.Get(ctx, k)
	return data, err
}

// Stat returns an object's size and etag through a point lookup. It never
// falls back to List or a full-body read when the backend lacks Stat support.
func (o *Objects) Stat(ctx context.Context, key string) (int64, string, error) {
	k, err := o.key(key)
	if err != nil {
		return 0, "", err
	}
	statter, ok := o.B.(bucket.Statter)
	if !ok {
		return 0, "", bucket.ErrNotSupported
	}
	return statter.Stat(ctx, k)
}

// Put stores an object and returns its etag.
func (o *Objects) Put(ctx context.Context, key string, data []byte) (string, error) {
	k, err := o.key(key)
	if err != nil {
		return "", err
	}
	return o.B.Put(ctx, k, data)
}

// Delete removes an object (missing key is not an error).
func (o *Objects) Delete(ctx context.Context, key string) error {
	k, err := o.key(key)
	if err != nil {
		return err
	}
	return o.B.Delete(ctx, k)
}

// PresignGet returns a short-lived read URL for an object.
func (o *Objects) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	k, err := o.key(key)
	if err != nil {
		return "", err
	}
	return o.B.PresignGet(ctx, k, ttl)
}

// List returns keys under the prefix (operator/diagnostic use only; never on a
// hot path).
func (o *Objects) List(ctx context.Context) ([]string, error) {
	if o.err != nil {
		return nil, o.err
	}
	return o.B.List(ctx, o.Prefix)
}

// ListPrefix lists keys under a sub-prefix that must stay within this store's
// prefix (cold path; used by delete/cleanup).
// ListPage returns one cursor page of object keys under prefix (which must sit
// inside the owned prefix). It uses the bucket's PagedLister when available so
// large prefixes are not materialised (ADR-169); the fallback slices a full
// List. next is the exclusive cursor ("" when exhausted).
func (o *Objects) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	if o.err != nil {
		return nil, "", o.err
	}
	if !strings.HasPrefix(prefix, o.Prefix) || strings.Contains(prefix, "..") {
		return nil, "", ErrBadKey
	}
	if after != "" && (!strings.HasPrefix(after, o.Prefix) || strings.Contains(after, "..")) {
		return nil, "", ErrBadKey
	}
	if limit <= 0 {
		limit = 1000
	}
	if pl, ok := o.B.(bucket.PagedLister); ok {
		return pl.ListPage(ctx, prefix, after, limit)
	}
	keys, err := o.B.List(ctx, prefix)
	if err != nil {
		return nil, "", err
	}
	sort.Strings(keys)
	out := make([]bucket.ObjectInfo, 0, limit)
	for _, k := range keys {
		if k <= after {
			continue
		}
		if len(out) == limit {
			return out, out[len(out)-1].Key, nil
		}
		out = append(out, bucket.ObjectInfo{Key: k})
	}
	return out, "", nil
}

func (o *Objects) ListPrefix(ctx context.Context, prefix string) ([]string, error) {
	if o.err != nil {
		return nil, o.err
	}
	if !strings.HasPrefix(prefix, o.Prefix) || strings.Contains(prefix, "..") {
		return nil, ErrBadKey
	}
	return o.B.List(ctx, prefix)
}
