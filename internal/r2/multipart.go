package r2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/bucket"
)

// Multipart upload limits (R2-compatible shape).
const (
	// MaxMultipartParts is the maximum number of parts in one upload.
	MaxMultipartParts = 10000
	// MaxMultipartBytes caps the assembled object size. Parts are assembled in
	// memory (the bucket contract has no streaming Put), so this bound also
	// bounds peak memory per complete call.
	MaxMultipartBytes = 512 << 20
)

// ErrBadUpload means the upload id, part number or object key is invalid.
var ErrBadUpload = errors.New("r2: bad multipart upload")

// ErrMultipartTooLarge means the assembled object would exceed MaxMultipartBytes.
var ErrMultipartTooLarge = errors.New("r2: multipart object too large")

// Part is one uploaded part reference (Cloudflare: {partNumber, etag}).
type Part struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
}

// multipartPrefix is the staging area for an upload's parts. It lives under
// "r2/.mpu/" (not under "r2/<ns>/<bucket>/") so user List never sees parts.
func multipartPrefix(ns, bucketName, uploadID string) (string, error) {
	if ns == "" || bucketName == "" || !isUploadID(uploadID) {
		return "", ErrBadUpload
	}
	if strings.ContainsAny(ns, "/\\") || strings.ContainsAny(bucketName, "/\\") {
		return "", ErrBadUpload
	}
	return fmt.Sprintf("r2/.mpu/%s/%s/%s/", ns, bucketName, uploadID), nil
}

func partKey(prefix string, part int) (string, error) {
	if part < 1 || part > MaxMultipartParts {
		return "", ErrBadUpload
	}
	return fmt.Sprintf("%s%05d", prefix, part), nil
}

// isUploadID reports whether s is a 32-char lowercase hex id (our minted format).
func isUploadID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// multipartRoot is the staging root; GC lists it to reclaim abandoned uploads.
const multipartRoot = "r2/.mpu/"

// defaultMultipartTTL is how long abandoned staged parts are retained.
const defaultMultipartTTL = 24 * time.Hour

const createMarkerName = "created"

// createMarkerKey is the per-upload creation-time marker object used by GC.
func createMarkerKey(prefix string) string { return prefix + createMarkerName }

// isCreateMarker reports whether key is a GC creation marker (not a part).
func isCreateMarker(key string) bool { return strings.HasSuffix(key, "/"+createMarkerName) }

// CreateMultipart mints a new upload id for (ns, bucket, key) and records its
// creation time so a future GC can reclaim it if the caller never completes or
// aborts.
func (s *Store) CreateMultipart(ctx context.Context, ns, bucketName, key string) (string, error) {
	if _, err := Key(ns, bucketName, key); err != nil {
		return "", err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b[:])
	p, err := multipartPrefix(ns, bucketName, id)
	if err != nil {
		return "", err
	}
	if _, err := s.B.Put(ctx, createMarkerKey(p), []byte(strconv.FormatInt(s.now().UnixMilli(), 10))); err != nil {
		return "", err
	}
	s.maybeGC(ctx)
	return id, nil
}

// maybeGC opportunistically reclaims stale multipart uploads, throttled to once
// per hour per process so create stays cheap.
func (s *Store) maybeGC(ctx context.Context) {
	s.mu.Lock()
	if !s.lastGC.IsZero() && s.now().Sub(s.lastGC) < time.Hour {
		s.mu.Unlock()
		return
	}
	s.lastGC = s.now()
	s.mu.Unlock()
	_, _ = s.GCStaleUploads(ctx, s.MultipartTTL)
}

// GCStaleUploads deletes staged parts of multipart uploads whose creation marker
// is older than ttl, returning the number of uploads reclaimed. Uploads without
// a marker are left alone (age cannot be proven).
func (s *Store) GCStaleUploads(ctx context.Context, ttl time.Duration) (int, error) {
	if ttl <= 0 {
		ttl = defaultMultipartTTL
	}
	keys, err := s.B.List(ctx, multipartRoot)
	if err != nil {
		return 0, err
	}
	now := s.now()
	cutoff := now.Add(-ttl).UnixMilli()
	byPrefix := map[string][]string{}
	for _, k := range keys {
		if i := strings.LastIndexByte(k, '/'); i >= 0 {
			byPrefix[k[:i+1]] = append(byPrefix[k[:i+1]], k)
		}
	}
	removed := 0
	for p, ks := range byPrefix {
		data, _, err := s.B.Get(ctx, createMarkerKey(p))
		if err != nil {
			if errors.Is(err, bucket.ErrNotFound) {
				continue
			}
			return removed, err
		}
		ms, perr := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if perr != nil || ms > cutoff {
			continue
		}
		for _, k := range ks {
			if derr := s.B.Delete(ctx, k); derr != nil && !errors.Is(derr, bucket.ErrNotFound) {
				return removed, derr
			}
		}
		removed++
	}
	return removed, nil
}

// UploadPart stores one part and returns its etag.
func (s *Store) UploadPart(ctx context.Context, ns, bucketName, key, uploadID string, part int, data []byte) (string, error) {
	if _, err := Key(ns, bucketName, key); err != nil {
		return "", err
	}
	p, err := multipartPrefix(ns, bucketName, uploadID)
	if err != nil {
		return "", err
	}
	k, err := partKey(p, part)
	if err != nil {
		return "", err
	}
	etag, err := s.B.Put(ctx, k, data)
	if err != nil {
		return "", err
	}
	// Refresh the creation marker so GC never reclaims a live upload whose parts
	// are still arriving: the marker doubles as a heartbeat. Best effort -- a
	// failed refresh must not fail a part that was already stored.
	_, _ = s.B.Put(ctx, createMarkerKey(p), []byte(strconv.FormatInt(s.now().UnixMilli(), 10)))
	return etag, nil
}

// CompleteMultipart assembles the listed parts in the given order, writes the
// final object and removes the staged parts. Part etags (when provided) are
// verified against the stored parts, so a mismatched part fails closed.
func (s *Store) CompleteMultipart(ctx context.Context, ns, bucketName, key, uploadID string, parts []Part) (string, int, error) {
	if len(parts) == 0 || len(parts) > MaxMultipartParts {
		return "", 0, ErrBadUpload
	}
	dst, err := Key(ns, bucketName, key)
	if err != nil {
		return "", 0, err
	}
	p, err := multipartPrefix(ns, bucketName, uploadID)
	if err != nil {
		return "", 0, err
	}
	total := 0
	buf := make([]byte, 0)
	for _, pt := range parts {
		k, err := partKey(p, pt.PartNumber)
		if err != nil {
			return "", 0, err
		}
		data, etag, err := s.B.Get(ctx, k)
		if err != nil {
			return "", 0, err
		}
		if pt.ETag != "" && pt.ETag != etag {
			return "", 0, fmt.Errorf("r2: part %d etag mismatch", pt.PartNumber)
		}
		if total+len(data) > MaxMultipartBytes {
			return "", 0, ErrMultipartTooLarge
		}
		total += len(data)
		buf = append(buf, data...)
	}
	etag, err := s.B.Put(ctx, dst, buf)
	if err != nil {
		return "", 0, err
	}
	// Best-effort cleanup: a failed cleanup must not fail the completed upload.
	_ = s.AbortMultipart(ctx, ns, bucketName, key, uploadID)
	return etag, total, nil
}

// AbortMultipart deletes all staged parts of an upload (missing parts are
// ignored, so abort is idempotent).
func (s *Store) AbortMultipart(ctx context.Context, ns, bucketName, key, uploadID string) error {
	if _, err := Key(ns, bucketName, key); err != nil {
		return err
	}
	p, err := multipartPrefix(ns, bucketName, uploadID)
	if err != nil {
		return err
	}
	keys, err := s.B.List(ctx, p)
	if err != nil {
		return err
	}
	// Delete everything we can: a single failure must not leave the rest behind,
	// so a retry sees a smaller, still-abortable set. Return the last real error.
	var lastErr error
	for _, k := range keys {
		if err := s.B.Delete(ctx, k); err != nil && !errors.Is(err, bucket.ErrNotFound) {
			lastErr = err
		}
	}
	return lastErr
}
