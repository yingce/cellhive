package r2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
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

// CreateMultipart mints a new upload id for (ns, bucket, key).
func (s *Store) CreateMultipart(ns, bucketName, key string) (string, error) {
	if _, err := Key(ns, bucketName, key); err != nil {
		return "", err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
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
	return s.B.Put(ctx, k, data)
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
	for _, k := range keys {
		if err := s.B.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}
