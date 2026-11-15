package r2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cellhive/internal/bucket"
)

const (
	MaxMultipartParts      = 10000
	MaxMultipartInflight   = 64
	multipartBitmapBytes   = (MaxMultipartParts + 7) / 8
	multipartStateMaxBytes = 64 << 10
	// Assembly is in memory because Bucket has no streaming Put.
	MaxMultipartBytes   = 512 << 20
	multipartRoot       = "r2/.mpu/"
	multipartPartsRoot  = multipartRoot + "v2/parts/"
	multipartStatesRoot = multipartRoot + "v2/states/"
	defaultMultipartTTL = 24 * time.Hour
	createMarkerName    = "created"
)

var (
	ErrBadUpload         = errors.New("r2: bad multipart upload")
	ErrMultipartTooLarge = errors.New("r2: multipart object too large")
	ErrLegacyUpload      = errors.New("r2: legacy multipart upload; restart upload using v2")
	ErrUploadBusy        = errors.New("r2: multipart operation in progress or outcome unknown")
)

type Part struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
}

// Parts is a fixed registration bitmap, including uncertain writes. Inflight
// tokens reserve a part until its writer confirms Put; they are never stolen.
type multipartState struct {
	Version   int            `json:"version"`
	Key       string         `json:"key"`
	State     string         `json:"state"`
	Revision  string         `json:"revision"`
	UpdatedMs int64          `json:"updated_ms"`
	Parts     []byte         `json:"parts,omitempty"`
	Inflight  map[int]string `json:"inflight,omitempty"`
}

func (st multipartState) hasPart(n int) bool {
	return n >= 1 && n <= MaxMultipartParts && len(st.Parts) == multipartBitmapBytes && st.Parts[(n-1)/8]&(1<<uint((n-1)%8)) != 0
}

// isUploadID is also used for immutable final-object version identifiers.
func isUploadID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func multipartNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func multipartPrefix(ns, bucketName, uploadID string) (string, error) {
	if isUploadID(uploadID) {
		return "", ErrLegacyUpload
	}
	if !strings.HasPrefix(uploadID, "v2-") || !isUploadID(strings.TrimPrefix(uploadID, "v2-")) {
		return "", ErrBadUpload
	}
	if _, err := Key(ns, bucketName, "multipart"); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s/%s/%s/", multipartPartsRoot, ns, bucketName, uploadID), nil
}

func partKey(prefix string, part int) (string, error) {
	if part < 1 || part > MaxMultipartParts {
		return "", ErrBadUpload
	}
	return fmt.Sprintf("%s%05d", prefix, part), nil
}

func createMarkerKey(prefix string) string {
	return multipartStatesRoot + strings.TrimPrefix(prefix, multipartPartsRoot) + createMarkerName
}
func isCreateMarker(key string) bool { return strings.HasSuffix(key, "/"+createMarkerName) }

func (s *Store) readMultipart(ctx context.Context, marker, key string) (multipartState, string, error) {
	data, etag, err := s.bodyObjects().GetVersion(ctx, marker)
	if err != nil {
		return multipartState{}, "", err
	}
	var st multipartState
	if err := json.Unmarshal(data, &st); err != nil {
		return st, "", fmt.Errorf("%w: invalid state", ErrBadUpload)
	}
	if st.Version != 2 || st.Key == "" || (key != "" && st.Key != key) || !isUploadID(st.Revision) || len(st.Inflight) > MaxMultipartInflight {
		return st, "", ErrBadUpload
	}
	switch st.State {
	case "active", "completing", "completed", "aborted", "expired":
	default:
		return st, "", ErrBadUpload
	}
	if len(st.Parts) != multipartBitmapBytes && !(multipartTerminal(st) && len(st.Parts) == 0 && len(st.Inflight) == 0) {
		return st, "", ErrBadUpload
	}
	for n, token := range st.Inflight {
		if !st.hasPart(n) || !isUploadID(token) {
			return st, "", ErrBadUpload
		}
	}
	return st, etag, nil
}

func (s *Store) casMultipart(ctx context.Context, marker string, st *multipartState, etag string) (string, error) {
	rev, err := multipartNonce()
	if err != nil {
		return "", err
	}
	st.Revision = rev
	data, err := s.encodeMultipart(marker, *st, false)
	if err != nil {
		return "", err
	}
	return s.bodyObjects().CAS(ctx, marker, data, etag)
}

// Admission includes the largest lifecycle state and timestamp. Create also
// reserves the full bounded writer table, so accepted keys support all parts.
func (s *Store) encodeMultipart(marker string, st multipartState, creating bool) ([]byte, error) {
	limit := int64(multipartStateMaxBytes)
	if b, ok := s.B.(interface{ MaxValueBytes(string) int64 }); ok {
		if n := b.MaxValueBytes(marker); n > 0 && n < limit {
			limit = n
		}
	}
	probe := st
	probe.State = "completing"
	probe.UpdatedMs = -1 << 63
	if creating {
		probe.Inflight = make(map[int]string, MaxMultipartInflight)
		for n := MaxMultipartParts; n > MaxMultipartParts-MaxMultipartInflight; n-- {
			probe.Inflight[n] = strings.Repeat("f", 32)
		}
	}
	data, err := json.Marshal(probe)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, ErrMultipartTooLarge
	}
	return json.Marshal(st)
}

func multipartBusy(st multipartState) bool { return len(st.Inflight) != 0 }
func multipartTerminal(st multipartState) bool {
	return st.State == "completed" || st.State == "aborted" || st.State == "expired"
}
func multipartPartNumbers(st multipartState) []int {
	var numbers []int
	for n := 1; n <= MaxMultipartParts; n++ {
		if st.hasPart(n) {
			numbers = append(numbers, n)
		}
	}
	return numbers
}

func (s *Store) CreateMultipart(ctx context.Context, ns, bucketName, key string) (string, error) {
	if _, err := Key(ns, bucketName, key); err != nil {
		return "", err
	}
	id, err := multipartNonce()
	if err != nil {
		return "", err
	}
	id = "v2-" + id
	p, err := multipartPrefix(ns, bucketName, id)
	if err != nil {
		return "", err
	}
	st := multipartState{Version: 2, Key: key, State: "active", Revision: strings.TrimPrefix(id, "v2-"), UpdatedMs: s.now().UnixMilli(), Parts: make([]byte, multipartBitmapBytes)}
	data, err := s.encodeMultipart(createMarkerKey(p), st, true)
	if err != nil {
		return "", err
	}
	if _, err := s.bodyObjects().ConditionalCreate(ctx, createMarkerKey(p), data); err != nil {
		return "", err
	}
	return id, nil
}

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
	marker := createMarkerKey(p)
	token, err := multipartNonce()
	if err != nil {
		return "", err
	}
	claimed := false
	for i := 0; i < 64; i++ {
		st, rev, err := s.readMultipart(ctx, marker, key)
		if err != nil {
			return "", err
		}
		if st.State != "active" {
			return "", ErrBadUpload
		}
		if st.Inflight[part] != "" || len(st.Inflight) >= MaxMultipartInflight {
			return "", ErrUploadBusy
		}
		if st.Inflight == nil {
			st.Inflight = make(map[int]string)
		}
		st.Parts[(part-1)/8] |= 1 << uint((part-1)%8)
		st.Inflight[part] = token
		st.UpdatedMs = s.now().UnixMilli()
		_, err = s.casMultipart(ctx, marker, &st, rev)
		if errors.Is(err, bucket.ErrPrecondition) {
			continue
		}
		if err != nil {
			return "", err
		}
		claimed = true
		break
	}
	if !claimed {
		return "", ErrUploadBusy
	}
	etag, err := s.bodyObjects().Put(ctx, k, data)
	// Even an error may mean a delayed Put will still arrive. Never release its
	// token and allow another writer to reuse this mutable part key.
	if err != nil {
		return "", err
	}
	for i := 0; i < 64; i++ {
		st, rev, err := s.readMultipart(ctx, marker, key)
		if err != nil {
			return "", err
		}
		if st.State != "active" || st.Inflight[part] != token {
			return "", ErrBadUpload
		}
		delete(st.Inflight, part)
		st.UpdatedMs = s.now().UnixMilli()
		_, err = s.casMultipart(ctx, marker, &st, rev)
		if errors.Is(err, bucket.ErrPrecondition) {
			continue
		}
		if err != nil {
			return "", err
		}
		return etag, nil
	}
	return "", ErrUploadBusy
}

func (s *Store) CompleteMultipart(ctx context.Context, ns, bucketName, key, uploadID string, parts []Part) (string, int, error) {
	if len(parts) == 0 || len(parts) > MaxMultipartParts {
		return "", 0, ErrBadUpload
	}
	if _, err := Key(ns, bucketName, key); err != nil {
		return "", 0, err
	}
	p, err := multipartPrefix(ns, bucketName, uploadID)
	if err != nil {
		return "", 0, err
	}
	marker := createMarkerKey(p)
	st, rev, err := s.readMultipart(ctx, marker, key)
	if err != nil {
		return "", 0, err
	}
	if st.State == "completing" || multipartBusy(st) {
		return "", 0, ErrUploadBusy
	}
	if st.State != "active" {
		return "", 0, ErrBadUpload
	}
	for _, pt := range parts {
		if !st.hasPart(pt.PartNumber) {
			return "", 0, ErrBadUpload
		}
	}
	st.State = "completing"
	rev, err = s.casMultipart(ctx, marker, &st, rev)
	if err != nil {
		return "", 0, err
	}
	// Until publication is invoked all failures are known not to have changed
	// the final object. Afterwards the exclusive completing fence is permanent
	// on uncertainty: retrying could overwrite a later ordinary Put.
	publishing := false
	defer func() {
		if !publishing {
			st.State = "active"
			_, _ = s.casMultipart(ctx, marker, &st, rev)
		}
	}()
	buf := make([]byte, 0)
	for _, pt := range parts {
		k, _ := partKey(p, pt.PartNumber)
		data, etag, err := s.bodyObjects().GetVersion(ctx, k)
		if err != nil {
			return "", 0, err
		}
		if pt.ETag != "" && pt.ETag != etag {
			return "", 0, fmt.Errorf("r2: part %d etag mismatch", pt.PartNumber)
		}
		if len(buf)+len(data) > MaxMultipartBytes {
			return "", 0, ErrMultipartTooLarge
		}
		buf = append(buf, data...)
	}
	publishing = true
	etag, err := s.Put(ctx, ns, bucketName, key, buf)
	if err != nil {
		return "", 0, err
	}
	st.State = "completed"
	if _, err := s.casMultipart(ctx, marker, &st, rev); err != nil {
		return "", 0, err
	}
	// Completion is durable independently of staging reclamation.
	_ = s.cleanupMultipart(ctx, p, st)
	return etag, len(buf), nil
}

func (s *Store) cleanupMultipart(ctx context.Context, p string, st multipartState) error {
	var lastErr error
	for _, n := range multipartPartNumbers(st) {
		if err := ctx.Err(); err != nil {
			return err
		}
		k, _ := partKey(p, n)
		if err := s.bodyObjects().Delete(ctx, k); err != nil && !errors.Is(err, bucket.ErrNotFound) {
			lastErr = err
		}
	}
	// Retain all registrations, including unknown writes that can arrive after
	// this cleanup; the background collector will revisit these tombstones.
	return lastErr
}

func (s *Store) AbortMultipart(ctx context.Context, ns, bucketName, key, uploadID string) error {
	if _, err := Key(ns, bucketName, key); err != nil {
		return err
	}
	p, err := multipartPrefix(ns, bucketName, uploadID)
	if err != nil {
		return err
	}
	marker := createMarkerKey(p)
	for i := 0; i < 64; i++ {
		st, rev, err := s.readMultipart(ctx, marker, key)
		if err != nil {
			return err
		}
		if st.State == "completing" {
			return ErrUploadBusy
		}
		if multipartTerminal(st) {
			return s.cleanupMultipart(ctx, p, st)
		}
		st.State = "aborted"
		_, err = s.casMultipart(ctx, marker, &st, rev)
		if errors.Is(err, bucket.ErrPrecondition) {
			continue
		}
		if err != nil {
			return err
		}
		return s.cleanupMultipart(ctx, p, st)
	}
	return ErrUploadBusy
}
