package r2

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

// publication is the single authority for both the body and its metadata.
// Body objects are never overwritten or reclaimed while readers may hold them.
type publication struct {
	Format   int       `json:"format"`
	Key      string    `json:"key"`
	Body     string    `json:"body"`
	ETag     string    `json:"etag"`
	Size     int64     `json:"size"`
	Metadata *Metadata `json:"metadata,omitempty"`
}

func (s *Store) metadataObjects() *objectstore.Objects {
	return objectstore.NewOwned(s.B, objectstore.PrefixR2Meta, objectstore.OwnerR2)
}
func (s *Store) bodyObjects() *objectstore.Objects {
	return objectstore.NewOwned(s.B, objectstore.PrefixR2, objectstore.OwnerR2)
}
func versionPrefix(ns, name, key string) string {
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("r2/.versions/%s/%s/%x/", ns, name, h)
}
func (s *Store) readPublication(ctx context.Context, ns, name, key string) (*publication, string, error) {
	mk, err := metaKey(ns, name, key)
	if err != nil {
		return nil, "", err
	}
	b, token, err := s.metadataObjects().GetVersion(ctx, mk)
	if err != nil {
		return nil, "", err
	}
	var p publication
	if err = json.Unmarshal(b, &p); err != nil {
		return nil, "", fmt.Errorf("r2 publication: %w", err)
	}
	suffix := strings.TrimPrefix(p.Body, versionPrefix(ns, name, key))
	if p.Format != 1 || p.Key != key || p.Size < 0 || p.ETag == "" || !strings.HasPrefix(p.Body, versionPrefix(ns, name, key)) || !isUploadID(suffix) {
		return nil, "", fmt.Errorf("r2: invalid or legacy publication record")
	}
	if p.Metadata != nil && !p.Metadata.Matches(p.Size, p.ETag) {
		return nil, "", fmt.Errorf("r2: inconsistent publication metadata")
	}
	return &p, token, nil
}

// Snapshot binds a request to one published version, including identical-body
// rewrites whose content ETags do not distinguish metadata revisions.
type Snapshot struct {
	Size     int64
	ETag     string
	Metadata *Metadata
	body     string
	store    *Store
}

func (s *Store) Open(ctx context.Context, ns, name, key string) (*Snapshot, error) {
	p, _, err := s.readPublication(ctx, ns, name, key)
	if err != nil {
		return nil, err
	}
	return &Snapshot{Size: p.Size, ETag: p.ETag, Metadata: p.Metadata, body: p.Body, store: s}, nil
}
func (v *Snapshot) Get(ctx context.Context) ([]byte, error) {
	b, etag, err := v.store.bodyObjects().GetVersion(ctx, v.body)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != v.Size || etag != v.ETag {
		return nil, fmt.Errorf("r2: published body changed")
	}
	return b, nil
}
func (v *Snapshot) GetRange(ctx context.Context, off, length int64) ([]byte, error) {
	b, etag, err := v.store.bodyObjects().RangedGet(ctx, v.body, off, length)
	if err != nil {
		return nil, err
	}
	if etag != v.ETag {
		return nil, fmt.Errorf("r2: published body changed")
	}
	return b, nil
}
func (v *Snapshot) Presign(ctx context.Context, ttl time.Duration) (string, error) {
	return v.store.bodyObjects().PresignGet(ctx, v.body, ttl)
}

func (s *Store) publish(ctx context.Context, ns, name, key string, data []byte, md *Metadata, condition *Condition) (string, bool, error) {
	mk, err := metaKey(ns, name, key)
	if err != nil {
		return "", false, err
	}
	old, token, err := s.readPublication(ctx, ns, name, key)
	if err != nil && !errors.Is(err, bucket.ErrNotFound) {
		return "", false, err
	}
	exists := err == nil
	currentETag := ""
	if exists {
		currentETag = old.ETag
	}
	if !condition.matches(exists, currentETag) {
		return "", false, nil
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return "", false, err
	}
	p := publication{Format: 1, Key: key, Body: versionPrefix(ns, name, key) + hex.EncodeToString(id[:]), Size: int64(len(data)), ETag: strings.Repeat("e", 128)}
	if md != nil && (len(md.HTTP) > 0 || len(md.Custom) > 0 || md.MD5 != "" || md.SHA256 != "") {
		copy := *md
		copy.Size = p.Size
		copy.ETag = p.ETag
		if copy.UploadedMs == 0 {
			copy.UploadedMs = s.now().UnixMilli()
		}
		p.Metadata = &copy
	}
	// Validate the complete record before any object mutations.
	probe, err := json.Marshal(p)
	if err != nil {
		return "", false, err
	}
	if limiter, ok := s.B.(interface{ MaxValueBytes(string) int64 }); ok {
		if limit := limiter.MaxValueBytes(mk); limit > 0 && int64(len(probe)) > limit {
			return "", false, fmt.Errorf("r2: metadata exceeds provider capacity")
		}
	}
	etag, err := s.bodyObjects().Put(ctx, p.Body, data)
	if err != nil {
		return "", false, err
	}
	p.ETag = etag
	if p.Metadata != nil {
		p.Metadata.ETag = etag
	}
	encoded, err := json.Marshal(p)
	if err != nil {
		return "", false, err
	}
	// Listing is only a candidate index. It must precede authority publication;
	// failed writes leave harmless entries that readers filter via the authority.
	index, _ := Key(ns, name, key)
	if _, err = s.bodyObjects().Put(ctx, index, []byte("r2-index-v1")); err != nil {
		return "", false, err
	}
	for attempts := 0; attempts < 32; attempts++ {
		if exists {
			_, err = s.metadataObjects().CAS(ctx, mk, encoded, token)
		} else {
			_, err = s.metadataObjects().ConditionalCreate(ctx, mk, encoded)
		}
		if err == nil {
			size := p.Size
			s.recordUsage(ctx, ns, name, key, &size)
			return etag, true, nil
		}
		if !errors.Is(err, bucket.ErrPrecondition) {
			return "", false, err
		}
		old, token, err = s.readPublication(ctx, ns, name, key)
		exists = err == nil
		if err != nil && !errors.Is(err, bucket.ErrNotFound) {
			return "", false, err
		}
		currentETag = ""
		if exists {
			currentETag = old.ETag
		}
		if !condition.matches(exists, currentETag) {
			return "", false, nil
		}
	}
	return "", false, fmt.Errorf("r2: publication contention: %w", bucket.ErrPrecondition)
}
