package r2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

// Usage is an estimate of final objects changed since tracking was enabled.
// Existing objects are deliberately excluded until a later put tracks them.
type Usage struct {
	Objects   int64  `json:"objects"`
	Bytes     int64  `json:"bytes"`
	Estimated bool   `json:"estimated"`
	Baseline  string `json:"baseline"`
	UpdatedMs int64  `json:"updated_ms,omitempty"`
}

func usageBase(ns, bucketName string) (string, error) {
	if _, err := prefix(ns, bucketName); err != nil {
		return "", err
	}
	if ns == ".." || bucketName == ".." {
		return "", ErrBadKey
	}
	return objectstore.PrefixR2Usage + ns + "/" + bucketName + "/", nil
}

func (s *Store) usageObjects() *objectstore.Objects {
	return objectstore.NewOwned(s.B, objectstore.PrefixR2Usage, objectstore.OwnerR2)
}

func (s *Store) Usage(ctx context.Context, ns, bucketName string) (Usage, error) {
	base, err := usageBase(ns, bucketName)
	if err != nil {
		return Usage{}, err
	}
	usage := Usage{Estimated: true, Baseline: "from_first_tracked_write"}
	data, err := s.usageObjects().Get(ctx, base+"summary.json")
	if errors.Is(err, bucket.ErrNotFound) {
		return usage, nil
	}
	if err != nil {
		return Usage{}, err
	}
	if err := json.Unmarshal(data, &usage); err != nil {
		return Usage{}, fmt.Errorf("r2 usage summary: %w", err)
	}
	usage.Estimated = true
	usage.Baseline = "from_first_tracked_write"
	return usage, nil
}

// recordUsage is best effort after a successful final-object mutation. A
// separate bucket object per key distinguishes tracked objects from old ones;
// summary CAS is an independent second write, so crashes may cause drift.
func (s *Store) recordUsage(ctx context.Context, ns, bucketName, key string, size *int64) {
	if err := s.updateUsage(ctx, ns, bucketName, key, size); err != nil {
		log.Printf("r2 usage update failed ns=%q bucket=%q: %v", ns, bucketName, err)
	}
}

func (s *Store) updateUsage(ctx context.Context, ns, bucketName, key string, size *int64) error {
	base, err := usageBase(ns, bucketName)
	if err != nil {
		return err
	}
	if _, err := Key(ns, bucketName, key); err != nil {
		return err
	}
	h := sha256.Sum256([]byte(key))
	marker := base + "objects/" + hex.EncodeToString(h[:])
	o := s.usageObjects()
	var objectDelta, byteDelta int64
	for i := 0; i < 32; i++ {
		objectDelta, byteDelta = 0, 0
		data, etag, err := o.GetVersion(ctx, marker)
		exists := err == nil
		if err != nil && !errors.Is(err, bucket.ErrNotFound) {
			return err
		}
		var oldSize int64
		if exists {
			oldSize, err = strconv.ParseInt(string(data), 10, 64)
			if err != nil || oldSize < 0 {
				return fmt.Errorf("r2 usage marker invalid")
			}
		}
		if size == nil {
			if !exists {
				return nil
			}
			err = o.ConditionalDelete(ctx, marker, etag)
			objectDelta, byteDelta = -1, -oldSize
		} else if exists {
			_, err = o.CAS(ctx, marker, []byte(strconv.FormatInt(*size, 10)), etag)
			byteDelta = *size - oldSize
		} else {
			_, err = o.ConditionalCreate(ctx, marker, []byte(strconv.FormatInt(*size, 10)))
			objectDelta, byteDelta = 1, *size
		}
		if errors.Is(err, bucket.ErrPrecondition) {
			continue
		}
		if err != nil {
			return err
		}
		if objectDelta == 0 && byteDelta == 0 {
			return nil
		}
		return s.addUsageDelta(ctx, o, base+"summary.json", objectDelta, byteDelta)
	}
	return fmt.Errorf("r2 usage marker CAS contention")
}

func (s *Store) addUsageDelta(ctx context.Context, o *objectstore.Objects, key string, objects, bytes int64) error {
	for i := 0; i < 32; i++ {
		data, etag, err := o.GetVersion(ctx, key)
		exists := err == nil
		if err != nil && !errors.Is(err, bucket.ErrNotFound) {
			return err
		}
		usage := Usage{Estimated: true, Baseline: "from_first_tracked_write"}
		if exists {
			if err := json.Unmarshal(data, &usage); err != nil {
				return fmt.Errorf("r2 usage summary invalid: %w", err)
			}
		}
		usage.Objects += objects
		usage.Bytes += bytes
		if usage.Objects < 0 {
			usage.Objects = 0
		}
		if usage.Bytes < 0 {
			usage.Bytes = 0
		}
		usage.Estimated = true
		usage.Baseline = "from_first_tracked_write"
		usage.UpdatedMs = time.Now().UnixMilli()
		buf, _ := json.Marshal(usage)
		if exists {
			_, err = o.CAS(ctx, key, buf, etag)
		} else {
			_, err = o.ConditionalCreate(ctx, key, buf)
		}
		if errors.Is(err, bucket.ErrPrecondition) {
			continue
		}
		return err
	}
	return fmt.Errorf("r2 usage summary CAS contention")
}
