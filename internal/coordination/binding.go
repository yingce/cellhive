package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

type binding struct {
	Version    int    `json:"version"`
	Region     string `json:"region"`
	Generation string `json:"generation"`
	State      string `json:"state"`
}

func readBinding(ctx context.Context, b bucket.Bucket) (binding, string, error) {
	o := objectstore.NewOwned(b, objectstore.PrefixFleet, objectstore.OwnerWaker)
	raw, e, err := o.GetVersion(ctx, BindingKey)
	var v binding
	if err != nil {
		return v, "", err
	}
	if err = json.Unmarshal(raw, &v); err != nil {
		return v, "", err
	}
	if (v.Version != 1 && v.Version != 2) || v.Region == "" || v.Generation == "" || (v.State != "loading" && v.State != "active") {
		return v, "", errors.New("coordination: invalid binding")
	}
	return v, e, nil
}
func CheckObjectMode(ctx context.Context, b bucket.Bucket) error {
	_, _, err := readBinding(ctx, b)
	if errors.Is(err, bucket.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("coordination: bucket is bound to Redis; object authority forbidden")
}
func Open(ctx context.Context, b bucket.Bucket, endpoint, region string) (*Bucket, error) {
	return openBound(ctx, b, endpoint, region, false)
}

func openBound(ctx context.Context, b bucket.Bucket, endpoint, region string, offlineUpgrade bool) (*Bucket, error) {
	v, _, err := readBinding(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("coordination binding: %w", err)
	}
	if v.State != "active" || v.Region != region {
		return nil, errors.New("coordination: binding is not active for this region")
	}
	if v.Version != 2 && !offlineUpgrade {
		return nil, errors.New("coordination: offline coordination-recovery-init required before indexed recovery startup")
	}
	s, err := New(endpoint, region)
	if err != nil {
		return nil, err
	}
	s.generation = v.Generation
	if err = s.check(ctx); err != nil {
		s.Close()
		return nil, err
	}
	wrapped := &Bucket{Base: b, Store: s}
	if !offlineUpgrade {
		if err = wrapped.RecoveryIndexReady(ctx); err != nil {
			s.Close()
			return nil, err
		}
	}
	return wrapped, nil
}

// Initialize is OFFLINE ONLY. The operator must stop all writers, including
// older binaries which do not understand the binding. No automatic failover.
func Initialize(ctx context.Context, b bucket.Bucket, endpoint, region string) error {
	s, err := New(endpoint, region)
	if err != nil {
		return err
	}
	defer s.Close()
	v, etag, err := readBinding(ctx, b)
	o := objectstore.NewOwned(b, objectstore.PrefixFleet, objectstore.OwnerWaker)
	if errors.Is(err, bucket.ErrNotFound) {
		// Refuse to adopt an unrelated/nonempty Redis authority namespace.
		n, e := s.client.Exists(ctx, s.keys...).Result()
		if e != nil {
			return e
		}
		if n != 0 {
			return errors.New("coordination: Redis namespace is not empty")
		}
		v = binding{Version: 2, Region: region, Generation: nonce(), State: "loading"}
		raw, _ := json.Marshal(v)
		etag, err = o.ConditionalCreate(ctx, BindingKey, raw)
	}
	if err != nil {
		return err
	}
	if v.Region != region {
		return errors.New("coordination: region mismatch")
	}
	if v.State == "active" {
		return errors.New("coordination: already initialized; refusing to overwrite active authority")
	}
	loading := "loading:" + v.Generation
	// Resuming a migration must never overwrite an active generation.
	ok, err := s.client.SetNX(ctx, s.keys[0], loading, 0).Result()
	if err != nil {
		return err
	}
	if !ok {
		g, e := s.client.Get(ctx, s.keys[0]).Result()
		if e != nil {
			return e
		}
		if g == v.Generation {
			// Crash after Redis activation: repeat the offline seed before
			// publishing the indexed-protocol binding (also covers legacy v1).
			s.generation = v.Generation
			if e = s.seedRecoveryIndex(ctx, b); e != nil {
				return e
			}
			v.Version = 2
			v.State = "active"
			raw, _ := json.Marshal(v)
			_, e = o.CAS(ctx, BindingKey, raw, etag)
			return e
		}
		if g != loading {
			return ErrGeneration
		}
	}
	s.generation = loading
	for _, p := range []string{"cells/", "nodes/", "fleet/", "wake/"} {
		keys, e := b.List(ctx, p)
		if e != nil {
			return e
		}
		for _, k := range keys {
			if !Selected(k) {
				continue
			}
			raw, _, e := b.Get(ctx, k)
			if e != nil {
				return e
			}
			if _, e = s.Put(ctx, k, raw); e != nil {
				return e
			}
		}
	}
	if err = s.seedRecoveryIndex(ctx, b); err != nil {
		return err
	}
	_, err = s.eval(ctx, `redis.call('SET',KEYS[1],ARGV[2]); return 1`, v.Generation)
	if err != nil {
		return err
	}
	v.State = "active"
	v.Version = 2
	raw, _ := json.Marshal(v)
	_, err = o.CAS(ctx, BindingKey, raw, etag)
	return err
}
