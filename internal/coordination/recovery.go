package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"cellhive/internal/bucket"
	"cellhive/internal/nodelog"
	"cellhive/internal/objectstore"
)

const recoveryReadyKey = "maintenance/recovery-index-version"
const recoveryPrefix = "maintenance/recovery/"
const recoveryGuard = `if redis.call('HGET',KEYS[2],'maintenance/recovery-index-version')~='1' then return redis.error_reply('CH_RECOVERY_INDEX_UNINITIALIZED') end
`

func (b *Bucket) RecoveryIndexReady(ctx context.Context) error {
	_, err := b.Store.eval(ctx, recoveryGuard+`return 1`)
	return err
}

func validateRecoveryHint(key string, raw []byte) (nodelog.Record, error) {
	var r nodelog.Record
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	if r.Node == "" || r.Session == "" || strings.ContainsAny(r.Node+r.Session, "/\x00\r\n") || nodelog.Key(r.Node, r.Session) != key {
		return r, errors.New("coordination: invalid recovery session identity")
	}
	switch r.Status {
	case nodelog.StatusOpen, nodelog.StatusRecovering, nodelog.StatusSealed:
	default:
		return r, errors.New("coordination: invalid recovery status")
	}
	return r, nil
}

func (b *Bucket) RegisterRecoverySession(ctx context.Context, key string, raw []byte) error {
	r, err := validateRecoveryHint(key, raw)
	if err != nil {
		return err
	}
	if r.Status != nodelog.StatusOpen {
		return errors.New("coordination: registration requires open session")
	}
	if len(raw) > maxValue {
		return errors.New("coordination: recovery hint too large")
	}
	// A retry never overwrites another hint. Hints are verified against the authority
	// before recovery (Redis in v3); a stale hint cannot authorize sealing.
	_, err = b.Store.eval(ctx, recoveryGuard+`
 local k=ARGV[2]
 if redis.call('HEXISTS',KEYS[2],k)==0 then
 redis.call('HSET',KEYS[2],k,ARGV[3]);redis.call('HSET',KEYS[3],k,ARGV[4])
 end
 redis.call('ZADD',KEYS[4],0,k)
 return 1`, recoveryPrefix+key, string(raw), nonce())
	return err
}
func (b *Bucket) RemoveRecoverySession(ctx context.Context, key string) error {
	if !strings.HasPrefix(key, "node-logs/") {
		return errors.New("coordination: invalid recovery key")
	}
	_, err := b.Store.eval(ctx, recoveryGuard+`local k=ARGV[2];redis.call('HDEL',KEYS[2],k);redis.call('HDEL',KEYS[3],k);redis.call('ZREM',KEYS[4],k);return 1`, recoveryPrefix+key)
	return err
}
func (b *Bucket) PendingRecoverySessions(ctx context.Context, prefix string) ([]string, map[string][]byte, error) {
	if !strings.HasPrefix(prefix, "node-logs/") {
		return nil, nil, errors.New("coordination: invalid recovery prefix")
	}
	p := recoveryPrefix + prefix
	after := "[" + p
	var keys []string
	values := map[string][]byte{}
	for {
		v, err := b.Store.eval(ctx, recoveryGuard+`local ks=redis.call('ZRANGEBYLEX',KEYS[4],ARGV[2],ARGV[3],'LIMIT',0,256); local out={};for _,k in ipairs(ks) do local body=redis.call('HGET',KEYS[2],k);if not body then return redis.error_reply('CH_RECOVERY_INDEX_CORRUPT') end;table.insert(out,k);table.insert(out,body) end;return out`, after, "["+p+"\xff")
		if err != nil {
			return nil, nil, err
		}
		a := v.([]any)
		for i := 0; i < len(a); i += 2 {
			full := a[i].(string)
			key := strings.TrimPrefix(full, recoveryPrefix)
			raw := []byte(a[i+1].(string))
			if _, err := validateRecoveryHint(key, raw); err != nil {
				return nil, nil, err
			}
			keys = append(keys, key)
			values[key] = raw
			after = "(" + full
		}
		if len(a) < 512 {
			return keys, values, nil
		}
	}
}

// seedRecoveryIndex is only called with all writers stopped. Publication of the
// ready marker is last; a failed seed cannot enable an incomplete directory.
func (s *Store) seedRecoveryIndex(ctx context.Context, base bucket.Bucket) error {
	objs := objectstore.NewOwned(base, objectstore.PrefixNodeLogs, objectstore.OwnerNodeLog)
	keys, err := objs.ListPrefix(ctx, objectstore.PrefixNodeLogs)
	if err != nil {
		return err
	}
	for _, key := range keys {
		raw, _, err := objs.GetVersion(ctx, key)
		if err != nil {
			return err
		}
		r, err := validateRecoveryHint(key, raw)
		if err != nil {
			return fmt.Errorf("recovery index seed %s: %w", key, err)
		}
		if _, err = s.Put(ctx, key, raw); err != nil {
			return err
		}
		if r.Status == nodelog.StatusSealed {
			err = s.Delete(ctx, recoveryPrefix+key)
		} else {
			_, err = s.Put(ctx, recoveryPrefix+key, raw)
		}
		if err != nil {
			return err
		}
	}
	_, err = s.Put(ctx, recoveryReadyKey, []byte("1"))
	return err
}

// seedRecoveryAuthority imports only newly selected metadata. Existing Redis
// owners/leases must never be overwritten with historical object-store values.
func (s *Store) seedRecoveryAuthority(ctx context.Context, base bucket.Bucket) error {
	objects := objectstore.NewOwned(base, objectstore.PrefixCells, objectstore.OwnerReplica)
	keys, err := objects.ListPrefix(ctx, objectstore.PrefixCells)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, "/do-recovery.json") {
			continue
		}
		raw, _, err := objects.GetVersion(ctx, key)
		if err != nil {
			return err
		}
		if !json.Valid(raw) {
			return fmt.Errorf("coordination: corrupt DO recovery pointer %s", key)
		}
		if _, err = s.Put(ctx, key, raw); err != nil {
			return err
		}
	}
	return s.seedRecoveryIndex(ctx, base)
}

// InitializeRecoveryIndex upgrades recovery authority OFFLINE ONLY. Active v3
// is never reseeded: old COS copies become stale as soon as v3 writers start.
func InitializeRecoveryIndex(ctx context.Context, base bucket.Bucket, endpoint, region string) error {
	v, etag, err := readBinding(ctx, base)
	if err != nil {
		return err
	}
	if v.Region != region {
		return errors.New("coordination: region mismatch")
	}
	s, err := New(endpoint, region)
	if err != nil {
		return err
	}
	defer s.Close()
	s.generation = v.Generation
	if err = s.check(ctx); err != nil {
		return err
	}
	b := &Bucket{Base: base, Store: s}
	if v.Version == 3 && v.State == "active" {
		return b.RecoveryIndexReady(ctx)
	}
	if v.State != "active" && !(v.Version == 3 && v.State == "loading") {
		return errors.New("coordination: finish coordination-init before recovery upgrade")
	}
	o := objectstore.NewOwned(base, objectstore.PrefixFleet, objectstore.OwnerWaker)
	v.Version, v.State = 3, "loading"
	raw, _ := json.Marshal(v)
	etag, err = o.CAS(ctx, BindingKey, raw, etag)
	if err != nil {
		return err
	}
	if err = s.Delete(ctx, recoveryReadyKey); err != nil {
		return err
	}
	if err = s.seedRecoveryAuthority(ctx, base); err != nil {
		return err
	}
	v.State = "active"
	raw, _ = json.Marshal(v)
	_, err = o.CAS(ctx, BindingKey, raw, etag)
	return err
}
