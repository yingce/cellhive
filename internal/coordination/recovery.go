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
	// A retry never overwrites another hint. All hints are verified against COS
	// before recovery; even an extra stale hint cannot authorize sealing.
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

// InitializeRecoveryIndex upgrades an existing Redis deployment OFFLINE ONLY.
// Old writers must remain stopped until all agents run the indexed protocol.
func InitializeRecoveryIndex(ctx context.Context, base bucket.Bucket, endpoint, region string) error {
	b, err := openBound(ctx, base, endpoint, region, true)
	if err != nil {
		return err
	}
	defer b.Close()
	if err = b.Store.check(ctx); err != nil {
		return err
	}
	// Invalidate readiness before reseeding so partial/corrupt migrations fail closed.
	if err = b.Store.Delete(ctx, recoveryReadyKey); err != nil {
		return err
	}
	if err = b.Store.seedRecoveryIndex(ctx, base); err != nil {
		return err
	}
	v, etag, err := readBinding(ctx, base)
	if err != nil {
		return err
	}
	if v.Generation != b.Store.generation || v.State != "active" || v.Region != region {
		return ErrGeneration
	}
	v.Version = 2 // Previous binaries only accept v1 and must refuse this binding.
	raw, _ := json.Marshal(v)
	o := objectstore.NewOwned(base, objectstore.PrefixFleet, objectstore.OwnerWaker)
	_, err = o.CAS(ctx, BindingKey, raw, etag)
	return err
}
