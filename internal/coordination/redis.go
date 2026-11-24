// Package coordination provides an explicit Redis authority for small control
// records. It never falls back to object storage on a Redis failure.
package coordination

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

var ErrGeneration = errors.New("coordination: authority generation missing or changed; offline recovery required")

const maxValue = 8 << 20

type Store struct {
	client     *redis.Client
	keys       []string
	generation string
}

func New(endpoint, region string) (*Store, error) {
	if region == "" || strings.ContainsAny(region, "{}\x00\r\n") {
		return nil, errors.New("coordination: invalid region")
	}
	opt, err := redis.ParseURL(endpoint)
	if err != nil || (!strings.HasPrefix(endpoint, "redis://") && !strings.HasPrefix(endpoint, "rediss://")) {
		return nil, errors.New("coordination: invalid Redis URL")
	}
	opt.Protocol = 2
	opt.DisableIdentity = true
	opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	opt.MaxRetries = -1
	opt.DialTimeout = time.Second
	opt.ReadTimeout = time.Second
	opt.WriteTimeout = time.Second
	opt.ContextTimeoutEnabled = true
	p := "cellhive:coord:{" + region + "}:"
	return &Store{client: redis.NewClient(opt), keys: []string{p + "generation", p + "values", p + "revisions", p + "directory", p + "owners", p + "owner-members"}}, nil
}
func (s *Store) Close() error { return s.client.Close() }
func nonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

const guard = `if redis.call('GET',KEYS[1])~=ARGV[1] then return redis.error_reply('CH_GENERATION') end
`

func (s *Store) eval(ctx context.Context, script string, args ...any) (any, error) {
	a := append([]any{s.generation}, args...)
	v, err := s.client.Eval(ctx, guard+script, s.keys, a...).Result()
	if err != nil && strings.Contains(err.Error(), "CH_GENERATION") {
		return nil, ErrGeneration
	}
	return v, err
}
func (s *Store) check(ctx context.Context) error { _, err := s.eval(ctx, `return 1`); return err }
func (s *Store) Get(ctx context.Context, key string) ([]byte, string, error) {
	v, err := s.eval(ctx, `local v=redis.call('HGET',KEYS[2],ARGV[2]); if not v then return {} end; return {v,redis.call('HGET',KEYS[3],ARGV[2])}`, key)
	if err != nil {
		return nil, "", err
	}
	a := v.([]any)
	if len(a) == 0 {
		return nil, "", bucket.ErrNotFound
	}
	return []byte(a[0].(string)), a[1].(string), nil
}

// Membership changes and the owner record are one atomic operation. The Lua
// script never decodes/re-encodes epoch numbers (uint64 exceeds Lua precision).
const writeScript = `
local k=ARGV[2]; local mode=ARGV[3]; local old=redis.call('HGET',KEYS[3],k)
if mode=='create' and old then return 0 end
if (mode=='cas' or mode=='compare-delete') and (not old or old~=ARGV[4]) then return 0 end
local member=redis.call('HGET',KEYS[6],k)
if member then redis.call('ZREM',KEYS[5],member); redis.call('HDEL',KEYS[6],k) end
if mode=='delete' or mode=='compare-delete' then
 redis.call('HDEL',KEYS[2],k);redis.call('HDEL',KEYS[3],k);redis.call('ZREM',KEYS[4],k)
else
 redis.call('HSET',KEYS[2],k,ARGV[5]);redis.call('HSET',KEYS[3],k,ARGV[6]);redis.call('ZADD',KEYS[4],0,k)
 if ARGV[7]~='' then redis.call('ZADD',KEYS[5],0,ARGV[7]);redis.call('HSET',KEYS[6],k,ARGV[7]) end
end
return 1`

func (s *Store) write(ctx context.Context, key, mode, expected string, data []byte) (string, error) {
	if len(data) > maxValue {
		return "", errors.New("coordination: value exceeds 8 MiB")
	}
	member, err := ownerMember(key, data)
	if err != nil {
		return "", err
	}
	token := nonce()
	v, err := s.eval(ctx, writeScript, key, mode, expected, string(data), token, member)
	if err != nil {
		return "", err
	}
	if v.(int64) == 0 {
		return "", bucket.ErrPrecondition
	}
	return token, nil
}
func (s *Store) Put(ctx context.Context, k string, v []byte) (string, error) {
	return s.write(ctx, k, "put", "", v)
}
func (s *Store) ConditionalCreate(ctx context.Context, k string, v []byte) (string, error) {
	return s.write(ctx, k, "create", "", v)
}
func (s *Store) CAS(ctx context.Context, k string, v []byte, e string) (string, error) {
	return s.write(ctx, k, "cas", e, v)
}
func (s *Store) Delete(ctx context.Context, k string) error {
	_, e := s.write(ctx, k, "delete", "", nil)
	return e
}
func (s *Store) ConditionalDelete(ctx context.Context, k, e string) error {
	_, err := s.write(ctx, k, "compare-delete", e, nil)
	return err
}
func (s *Store) list(ctx context.Context, prefix, after string, limit int) ([]string, error) {
	min := "[" + prefix
	if after >= prefix && after != "" {
		min = "(" + after
	}
	v, err := s.eval(ctx, `return redis.call('ZRANGEBYLEX',KEYS[4],ARGV[2],ARGV[3],'LIMIT',0,ARGV[4])`, min, "["+prefix+"\xff", limit)
	if err != nil {
		return nil, err
	}
	a := v.([]any)
	out := make([]string, len(a))
	for i, x := range a {
		out[i] = x.(string)
	}
	return out, nil
}
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var all []string
	after := ""
	for {
		p, err := s.list(ctx, prefix, after, 512)
		if err != nil {
			return nil, err
		}
		all = append(all, p...)
		if len(p) < 512 {
			return all, nil
		}
		after = p[len(p)-1]
	}
}
func (s *Store) OwnedCount(ctx context.Context, node, session string) (int, error) {
	p := node + "\x00" + session + "\x00"
	v, err := s.eval(ctx, `return redis.call('ZLEXCOUNT',KEYS[5],ARGV[2],ARGV[3])`, "["+p, "["+p+"\xff")
	if err != nil {
		return 0, err
	}
	return int(v.(int64)), nil
}
func (s *Store) String() string { return fmt.Sprintf("Redis coordination generation %s", s.generation) }
