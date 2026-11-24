// Package scheduleindex projects disposable scheduling hints into a regional Redis or Kvrocks endpoint.
package scheduleindex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/timer"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

type Hint struct {
	Kind  string
	Scope string
	Due   int64
}

func (h Hint) Valid() bool {
	sc, err := cell.ParseScope(h.Scope)
	return err == nil && h.Due >= 0 && (h.Kind == "timer" || h.Kind == "queue" && sc.Class == "__queue__" || (h.Kind == "cron" || h.Kind == "purge" || h.Kind == "resource-cleanup") && sc == control.Scope())
}

type Index struct {
	client  *redis.Client
	keys    []string
	pending chan Hint
	dropped atomic.Uint64
	log     *slog.Logger
}

func New(endpoint, region string, log *slog.Logger) (*Index, error) {
	opt, err := redis.ParseURL(endpoint)
	if err != nil || (!strings.HasPrefix(endpoint, "redis://") && !strings.HasPrefix(endpoint, "rediss://")) {
		return nil, errors.New("invalid schedule index Redis URL")
	}
	if region == "" || strings.ContainsAny(region, "{}") {
		return nil, errors.New("invalid schedule index region")
	}
	opt.Protocol = 2
	opt.DisableIdentity = true
	opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	opt.MaxRetries = -1
	opt.DialTimeout = 500 * time.Millisecond
	opt.ReadTimeout = time.Second
	opt.WriteTimeout = time.Second
	opt.ContextTimeoutEnabled = true
	prefix := "cellhive:{" + region + ".0}:v2:"
	return &Index{client: redis.NewClient(opt), keys: []string{prefix + "due", prefix + "claims", prefix + "revisions", prefix + "claimed-revisions", prefix + "payloads", prefix + "pending-due"}, pending: make(chan Hint, 1024), log: log}, nil
}
func (i *Index) Close() error { return i.client.Close() }

// Notify never blocks a cell write. Durable discovery remains active if the buffer fills.
func (i *Index) Notify(h Hint) {
	select {
	case i.pending <- h:
	default:
		i.dropped.Add(1)
	}
}

// Publish synchronously persists a hint. Durable outbox relay ACKs only after success.
func (i *Index) Publish(ctx context.Context, h Hint) error { return i.publish(ctx, h) }
func hintKey(h Hint) string                                { return h.Kind + "\x00" + h.Scope }
func (i *Index) publish(ctx context.Context, h Hint) error {
	if !h.Valid() {
		return errors.New("invalid schedule hint")
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return err
	}
	var revision [16]byte
	if _, err := rand.Read(revision[:]); err != nil {
		return err
	}
	return i.client.Eval(ctx, publishScript, i.keys, hintKey(h), h.Due, hex.EncodeToString(revision[:]), string(raw)).Err()
}

// Re-publication during a claim must survive the old consumer's ACK.
const publishScript = `
local score=redis.call('ZSCORE',KEYS[1],ARGV[1])
if not score then redis.call('ZADD',KEYS[1],ARGV[2],ARGV[1])
elseif redis.call('HGET',KEYS[2],ARGV[1]) then
 local pending=redis.call('HGET',KEYS[6],ARGV[1]);if not pending or tonumber(ARGV[2])<tonumber(pending) then redis.call('HSET',KEYS[6],ARGV[1],ARGV[2]) end
elseif tonumber(ARGV[2])<tonumber(score) then redis.call('ZADD',KEYS[1],ARGV[2],ARGV[1]) end
redis.call('HSET',KEYS[3],ARGV[1],ARGV[3]);redis.call('HSET',KEYS[5],ARGV[1],ARGV[4]);return 1`
const claimScript = `local m=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'LIMIT',0,1)
if #m==0 then return {} end
redis.call('ZADD',KEYS[1],ARGV[2],m[1]);redis.call('HSET',KEYS[2],m[1],ARGV[3])
redis.call('HSET',KEYS[4],m[1],redis.call('HGET',KEYS[3],m[1]) or '');redis.call('HDEL',KEYS[6],m[1])
return {redis.call('HGET',KEYS[5],m[1]) or ''}`
const ackScript = `if redis.call('HGET',KEYS[2],ARGV[1])~=ARGV[2] then return 0 end
local current=redis.call('HGET',KEYS[3],ARGV[1]) or ''
local claimed=redis.call('HGET',KEYS[4],ARGV[1]) or ''
if current~=claimed then redis.call('ZADD',KEYS[1],redis.call('HGET',KEYS[6],ARGV[1]) or '0',ARGV[1])
else redis.call('ZREM',KEYS[1],ARGV[1]);redis.call('HDEL',KEYS[3],ARGV[1]);redis.call('HDEL',KEYS[5],ARGV[1]) end
redis.call('HDEL',KEYS[2],ARGV[1]);redis.call('HDEL',KEYS[4],ARGV[1]);redis.call('HDEL',KEYS[6],ARGV[1]);return 1`

func (i *Index) claim(ctx context.Context, now time.Time, token string) (string, error) {
	items, err := i.client.Eval(ctx, claimScript, i.keys, now.UnixMilli(), now.Add(30*time.Second).UnixMilli(), token).StringSlice()
	if err != nil || len(items) == 0 {
		return "", err
	}
	return items[0], nil
}
func (i *Index) ack(ctx context.Context, item, token string) (int64, error) {
	var h Hint
	if err := json.Unmarshal([]byte(item), &h); err != nil {
		return 0, err
	}
	return i.client.Eval(ctx, ackScript, i.keys, hintKey(h), token).Int64()
}

// retry releases only our failed claim. A newer claimant is never disturbed.
func (i *Index) retry(ctx context.Context, item, token string, now time.Time) (int64, error) {
	var h Hint
	if err := json.Unmarshal([]byte(item), &h); err != nil {
		return 0, err
	}
	return i.client.Eval(ctx, `if redis.call('HGET',KEYS[2],ARGV[1])~=ARGV[2] then return 0 end
redis.call('ZADD',KEYS[1],ARGV[3],ARGV[1])
redis.call('HDEL',KEYS[2],ARGV[1]);redis.call('HDEL',KEYS[4],ARGV[1]);redis.call('HDEL',KEYS[6],ARGV[1]);return 1`, i.keys, hintKey(h), token, now.Add(time.Second).UnixMilli()).Int64()
}

func (i *Index) consume(ctx context.Context, handle func(context.Context, Hint) error) error {
	for range 32 {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		token := hex.EncodeToString(nonce[:])
		item, err := i.claim(ctx, time.Now(), token)
		if err != nil || item == "" {
			return err
		}
		var h Hint
		if json.Unmarshal([]byte(item), &h) == nil && h.Valid() {
			work, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = handle(work, h)
			cancel()
			if err != nil {
				_, retryErr := i.retry(ctx, item, token, time.Now())
				return errors.Join(err, retryErr)
			}
		}
		if _, err = i.ack(ctx, item, token); err != nil {
			return err
		}
	}
	return nil
}

// Run keeps publication independent of claiming and owner routing.
func (i *Index) Run(ctx context.Context, handle func(context.Context, Hint) error) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case h := <-i.pending:
				if err := i.publish(ctx, h); err != nil {
					i.dropped.Add(1)
				}
			}
		}
	}()
	defer func() { <-done }()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var lastWarn time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			err := i.consume(ctx, handle)
			if time.Since(lastWarn) >= time.Minute {
				dropped := i.dropped.Swap(0)
				if err != nil || dropped != 0 {
					i.log.Warn("shared schedule index degraded", "missed_projections", dropped, "consume_failed", err != nil)
					lastWarn = time.Now()
				}
			}
		}
	}
}

// TimerIndex publishes a hint only after the durable wake operation succeeds.
type TimerIndex struct {
	Durable timer.Index
	Shared  *Index
}

func (t TimerIndex) Put(ctx context.Context, scope string, due int64, kind, token string) error {
	if err := t.Durable.Put(ctx, scope, due, kind, token); err != nil {
		return err
	}
	t.Shared.Notify(Hint{Kind: "timer", Scope: scope, Due: due})
	return nil
}
func (t TimerIndex) Delete(ctx context.Context, scope string) error {
	return t.Durable.Delete(ctx, scope)
}
