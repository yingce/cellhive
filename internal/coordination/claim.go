package coordination

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"

	"cellhive/internal/bucket"
)

// Decimal arithmetic avoids Lua's 53-bit integer limit and Redis signed INCR
// overflow. The epoch stays uint64-compatible and is never reused on release.
const incrementEpoch = `if not string.match(gen,'^%d+$') or (#gen>1 and string.sub(gen,1,1)=='0') or #gen>20 or (#gen==20 and gen>'18446744073709551615') then return redis.error_reply('CH_EPOCH_CORRUPT') end
if #floor>#gen or (#floor==#gen and floor>gen) then gen=floor end
if gen=='18446744073709551615' then return redis.error_reply('CH_EPOCH_EXHAUSTED') end
local carry=1;local next=''
for i=#gen,1,-1 do local d=string.byte(gen,i)-48+carry;if d==10 then d=0;carry=1 else carry=0 end;next=string.char(d+48)..next end
if carry==1 then next='1'..next end
`

const claimScript = `
local k=ARGV[2];local old=redis.call('HGET',KEYS[3],k)
if (ARGV[3]=='' and old) or (ARGV[3]~='' and old~=ARGV[3]) then return {} end
local gen=redis.call('HGET',KEYS[2],ARGV[9]) or '0'
local floor=ARGV[4]
` + incrementEpoch + `
local value=ARGV[5]..next..ARGV[6]
local member=redis.call('HGET',KEYS[6],k)
if member then redis.call('ZREM',KEYS[5],member);redis.call('HDEL',KEYS[6],k) end
redis.call('HSET',KEYS[2],ARGV[9],next);redis.call('HSET',KEYS[3],ARGV[9],ARGV[7]);redis.call('ZADD',KEYS[4],0,ARGV[9])
redis.call('HSET',KEYS[2],k,value);redis.call('HSET',KEYS[3],k,ARGV[7]);redis.call('ZADD',KEYS[4],0,k)
if ARGV[8]~='' then redis.call('ZADD',KEYS[5],0,ARGV[8]);redis.call('HSET',KEYS[6],k,ARGV[8]) end
return {value,ARGV[7]}`

func (b *Bucket) ClaimOwner(ctx context.Context, k string, body []byte, expected string, floor uint64) ([]byte, string, error) {
	if !strings.HasPrefix(k, "cells/") || !strings.HasSuffix(k, "/own.json") {
		return nil, "", errors.New("coordination: invalid owner key")
	}
	if len(body) > maxValue-20 {
		return nil, "", errors.New("coordination: owner too large")
	}
	marker := []byte(`"epoch":0`)
	parts := bytes.Split(body, marker)
	if len(parts) != 2 {
		return nil, "", errors.New("coordination: owner epoch placeholder missing")
	}
	member, err := ownerMember(k, body)
	if err != nil {
		return nil, "", err
	}
	v, err := b.Store.eval(ctx, claimScript, k, expected, strconv.FormatUint(floor, 10), string(parts[0])+`"epoch":`, string(parts[1]), nonce(), member, strings.TrimSuffix(k, "own.json")+"owner-gen")
	if err != nil {
		return nil, "", err
	}
	a := v.([]any)
	if len(a) == 0 {
		return nil, "", bucket.ErrPrecondition
	}
	return []byte(a[0].(string)), a[1].(string), nil
}

// ReserveOwnerEpoch replaces the object's read/CAS loop with one Lua call.
// Maintenance still publishes separately so an ambiguous publication can be
// cleaned up using the preallocated exact epoch, never a successor's epoch.
func (b *Bucket) ReserveOwnerEpoch(ctx context.Context, key string, floor uint64) (uint64, error) {
	if !strings.HasPrefix(key, "cells/") || !strings.HasSuffix(key, "/owner-gen") {
		return 0, errors.New("coordination: invalid generation key")
	}
	const script = `local gen=redis.call('HGET',KEYS[2],ARGV[2]) or '0'
 local floor=ARGV[3]
 ` + incrementEpoch + `
 redis.call('HSET',KEYS[2],ARGV[2],next);redis.call('HSET',KEYS[3],ARGV[2],ARGV[4]);redis.call('ZADD',KEYS[4],0,ARGV[2])
 return next`
	v, err := b.Store.eval(ctx, script, key, strconv.FormatUint(floor, 10), nonce())
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(v.(string), 10, 64)
}
