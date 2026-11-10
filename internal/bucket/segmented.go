package bucket

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrAuthoritySegmentFull = errors.New("bucket: authority capacity exhausted")
var errAuthorityRetired = errors.New("bucket: retired authority segment")

const (
	segMagic                              = "CHA3"
	segHeader                             = 17
	segFooter                             = 8
	segOverhead                           = segHeader + segFooter
	segProbeBytes                         = 4096
	rootDescriptorBytes                   = 64
	DefaultAuthoritySegment               = time.Hour
	DefaultAuthoritySegmentMaxBytes int64 = 1 << 20
	// At 64 bytes per transition this permits approximately 262,144
	// rollovers (29 years hourly, about 182 days minutely), less the bootstrap frame.
	DefaultAuthorityRootMaxBytes int64 = 16 << 20
	authorityMaxValue                  = 64 << 10
)
const (
	authorityValue byte = iota
	authorityDeleted
	authoritySeal
	authorityBootstrap
	authorityDescriptor
)

type AuthorityOptions struct {
	Segment         time.Duration
	MaxSegmentBytes int64
	MaxRootBytes    int64
	Now             func() time.Time
}

type segmentData interface {
	TailGet(context.Context, string, int) ([]byte, int64, string, error)
	RangedGet(context.Context, string, int64, int64) ([]byte, string, error)
	Stat(context.Context, string) (int64, string, error)
	List(context.Context, string) ([]string, error)
	Delete(context.Context, string) error
	Put(context.Context, string, []byte) (string, error)
}

// SegmentedAuthority serializes initial creation on a permanent append root.
// Values append within a generation; a terminal seal atomically consumes the
// old position before publishing a successor in the root. Root and retired
// one-byte segment stubs must never be deleted or reset online.
// Neither discovery nor rollover uses List. Legacy layouts fail closed.
type SegmentedAuthority struct {
	raw                    PositionAppender
	data                   segmentData
	segment                time.Duration
	max, rootMax, valueMax int64
	now                    func() time.Time
	readMu                 sync.Mutex
	readRoots              map[string]authorityRoot
}

func NewSegmentedAuthority(raw PositionAppender, data segmentData, opts AuthorityOptions) *SegmentedAuthority {
	if opts.Segment <= 0 {
		opts.Segment = DefaultAuthoritySegment
	}
	if opts.MaxSegmentBytes <= 0 {
		opts.MaxSegmentBytes = DefaultAuthoritySegmentMaxBytes
	}
	if opts.MaxRootBytes <= 0 {
		opts.MaxRootBytes = DefaultAuthorityRootMaxBytes
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	// Every accepted live frame leaves space for a maximum-sized terminal seal.
	valueMax := (opts.MaxSegmentBytes - 2*segOverhead - 17) / 2
	if valueMax > authorityMaxValue {
		valueMax = authorityMaxValue
	}
	return &SegmentedAuthority{raw: raw, data: data, segment: opts.Segment, max: opts.MaxSegmentBytes, rootMax: opts.MaxRootBytes, valueMax: valueMax, now: opts.Now}
}
func (s *SegmentedAuthority) window() int64 { return s.now().UnixNano() / int64(s.segment) }
func (s *SegmentedAuthority) physical(key string, generation int64) string {
	return key + ".g" + strconv.FormatInt(generation, 10)
}
func ParsePhysicalAuthorityKey(key string) (string, int64, bool) {
	i := strings.LastIndex(key, ".g")
	if i < 0 || !authorityKey(key[:i]) {
		return "", 0, false
	}
	n, err := strconv.ParseInt(key[i+2:], 10, 64)
	if err != nil || n < 0 {
		return "", 0, false
	}
	return key[:i], n, true
}

type authoritySegment struct {
	logical        string
	window, length int64 // window is the monotonically increasing generation.
	instance       [8]byte
	deleted        bool
	value          []byte
	op             byte
	root           *authorityRoot
}
type authorityRoot struct {
	generation, clock, length int64
	instance                  [8]byte
	seed                      []byte
}

func (g *authoritySegment) token() string {
	return "a3:" + hex.EncodeToString(g.instance[:]) + ":" + strconv.FormatInt(g.window, 10) + ":" + strconv.FormatInt(g.length, 10)
}
func newInstance() [8]byte {
	var v [8]byte
	if _, err := rand.Read(v[:]); err != nil {
		panic("bucket: authority identity entropy unavailable: " + err.Error())
	}
	return v
}
func encodeAuthorityFrame(op byte, data []byte, instance [8]byte) []byte {
	out := make([]byte, segOverhead+len(data))
	copy(out, segMagic)
	out[4] = op
	copy(out[5:13], instance[:])
	binary.BigEndian.PutUint32(out[13:17], uint32(len(data)))
	copy(out[17:], data)
	binary.BigEndian.PutUint32(out[len(out)-8:], uint32(len(out)))
	binary.BigEndian.PutUint32(out[len(out)-4:], crc32.ChecksumIEEE(out[:len(out)-8]))
	return out
}
func decodeAuthorityFrame(frame []byte) (byte, []byte, [8]byte, error) {
	var instance [8]byte
	if len(frame) < segOverhead || string(frame[:4]) != segMagic || frame[4] > authorityDescriptor ||
		uint64(binary.BigEndian.Uint32(frame[13:17]))+segOverhead != uint64(len(frame)) ||
		binary.BigEndian.Uint32(frame[len(frame)-8:]) != uint32(len(frame)) ||
		binary.BigEndian.Uint32(frame[len(frame)-4:]) != crc32.ChecksumIEEE(frame[:len(frame)-8]) {
		return 0, nil, instance, errors.New("bucket: corrupt or unsupported authority format")
	}
	copy(instance[:], frame[5:13])
	return frame[4], frame[17 : len(frame)-8], instance, nil
}
func (s *SegmentedAuthority) tail(ctx context.Context, key string, limit int64) (byte, []byte, [8]byte, int64, error) {
	var zero [8]byte
	tail, total, _, err := s.data.TailGet(ctx, key, segProbeBytes)
	// COS can expose an empty object during the first conditional append.
	// Re-read only this exact empty response; never treat it as absence or
	// repair it with a write. A persistent empty/corrupt object fails closed.
	for attempt := 0; err == nil && total == 0 && len(tail) == 0 && attempt < 3; attempt++ {
		timer := time.NewTimer(time.Duration(10<<attempt) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, nil, zero, 0, ctx.Err()
		case <-timer.C:
		}
		tail, total, _, err = s.data.TailGet(ctx, key, segProbeBytes)
	}
	if err != nil {
		return 0, nil, zero, 0, err
	}
	if total == 1 && len(tail) == 1 && tail[0] == 0 {
		return 0, nil, zero, 0, errAuthorityRetired
	}
	if total < segOverhead || total > limit || len(tail) < segFooter {
		return 0, nil, zero, 0, fmt.Errorf("bucket: invalid authority object length: key=%s total=%d tail=%d limit=%d", key, total, len(tail), limit)
	}
	n := int64(binary.BigEndian.Uint32(tail[len(tail)-8:]))
	if n < segOverhead || n > total || n > authorityMaxValue+128 {
		return 0, nil, zero, 0, errors.New("bucket: invalid authority frame length")
	}
	var frame []byte
	if n <= int64(len(tail)) {
		frame = tail[len(tail)-int(n):]
	} else {
		frame, _, err = s.data.RangedGet(ctx, key, total-n, n)
		if err != nil {
			return 0, nil, zero, 0, err
		}
		if int64(len(frame)) != n {
			return 0, nil, zero, 0, errors.New("bucket: short authority frame")
		}
	}
	op, value, instance, err := decodeAuthorityFrame(frame)
	return op, value, instance, total, err
}
func rootMetadata(generation, clock int64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, uint64(generation))
	binary.BigEndian.PutUint64(b[8:], uint64(clock))
	return b
}
func (s *SegmentedAuthority) readRoot(ctx context.Context, key string) (*authorityRoot, error) {
	op, v, inst, n, err := s.tail(ctx, key, s.rootMax)
	if err != nil {
		return nil, err
	}
	if len(v) < 16 || (op != authorityBootstrap && op != authorityDescriptor) {
		return nil, errors.New("bucket: invalid authority root")
	}
	r := &authorityRoot{generation: int64(binary.BigEndian.Uint64(v)), clock: int64(binary.BigEndian.Uint64(v[8:])), length: n, instance: inst}
	if r.generation < 0 || (op == authorityBootstrap && r.generation != 0) || (op == authorityDescriptor && (r.generation == 0 || len(v) != 39)) {
		return nil, errors.New("bucket: invalid authority root descriptor")
	}
	if op == authorityBootstrap {
		r.seed = encodeAuthorityFrame(authorityValue, v[16:], inst)
	}
	return r, nil
}
func (s *SegmentedAuthority) readSegmentAt(ctx context.Context, key string, generation int64) (*authoritySegment, error) {
	op, v, inst, n, err := s.tail(ctx, s.physical(key, generation), s.max)
	if err != nil {
		return nil, err
	}
	if op > authoritySeal {
		return nil, errors.New("bucket: invalid authority segment operation")
	}
	return &authoritySegment{logical: key, window: generation, length: n, instance: inst, deleted: op == authorityDeleted, value: v, op: op}, nil
}

// ensureGenesis is safe even for a delayed helper: retired keys retain a
// nonzero stub, and therefore can never accept append-at-zero again.
func (s *SegmentedAuthority) ensureGenesis(ctx context.Context, key string, seed []byte) error {
	err := s.raw.Append(ctx, key, 0, seed)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrPrecondition) {
		return err
	}
	existing, _, rerr := s.data.RangedGet(ctx, key, 0, int64(len(seed)))
	if rerr != nil {
		return rerr
	}
	if !bytes.Equal(existing, seed) {
		return ErrPrecondition
	}
	return nil
}
func (s *SegmentedAuthority) retire(ctx context.Context, key string, generation int64) {
	// This unconditional overwrite is legal ONLY after root publication, on a
	// terminal key which will never be selected or reused again.
	_, _ = s.data.Put(ctx, s.physical(key, generation), []byte{0})
}
func (s *SegmentedAuthority) publish(ctx context.Context, seg *authoritySegment) (*authoritySegment, error) {
	if len(seg.value) < 17 || seg.value[16] > authorityDeleted {
		return nil, errors.New("bucket: invalid authority seal")
	}
	generation := int64(binary.BigEndian.Uint64(seg.value))
	clock := int64(binary.BigEndian.Uint64(seg.value[8:]))
	if generation != seg.window+1 {
		return nil, errors.New("bucket: invalid authority successor")
	}
	seed := encodeAuthorityFrame(seg.value[16], seg.value[17:], seg.instance)
	next := &authoritySegment{logical: seg.logical, window: generation, length: int64(len(seed)), instance: seg.instance, deleted: seg.value[16] == authorityDeleted, value: seg.value[17:]}
	// A delayed helper may find its successor already retired. Check root to
	// recognize that publication was completed rather than resurrecting it.
	r, err := s.readRoot(ctx, seg.logical)
	if err != nil {
		return nil, err
	}
	if r.instance != seg.instance {
		return nil, ErrPrecondition
	}
	if r.generation > seg.window {
		return next, nil
	}
	if r.generation != seg.window || r.length != seg.root.length {
		return nil, ErrPrecondition
	}
	if err = s.ensureGenesis(ctx, s.physical(seg.logical, generation), seed); err != nil {
		if curr, e := s.readRoot(ctx, seg.logical); e == nil && curr.instance == seg.instance && curr.generation >= generation {
			return next, nil
		}
		return nil, err
	}
	payload := make([]byte, 39)
	copy(payload, rootMetadata(generation, clock))
	descriptor := encodeAuthorityFrame(authorityDescriptor, payload, seg.instance)
	if r.length+int64(len(descriptor)) > s.rootMax {
		return nil, ErrAuthoritySegmentFull
	}
	err = s.raw.Append(ctx, seg.logical, r.length, descriptor)
	if err != nil {
		if !errors.Is(err, ErrPrecondition) {
			return nil, err
		}
		curr, e := s.readRoot(ctx, seg.logical)
		if e != nil {
			return nil, e
		}
		if curr.instance != seg.instance || curr.generation < generation {
			return nil, ErrPrecondition
		}
	}
	s.retire(ctx, seg.logical, seg.window)
	return next, nil
}
func (s *SegmentedAuthority) latest(ctx context.Context, key string) (*authoritySegment, error) {
	for attempt := 0; attempt < 16; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r, err := s.readRoot(ctx, key)
		if err != nil {
			return nil, err
		}
		seg, err := s.readSegmentAt(ctx, key, r.generation)
		if errors.Is(err, ErrNotFound) && r.generation == 0 {
			if err = s.ensureGenesis(ctx, s.physical(key, 0), r.seed); err != nil {
				if curr, e := s.readRoot(ctx, key); e == nil && curr.generation > 0 {
					continue
				}
				return nil, err
			}
			continue
		}
		if errors.Is(err, errAuthorityRetired) {
			continue
		}
		if err != nil {
			// Retirement may replace a large frame between its suffix probe
			// and payload range. Retry only if the same lifetime's root proves
			// that this generation is no longer authoritative.
			if ctx.Err() == nil {
				if current, e := s.readRoot(ctx, key); e == nil && current.instance == r.instance && current.generation > r.generation {
					continue
				}
			}
			return nil, err
		}
		if seg.instance != r.instance {
			return nil, errors.New("bucket: authority instance mismatch")
		}
		seg.root = r
		if seg.op == authoritySeal {
			if _, err = s.publish(ctx, seg); err != nil {
				return nil, err
			}
			continue
		}
		return seg, nil
	}
	return nil, ErrPrecondition
}

// Diagnostic listings resolve through the authoritative root. A physical
// generation selected by an earlier List is not evidence of current state.
func (s *SegmentedAuthority) listed(ctx context.Context, key string, _ int64) (*authoritySegment, error) {
	return s.latest(ctx, key)
}
func (s *SegmentedAuthority) Get(ctx context.Context, key string) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	s.readMu.Lock()
	root, cached := s.readRoots[key]
	s.readMu.Unlock()
	if cached {
		seg, err := s.readSegmentAt(ctx, key, root.generation)
		// Only an ordinary value/tombstone is reusable. Seals and retirement
		// force discovery; the descriptor is a locator, never cached data.
		if err == nil && seg.instance == root.instance && seg.op != authoritySeal {
			if seg.deleted {
				return nil, "", ErrNotFound
			}
			return seg.value, seg.token(), nil
		}
	}
	seg, err := s.latest(ctx, key)
	if err != nil {
		return nil, "", err
	}
	s.readMu.Lock()
	if s.readRoots == nil {
		s.readRoots = make(map[string]authorityRoot)
	}
	if prior, ok := s.readRoots[key]; !ok || prior.instance != seg.root.instance || prior.generation <= seg.root.generation {
		if len(s.readRoots) >= 4096 {
			for victim := range s.readRoots {
				delete(s.readRoots, victim)
				break
			}
		}
		copyRoot := *seg.root
		copyRoot.seed = nil // Never retain bootstrap payloads in the locator cache.
		s.readRoots[key] = copyRoot
	}
	s.readMu.Unlock()
	if seg.deleted {
		return nil, "", ErrNotFound
	}
	return seg.value, seg.token(), nil
}
func (s *SegmentedAuthority) validValue(data []byte) error {
	if s.valueMax < 0 || int64(len(data)) > s.valueMax {
		return ErrAuthoritySegmentFull
	}
	return nil
}
func (s *SegmentedAuthority) mutate(ctx context.Context, seg *authoritySegment, data []byte, deleted bool) (string, error) {
	if err := s.validValue(data); err != nil {
		return "", err
	}
	op := authorityValue
	if deleted {
		op = authorityDeleted
	}
	frame := encodeAuthorityFrame(op, data, seg.instance)
	reserve := int64(segOverhead+17) + s.valueMax
	rootRoom := seg.root.length+rootDescriptorBytes <= s.rootMax
	capacityRollover := seg.length+int64(len(frame))+reserve > s.max
	if capacityRollover || (s.window() > seg.root.clock && rootRoom) {
		if !rootRoom {
			return "", ErrAuthoritySegmentFull
		}
		clock := s.window()
		if clock < seg.root.clock {
			clock = seg.root.clock
		}
		payload := append(rootMetadata(seg.window+1, clock), op)
		payload = append(payload, data...)
		seal := encodeAuthorityFrame(authoritySeal, payload, seg.instance)
		if seg.length+int64(len(seal)) > s.max {
			return "", ErrAuthoritySegmentFull
		}
		if err := s.raw.Append(ctx, s.physical(seg.logical, seg.window), seg.length, seal); err != nil {
			return "", err
		}
		seg.op = authoritySeal
		seg.value = payload
		next, err := s.publish(ctx, seg)
		if err != nil {
			return "", err
		}
		return next.token(), nil
	}
	if err := s.raw.Append(ctx, s.physical(seg.logical, seg.window), seg.length, frame); err != nil {
		return "", err
	}
	seg.length += int64(len(frame))
	return seg.token(), nil
}
func (s *SegmentedAuthority) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	if err := s.validValue(data); err != nil {
		return "", err
	}
	seg, err := s.latest(ctx, key)
	if err == nil {
		if !seg.deleted {
			return "", ErrPrecondition
		}
		return s.mutate(ctx, seg, data, false)
	}
	if !errors.Is(err, ErrNotFound) {
		return "", err
	}
	inst := newInstance()
	bootstrap := encodeAuthorityFrame(authorityBootstrap, append(rootMetadata(0, s.window()), data...), inst)
	if int64(len(bootstrap)) > s.rootMax {
		return "", ErrAuthoritySegmentFull
	}
	if err = s.raw.Append(ctx, key, 0, bootstrap); err != nil {
		return "", err
	}
	seed := encodeAuthorityFrame(authorityValue, data, inst)
	if err = s.ensureGenesis(ctx, s.physical(key, 0), seed); err != nil {
		if r, e := s.readRoot(ctx, key); e != nil || r.instance != inst || r.generation == 0 {
			return "", err
		}
	}
	return (&authoritySegment{window: 0, length: int64(len(seed)), instance: inst}).token(), nil
}
func (s *SegmentedAuthority) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	seg, err := s.latest(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return "", ErrPrecondition
	}
	if err != nil {
		return "", err
	}
	if seg.deleted || seg.token() != token {
		return "", ErrPrecondition
	}
	return s.mutate(ctx, seg, data, false)
}
func (s *SegmentedAuthority) ConditionalDelete(ctx context.Context, key, token string) error {
	seg, err := s.latest(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return ErrPrecondition
	}
	if err != nil {
		return err
	}
	if seg.deleted || seg.token() != token {
		return ErrPrecondition
	}
	_, err = s.mutate(ctx, seg, nil, true)
	return err
}
func (s *SegmentedAuthority) Touch(ctx context.Context, key string) error {
	seg, err := s.latest(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if seg.deleted || s.window() <= seg.root.clock {
		return nil
	}
	_, err = s.mutate(ctx, seg, seg.value, false)
	return err
}
func (s *SegmentedAuthority) Put(ctx context.Context, key string, data []byte) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		seg, err := s.latest(ctx, key)
		var token string
		if errors.Is(err, ErrNotFound) || err == nil && seg.deleted {
			token, err = s.ConditionalCreate(ctx, key, data)
		} else if err == nil {
			token, err = s.mutate(ctx, seg, data, false)
		}
		if !errors.Is(err, ErrPrecondition) {
			return token, err
		}
	}
	return "", ErrPrecondition
}

// Delete is a logical tombstone. Destructive physical root deletion would
// reset the lifetime fence and is never a valid online authority operation.
func (s *SegmentedAuthority) Delete(ctx context.Context, key string) error {
	for attempt := 0; attempt < 8; attempt++ {
		seg, err := s.latest(ctx, key)
		if errors.Is(err, ErrNotFound) || err == nil && seg.deleted {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = s.mutate(ctx, seg, nil, true)
		if !errors.Is(err, ErrPrecondition) {
			return err
		}
	}
	return ErrPrecondition
}
func (s *SegmentedAuthority) Stat(ctx context.Context, key string) (int64, string, error) {
	v, t, err := s.Get(ctx, key)
	return int64(len(v)), t, err
}
func (s *SegmentedAuthority) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	v, t, err := s.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if off < 0 || length < 0 || off > int64(len(v)) || length > int64(len(v))-off {
		return nil, "", fmt.Errorf("authority range out of bounds")
	}
	return v[off : off+length], t, nil
}
