package bucket

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"
)

// ErrAuthoritySegmentFull means one authority segment reached its hard byte
// cap. It is a fail-closed safety valve: the writer must retry in the next
// segment window instead of growing the object without bound.
var ErrAuthoritySegmentFull = errors.New("bucket: authority segment full")

const (
	segMagic = "CHL2"
	// segHeader is magic(4) + operation(1) + instance(8) + payload length(4).
	segHeader = 17
	// segFooter is frame length(4) + CRC32(4), written after the payload.
	segFooter = 8
	// segOverhead is header + footer; a frame with an empty payload is valid
	// (a tombstone).
	segOverhead = segHeader + segFooter

	segProbeBytes = 4096
)

const (
	// DefaultAuthoritySegment bounds one physical authority object. Renewals
	// append frames inside the current segment; at the next window the logical
	// record moves to a fresh object and the old one is garbage-collected.
	DefaultAuthoritySegment = time.Hour
	// DefaultAuthoritySegmentMaxBytes is a hard per-object safety cap.
	DefaultAuthoritySegmentMaxBytes int64 = 8 << 20
)

// AuthorityOptions configures the segmented authority layout.
type AuthorityOptions struct {
	// Segment is the window duration. Zero means DefaultAuthoritySegment.
	Segment time.Duration
	// MaxSegmentBytes caps one physical segment. Zero means
	// DefaultAuthoritySegmentMaxBytes.
	MaxSegmentBytes int64
	// Now is injectable for tests. Nil means time.Now.
	Now func() time.Time
}

// segmentData is the bulk-object surface needed by the segmented authority:
// logical reads, tail probes, physical deletion and enumeration.
type segmentData interface {
	TailGet(ctx context.Context, key string, n int) ([]byte, int64, string, error)
	RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error)
	Stat(ctx context.Context, key string) (int64, string, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// SegmentedAuthority keeps the existing append-position fencing (conditional
// create, CAS, conditional delete) but bounds the physical object size:
//
//   - a logical key maps to one segment per time window;
//   - the newest segment is read via a bounded tail probe (payload is suffix
//     framed, so the last value does not require downloading history);
//   - after a segment rolls, readers only need the newest and the previous
//     window; older windows are physically deleted;
//   - legacy single-key append logs are migrated on the first write: the new
//     value is created atomically in the current window and the old object is
//     deleted, so previously bloated owner.json/waker.json objects shrink
//     immediately.
type SegmentedAuthority struct {
	raw     PositionAppender
	data    segmentData
	segment time.Duration
	max     int64
	now     func() time.Time
}

// NewSegmentedAuthority builds a segmented authority. raw performs native
// position-conditional appends; data performs ordinary object reads/lists.
func NewSegmentedAuthority(raw PositionAppender, data segmentData, opts AuthorityOptions) *SegmentedAuthority {
	segment := opts.Segment
	if segment <= 0 {
		segment = DefaultAuthoritySegment
	}
	max := opts.MaxSegmentBytes
	if max <= 0 {
		max = DefaultAuthoritySegmentMaxBytes
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &SegmentedAuthority{raw: raw, data: data, segment: segment, max: max, now: now}
}

func (s *SegmentedAuthority) rawAppender() PositionAppender { return s.raw }

func (s *SegmentedAuthority) window() int64 {
	return s.now().UnixNano() / int64(s.segment)
}

// nearBoundary reports whether the local clock is close enough to the next
// window that a peer with bounded clock skew could already be writing it.
// Outside this guard, a same-window read/write does not need a future-window
// probe on every call.
func (s *SegmentedAuthority) nearBoundary() bool {
	seg := int64(s.segment)
	if seg <= 10 {
		return true
	}
	rem := s.now().UnixNano() % seg
	return rem >= seg-seg/10
}

func (s *SegmentedAuthority) physical(key string, window int64) string {
	return key + ".w" + strconv.FormatInt(window, 10)
}

// ParsePhysicalAuthorityKey splits a physical window key back into its logical
// key and window. It reports false for ordinary objects.
func ParsePhysicalAuthorityKey(key string) (string, int64, bool) {
	i := strings.LastIndex(key, ".w")
	if i < 0 || i+2 >= len(key) {
		return "", 0, false
	}
	base := key[:i]
	if !authorityKey(base) {
		return "", 0, false
	}
	window, err := strconv.ParseInt(key[i+2:], 10, 64)
	if err != nil || window < 0 {
		return "", 0, false
	}
	return base, window, true
}

type authoritySegment struct {
	logical  string
	window   int64
	length   int64
	instance [8]byte
	deleted  bool
	value    []byte
	legacy   bool
}

func (g *authoritySegment) token() string {
	if g.legacy {
		return "l:" + strconv.FormatInt(g.length, 10)
	}
	return "s" + strconv.FormatInt(g.window, 10) + ":" +
		hex.EncodeToString(g.instance[:]) + ":" + strconv.FormatInt(g.length, 10)
}

type segmentToken struct {
	window   int64
	instance [8]byte
	length   int64
	legacy   bool
}

func parseSegmentToken(token string) (segmentToken, bool) {
	if strings.HasPrefix(token, "s") {
		parts := strings.Split(token, ":")
		if len(parts) != 3 {
			return segmentToken{}, false
		}
		window, err := strconv.ParseInt(strings.TrimPrefix(parts[0], "s"), 10, 64)
		if err != nil || window < 0 {
			return segmentToken{}, false
		}
		raw, err := hex.DecodeString(parts[1])
		if err != nil || len(raw) != 8 {
			return segmentToken{}, false
		}
		length, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || length <= 0 {
			return segmentToken{}, false
		}
		var instance [8]byte
		copy(instance[:], raw)
		return segmentToken{window: window, instance: instance, length: length}, true
	}
	if strings.HasPrefix(token, "l:") {
		length, err := strconv.ParseInt(strings.TrimPrefix(token, "l:"), 10, 64)
		if err != nil || length <= 0 {
			return segmentToken{}, false
		}
		return segmentToken{legacy: true, length: length}, true
	}
	// Pre-segmentation AppendAuthority tokens were decimal byte lengths.
	if length, err := strconv.ParseInt(token, 10, 64); err == nil && length > 0 {
		return segmentToken{legacy: true, length: length}, true
	}
	return segmentToken{}, false
}

// encodeSegmentFrame writes one CHL2 frame:
//
//	magic(4) | op(1) | instance(8) | payload len(4) | payload |
//	frame len(4) | crc32(4)
func encodeSegmentFrame(data []byte, deleted bool, instance [8]byte) []byte {
	n := len(data)
	out := make([]byte, segOverhead+n)
	copy(out, segMagic)
	if deleted {
		out[4] = 1
	}
	copy(out[5:13], instance[:])
	binary.BigEndian.PutUint32(out[13:17], uint32(n))
	copy(out[segHeader:], data)
	binary.BigEndian.PutUint32(out[segHeader+n:segHeader+n+4], uint32(segOverhead+n))
	binary.BigEndian.PutUint32(out[segHeader+n+4:segHeader+n+8], crc32.ChecksumIEEE(out[:segHeader+n]))
	return out
}

func decodeSegmentFrame(frame []byte) (value []byte, deleted bool, instance [8]byte, err error) {
	if len(frame) < segOverhead || string(frame[:4]) != segMagic || frame[4] > 1 {
		return nil, false, instance, errors.New("bucket: corrupt authority segment frame")
	}
	n := int(binary.BigEndian.Uint32(frame[13:17]))
	if n < 0 || segOverhead+n != len(frame) {
		return nil, false, instance, errors.New("bucket: truncated authority segment frame")
	}
	if binary.BigEndian.Uint32(frame[segHeader+n:segHeader+n+4]) != uint32(len(frame)) {
		return nil, false, instance, errors.New("bucket: bad authority frame length")
	}
	if binary.BigEndian.Uint32(frame[segHeader+n+4:segHeader+n+8]) != crc32.ChecksumIEEE(frame[:segHeader+n]) {
		return nil, false, instance, errors.New("bucket: corrupt authority segment frame")
	}
	copy(instance[:], frame[5:13])
	return frame[segHeader : segHeader+n], frame[4] == 1, instance, nil
}

// decodeAnyFrames reads a complete object that may mix pre-segmentation CHL1
// frames and new CHL2 frames. It returns the last logical value.
func decodeAnyFrames(raw []byte) (value []byte, deleted bool, instance [8]byte, legacy bool, err error) {
	if len(raw) == 0 {
		return nil, false, instance, false, errors.New("bucket: empty authority object")
	}
	for len(raw) != 0 {
		switch {
		case len(raw) >= 4 && string(raw[:4]) == "CHL1":
			if len(raw) < frameHeader || raw[4] > 1 {
				return nil, false, instance, false, errors.New("bucket: corrupt authority frame")
			}
			n := binary.BigEndian.Uint32(raw[5:9])
			if uint64(n) > uint64(len(raw)-frameHeader) {
				return nil, false, instance, false, errors.New("bucket: truncated authority frame")
			}
			frameLen := frameHeader + int(n)
			if frameCRC(raw[:frameLen]) != binary.BigEndian.Uint32(raw[9:13]) {
				return nil, false, instance, false, errors.New("bucket: corrupt authority frame")
			}
			deleted = raw[4] == 1
			value = raw[frameHeader:frameLen]
			instance = [8]byte{}
			legacy = true
			raw = raw[frameLen:]
		case len(raw) >= 4 && string(raw[:4]) == segMagic:
			if len(raw) < segOverhead {
				return nil, false, instance, false, errors.New("bucket: truncated authority segment frame")
			}
			n := int(binary.BigEndian.Uint32(raw[13:17]))
			frameLen := segOverhead + n
			if n < 0 || frameLen > len(raw) || frameLen < segOverhead {
				return nil, false, instance, false, errors.New("bucket: truncated authority segment frame")
			}
			v, del, inst, derr := decodeSegmentFrame(raw[:frameLen])
			if derr != nil {
				return nil, false, instance, false, derr
			}
			value, deleted, instance, legacy = v, del, inst, false
			raw = raw[frameLen:]
		default:
			return nil, false, instance, false, errors.New("bucket: corrupt authority frame")
		}
	}
	return value, deleted, instance, legacy, nil
}

// readSegmentAt reads the newest frame of one physical segment through a
// bounded tail probe. It falls back to a full decode for legacy CHL1 objects or
// when a provider does not honour suffix ranges.
func (s *SegmentedAuthority) readSegmentAt(ctx context.Context, logical string, window int64) (*authoritySegment, error) {
	key := s.physical(logical, window)
	tail, total, _, err := s.data.TailGet(ctx, key, segProbeBytes)
	if err != nil {
		return nil, err
	}
	if total <= 0 {
		return nil, errors.New("bucket: empty authority segment")
	}
	if len(tail) > segProbeBytes {
		tail = tail[len(tail)-segProbeBytes:]
	}
	if len(tail) >= segFooter {
		frameLen := int64(binary.BigEndian.Uint32(tail[len(tail)-segFooter:]))
		if frameLen >= segOverhead && frameLen <= total {
			var frame []byte
			if frameLen <= int64(len(tail)) {
				frame = tail[len(tail)-int(frameLen):]
			} else {
				off := total - frameLen
				frame, _, rerr := s.data.RangedGet(ctx, key, off, frameLen)
				if rerr != nil {
					return nil, rerr
				}
				if int64(len(frame)) != frameLen {
					return nil, errors.New("bucket: short authority tail read")
				}
			}
			value, deleted, instance, derr := decodeSegmentFrame(frame)
			if derr == nil {
				return &authoritySegment{
					logical: logical, window: window, length: total,
					instance: instance, deleted: deleted, value: value,
				}, nil
			}
		}
	}
	// Legacy object (CHL1) or an unreadable tail: decode the whole object once.
	raw, err := s.raw.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	value, deleted, instance, legacy, err := decodeAnyFrames(raw)
	if err != nil {
		return nil, err
	}
	return &authoritySegment{
		logical: logical, window: window, length: int64(len(raw)),
		instance: instance, deleted: deleted, value: value, legacy: legacy,
	}, nil
}

func (s *SegmentedAuthority) readLegacy(ctx context.Context, logical string) (*authoritySegment, error) {
	raw, err := s.raw.Read(ctx, logical)
	if err != nil {
		return nil, err
	}
	value, deleted, instance, _, err := decodeAnyFrames(raw)
	if err != nil {
		// Resource-deletion fences predate the append authority: they are
		// plain JSON point objects. Treat the raw bytes as one legacy value;
		// the first CAS migrates them into a bounded window segment.
		if strings.HasSuffix(logical, "/resource-fence.json") {
			return &authoritySegment{
				logical: logical, window: -1, length: int64(len(raw)),
				value: raw, legacy: true,
			}, nil
		}
		return nil, err
	}
	return &authoritySegment{
		logical: logical, window: -1, length: int64(len(raw)),
		instance: instance, deleted: deleted, value: value, legacy: true,
	}, nil
}

// newestWindow returns the newest physical window segment for a logical key,
// or ErrNotFound. It never touches the legacy unscoped object.
func (s *SegmentedAuthority) newestWindow(ctx context.Context, logical string) (*authoritySegment, error) {
	h := s.window()
	seg, err := s.readSegmentAt(ctx, logical, h)
	if err == nil {
		// A clock-ahead peer can only have written h+1 near the boundary (the
		// guard must exceed the fleet's clock skew, see ADR-266).
		if s.nearBoundary() {
			if future, ferr := s.readSegmentAt(ctx, logical, h+1); ferr == nil {
				return future, nil
			} else if !errors.Is(ferr, ErrNotFound) {
				return nil, ferr
			}
		}
		return seg, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	// The record was not written in the current window. One bounded lookup of
	// this single logical key recovers the previous/future window or a segment
	// left behind by a long idle period; active paths never reach it.
	keys, lerr := s.data.List(ctx, logical+".w")
	if lerr != nil {
		return nil, lerr
	}
	var newest int64 = -1
	for _, key := range keys {
		base, window, ok := ParsePhysicalAuthorityKey(key)
		if ok && base == logical && window > newest {
			newest = window
		}
	}
	if newest >= 0 {
		return s.readSegmentAt(ctx, logical, newest)
	}
	return nil, ErrNotFound
}

func (s *SegmentedAuthority) latest(ctx context.Context, logical string) (*authoritySegment, error) {
	seg, err := s.newestWindow(ctx, logical)
	if err == nil {
		return seg, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	legacy, gerr := s.readLegacy(ctx, logical)
	if errors.Is(gerr, ErrNotFound) {
		return nil, ErrNotFound
	}
	return legacy, gerr
}

// listed reads the newest window already found by a physical bucket List.
// A diagnostic listing is a snapshot: rediscovering each key's window would
// issue one extra List per historical authority record. A concurrent GC may
// remove the selected window, in which case the caller can retry via Get.
func (s *SegmentedAuthority) listed(ctx context.Context, logical string, window int64) (*authoritySegment, error) {
	if window >= 0 {
		return s.readSegmentAt(ctx, logical, window)
	}
	return s.readLegacy(ctx, logical)
}

func (s *SegmentedAuthority) appendAt(ctx context.Context, seg *authoritySegment, window, pos int64, instance [8]byte, data []byte, deleted bool) (string, error) {
	frame := encodeSegmentFrame(data, deleted, instance)
	if pos+int64(len(frame)) > s.max {
		return "", ErrAuthoritySegmentFull
	}
	if err := s.raw.Append(ctx, s.physical(seg.logical, window), pos, frame); err != nil {
		return "", err
	}
	return (&authoritySegment{
		logical: seg.logical, window: window, length: pos + int64(len(frame)),
		instance: instance, deleted: deleted,
	}).token(), nil
}

func newInstance() [8]byte {
	var instance [8]byte
	if _, err := rand.Read(instance[:]); err != nil {
		binary.BigEndian.PutUint64(instance[:], uint64(time.Now().UnixNano()))
	}
	return instance
}

// gc removes the windows immediately behind the retained previous window. It
// is cheap enough for the hot renewal path.
func (s *SegmentedAuthority) gc(ctx context.Context, logical string, window int64) {
	for w := window - 2; w >= window-4; w-- {
		_ = s.data.Delete(ctx, s.physical(logical, w))
	}
}

// deepGC enumerates this logical key only (a bounded, single-key prefix), so a
// rollover after a long idle period also removes segments far behind the new
// current window. It runs only on window rollovers/creation, not on every
// renewal append.
func (s *SegmentedAuthority) deepGC(ctx context.Context, logical string, window int64) {
	keys, err := s.data.List(ctx, logical+".w")
	if err != nil {
		return
	}
	for _, key := range keys {
		base, w, ok := ParsePhysicalAuthorityKey(key)
		if ok && base == logical && w < window-1 {
			_ = s.data.Delete(ctx, key)
		}
	}
}

// Get returns the newest logical value and an opaque fencing token.
func (s *SegmentedAuthority) Get(ctx context.Context, key string) ([]byte, string, error) {
	seg, err := s.latest(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if seg.deleted {
		return nil, "", ErrNotFound
	}
	return seg.value, seg.token(), nil
}

// ConditionalCreate atomically creates the record when it is logically absent
// (or recreates it after a tombstone). It never grows the legacy key.
func (s *SegmentedAuthority) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	seg, err := s.latest(ctx, key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return "", err
	}
	h := s.window()
	if seg != nil && !seg.deleted {
		return "", ErrPrecondition
	}
	window, pos := h, int64(0)
	instance := newInstance()
	if seg != nil && !seg.legacy {
		if seg.window > window {
			// Reuse a clock-ahead window's tombstone instead of creating an
			// older window that readers would ignore.
			window = seg.window
		}
		if seg.window == window && seg.deleted {
			pos = seg.length
			instance = seg.instance
		}
	}
	token, err := s.appendAt(ctx, &authoritySegment{logical: key}, window, pos, instance, data, false)
	if err != nil {
		return "", err
	}
	// A clock-ahead writer may have created a higher window concurrently. Our
	// frame is valid but not the newest value; report contention.
	curr, cerr := s.latest(ctx, key)
	if cerr != nil {
		return "", cerr
	}
	if curr.window != window || curr.length != pos+int64(segOverhead+len(data)) {
		return "", ErrPrecondition
	}
	if seg != nil && seg.legacy {
		_ = s.data.Delete(ctx, key)
	}
	s.deepGC(ctx, key, window)
	return token, nil
}

// CAS replaces the record only when token still identifies the newest value.
func (s *SegmentedAuthority) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	tok, ok := parseSegmentToken(token)
	if !ok {
		return "", ErrPrecondition
	}
	h := s.window()
	if tok.legacy {
		seg, err := s.readLegacy(ctx, key)
		if errors.Is(err, ErrNotFound) || (err == nil && (seg.length != tok.length || seg.deleted)) {
			return "", ErrPrecondition
		}
		if err != nil {
			return "", err
		}
		// A window already exists, so the legacy value is not current.
		if _, cerr := s.newestWindow(ctx, key); cerr == nil {
			return "", ErrPrecondition
		} else if !errors.Is(cerr, ErrNotFound) {
			return "", cerr
		}
		instance := newInstance()
		newToken, err := s.appendAt(ctx, &authoritySegment{logical: key}, h, 0, instance, data, false)
		if err != nil {
			return "", err
		}
		if curr, cerr := s.latest(ctx, key); cerr != nil || curr.window != h {
			return "", ErrPrecondition
		}
		_ = s.data.Delete(ctx, key)
		s.deepGC(ctx, key, h)
		return newToken, nil
	}

	seg, err := s.readSegmentAt(ctx, key, tok.window)
	if errors.Is(err, ErrNotFound) {
		return "", ErrPrecondition
	}
	if err != nil {
		return "", err
	}
	if seg.length != tok.length || seg.instance != tok.instance || seg.deleted {
		return "", ErrPrecondition
	}
	// Refuse to write an older window after a newer one appeared.
	if higher, err := s.higherSegment(ctx, key, tok.window); err != nil {
		return "", err
	} else if higher {
		return "", ErrPrecondition
	}
	if tok.window < h {
		// The lease survived a segment rollover. Move the new value into the
		// current window at position zero instead of appending to a segment
		// that readers will stop considering next window.
		if _, err := s.readSegmentAt(ctx, key, h); err == nil {
			return "", ErrPrecondition
		} else if !errors.Is(err, ErrNotFound) {
			return "", err
		}
		instance := newInstance()
		tokenOut, err := s.appendAt(ctx, seg, h, 0, instance, data, false)
		if err != nil {
			return "", err
		}
		if latest, lerr := s.latest(ctx, key); lerr != nil || latest.window != h {
			return "", ErrPrecondition
		}
		s.deepGC(ctx, key, h)
		return tokenOut, nil
	}
	instance := tok.instance
	tokenOut, err := s.appendAt(ctx, seg, tok.window, tok.length, instance, data, false)
	if err != nil {
		return "", err
	}
	if tok.window == h && !s.nearBoundary() {
		s.gc(ctx, key, tok.window)
		return tokenOut, nil
	}
	if latest, lerr := s.latest(ctx, key); lerr != nil || latest.window > tok.window {
		return "", ErrPrecondition
	}
	s.gc(ctx, key, tok.window)
	return tokenOut, nil
}

func (s *SegmentedAuthority) higherSegment(ctx context.Context, key string, window int64) (bool, error) {
	h := s.window()
	for _, candidate := range []int64{h + 1, h, h - 1} {
		if candidate <= window {
			continue
		}
		if candidate == h+1 && !s.nearBoundary() {
			continue
		}
		if _, err := s.readSegmentAt(ctx, key, candidate); err == nil {
			return true, nil
		} else if !errors.Is(err, ErrNotFound) {
			return false, err
		}
	}
	return false, nil
}

// ConditionalDelete appends a tombstone, fencing out a stale writer without
// physically resurrecting the old value.
func (s *SegmentedAuthority) ConditionalDelete(ctx context.Context, key, token string) error {
	tok, ok := parseSegmentToken(token)
	if !ok {
		return ErrPrecondition
	}
	h := s.window()
	if tok.legacy {
		seg, err := s.readLegacy(ctx, key)
		if errors.Is(err, ErrNotFound) || (err == nil && (seg.length != tok.length || seg.deleted)) {
			return ErrPrecondition
		}
		if err != nil {
			return err
		}
		if _, cerr := s.newestWindow(ctx, key); cerr == nil {
			return ErrPrecondition
		} else if !errors.Is(cerr, ErrNotFound) {
			return cerr
		}
		instance := newInstance()
		if _, err := s.appendAt(ctx, &authoritySegment{logical: key}, h, 0, instance, nil, true); err != nil {
			return err
		}
		if curr, cerr := s.latest(ctx, key); cerr != nil || curr.window != h || !curr.deleted {
			return ErrPrecondition
		}
		_ = s.data.Delete(ctx, key)
		s.gc(ctx, key, h)
		return nil
	}
	seg, err := s.readSegmentAt(ctx, key, tok.window)
	if errors.Is(err, ErrNotFound) {
		return ErrPrecondition
	}
	if err != nil {
		return err
	}
	if seg.length != tok.length || seg.instance != tok.instance || seg.deleted {
		return ErrPrecondition
	}
	if higher, err := s.higherSegment(ctx, key, tok.window); err != nil {
		return err
	} else if higher {
		return ErrPrecondition
	}
	if tok.window < h {
		if _, err := s.readSegmentAt(ctx, key, h); err == nil {
			return ErrPrecondition
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		instance := newInstance()
		if _, err := s.appendAt(ctx, seg, h, 0, instance, nil, true); err != nil {
			return err
		}
		if latest, lerr := s.latest(ctx, key); lerr != nil || latest.window != h || !latest.deleted {
			return ErrPrecondition
		}
		s.deepGC(ctx, key, h)
		return nil
	}
	if _, err := s.appendAt(ctx, seg, tok.window, tok.length, tok.instance, nil, true); err != nil {
		return err
	}
	if tok.window == h && !s.nearBoundary() {
		s.gc(ctx, key, tok.window)
		return nil
	}
	if latest, lerr := s.latest(ctx, key); lerr != nil || latest.window > tok.window {
		return ErrPrecondition
	}
	s.gc(ctx, key, tok.window)
	return nil
}

// Put implements unconditional replace with bounded retries.
func (s *SegmentedAuthority) Put(ctx context.Context, key string, data []byte) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		seg, err := s.latest(ctx, key)
		switch {
		case errors.Is(err, ErrNotFound), err == nil && seg.deleted:
			token, cerr := s.ConditionalCreate(ctx, key, data)
			if errors.Is(cerr, ErrPrecondition) {
				continue
			}
			return token, cerr
		case err != nil:
			return "", err
		default:
			token, cerr := s.CAS(ctx, key, data, seg.token())
			if errors.Is(cerr, ErrPrecondition) {
				continue
			}
			return token, cerr
		}
	}
	return "", ErrPrecondition
}

// Delete removes the logical record and all its physical segments/legacy key.
// ConditionalDelete remains the fenced variant used by owner release.
func (s *SegmentedAuthority) Delete(ctx context.Context, key string) error {
	keys, err := s.data.List(ctx, key+".w")
	if err != nil {
		return err
	}
	for _, physical := range keys {
		if _, _, ok := ParsePhysicalAuthorityKey(physical); !ok {
			continue
		}
		if err := s.data.Delete(ctx, physical); err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	if err := s.data.Delete(ctx, key); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

// Stat returns the logical value size and its fencing token.
func (s *SegmentedAuthority) Stat(ctx context.Context, key string) (int64, string, error) {
	seg, err := s.latest(ctx, key)
	if err != nil {
		return 0, "", err
	}
	if seg.deleted {
		return 0, "", ErrNotFound
	}
	return int64(len(seg.value)), seg.token(), nil
}

// RangedGet returns a byte range of the logical value.
func (s *SegmentedAuthority) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	data, token, err := s.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if off < 0 || length < 0 || off > int64(len(data)) || length > int64(len(data))-off {
		return nil, "", fmt.Errorf("authority range out of bounds")
	}
	return data[off : off+length], token, nil
}
