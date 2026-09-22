package bucket

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
)

// PositionAppender atomically appends only when the object's byte length matches
// position. A failed append must not change the object.
type PositionAppender interface {
	Read(context.Context, string) ([]byte, error)
	Append(context.Context, string, int64, []byte) error
}

// AppendAuthority implements conditional records without target-object overwrite
// or conditional delete. Tokens are monotonically increasing byte positions.
type AppendAuthority struct{ raw PositionAppender }

func NewAppendAuthority(raw PositionAppender) *AppendAuthority { return &AppendAuthority{raw: raw} }

const frameHeader = 13 // magic(4), operation(1), payload length(4), crc32(4)

func encodeFrame(data []byte, deleted bool) []byte {
	out := make([]byte, frameHeader+len(data))
	copy(out, "CHL1")
	if deleted {
		out[4] = 1
	}
	binary.BigEndian.PutUint32(out[5:9], uint32(len(data)))
	copy(out[frameHeader:], data)
	binary.BigEndian.PutUint32(out[9:13], frameCRC(out))
	return out
}

func decodeFrames(raw []byte) ([]byte, bool, error) {
	var value []byte
	var deleted bool
	if len(raw) == 0 {
		return nil, false, fmt.Errorf("append authority: empty object")
	}
	for len(raw) != 0 {
		if len(raw) < frameHeader || string(raw[:4]) != "CHL1" || raw[4] > 1 {
			return nil, false, fmt.Errorf("append authority: corrupt frame")
		}
		n := binary.BigEndian.Uint32(raw[5:9])
		if uint64(n) > uint64(len(raw)-frameHeader) {
			return nil, false, fmt.Errorf("append authority: truncated frame")
		}
		if frameCRC(raw[:frameHeader+int(n)]) != binary.BigEndian.Uint32(raw[9:13]) {
			return nil, false, fmt.Errorf("append authority: corrupt frame")
		}
		deleted = raw[4] == 1
		value = raw[frameHeader : frameHeader+int(n)]
		raw = raw[frameHeader+int(n):]
	}
	return value, deleted, nil
}

func frameCRC(frame []byte) uint32 {
	h := crc32.NewIEEE()
	_, _ = h.Write(frame[:9])
	_, _ = h.Write(frame[13:])
	return h.Sum32()
}

func (b *AppendAuthority) Get(ctx context.Context, key string) ([]byte, string, error) {
	raw, err := b.raw.Read(ctx, key)
	if err != nil {
		return nil, "", err
	}
	value, deleted, err := decodeFrames(raw)
	if err != nil {
		return nil, "", err
	}
	if deleted {
		return nil, "", ErrNotFound
	}
	return value, strconv.FormatInt(int64(len(raw)), 10), nil
}

func (b *AppendAuthority) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	raw, err := b.raw.Read(ctx, key)
	pos := int64(0)
	if err == nil {
		_, deleted, parseErr := decodeFrames(raw)
		if parseErr != nil {
			return "", parseErr
		}
		if !deleted {
			return "", ErrPrecondition
		}
		pos = int64(len(raw))
	} else if !errors.Is(err, ErrNotFound) {
		return "", err
	}
	frame := encodeFrame(data, false)
	if err := b.raw.Append(ctx, key, pos, frame); err != nil {
		return "", err
	}
	return strconv.FormatInt(pos+int64(len(frame)), 10), nil
}

func (b *AppendAuthority) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	pos, err := strconv.ParseInt(token, 10, 64)
	if err != nil || pos <= 0 {
		return "", ErrPrecondition
	}
	_, current, err := b.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return "", ErrPrecondition
	}
	if err != nil {
		return "", err
	}
	if current != token {
		return "", ErrPrecondition
	}
	frame := encodeFrame(data, false)
	if err := b.raw.Append(ctx, key, pos, frame); err != nil {
		return "", err
	}
	return strconv.FormatInt(pos+int64(len(frame)), 10), nil
}

func (b *AppendAuthority) ConditionalDelete(ctx context.Context, key, token string) error {
	pos, err := strconv.ParseInt(token, 10, 64)
	if err != nil || pos <= 0 {
		return ErrPrecondition
	}
	_, current, err := b.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return ErrPrecondition
	}
	if err != nil {
		return err
	}
	if current != token {
		return ErrPrecondition
	}
	return b.raw.Append(ctx, key, pos, encodeFrame(nil, true))
}
