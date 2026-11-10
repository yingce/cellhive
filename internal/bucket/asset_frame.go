package bucket

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

const assetHeaderSize = 16
const assetMagic = "CHASSET1"

// Only COS needs this adapter. The nonempty frame makes append-at-zero a
// create-if-absent operation even when the logical asset is empty.
func (b *ProviderBucket) framedAsset(key string) bool {
	_, ok := b.appender.(*COSAppender)
	return ok && strings.HasPrefix(key, "assets/")
}

func encodeAsset(data []byte) []byte {
	frame := make([]byte, assetHeaderSize+len(data))
	copy(frame, assetMagic)
	binary.BigEndian.PutUint64(frame[8:16], uint64(len(data)))
	copy(frame[assetHeaderSize:], data)
	return frame
}

func assetLength(header []byte, physical int64) (int64, error) {
	if len(header) != assetHeaderSize || string(header[:8]) != assetMagic || physical < assetHeaderSize || binary.BigEndian.Uint64(header[8:]) != uint64(physical-assetHeaderSize) {
		return 0, fmt.Errorf("bucket: invalid or incomplete COS asset frame")
	}
	return physical - assetHeaderSize, nil
}

func (b *ProviderBucket) openAsset(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	r, physical, tag, err := b.S3Bucket.OpenReader(ctx, key)
	if err != nil {
		return nil, 0, "", err
	}
	header := make([]byte, assetHeaderSize)
	_, err = io.ReadFull(r, header)
	var size int64
	if err == nil {
		size, err = assetLength(header, physical)
	}
	if err != nil {
		r.Close()
		return nil, 0, "", err
	}
	return r, size, tag, nil
}

func (b *ProviderBucket) statAsset(ctx context.Context, key string) (int64, string, error) {
	physical, tag, err := b.S3Bucket.Stat(ctx, key)
	if err != nil {
		return 0, "", err
	}
	if physical < assetHeaderSize {
		return 0, "", fmt.Errorf("bucket: incomplete COS asset")
	}
	header, readTag, err := b.S3Bucket.RangedGet(ctx, key, 0, assetHeaderSize)
	if err != nil {
		return 0, "", err
	}
	if tag != readTag {
		return 0, "", ErrPrecondition
	}
	size, err := assetLength(header, physical)
	return size, tag, err
}

func (b *ProviderBucket) getAsset(ctx context.Context, key string) ([]byte, string, error) {
	r, size, tag, err := b.openAsset(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer r.Close()
	data, err := readAllBounded(key, r, size, b.maxRead)
	if err == nil && int64(len(data)) != size {
		err = io.ErrUnexpectedEOF
	}
	return data, tag, err
}

func (b *ProviderBucket) rangeAsset(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	size, tag, err := b.statAsset(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if off < 0 || length <= 0 || off > size || length > size-off {
		return nil, "", fmt.Errorf("bucket: invalid asset range")
	}
	data, readTag, err := b.S3Bucket.RangedGet(ctx, key, off+assetHeaderSize, length)
	if err == nil && tag != readTag {
		err = ErrPrecondition
	}
	if err == nil && int64(len(data)) != length {
		err = io.ErrUnexpectedEOF
	}
	return data, tag, err
}

// Listings are discovery/cleanup, not read validation. Strip framing overhead
// without extra object reads; keep even interrupted or old objects discoverable
// so namespace purge can remove them. Get/Stat validate before serving bytes.
func (b *ProviderBucket) logicalAssetSizes(items []ObjectInfo) []ObjectInfo {
	for i := range items {
		if b.framedAsset(items[i].Key) {
			items[i].Size = max(0, items[i].Size-assetHeaderSize)
		}
	}
	return items
}

// TailGet must not leak the physical header through the optional suffix API.
func (b *ProviderBucket) TailGet(ctx context.Context, key string, n int) ([]byte, int64, string, error) {
	if !b.framedAsset(key) {
		return b.S3Bucket.TailGet(ctx, key, n)
	}
	size, tag, err := b.statAsset(ctx, key)
	if err != nil {
		return nil, 0, "", err
	}
	if n < 0 {
		return nil, 0, "", fmt.Errorf("bucket: invalid tail size")
	}
	length := min(int64(n), size)
	if length == 0 {
		return []byte{}, size, tag, nil
	}
	data, readTag, err := b.rangeAsset(ctx, key, size-length, length)
	if err == nil && readTag != tag {
		err = ErrPrecondition
	}
	return data, size, tag, err
}
