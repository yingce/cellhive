package bucket

import (
	"fmt"
	"io"
	"strings"
	"time"

	"cellhive/internal/ltx"
)

const ltxTimestampMetadataKey = "litestream-timestamp"

// ltxTimestamp reads only the native header and restores the caller's cursor.
// Bucket APIs also accept arbitrary payloads; only native LTX gets metadata.
func ltxTimestamp(key string, src io.ReadSeeker, size int64) (string, error) {
	if size < ltx.NativeHeaderSize || !strings.HasPrefix(key, "cells/") || !strings.Contains(key, "/ltx/e") || !strings.HasSuffix(key, ".ltx") {
		return "", nil
	}
	pos, err := src.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}
	var raw [ltx.NativeHeaderSize]byte
	_, readErr := io.ReadFull(src, raw[:])
	if _, err := src.Seek(pos, io.SeekStart); err != nil {
		return "", err
	}
	if readErr != nil {
		return "", readErr
	}
	h, err := ltx.ParseNativeHeader(raw[:])
	if err != nil {
		return "", nil
	}
	ts := time.UnixMilli(max(h.Timestamp, 0)).UTC()
	if ts.Year() > 9999 {
		return "", fmt.Errorf("bucket: LTX timestamp outside RFC3339 range")
	}
	return ts.Format(time.RFC3339Nano), nil
}
