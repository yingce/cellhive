package ltx

import (
	"fmt"
	"io"
)

// CompactNative folds ordered native files directly into one native file.
// Like celld's Compactor, it retains one decoded page per input, picks the
// newest occurrence of each page, and applies the final database truncation.
// The caller must publish the output only after this function succeeds.
func CompactNative(w io.Writer, readers []io.Reader) (NativeHeader, error) {
	return compactNative(w, readers, 0)
}

// CompactNativeFrom clips a previously covered prefix of the first input.
// NoChecksum permits this exact range adjustment, as in celld compact_from.
func CompactNativeFrom(w io.Writer, readers []io.Reader, min uint64) (NativeHeader, error) {
	if min == 0 {
		return NativeHeader{}, fmt.Errorf("ltx: zero compaction minimum")
	}
	return compactNative(w, readers, min)
}

func compactNative(w io.Writer, readers []io.Reader, min uint64) (NativeHeader, error) {
	if len(readers) == 0 {
		return NativeHeader{}, fmt.Errorf("ltx: compaction requires inputs")
	}
	type input struct {
		decoder *NativeDecoder
		page    WALPage
		done    bool
	}
	inputs := make([]input, len(readers))
	for i, r := range readers {
		d, err := NewNativeDecoder(r)
		if err != nil {
			return NativeHeader{}, err
		}
		inputs[i].decoder = d
		if i > 0 {
			previous := inputs[i-1].decoder.Header
			if d.Header.PageSize != previous.PageSize || previous.MaxTXID == ^uint64(0) || d.Header.MinTXID > previous.MaxTXID+1 || d.Header.MaxTXID <= previous.MaxTXID {
				return NativeHeader{}, fmt.Errorf("ltx: non-contiguous compaction inputs")
			}
		}
	}
	first, last := inputs[0].decoder.Header, inputs[len(inputs)-1].decoder.Header
	h := NativeHeader{Flags: nativeNoChecksum, PageSize: first.PageSize, Commit: last.Commit, MinTXID: first.MinTXID, MaxTXID: last.MaxTXID, Timestamp: last.Timestamp}
	if min != 0 {
		if min < first.MinTXID || min > first.MaxTXID {
			return NativeHeader{}, fmt.Errorf("ltx: compaction minimum outside first input")
		}
		h.MinTXID = min
	}
	e, err := NewNativeEncoder(w, h)
	if err != nil {
		return NativeHeader{}, err
	}
	for {
		var lowest uint32
		for i := range inputs {
			in := &inputs[i]
			if in.page.PageNo == 0 && !in.done {
				page, err := in.decoder.ReadPage()
				if err == io.EOF {
					in.done = true
				} else if err != nil {
					return NativeHeader{}, err
				} else {
					in.page = page
				}
			}
			if pg := in.page.PageNo; pg != 0 && (lowest == 0 || pg < lowest) {
				lowest = pg
			}
		}
		if lowest == 0 {
			break
		}
		written := false
		for i := len(inputs) - 1; i >= 0; i-- {
			in := &inputs[i]
			if in.page.PageNo != lowest {
				continue
			}
			if !written && lowest <= h.Commit {
				if err := e.WritePage(in.page); err != nil {
					return NativeHeader{}, err
				}
				written = true
			}
			in.page = WALPage{}
		}
	}
	if err := e.Close(); err != nil {
		return NativeHeader{}, err
	}
	return h, nil
}
