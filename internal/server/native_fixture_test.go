package server

import (
	"cellhive/internal/ltx"
	"testing"
)

// nativeTestFile encodes real native pages. Scope and epoch belong to the
// authenticated request envelope, not the file header.
func nativeTestFile(t *testing.T, lo, hi uint64, payload []byte) []byte {
	t.Helper()
	const ps = 4096
	count := (len(payload) + ps - 1) / ps
	if count == 0 {
		count = 1
	}
	pages := make([]ltx.WALPage, count)
	for i := range pages {
		data := make([]byte, ps)
		if i*ps < len(payload) {
			copy(data, payload[i*ps:min((i+1)*ps, len(payload))])
		}
		pages[i] = ltx.WALPage{PageNo: uint32(i + 1), Data: data}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: ps, Commit: uint32(count), MinTXID: lo, MaxTXID: hi}, pages)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCommitBinaryRejectsPrivateFormat(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	if rr := do(t, h, "POST", "/v1/internal/claim", "tok", []byte(`{"scope":"demo/__kv__/native-only"}`)); rr.Code != 200 {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}
	raw := []byte("\x4c\x54\x58\x31\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x07\x56\x50\x41\xe6\x70\x72\x69\x76\x61\x74\x65")
	rr := do(t, h, "POST", "/v1/internal/commit_binary?scope=demo/__kv__/native-only&epoch=1", "tok", raw)
	if rr.Code != 400 {
		t.Fatalf("private format must fail validation: %d %s", rr.Code, rr.Body.String())
	}
}
