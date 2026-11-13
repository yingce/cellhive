package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/artifacts"
)

// A control request whose body exceeds the authorization probe limit must reach
// the handler intact. The probe used to replace r.Body with its truncated
// prefix, so `cellhive bundle put` (no namespace query) stored a 4 MiB prefix
// under that prefix's SHA.
func TestBundleUploadWithoutNamespaceKeepsFullBody(t *testing.T) {
	s := newControlServer(t)
	data := bytes.Repeat([]byte("cellhive-bundle-padding-"), 400<<10)
	if len(data) <= controlProbeLimit {
		t.Fatalf("test body %d must exceed probe limit %d", len(data), controlProbeLimit)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/control/bundle", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "admin"))
	req.Header.Set("content-type", "application/octet-stream")
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bundle upload: %d %s", rr.Code, rr.Body.String())
	}
	var out struct {
		SHA  string `json:"sha"`
		Size int    `json:"size"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("response: %s: %v", rr.Body.String(), err)
	}
	if want := artifacts.Hash(data); out.SHA != want {
		t.Fatalf("stored sha %s, want %s (body truncated)", out.SHA, want)
	}
	if out.Size != len(data) {
		t.Fatalf("stored size %d, want %d", out.Size, len(data))
	}
}

// An oversized body cannot be authorized from a prefix, so it must not be
// silently attributed to a namespace found in the probe, and the stream must be
// restored for the handler.
func TestControlTargetNSRestoresOversizedBody(t *testing.T) {
	data := bytes.Repeat([]byte{0xa5}, controlProbeLimit+1)
	req := httptest.NewRequest(http.MethodPost, "/v1/control/bundle", bytes.NewReader(data))
	ns, body, err := controlTargetNS(req)
	if err != nil || ns != "" || body != nil {
		t.Fatalf("oversized probe = %q %v %v, want empty ns and no buffered body", ns, body, err)
	}
	got, err := io.ReadAll(req.Body)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("body not restored: got %d bytes, want %d", len(got), len(data))
	}
}

// The truncated prefix must never be treated as an authorization target: a JSON
// body that is cut mid-object still yields an empty namespace.
func TestControlTargetNSRejectsTruncatedJSON(t *testing.T) {
	head := []byte(`{"namespace":"tenant-a","padding":"`)
	data := append(append([]byte{}, head...), bytes.Repeat([]byte("x"), controlProbeLimit)...)
	req := httptest.NewRequest(http.MethodPost, "/v1/control/deploy", bytes.NewReader(data))
	ns, body, err := controlTargetNS(req)
	if err != nil || ns != "" || body != nil {
		t.Fatalf("truncated JSON probe = %q %v %v, want empty ns", ns, body, err)
	}
	got, rerr := io.ReadAll(req.Body)
	if rerr != nil || !bytes.Equal(got, data) {
		t.Fatalf("body not restored: got %d bytes, want %d", len(got), len(data))
	}
}
