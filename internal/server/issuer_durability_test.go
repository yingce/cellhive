package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bearertoken"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/control"
	"cellhive/internal/replica"
)

func TestIssuerCreateCaptureFailureRecovery(t *testing.T) {
	s, _, _ := newControlForwardServer(t, t.TempDir(), "issuer-capture")
	s.Owner = nil
	admin := testRoleJWT(t, "admin")
	rep := replica.New(s.Bucket)
	var fail atomic.Bool
	var tracked atomic.Value
	tracked.Store("")
	fail.Store(true)
	s.Capture = &cellcapture.Manager{
		Store: s.Store, AutoSnapshot: true, Checkpoint: time.Hour,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: testCommitterFunc(func(ctx context.Context, sc cell.Scope, epoch uint64, data []byte) error {
			issuers, err := s.Control.ListIssuers(ctx)
			if err != nil {
				return err
			}
			present := len(issuers) > 0
			if id := tracked.Load().(string); id != "" {
				_, err := s.Control.GetIssuer(ctx, id)
				present = err == nil
			}
			if present && fail.CompareAndSwap(true, false) {
				return errors.New("injected issuer durability failure")
			}
			_, _, err = rep.Append(ctx, sc, epoch, data)
			return err
		}),
	}
	defer s.Capture.Stop()
	body := `{"name":"client","namespaces":["acme"],"max_token_ttl_seconds":60}`
	w := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/issuers", admin, body)
	var failed struct {
		Error    string `json:"error"`
		IssuerID string `json:"issuer_id"`
		Key      string `json:"key"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &failed); err != nil {
		t.Fatal(err)
	}
	if w.Code != 503 || failed.Error != "outcome_unknown" || failed.IssuerID == "" || failed.Key != "" {
		t.Fatalf("uncertain create: %d %s", w.Code, w.Body.String())
	}
	w = adminDo(t, s.AdminHandler(), http.MethodGet, "/v1/control/issuers/"+failed.IssuerID, admin, "")
	if w.Code != 200 || strings.Contains(w.Body.String(), `"key"`) {
		t.Fatalf("authoritative recovery read: %d %s", w.Code, w.Body.String())
	}
	w = adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/issuers", admin, body)
	if w.Code != 409 || strings.Contains(w.Body.String(), `"key"`) {
		t.Fatalf("retry existing create: %d %s", w.Code, w.Body.String())
	}
	w = adminDo(t, s.AdminHandler(), http.MethodDelete, "/v1/control/issuers/"+failed.IssuerID, admin, "")
	if w.Code != 200 {
		t.Fatalf("recovery delete: %d %s", w.Code, w.Body.String())
	}
	w = adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/issuers", admin, body)
	var fresh struct {
		Issuer control.Issuer `json:"issuer"`
		Key    string         `json:"key"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &fresh); err != nil {
		t.Fatal(err)
	}
	if w.Code != 201 || fresh.Key == "" || fresh.Issuer.ID == failed.IssuerID {
		t.Fatalf("fresh recovery create: %d %s", w.Code, w.Body.String())
	}
	tracked.Store(fresh.Issuer.ID)
	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		fail.Store(true)
		payload := ""
		if method == http.MethodPatch {
			payload = `{"enabled":false}`
		}
		w = adminDo(t, s.AdminHandler(), method, "/v1/control/issuers/"+fresh.Issuer.ID, admin, payload)
		if w.Code != 503 || !strings.Contains(w.Body.String(), `"error":"outcome_unknown"`) || strings.Contains(w.Body.String(), `"key"`) {
			t.Fatalf("uncertain %s: %d %s", method, w.Code, w.Body.String())
		}
		local, err := s.Control.GetIssuer(t.Context(), fresh.Issuer.ID)
		if err != nil || local.Enabled || (method == http.MethodDelete && local.DeletedMs == 0) {
			t.Fatalf("%s was not locally committed: %+v %v", method, local, err)
		}
		fail.Store(false)
		w = adminDo(t, s.AdminHandler(), method, "/v1/control/issuers/"+fresh.Issuer.ID, admin, payload)
		if w.Code != 200 {
			t.Fatalf("retry %s: %d %s", method, w.Code, w.Body.String())
		}
	}
}

func TestIssuerValidationBeforeCaptureAndStorageErrors(t *testing.T) {
	s, _, _ := newControlForwardServer(t, t.TempDir(), "issuer-errors")
	s.Owner = nil
	admin := testRoleJWT(t, "admin")
	var captures atomic.Int32
	s.Capture = &cellcapture.Manager{Store: s.Store, AutoSnapshot: true,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error {
			captures.Add(1)
			return errors.New("capture unavailable")
		}),
	}
	defer s.Capture.Stop()
	w := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/issuers", admin, `{"name":"invalid","namespaces":["acme"],"max_token_ttl_seconds":0}`)
	if w.Code != 400 || captures.Load() != 0 {
		t.Fatalf("validation touched capture: %d calls=%d", w.Code, captures.Load())
	}
	w = adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/issuers", admin, `{"name":"valid","namespaces":["acme"],"max_token_ttl_seconds":60}`)
	if w.Code != 503 || strings.Contains(w.Body.String(), `"key"`) {
		t.Fatalf("capture unavailable: %d %s", w.Code, w.Body.String())
	}
}

func TestIssuerMetadataHTTPBudgetAndRemoteLookup(t *testing.T) {
	dir := t.TempDir()
	a, om, hs := newControlForwardServer(t, dir, "issuer-budget-owner")
	b, _, _ := newControlForwardServer(t, dir, "issuer-budget-follower")
	om.Advertise = hs.URL
	if _, err := om.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	admin := testRoleJWT(t, "admin")
	for _, size := range []int{20000, 256} {
		body, _ := json.Marshal(map[string]any{"name": strings.Repeat("a", size), "namespaces": []string{"acme"}, "max_token_ttl_seconds": 60})
		w := adminDo(t, b.AdminHandler(), http.MethodPost, "/v1/control/issuers", admin, string(body))
		if size > 256 {
			if w.Code != 400 {
				t.Fatalf("oversized issuer: %d", w.Code)
			}
			continue
		}
		var created struct {
			Issuer control.Issuer `json:"issuer"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
			t.Fatal(err)
		}
		if w.Code != 201 {
			t.Fatalf("valid issuer: %d %s", w.Code, w.Body.String())
		}
		token := jwtTestToken(t, a, "issuer/"+created.Issuer.ID, bearertoken.Claims{Type: "namespace", Namespaces: []string{"acme"}})
		w = adminDo(t, b.AdminHandler(), http.MethodGet, "/v1/control/apps", token, "")
		if w.Code != 200 {
			t.Fatalf("remote authorization: %d %s", w.Code, w.Body.String())
		}
	}
}
