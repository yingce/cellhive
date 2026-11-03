package doruntime

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/doticket"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestJWTTrustedModules(t *testing.T) {
	if !strings.Contains(capnpTemplate, `(name = "jwt.js", esModule = embed "jwt.js")`) {
		t.Fatal("trusted host needs JWT module")
	}
	if !strings.Contains(capnpTemplate, `(name = "BINDING_SIGNING_KEY", fromEnvironment = "CELLHIVE_HOST_BINDING_SIGNING_KEY")`) {
		t.Fatal("trusted host needs purpose-isolated binding key")
	}
}

// Load the complete generated host in workerd, not extracted JS functions.
// An early parser failure must fail immediately rather than wait for healthz.
func TestJWTCompleteHostStartsWorkerd(t *testing.T) {
	workerd, err := FindWorkerd()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	port := freePort(t)
	ticketKey := bytes.Repeat([]byte{7}, 32)
	upstream := httptest.NewServer((&ownerStub{}).handler(`import { DurableObject } from "cloudflare:workers"; export class Room extends DurableObject { fetch() { return new Response("ticket-ok"); } }`))
	defer upstream.Close()
	path, err := Render(t.TempDir(), Config{
		CellURL: upstream.URL, CellToken: base64.RawURLEncoding.EncodeToString(ticketKey), Addr: fmt.Sprintf("127.0.0.1:%d", port),
		DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "jwt-startup",
		DoTicketSecret: base64.RawURLEncoding.EncodeToString(ticketKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, path) }()
	exited := false
	defer func() {
		cancel()
		if !exited {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("workerd did not exit after cancellation")
			}
		}
	}()
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 200 * time.Millisecond}
	defer client.CloseIdleConnections()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			exited = true
			t.Fatalf("complete host exited before health check: %v", err)
		case <-deadline.C:
			t.Fatal("complete host failed to become healthy")
		case <-tick.C:
			response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
			if err == nil {
				response.Body.Close()
				if response.StatusCode == http.StatusOK {
					testNarrowOwnerTickets(t, client, fmt.Sprintf("http://127.0.0.1:%d", port), ticketKey)
					return
				}
			}
		}
	}
}

func testNarrowOwnerTickets(t *testing.T, client *http.Client, base string, key []byte) {
	t.Helper()
	claims := doticket.Claims{NS: "acme", Worker: "web", StorageClass: "Room", Shard: cell.DOShard("acme", "web", "Room", "object"), Class: "Room", BundleSHA: "bundle", StorageID: "incarnation", Version: 2, Limits: "null", Exp: time.Now().Add(30 * time.Second).UnixMilli()}
	token, err := doticket.Mint(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	valid := []byte(`{"namespace":"acme","worker":"web","class":"Room","storage_class":"Room","id":"object","bundle_sha":"bundle","storage_id":"incarnation","version":2,"request":{"path":"/"}}`)
	req, _ := http.NewRequest("POST", base+"/v1/do/invoke", bytes.NewReader(valid))
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("valid signed execution status=%d body=%s", res.StatusCode, responseBody)
	}
	for field, value := range map[string]any{"class": "Other", "bundle_sha": "other", "storage_id": "other", "version": 3, "kind": "alarm", "compat_flags": []string{"nodejs_compat"}} {
		t.Run("ticket-rejects-"+field, func(t *testing.T) {
			body := map[string]any{"namespace": "acme", "worker": "web", "class": "Room", "storage_class": "Room", "id": "object", "bundle_sha": "bundle", "storage_id": "incarnation", "version": 2}
			body[field] = value
			raw, _ := json.Marshal(body)
			req, _ := http.NewRequest("POST", base+"/v1/do/invoke", bytes.NewReader(raw))
			req.Header.Set("Authorization", "Bearer "+token)
			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusForbidden {
				t.Fatalf("tampered %s: status=%d", field, res.StatusCode)
			}
		})
	}
	for _, query := range []string{"class=Other&bundle_sha=bundle&storage_id=incarnation&version=2", "class=Room&bundle_sha=other&storage_id=incarnation&version=2", "class=Room&bundle_sha=bundle&storage_id=other&version=2", "class=Room&bundle_sha=bundle&storage_id=incarnation&version=3"} {
		req, _ := http.NewRequest("GET", base+"/v1/do/connect?namespace=acme&worker=web&storage_class=Room&id=object&"+query, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 403 {
			t.Fatalf("connect %s status=%d", query, res.StatusCode)
		}
	}
}
