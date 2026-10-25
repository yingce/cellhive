package server

import (
	"cellhive/internal/workflow"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type workflowFenceBody struct {
	io.Reader
	before func()
}

func (b *workflowFenceBody) Read(p []byte) (int, error) {
	if b.before != nil {
		f := b.before
		b.before = nil
		f()
	}
	return b.Reader.Read(p)
}
func TestOldWorkflowCallbackAfterRestart(t *testing.T) {
	for _, endpoint := range []string{"step", "finish"} {
		t.Run(endpoint, func(t *testing.T) {
			s, _ := newTestServer(t)
			ctx := context.Background()
			if _, err := s.Owner.Claim(ctx, workflow.Scope("acme", "wf"), time.Now()); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Workflows.Create(ctx, "acme", "wf", "i", nil); err != nil {
				t.Fatal(err)
			}
			token, _, _, ok, err := s.Workflows.ClaimRun(ctx, "acme", "wf", "i", 60000)
			if err != nil || !ok {
				t.Fatal(err)
			}
			b := &workflowFenceBody{Reader: strings.NewReader(base64.StdEncoding.EncodeToString([]byte(`"old-result"`))), before: func() {
				if err := s.Workflows.Restart(ctx, "acme", "wf", "i"); err != nil {
					t.Fatal(err)
				}
				_, _, _, ok, err := s.Workflows.ClaimRun(ctx, "acme", "wf", "i", 60000)
				if err != nil || !ok {
					t.Fatal(err)
				}
			}}
			method := http.MethodPut
			if endpoint == "finish" {
				method = http.MethodPost
			}
			req := httptest.NewRequest(method, "/v1/internal/workflow/"+endpoint+"?ns=acme&workflow=wf&id=i&name=a&run="+token, b)
			req.Header.Set("x-cellhive-internal-token", "tok")
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			in, err := s.Workflows.Get(ctx, "acme", "wf", "i")
			if err != nil {
				t.Fatal(err)
			}
			value, found, err := s.Workflows.GetStep(ctx, "acme", "wf", "i", "a")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("old callback HTTP=%d; new run status=%s output=%s step=%s found=%v", rr.Code, in.Status, in.Output, value, found)
			if in.Status != workflow.StatusRunning || len(in.Output) != 0 || found {
				t.Fatalf("stale callback mutated new run: status=%s output=%s step=%s", in.Status, in.Output, value)
			}
			if rr.Code != http.StatusConflict {
				t.Fatalf("stale callback accepted after restart: HTTP %d", rr.Code)
			}
		})
	}
}
