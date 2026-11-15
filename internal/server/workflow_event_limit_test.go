package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"cellhive/internal/scopedtoken"
)

type failingEventReader struct{}

func (failingEventReader) Read([]byte) (int, error) {
	return 0, errors.New("event body read failed")
}

func TestWorkflowEventBodyLimit(t *testing.T) {
	for _, size := range []int{maxWorkflowPayloadBytes - 1, maxWorkflowPayloadBytes, maxWorkflowPayloadBytes + 1, 2 * maxWorkflowPayloadBytes} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			payload := append([]byte(`{"payload":"`), bytes.Repeat([]byte("x"), size-len(`{"payload":""}`))...)
			payload = append(payload, []byte(`"}`)...)
			if !json.Valid(payload) || len(payload) != size {
				t.Fatal("invalid test payload")
			}
			testWorkflowEventBody(t, bytes.NewReader(payload), payload, size > maxWorkflowPayloadBytes, false)
		})
	}
}

func TestWorkflowEventReadFailureDoesNotAppend(t *testing.T) {
	testWorkflowEventBody(t, io.MultiReader(bytes.NewReader([]byte(`{"payload":`)), failingEventReader{}), nil, false, true)
}

func testWorkflowEventBody(t *testing.T, body io.Reader, payload []byte, tooLarge, readFailure bool) {
	t.Helper()
	s, _ := newTestServer(t)
	if err := registerTestBinding(s, "acme", "workflow", "wf", "acme/__workflow__/wf"); err != nil {
		t.Fatal(err)
	}
	token, err := testBindingJWT(s, scopedtoken.Claims{Namespace: "acme", Kind: "workflow", Name: "wf"})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	create := httptest.NewRequest(http.MethodPost, "/v1/workflow/create?ns=acme&workflow=wf&id=i", bytes.NewReader([]byte(`{}`)))
	create.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, create)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/workflow/event?ns=acme&workflow=wf&id=i", body)
	req.ContentLength = -1
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	wantStatus := http.StatusOK
	if tooLarge {
		wantStatus = http.StatusRequestEntityTooLarge
	} else if readFailure {
		wantStatus = http.StatusBadRequest
	}
	if w.Code != wantStatus {
		t.Fatalf("event status=%d, want %d: %s", w.Code, wantStatus, w.Body.String())
	}
	events, err := s.Workflows.Events(t.Context(), "acme", "wf", "i")
	if err != nil {
		t.Fatal(err)
	}
	if tooLarge || readFailure {
		if len(events) != 0 {
			t.Fatalf("rejected event was stored: %d events", len(events))
		}
	} else if len(events) != 1 || !bytes.Equal(events[0], payload) {
		t.Fatal("accepted event was not preserved byte for byte")
	}
}
