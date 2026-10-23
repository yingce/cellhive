package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeLimitRejectsTrailingAndOversizedBodies(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		code int
	}{
		{"trailing value", `{"a":1}{"b":2}`, http.StatusBadRequest},
		{"oversized", `{"a":"` + strings.Repeat("x", 64) + `"}`, http.StatusRequestEntityTooLarge},
		{"whitespace ok", "{\"a\":1}\n\t ", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			var out struct {
				A int `json:"a"`
			}
			ok := decodeLimit(rr, req, &out, 32)
			if tc.code == http.StatusOK {
				if !ok {
					t.Fatalf("decode rejected valid body: %s", rr.Body.String())
				}
				return
			}
			if ok || rr.Code != tc.code {
				t.Fatalf("decode = ok=%v code=%d body=%s, want code %d", ok, rr.Code, rr.Body.String(), tc.code)
			}
		})
	}
}
