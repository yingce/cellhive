package server

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQueueProducerContextDoesNotRejectOversizedHeaders(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/queue/send", nil)
	r.Header.Set("x-request-id", strings.Repeat("r", 161))
	r.Header.Set("traceparent", strings.Repeat("t", 257))
	requestID, traceparent := queueProducerContext(r)
	if requestID != "" || traceparent != "" {
		t.Fatalf("oversized correlation headers retained: %q %q", requestID, traceparent)
	}
	r.Header.Set("x-request-id", "r1")
	r.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
	requestID, traceparent = queueProducerContext(r)
	if requestID != "r1" || traceparent != "00-11111111111111111111111111111111-2222222222222222-01" {
		t.Fatalf("valid context lost: %q %q", requestID, traceparent)
	}
}
