package dispatch

import (
	"bytes"
	"testing"
)

func TestDispatchSuccessBodyIsBounded(t *testing.T) {
	body := bytes.Repeat([]byte("x"), maxDispatchResponseBody*4)
	got, err := readSuccessBody(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != maxDispatchResponseBody {
		t.Fatalf("read %d bytes, want cap %d", len(got), maxDispatchResponseBody)
	}
}

func TestDispatchSuccessBodyKeepsSmallJSON(t *testing.T) {
	got, err := readSuccessBody(bytes.NewReader([]byte(`{"no_retry":true}`)))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"no_retry":true}` {
		t.Fatalf("body = %q", got)
	}
}
