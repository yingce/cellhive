package logbuf

import "testing"

func TestBufferBoundedAndSince(t *testing.T) {
	b := New(3, 2)
	for i := 0; i < 5; i++ {
		b.Add(Entry{Namespace: "acme", Worker: "web", Level: "log", Message: "m"})
	}
	got := b.Since("acme", "web", 0, 100)
	if len(got) != 3 {
		t.Fatalf("bounded entries = %d, want 3", len(got))
	}
	if got[0].Seq != 3 || got[2].Seq != 5 {
		t.Fatalf("seqs = %d..%d, want 3..5", got[0].Seq, got[2].Seq)
	}
	if since := b.Since("acme", "web", 4, 100); len(since) != 1 || since[0].Seq != 5 {
		t.Fatalf("since(4) = %+v", since)
	}
	// LRU cap on distinct workers.
	b.Add(Entry{Namespace: "acme", Worker: "api", Message: "x"})
	b.Add(Entry{Namespace: "acme", Worker: "other", Message: "y"})
	if len(b.Since("acme", "web", 0, 100)) != 0 {
		t.Fatal("oldest worker not evicted")
	}
}
