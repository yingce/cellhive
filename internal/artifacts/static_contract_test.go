package artifacts

import "testing"

func TestAssetVersionRejectsMutation(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	for i := 0; i < 2; i++ {
		if _, err := s.PutAssetAt(ctx, "acme", "web", "version1", "index.html", []byte("original")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.PutAssetAt(ctx, "acme", "web", "version1", "index.html", []byte("changed")); err == nil {
		t.Fatal("version bytes were overwritten")
	}
	got, err := s.GetAsset(ctx, "acme", "web", "version1", "index.html")
	if err != nil || string(got) != "original" {
		t.Fatalf("old version = %q, %v", got, err)
	}
}
