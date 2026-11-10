package dosupervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

type failingPageSource struct {
	pf       *replica.PageFetcher
	err      error
	restores int
}

func (s *failingPageSource) PageFetcher(context.Context, cell.Scope, uint64) (*replica.PageFetcher, error) {
	return s.pf, s.err
}
func (s *failingPageSource) Restore(context.Context, cell.Scope, uint64) ([][]byte, error) {
	s.restores++
	return nil, nil
}

func TestRestorePagingErrorsNeverFallBack(t *testing.T) {
	for _, mode := range []string{"index", "materialize", "empty"} {
		for _, all := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/scope", true: "/all"}[all], func(t *testing.T) {
				b, err := bucket.NewFSBucket(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				s := &Supervisor{Bucket: b, ScopePrefix: "n/c", Epoch: 1}
				source := &failingPageSource{}
				s.Segments = source
				sc := s.scopeForRel("test.sqlite")
				out := t.TempDir()
				if err := s.writeManifest(t.Context(), []string{"test.sqlite"}, nil); err != nil {
					t.Fatal(err)
				}
				sentinel := errors.New("index unavailable")
				switch mode {
				case "index":
					source.err = sentinel
				case "empty":
					source.err = replica.ErrNoPageIndex
				case "materialize":
					raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 512)}})
					if err != nil {
						t.Fatal(err)
					}
					m := replica.New(b)
					if _, _, err = m.Append(t.Context(), sc, 1, raw); err != nil {
						t.Fatal(err)
					}
					source.pf, err = m.NewPageFetcher(t.Context(), sc, 1)
					if err != nil {
						t.Fatal(err)
					}
					// A file in place of the output parent forces materialization to fail.
					out = filepath.Join(out, "blocked")
					if err := os.WriteFile(out, []byte("file"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if all {
					_, err = s.RestoreAll(t.Context(), out)
				} else {
					err = s.restoreScope(t.Context(), sc, filepath.Join(out, "test.sqlite"))
				}
				if mode == "empty" && all {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("expected restore error")
				}
				if mode == "index" && !errors.Is(err, sentinel) {
					t.Fatalf("lost index error: %v", err)
				}
				if source.restores != 0 {
					t.Fatalf("buffered fallback called %d times", source.restores)
				}
			})
		}
	}
}

type failedRestoreBucket struct {
	*bucket.FSBucket
	fail      bool
	fullReads int
	err       error
}

func (b *failedRestoreBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if b.fail && strings.HasSuffix(key, ".ltx") {
		b.fullReads++
	}
	return b.FSBucket.Get(ctx, key)
}
func (b *failedRestoreBucket) RangedGet(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if b.fail {
		return nil, "", b.err
	}
	return b.FSBucket.RangedGet(ctx, key, off, n)
}

func TestDirectRestorePagingFailureDoesNotReadBodies(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprint(all), func(t *testing.T) {
			fs, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			sentinel := errors.New("range transport failure")
			b := &failedRestoreBucket{FSBucket: fs, err: sentinel}
			m := replica.New(b)
			s := &Supervisor{Bucket: b, Store: m, ScopePrefix: "n/c", Epoch: 1}
			sc := s.scopeForRel("test.sqlite")
			raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 512)}})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = m.Append(t.Context(), sc, 1, raw); err != nil {
				t.Fatal(err)
			}
			if err = s.writeManifest(t.Context(), []string{"test.sqlite"}, nil); err != nil {
				t.Fatal(err)
			}
			b.fail = true
			out := t.TempDir()
			if all {
				_, err = s.RestoreAll(t.Context(), out)
			} else {
				err = s.restoreScope(t.Context(), sc, filepath.Join(out, "test.sqlite"))
			}
			if !errors.Is(err, sentinel) {
				t.Fatalf("error=%v", err)
			}
			if b.fullReads != 0 {
				t.Fatalf("full-body LTX reads=%d", b.fullReads)
			}
		})
	}
}
