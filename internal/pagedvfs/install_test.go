//go:build pagedvfs_test

package pagedvfs

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockedSource struct {
	*fakeSource
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func TestPartialWriteCannotMarkMissingPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	f := openTestVFS(path)
	if f == nil {
		t.Fatal("open")
	}
	defer f.close()
	if rc := f.write([]byte{9}, 0); rc == 0 {
		t.Fatal("partial write accepted over missing page")
	}
	if Complete(path) {
		t.Fatal("partial write marked a complete page")
	}
}

type closingSource struct {
	*blockedSource
	closed chan struct{}
}

func (s *closingSource) Close() error { close(s.closed); return nil }

func TestReleaseWaitsForSourceAndRejectsLateInstall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &closingSource{blockedSource: &blockedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}, entered: make(chan struct{}), resume: make(chan struct{})}, closed: make(chan struct{})}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- HydrateAll(context.Background(), path) }()
	<-src.entered
	released := make(chan struct{})
	go func() { Release(path); close(released) }()
	deadline := time.Now().Add(time.Second)
	for IsPaged(path) {
		if time.Now().After(deadline) {
			t.Fatal("release failed to invalidate")
		}
		runtime.Gosched()
	}
	select {
	case <-src.closed:
		t.Error("source closed with read in flight")
	default:
	}
	close(src.resume)
	if err := <-result; err == nil {
		t.Error("retired registration installed stale page")
	}
	<-released
	select {
	case <-src.closed:
	default:
		t.Error("source not closed")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, make([]byte, 512)) {
		t.Error("retired read changed file")
	}
}

func TestPrepareRejectsActiveRegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	if err := HydrateAll(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	if err := Prepare(path, src); err == nil {
		t.Fatal("Prepare replaced active registration")
	}
}

func TestFailedCheckpointCannotPublishHydrated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	f := openTestVFSFlags(path, true)
	if f == nil {
		t.Fatal("open")
	}
	defer f.close()
	if rc := f.write(bytes.Repeat([]byte{9}, 512), 0); rc == 0 {
		t.Fatal("readonly write unexpectedly succeeded")
	}
	if Complete(path) {
		t.Fatal("failed checkpoint marked page hydrated")
	}
	if err := HydrateAll(context.Background(), path); err == nil {
		t.Fatal("failed checkpoint registration accepted old cut")
	}
}

func TestHydrateRejectsReplacedInode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	fresh := bytes.Repeat([]byte{9}, 512)
	if err := os.WriteFile(path, fresh, 0600); err != nil {
		t.Fatal(err)
	}
	if err := HydrateAll(context.Background(), path); err == nil {
		t.Fatal("replaced inode accepted")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatal("replacement file overwritten")
	}
}

func TestReregisterRejectsOldFetch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	old := &closingSource{blockedSource: &blockedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}, entered: make(chan struct{}), resume: make(chan struct{})}, closed: make(chan struct{})}
	fresh := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{9}, 512)}}
	if err := Prepare(path, old); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	id, _ := regID(path)
	f := openTestVFS(path)
	if f == nil {
		t.Fatal("open")
	}
	defer f.close()
	read := make(chan int, 1)
	go func() { read <- f.read(make([]byte, 512), 0) }()
	<-old.entered
	registered := make(chan error, 1)
	go func() { registered <- Register(path, fresh) }()
	deadline := time.Now().Add(time.Second)
	for {
		newID, ok := regID(path)
		if ok && newID != id {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("register stuck")
		}
		runtime.Gosched()
	}
	select {
	case <-old.closed:
		t.Error("source closed before fetch returned")
	default:
	}
	close(old.resume)
	if rc := <-read; rc == 0 {
		t.Error("old fault accepted replacement")
	}
	if err := <-registered; err != nil {
		t.Fatal(err)
	}
	if err := HydrateAll(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fresh.pages[1]) {
		t.Fatal("old fetch overwrote new registration")
	}
}

type blockedRunSource struct{ *blockedSource }

func (s *blockedRunSource) ReadRun(pg uint32, n int) ([][]byte, error) {
	p, err := s.ReadPage(pg)
	if err != nil {
		return nil, err
	}
	return [][]byte{p}, nil
}

func TestTruncateDuringFetchDoesNotResurrect(t *testing.T) {
	for _, mode := range []string{"fault", "all", "run", "rate"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			blocked := &blockedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}, entered: make(chan struct{}), resume: make(chan struct{})}
			var src Source = blocked
			if mode == "run" {
				src = &blockedRunSource{blocked}
			}
			if err := Prepare(path, src); err != nil {
				t.Fatal(err)
			}
			defer Release(path)
			f := openTestVFS(path)
			if f == nil {
				t.Fatal("open")
			}
			defer f.close()
			finished := make(chan struct{})
			var stop func()
			if mode == "rate" {
				stop = HydrateRate(path, 100, func(bool) { close(finished) })
			} else {
				go func() {
					defer close(finished)
					if mode == "fault" {
						f.read(make([]byte, 512), 0)
					} else {
						if err := HydrateAll(context.Background(), path); err != nil {
							t.Error(err)
						}
					}
				}()
			}
			<-blocked.entered
			writer := openTestVFS(path)
			if writer == nil {
				t.Fatal("writer open")
			}
			rc := writer.truncate(0)
			writer.close()
			close(blocked.resume)
			<-finished
			if stop != nil {
				stop()
			}
			if rc != 0 {
				t.Fatal(rc)
			}
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if st.Size() != 0 {
				t.Fatalf("late %s resurrected %d bytes", mode, st.Size())
			}
		})
	}
}

func TestRateCheckedPageCannotOverwriteCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	_, src := buildSourceDB(t, dir, 1)
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	id, _ := regID(path)
	checked, resume := make(chan struct{}), make(chan struct{})
	var checks atomic.Int32
	setCheckedHook(func(got int, pg uint32, hydrated bool) {
		if got == id && pg == 2 && !hydrated && checks.Add(1) == 2 {
			close(checked)
			<-resume
		}
	})
	defer setCheckedHook(nil)
	done := make(chan bool, 2)
	stop := HydrateRate(path, 100, func(ok bool) { done <- ok })
	<-checked
	db, err := sql.Open("sqlite3", "file:"+path+"?vfs="+VFSName+"&_journal_mode=WAL")
	if err != nil {
		close(resume)
		stop()
		t.Fatal(err)
	}
	_, writeErr := db.Exec("UPDATE t SET body='fresh' WHERE id=1")
	_, checkpointErr := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	closeErr := db.Close()
	close(resume)
	ok := <-done
	stop()
	if writeErr != nil || checkpointErr != nil || closeErr != nil || !ok {
		t.Fatalf("write=%v checkpoint=%v close=%v complete=%v", writeErr, checkpointErr, closeErr, ok)
	}
	db, err = sql.Open("sqlite3", "file:"+path+"?vfs="+VFSName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got string
	if err := db.QueryRow("SELECT body FROM t WHERE id=1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "fresh" {
		t.Fatal("rate hydrate overwrote checkpoint after final missing check")
	}
}

func TestHydrationIOFailureNotComplete(t *testing.T) {
	for _, mode := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}
			if err := Prepare(path, src); err != nil {
				t.Fatal(err)
			}
			defer Release(path)
			injectFailure(mode)
			defer injectFailure(0)
			done := make(chan bool, 2)
			stop := HydrateRate(path, 100, func(ok bool) { done <- ok })
			ok := <-done
			stop()
			if ok {
				t.Fatal("failed I/O reported completion")
			}
			if mode != 3 && Complete(path) {
				t.Fatal("partial/failed write reported hydrated")
			}
		})
	}
}

func (s *blockedSource) ReadPage(p uint32) ([]byte, error) {
	s.once.Do(func() { close(s.entered); <-s.resume })
	return s.fakeSource.ReadPage(p)
}

func TestInstallDoesNotOverwriteCheckpoint(t *testing.T) {
	for _, mode := range []string{"fault", "all"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			old, fresh := bytes.Repeat([]byte{1}, 512), bytes.Repeat([]byte{2}, 512)
			src := &blockedSource{fakeSource: &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: old}}, entered: make(chan struct{}), resume: make(chan struct{})}
			if err := Prepare(path, src); err != nil {
				t.Fatal(err)
			}
			defer Release(path)
			f := openTestVFS(path)
			if f == nil {
				t.Fatal("open")
			}
			defer f.close()
			result := make(chan error, 1)
			go func() {
				if mode == "all" {
					result <- HydrateAll(context.Background(), path)
					return
				}
				buf := make([]byte, 512)
				rc := f.read(buf, 0)
				if rc != 0 {
					result <- os.ErrInvalid
					return
				}
				result <- nil
			}()
			<-src.entered
			writer := openTestVFS(path)
			if writer == nil {
				t.Fatal("writer open")
			}
			rc := writer.write(fresh, 0)
			writer.close()
			close(src.resume)
			if rc != 0 {
				t.Fatalf("checkpoint rc=%d", rc)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, fresh) {
				t.Fatal("old cut overwrote checkpoint page")
			}
		})
	}
}

func TestTruncateDoesNotResurrectCut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 2, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512), 2: bytes.Repeat([]byte{2}, 512)}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	f := openTestVFS(path)
	if f == nil {
		t.Fatal("open")
	}
	defer f.close()
	if rc := f.truncate(512); rc != 0 {
		t.Fatal(rc)
	}
	if err := HydrateAll(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 512 {
		t.Fatalf("truncated cut resurrected: size=%d", st.Size())
	}
}

func TestOldHandleCannotWriteReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	f := openTestVFS(path)
	if f == nil {
		t.Fatal("open")
	}
	defer f.close()
	Release(path)
	if err := Register(path, src); err != nil {
		t.Fatal(err)
	}
	if rc := f.write(bytes.Repeat([]byte{9}, 512), 0); rc == 0 {
		t.Fatal("stale VFS handle wrote replacement registration")
	}
}

func TestBackgroundRejectsShortPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: {1, 2, 3}}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	done := make(chan bool, 2)
	stop := HydrateRate(path, 100, func(ok bool) { done <- ok })
	complete := <-done
	stop()
	if complete || Complete(path) {
		t.Fatal("short page reported complete")
	}
}
