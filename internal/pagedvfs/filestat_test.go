//go:build pagedvfs_test

package pagedvfs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileStatRejectsInvalidIdentity(t *testing.T) {
	other, err := os.CreateTemp(t.TempDir(), "unrelated")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	for _, tc := range []struct {
		name string
		rc   int
		body string
	}{
		{"unsupported", 12, ""},
		{"io-error", 10, ""},
		{"empty", 0, ""},
		{"broken", 0, `{"h":`},
		{"missing", 0, `{"vfs":"unix"}`},
		{"nested", 0, `{"inode":{"h":3}}`},
		{"duplicate", 0, `{"h":3,"h":4}`},
		{"escaped-duplicate", 0, `{"h":3,"\u0068":4}`},
		{"negative", 0, `{"h":-1}`},
		{"overflow", 0, `{"h":2147483648}`},
		{"fraction", 0, `{"h":3.0}`},
		{"exponent", 0, `{"h":3e0}`},
		{"string", 0, `{"h":"3"}`},
		{"null", 0, `{"h":null}`},
		{"array", 0, `[{"h":3}]`},
		{"trailing", 0, `{"h":3} {}`},
		{"bad-tail", 0, `{"h":3,"other":}`},
		{"nul-tail", 0, "{\"h\":3}\x00"},
		{"invalid-utf8", 0, "{\"h\":3,\"vfs\":\"\xff\"}"},
		{"oversize", 0, `{"h":3,"extra":"` + strings.Repeat("a", 64*1024) + `"}`},
		{"invalid-fd", 0, `{"h":2147483647}`},
		{"unrelated-fd", 0, fmt.Sprintf(`{"h":%d}`, other.Fd())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			src := &fakeSource{pageSize: 512, commit: 1}
			if err := Prepare(path, src); err != nil {
				t.Fatal(err)
			}
			defer Release(path)
			reset := injectFileStat(tc.rc, tc.body, nil)
			defer reset()
			f := openTestVFS(path)
			if f != nil {
				f.close()
				t.Error("accepted invalid FILESTAT identity")
			}
			if fileCloses() != 1 {
				t.Errorf("base file closes=%d, want 1", fileCloses())
			}
			if fileStatCalls() != 1 {
				t.Errorf("FILESTAT calls=%d, want 1", fileStatCalls())
			}
		})
	}
}

func TestFileStatBuildContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	enabled, rc := rawFileStatSupport(path)
	if enabled && rc != 0 {
		t.Fatalf("build macro did not enable dependency FILESTAT: rc=%d", rc)
	}
	if !enabled && rc != 12 {
		t.Fatalf("unexpected unsupported-build FILESTAT rc=%d", rc)
	}
	f := openTestVFS(path)
	if f != nil {
		f.close()
	}
	if (f != nil) != enabled {
		t.Fatalf("FILESTAT enabled=%v, paged open=%v", enabled, f != nil)
	}
	Release(path)
	f = openTestVFS(path)
	if f == nil {
		t.Fatal("FILESTAT capability affected an unregistered database")
	}
	f.close()
}

func TestFileStatKeepsOpenedRegisteredInode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	cut := bytes.Repeat([]byte{1}, 512)
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: cut}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	fresh := bytes.Repeat([]byte{9}, 512)
	var swapErr error
	reset := injectFileStat(-1, "", func() {
		if swapErr = os.Rename(path, path+".old"); swapErr == nil {
			swapErr = os.WriteFile(path, fresh, 0600)
		}
	})
	defer reset()
	f := openTestVFS(path)
	if swapErr != nil {
		t.Fatal(swapErr)
	}
	if f == nil {
		t.Fatal("rejected actual registered inode because path changed")
	}
	defer f.close()
	got := make([]byte, 512)
	if rc := f.read(got, 0); rc != 0 || !bytes.Equal(got, cut) {
		t.Fatalf("read registered inode rc=%d", rc)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fresh) {
		t.Fatal("fault changed replacement path")
	}
}

func TestFileStatUsesOpenedFileNotRestoredPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	src := &fakeSource{pageSize: 512, commit: 1, pages: map[uint32][]byte{1: bytes.Repeat([]byte{1}, 512)}}
	if err := Prepare(path, src); err != nil {
		t.Fatal(err)
	}
	defer Release(path)
	if err := os.Rename(path, path+".registered"); err != nil {
		t.Fatal(err)
	}
	fresh := bytes.Repeat([]byte{9}, 512)
	if err := os.WriteFile(path, fresh, 0600); err != nil {
		t.Fatal(err)
	}
	var swapErr error
	reset := injectFileStat(-1, "", func() {
		// The base opened the replacement. Restore the registered inode at the
		// path before the wrapper validates: a path-stat check would now pass.
		if swapErr = os.Rename(path, path+".replacement"); swapErr == nil {
			swapErr = os.Rename(path+".registered", path)
		}
	})
	defer reset()
	f := openTestVFS(path)
	if f != nil {
		f.read(make([]byte, 512), 0)
		f.close()
		t.Error("accepted wrong opened inode despite restored path")
	}
	if swapErr != nil {
		t.Fatal(swapErr)
	}
	got, err := os.ReadFile(path + ".replacement")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fresh) {
		t.Error("overwrote replacement opened before path swap")
	}
}
