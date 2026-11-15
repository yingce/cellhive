//go:build pagedvfs_test

package pagedvfs

/*
#cgo LDFLAGS: -Wl,--wrap=cellhive_paged_hydrated -Wl,--wrap=pwrite -Wl,--wrap=fsync
#include "sqlite3.h"
#include <stdlib.h>
void testFailure(int mode);
void testFileStatSet(int enabled, int rc, const char *json, int len);
int testFileStatCalls(void);
int testFileCloses(void);
#ifdef SQLITE_ENABLE_FILESTAT
enum { testFileStatEnabled = 1 };
#else
enum { testFileStatEnabled = 0 };
#endif
static int testRawFileStat(const char *path) {
  sqlite3_vfs *v = sqlite3_vfs_find("unix");
  sqlite3_file *f = calloc(1, v->szOsFile);
  int rc = v->xOpen(v, path, f, SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_READWRITE, 0);
  if (rc == SQLITE_OK) {
    sqlite3_str *s = sqlite3_str_new(0);
    rc = f->pMethods->xFileControl(f, SQLITE_FCNTL_FILESTAT, s);
    sqlite3_free(sqlite3_str_finish(s));
    f->pMethods->xClose(f);
  }
  free(f);
  return rc;
}
static sqlite3_file *testOpen(const char *path, int readonly) {
  sqlite3_vfs *v = sqlite3_vfs_find("cellhive-paged");
  sqlite3_file *f = calloc(1, v->szOsFile);
  if (v->xOpen(v, path, f, SQLITE_OPEN_MAIN_DB | (readonly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE), 0)) {
    free(f); return 0;
  }
  return f;
}
static int testRead(sqlite3_file *f, void *p, int n, int off) { return f->pMethods->xRead(f,p,n,off); }
static int testWrite(sqlite3_file *f, void *p, int n, int off) { return f->pMethods->xWrite(f,p,n,off); }
static int testTruncate(sqlite3_file *f, int n) { return f->pMethods->xTruncate(f,n); }
static void testClose(sqlite3_file *f) { f->pMethods->xClose(f); free(f); }
*/
import "C"

import (
	"sync"
	"unsafe"
)

var checkedHook struct {
	sync.Mutex
	fn func(int, uint32, bool)
}

//export goPagedTestChecked
func goPagedTestChecked(id C.int, pg C.uint, hydrated C.int) {
	checkedHook.Lock()
	fn := checkedHook.fn
	checkedHook.Unlock()
	if fn != nil {
		fn(int(id), uint32(pg), hydrated != 0)
	}
}

func setCheckedHook(fn func(int, uint32, bool)) {
	checkedHook.Lock()
	checkedHook.fn = fn
	checkedHook.Unlock()
}

func injectFailure(mode int) { C.testFailure(C.int(mode)) }

var openedHook func()

//export goPagedTestOpened
func goPagedTestOpened() {
	if openedHook != nil {
		openedHook()
	}
}

func injectFileStat(rc int, json string, hook func()) func() {
	openedHook = hook
	p := C.CString(json)
	C.testFileStatSet(1, C.int(rc), p, C.int(len(json)))
	C.free(unsafe.Pointer(p))
	return func() {
		p := C.CString("")
		C.testFileStatSet(0, 0, p, 0)
		C.free(unsafe.Pointer(p))
		openedHook = nil
	}
}

func fileStatCalls() int { return int(C.testFileStatCalls()) }
func fileCloses() int    { return int(C.testFileCloses()) }

func rawFileStatSupport(path string) (enabled bool, rc int) {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	return C.testFileStatEnabled != 0, int(C.testRawFileStat(p))
}

type testVFSFile struct{ f *C.sqlite3_file }

func openTestVFS(path string) *testVFSFile {
	return openTestVFSFlags(path, false)
}

func openTestVFSFlags(path string, readonly bool) *testVFSFile {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	var flag C.int
	if readonly {
		flag = 1
	}
	f := C.testOpen(p, flag)
	if f == nil {
		return nil
	}
	return &testVFSFile{f}
}
func (f *testVFSFile) read(p []byte, off int) int {
	return int(C.testRead(f.f, unsafe.Pointer(&p[0]), C.int(len(p)), C.int(off)))
}
func (f *testVFSFile) write(p []byte, off int) int {
	return int(C.testWrite(f.f, unsafe.Pointer(&p[0]), C.int(len(p)), C.int(off)))
}
func (f *testVFSFile) truncate(n int) int { return int(C.testTruncate(f.f, C.int(n))) }
func (f *testVFSFile) close()             { C.testClose(f.f) }
