//go:build pagedvfs_test

#include <unistd.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

extern void goPagedTestChecked(int, unsigned int, int);
extern int __real_cellhive_paged_hydrated(int, unsigned int);
extern ssize_t __real_pwrite(int, const void *, size_t, off_t);
extern int __real_fsync(int);
static atomic_int failure;
void testFailure(int mode) { atomic_store(&failure, mode); }

int __wrap_cellhive_paged_hydrated(int id, unsigned int pg) {
  int out = __real_cellhive_paged_hydrated(id, pg);
  goPagedTestChecked(id, pg, out);
  return out;
}
ssize_t __wrap_pwrite(int fd, const void *buf, size_t n, off_t off) {
  int mode = atomic_load(&failure);
  if (mode == 1) return __real_pwrite(fd, buf, n / 2, off);
  if (mode == 2) { errno = EIO; return -1; }
  return __real_pwrite(fd, buf, n, off);
}
int __wrap_fsync(int fd) {
  if (atomic_load(&failure) == 3) { errno = EIO; return -1; }
  return __real_fsync(fd);
}

/* Test-only fault injection around a real Unix xOpen and file object. Tests
** install this serially and close each file before changing the injection. */
extern void goPagedTestOpened(void);
static int (*realOpen)(sqlite3_vfs*, sqlite3_filename, sqlite3_file*, int, int*);
static const sqlite3_io_methods *realMethods;
static sqlite3_io_methods injectedMethods;
static int fileStatRC, fileStatCalls, fileCloses;
static char *fileStatJSON;
static int fileStatLen;
static int injectedControl(sqlite3_file *f, int op, void *arg) {
  if (op != SQLITE_FCNTL_FILESTAT) return realMethods->xFileControl(f, op, arg);
  fileStatCalls++;
  if (fileStatRC < 0) return realMethods->xFileControl(f, op, arg);
  sqlite3_str_append((sqlite3_str*)arg, fileStatJSON, fileStatLen);
  return fileStatRC;
}
static int injectedClose(sqlite3_file *f) {
  fileCloses++;
  f->pMethods = realMethods;
  return realMethods->xClose(f);
}
static int injectedOpen(sqlite3_vfs *v, sqlite3_filename name, sqlite3_file *f,
                        int flags, int *out) {
  int rc = realOpen(v, name, f, flags, out);
  if (rc == SQLITE_OK && (flags & SQLITE_OPEN_MAIN_DB)) {
    realMethods = f->pMethods;
    injectedMethods = *realMethods;
    injectedMethods.xFileControl = injectedControl;
    injectedMethods.xClose = injectedClose;
    f->pMethods = &injectedMethods;
    goPagedTestOpened();
  }
  return rc;
}
void testFileStatSet(int enabled, int rc, const char *json, int len) {
  sqlite3_vfs *v = sqlite3_vfs_find(0);
  if (!realOpen) realOpen = v->xOpen;
  v->xOpen = enabled ? injectedOpen : realOpen;
  free(fileStatJSON);
  fileStatJSON = malloc((size_t)len + 1);
  memcpy(fileStatJSON, json, (size_t)len);
  fileStatJSON[len] = 0;
  fileStatLen = len;
  fileStatRC = rc;
  fileStatCalls = fileCloses = 0;
}
int testFileStatCalls(void) { return fileStatCalls; }
int testFileCloses(void) { return fileCloses; }
