/*
** pagedvfs.c — a SQLite VFS that faults main-database pages in on demand.
**
** Model (ADR-160, following celld's crates/ltx/src/paged_vfs.rs): the cell's
** main database is a local sparse file sized to a pinned replication cut. On a
** read of a page the local file does not hold yet, the VFS asks Go for that
** page (a ranged read of the replica / hot L0 lookup), writes it into the file
** and lets SQLite read it normally. SQLite otherwise runs as usual over the
** local file — WAL, checkpoints — and faulting is just hydration.
**
** Only the registered main database is wrapped; every other file (WAL, SHM,
** journals, temp, other databases) is forwarded untouched to the base VFS.
*/
#include "sqlite3.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

/* Implemented in Go (pagedvfs.go). Returns 0 on success; on failure the VFS
** surfaces SQLITE_IOERR_READ and SQLite poisons the read (fail closed). */
extern int goPagedFetch(int regId, unsigned int pgno, void *buf, int bufLen);
/* Informs Go that pages above keepPages were truncated away (window cache). */
extern void goPagedTruncated(int regId, unsigned int keepPages);
extern int goPagedFileStatFD(char *json, int len);

#define PAGED_MAX_REGS 64
#define PAGED_MAX_PATH 1024

typedef struct PagedReg {
  int used;
  int id;
  char path[PAGED_MAX_PATH];
  uint32_t pageSize;
  uint32_t commit;
  uint32_t cutPages; /* monotonically shrinks: removed cut pages never return */
  int failed;
  dev_t dev;
  ino_t ino;
  uint8_t *hydrated; /* bitmap, page 1 at bit 0 */
  sqlite3_mutex *mu; /* guards the bitmap */
} PagedReg;

typedef struct PagedFile {
  sqlite3_file base;      /* our file object; SQLite allocates szOsFile bytes */
  sqlite3_file *inner;    /* the base VFS's file object, placed right after us */
  PagedReg *reg;          /* NULL for non-paged files */
  uint32_t pageSize;
  uint32_t commit;
  int regId;
} PagedFile;

static sqlite3_vfs gBase;              /* our VFS struct (copy of the default) */
static sqlite3_vfs *gReal = 0;         /* the real base VFS */
static PagedReg gRegs[PAGED_MAX_REGS];
static sqlite3_mutex *gRegMu = 0;

static PagedReg *regFindLocked(const char *path) {
  for (int i = 0; i < PAGED_MAX_REGS; i++) {
    if (gRegs[i].used && strcmp(gRegs[i].path, path) == 0) return &gRegs[i];
  }
  return 0;
}

static int bitGet(const PagedReg *r, uint32_t pgno) {
  if (pgno < 1 || pgno > r->commit) return 1;
  uint32_t i = pgno - 1;
  return (r->hydrated[i >> 3] >> (i & 7)) & 1;
}

static void bitSet(PagedReg *r, uint32_t pgno) {
  if (pgno < 1 || pgno > r->commit) return;
  uint32_t i = pgno - 1;
  r->hydrated[i >> 3] |= (uint8_t)(1u << (i & 7));
}

static int valid(PagedFile *pf) {
  return pf->reg->used && pf->reg->id == pf->regId && !pf->reg->failed;
}

/* Caller holds gRegMu through both I/O and publication. Never call Go here. */
static int installLocked(PagedReg *r, sqlite3_file *inner, int fd,
                         uint32_t pgno, const void *page, int len) {
  if (r->failed || len != (int)r->pageSize || pgno == 0 || pgno > r->commit)
    return SQLITE_IOERR_WRITE;
  if (pgno > r->cutPages || bitGet(r, pgno)) return SQLITE_OK;
  sqlite3_int64 off = (sqlite3_int64)(pgno - 1) * r->pageSize;
  int rc;
  if (inner) {
    rc = inner->pMethods->xWrite(inner, page, len, off);
  } else {
    ssize_t n;
    do { n = pwrite(fd, page, (size_t)len, off); } while (n < 0 && errno == EINTR);
    rc = n == len ? SQLITE_OK : SQLITE_IOERR_WRITE;
  }
  if (rc == SQLITE_OK) bitSet(r, pgno);
  else r->failed = 1; /* A short/failed write may have changed part of the page. */
  return rc;
}

static int pagedRead(sqlite3_file *file, void *buf, int iAmt, sqlite3_int64 iOfst) {
  PagedFile *pf = (PagedFile *)file;
  if (!pf->reg) return pf->inner->pMethods->xRead(pf->inner, buf, iAmt, iOfst);
  PagedReg *r = pf->reg;
  uint32_t ps = pf->pageSize;
  if (ps == 0) return SQLITE_IOERR_READ;
  uint32_t first, last;
  if (iAmt <= 0) return SQLITE_OK;
  first = (uint32_t)(iOfst / ps) + 1;
  last = (uint32_t)((iOfst + iAmt - 1) / ps) + 1;
  if (last > pf->commit) last = pf->commit;
  sqlite3_mutex_enter(gRegMu);
  if (!valid(pf)) { sqlite3_mutex_leave(gRegMu); return SQLITE_IOERR_READ; }
  for (uint32_t pgno = first; pgno <= last; pgno++) {
    if (bitGet(r, pgno)) continue;
    sqlite3_mutex_leave(gRegMu);
    /* Fault one page at a time on the calling thread (the Rust original does
    ** the same: a synchronous read that touches no runtime). */
    unsigned char *page = (unsigned char *)sqlite3_malloc64(ps);
    if (!page) return SQLITE_NOMEM;
    int rc = goPagedFetch(pf->regId, pgno, page, (int)ps);
    sqlite3_mutex_enter(gRegMu);
    if (!valid(pf)) rc = SQLITE_IOERR_READ;
    if (rc == 0) {
      rc = installLocked(r, pf->inner, -1, pgno, page, (int)ps);
    }
    sqlite3_free(page);
    if (rc != 0) { sqlite3_mutex_leave(gRegMu); return SQLITE_IOERR_READ; }
  }
  int rc = pf->inner->pMethods->xRead(pf->inner, buf, iAmt, iOfst);
  sqlite3_mutex_leave(gRegMu);
  return rc;
}

static int pagedWrite(sqlite3_file *file, const void *buf, int iAmt, sqlite3_int64 iOfst) {
  PagedFile *pf = (PagedFile *)file;
  if (!pf->reg) return pf->inner->pMethods->xWrite(pf->inner, buf, iAmt, iOfst);
  sqlite3_mutex_enter(gRegMu);
  if (!valid(pf)) { sqlite3_mutex_leave(gRegMu); return SQLITE_IOERR_WRITE; }
  /* A partial write cannot make an otherwise sparse page readable. SQLite
  ** normally reads such a page first; fail closed if that precondition breaks. */
  if (iAmt > 0) {
    uint32_t first = (uint32_t)(iOfst / pf->pageSize) + 1;
    uint32_t last = (uint32_t)((iOfst + iAmt - 1) / pf->pageSize) + 1;
    if ((iOfst % pf->pageSize && !bitGet(pf->reg, first)) ||
        ((iOfst + iAmt) % pf->pageSize && !bitGet(pf->reg, last))) {
      sqlite3_mutex_leave(gRegMu);
      return SQLITE_IOERR_WRITE;
    }
  }
  int rc = pf->inner->pMethods->xWrite(pf->inner, buf, iAmt, iOfst);
  if (rc != SQLITE_OK) pf->reg->failed = 1;
  if (rc == SQLITE_OK && pf->pageSize > 0 && iAmt > 0) {
    uint32_t first = (uint32_t)(iOfst / pf->pageSize) + 1;
    uint32_t last = (uint32_t)((iOfst + iAmt - 1) / pf->pageSize) + 1;
    for (uint32_t pgno = first; pgno <= last; pgno++) bitSet(pf->reg, pgno);
  }
  sqlite3_mutex_leave(gRegMu);
  return rc;
}

static int pagedTruncate(sqlite3_file *file, sqlite3_int64 size) {
  PagedFile *pf = (PagedFile *)file;
  if (!pf->reg) return pf->inner->pMethods->xTruncate(pf->inner, size);
  sqlite3_mutex_enter(gRegMu);
  if (!valid(pf)) { sqlite3_mutex_leave(gRegMu); return SQLITE_IOERR_TRUNCATE; }
  int rc = pf->inner->pMethods->xTruncate(pf->inner, size);
  if (rc != SQLITE_OK) pf->reg->failed = 1;
  if (rc == SQLITE_OK && pf->pageSize > 0) {
    uint32_t keep = (size <= 0) ? 0 : (uint32_t)(size / pf->pageSize);
    if (keep < pf->reg->cutPages) pf->reg->cutPages = keep;
    for (uint32_t p = keep + 1; p <= pf->reg->commit; p++) bitSet(pf->reg, p);
  }
  sqlite3_mutex_leave(gRegMu);
  /* Cached cut pages are harmless: installLocked checks the monotonic bound. */
  return rc;
}

static int pagedClose(sqlite3_file *file) {
  PagedFile *pf = (PagedFile *)file;
  int rc = pf->inner->pMethods->xClose(pf->inner);
  pf->reg = 0; /* the registration outlives the file object */
  return rc;
}

static const sqlite3_io_methods gPagedIoMethods = {
  3,                        /* iVersion */
  pagedClose,               /* xClose */
  pagedRead,                /* xRead */
  pagedWrite,               /* xWrite */
  pagedTruncate,            /* xTruncate */
  0,                        /* xSync (set at init: forward) */
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
};

/* Forwarders for the remaining file methods (assigned at init from the base
** VFS's own method table so shm/lock/fetch behavior is byte-for-byte the base). */
static int fwSync(sqlite3_file *f, int flags) { return ((PagedFile *)f)->inner->pMethods->xSync(((PagedFile *)f)->inner, flags); }
static int fwFileSize(sqlite3_file *f, sqlite3_int64 *p) { return ((PagedFile *)f)->inner->pMethods->xFileSize(((PagedFile *)f)->inner, p); }
static int fwLock(sqlite3_file *f, int l) { return ((PagedFile *)f)->inner->pMethods->xLock(((PagedFile *)f)->inner, l); }
static int fwUnlock(sqlite3_file *f, int l) { return ((PagedFile *)f)->inner->pMethods->xUnlock(((PagedFile *)f)->inner, l); }
static int fwCheckReservedLock(sqlite3_file *f, int *p) { return ((PagedFile *)f)->inner->pMethods->xCheckReservedLock(((PagedFile *)f)->inner, p); }
static int fwFileControl(sqlite3_file *f, int op, void *p) { return ((PagedFile *)f)->inner->pMethods->xFileControl(((PagedFile *)f)->inner, op, p); }
static int fwSectorSize(sqlite3_file *f) { return ((PagedFile *)f)->inner->pMethods->xSectorSize(((PagedFile *)f)->inner); }
static int fwDeviceCharacteristics(sqlite3_file *f) { return ((PagedFile *)f)->inner->pMethods->xDeviceCharacteristics(((PagedFile *)f)->inner); }
static int fwShmMap(sqlite3_file *f, int i, int sz, int e, void volatile **p) { return ((PagedFile *)f)->inner->pMethods->xShmMap(((PagedFile *)f)->inner, i, sz, e, p); }
static int fwShmLock(sqlite3_file *f, int o, int n, int fl) { return ((PagedFile *)f)->inner->pMethods->xShmLock(((PagedFile *)f)->inner, o, n, fl); }
static void fwShmBarrier(sqlite3_file *f) { ((PagedFile *)f)->inner->pMethods->xShmBarrier(((PagedFile *)f)->inner); }
static int fwShmUnmap(sqlite3_file *f, int d) { return ((PagedFile *)f)->inner->pMethods->xShmUnmap(((PagedFile *)f)->inner, d); }
static int fwFetch(sqlite3_file *f, sqlite3_int64 o, int n, void **p) { return ((PagedFile *)f)->inner->pMethods->xFetch(((PagedFile *)f)->inner, o, n, p); }
static int fwUnfetch(sqlite3_file *f, sqlite3_int64 o, void *p) { return ((PagedFile *)f)->inner->pMethods->xUnfetch(((PagedFile *)f)->inner, o, p); }

static sqlite3_io_methods gMethods;

/* Public FILESTAT opcode, pinned diagnostic JSON contract from mattn 1.14.52
** (SQLite 3.53.4, built with SQLITE_ENABLE_FILESTAT). This is not a generic fd
** API: unknown versions/VFS implementations fail closed, never inspect a
** private sqlite3_file layout or reopen/stat the path to infer its identity. */
static int openedIdentity(sqlite3_file *file, struct stat *st) {
  if (sqlite3_libversion_number() != 3053004 ||
      gReal != sqlite3_vfs_find("unix")) return SQLITE_CANTOPEN;
  sqlite3_str *s = sqlite3_str_new(0);
  if (!s) return SQLITE_NOMEM;
  int rc = file->pMethods->xFileControl(file, SQLITE_FCNTL_FILESTAT, s);
  int err = sqlite3_str_errcode(s);
  int len = sqlite3_str_length(s);
  char *json = sqlite3_str_finish(s);
  int fd = -1;
  if (rc == SQLITE_OK && err == SQLITE_OK && json)
    fd = goPagedFileStatFD(json, len);
  sqlite3_free(json);
  if (fd < 0 || fstat(fd, st) != 0 || !S_ISREG(st->st_mode))
    return SQLITE_CANTOPEN;
  return SQLITE_OK;
}

static int pagedOpen(sqlite3_vfs *vfs, sqlite3_filename zName, sqlite3_file *file,
                     int flags, int *pOutFlags) {
  int rc = gReal->xOpen(gReal, zName, file, flags, pOutFlags);
  if (rc != SQLITE_OK) return rc;
  if (!(flags & SQLITE_OPEN_MAIN_DB) || zName == 0) return SQLITE_OK; /* untouched */
  const char *path = (const char *)zName;
  sqlite3_mutex_enter(gRegMu);
  PagedReg *r = regFindLocked(path);
  if (!r) { sqlite3_mutex_leave(gRegMu); return SQLITE_OK; }
  int id = r->id;
  sqlite3_mutex_leave(gRegMu);
  struct stat st;
  rc = openedIdentity(file, &st); /* no C registry lock across the Go parser */
  sqlite3_mutex_enter(gRegMu);
  if (rc != SQLITE_OK || !r->used || r->id != id || r->failed ||
      r->dev != st.st_dev || r->ino != st.st_ino) {
    sqlite3_mutex_leave(gRegMu);
    file->pMethods->xClose(file);
    file->pMethods = 0;
    return SQLITE_CANTOPEN;
  }

  /* Wrap: keep the base file object right after our header and hand SQLite our
  ** methods, forwarding the rest to the base file. */
  PagedFile *pf = (PagedFile *)file;
  sqlite3_file *inner = (sqlite3_file *)((char *)file + sizeof(PagedFile));
  memmove(inner, file, (size_t)gReal->szOsFile); /* the base just initialized it */
  memset(file, 0, sizeof(PagedFile));
  pf->inner = inner;
  pf->reg = r;
  pf->pageSize = r->pageSize;
  pf->commit = r->commit;
  pf->regId = r->id;
  pf->base.pMethods = &gMethods;
  sqlite3_mutex_leave(gRegMu);
  return SQLITE_OK;
}

/* --- C API used by the Go side --- */

int cellhive_paged_init(void) {
  if (gReal) return 0;
  sqlite3_vfs *base = sqlite3_vfs_find(0);
  if (!base) return -1;
  gReal = base;
  if (!gRegMu) gRegMu = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  /* Our file-methods table: paged xRead/xWrite/xTruncate/xClose, everything
  ** else forwards to the base file object. */
  gMethods.iVersion = 3;
  gMethods.xClose = pagedClose;
  gMethods.xRead = pagedRead;
  gMethods.xWrite = pagedWrite;
  gMethods.xTruncate = pagedTruncate;
  gMethods.xSync = fwSync;
  gMethods.xFileSize = fwFileSize;
  gMethods.xLock = fwLock;
  gMethods.xUnlock = fwUnlock;
  gMethods.xCheckReservedLock = fwCheckReservedLock;
  gMethods.xFileControl = fwFileControl;
  gMethods.xSectorSize = fwSectorSize;
  gMethods.xDeviceCharacteristics = fwDeviceCharacteristics;
  gMethods.xShmMap = fwShmMap;
  gMethods.xShmLock = fwShmLock;
  gMethods.xShmBarrier = fwShmBarrier;
  gMethods.xShmUnmap = fwShmUnmap;
  gMethods.xFetch = fwFetch;
  gMethods.xUnfetch = fwUnfetch;

  gBase = *base;
  gBase.pNext = 0;
  gBase.zName = "cellhive-paged";
  gBase.szOsFile = (int)(sizeof(PagedFile) + (size_t)base->szOsFile);
  gBase.pAppData = base;
  gBase.xOpen = pagedOpen;
  /* Other VFS-level methods (xDelete/xAccess/xFullPathname/xRandomness/xSleep/
  ** xCurrentTime/xGetLastError/xCurrentTimeInt64) stay the base's, which is
  ** what a wrapper wants. */
  return sqlite3_vfs_register(&gBase, 0);
}

int cellhive_paged_register(int id, const char *path, unsigned int pageSize, unsigned int commit) {
  if (!gRegMu || !path || pageSize == 0 || commit == 0) return -1;
  size_t plen = strlen(path);
  if (plen == 0 || plen >= PAGED_MAX_PATH) return -1;
  struct stat st;
  if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) return -1;
  sqlite3_mutex_enter(gRegMu);
  PagedReg *slot = 0;
  PagedReg *existing = regFindLocked(path);
  if (existing) {
    slot = existing;
  } else {
    for (int i = 0; i < PAGED_MAX_REGS; i++) {
      if (!gRegs[i].used) { slot = &gRegs[i]; break; }
    }
    if (!slot) { sqlite3_mutex_leave(gRegMu); return -2; }
  }
  size_t bytes = ((size_t)commit + 7) / 8;
  uint8_t *bits = (uint8_t *)sqlite3_malloc64(bytes);
  if (!bits) { sqlite3_mutex_leave(gRegMu); return -3; }
  memset(bits, 0, bytes);
  sqlite3_free(slot->hydrated);
  memset(slot, 0, sizeof(*slot));
  slot->used = 1;
  slot->id = id;
  memcpy(slot->path, path, plen + 1);
  slot->pageSize = pageSize;
  slot->commit = commit;
  slot->cutPages = commit;
  slot->dev = st.st_dev;
  slot->ino = st.st_ino;
  slot->hydrated = bits;
  sqlite3_mutex_leave(gRegMu);
  return 0;
}

int cellhive_paged_release(int id) {
  if (!gRegMu) return -1;
  sqlite3_mutex_enter(gRegMu);
  for (int i = 0; i < PAGED_MAX_REGS; i++) {
    if (gRegs[i].used && gRegs[i].id == id) {
      sqlite3_free(gRegs[i].hydrated);
      memset(&gRegs[i], 0, sizeof(gRegs[i]));
      break;
    }
  }
  sqlite3_mutex_leave(gRegMu);
  return 0;
}

int cellhive_paged_hydrated_count(int id) {
  int out = 0;
  sqlite3_mutex_enter(gRegMu);
  for (int i = 0; i < PAGED_MAX_REGS; i++) {
    if (gRegs[i].used && gRegs[i].id == id) {
      if (gRegs[i].failed) break;
      for (uint32_t p = 1; p <= gRegs[i].commit; p++) {
        if (bitGet(&gRegs[i], p)) out++;
      }
      break;
    }
  }
  sqlite3_mutex_leave(gRegMu);
  return out;
}

int cellhive_paged_hydrated(int id, unsigned int pgno) {
  int out = 0;
  sqlite3_mutex_enter(gRegMu);
  for (int i = 0; i < PAGED_MAX_REGS; i++) {
    if (gRegs[i].used && gRegs[i].id == id) {
      if (!gRegs[i].failed) out = bitGet(&gRegs[i], pgno);
      break;
    }
  }
  sqlite3_mutex_leave(gRegMu);
  return out;
}

void cellhive_paged_mark(int id, unsigned int pgno) {
  sqlite3_mutex_enter(gRegMu);
  for (int i = 0; i < PAGED_MAX_REGS; i++) {
    if (gRegs[i].used && gRegs[i].id == id) { bitSet(&gRegs[i], pgno); break; }
  }
  sqlite3_mutex_leave(gRegMu);
}

int cellhive_paged_commit(int id) {
  int out = 0;
  sqlite3_mutex_enter(gRegMu);
  for (int i = 0; i < PAGED_MAX_REGS; i++) {
    if (gRegs[i].used && gRegs[i].id == id) { out = (int)gRegs[i].commit; break; }
  }
  sqlite3_mutex_leave(gRegMu);
  return out;
}

int cellhive_paged_page_size(int id) {
  int out = 0;
  sqlite3_mutex_enter(gRegMu);
  for (int i = 0; i < PAGED_MAX_REGS; i++) {
    if (gRegs[i].used && gRegs[i].id == id) { out = (int)gRegs[i].pageSize; break; }
  }
  sqlite3_mutex_leave(gRegMu);
  return out;
}

static PagedReg *regFDLocked(int id, int fd) {
  struct stat st;
  if (fstat(fd, &st)) return 0;
  for (int i = 0; i < PAGED_MAX_REGS; i++) {
    PagedReg *r = &gRegs[i];
    if (r->used && r->id == id && !r->failed && r->dev == st.st_dev && r->ino == st.st_ino)
      return r;
  }
  return 0;
}

int cellhive_paged_install(int id, int fd, unsigned int pgno, const void *page, int len) {
  sqlite3_mutex_enter(gRegMu);
  PagedReg *r = regFDLocked(id, fd);
  int rc = r ? installLocked(r, 0, fd, pgno, page, len) : SQLITE_IOERR_WRITE;
  sqlite3_mutex_leave(gRegMu);
  return rc;
}

int cellhive_paged_sync(int id, int fd) {
  sqlite3_mutex_enter(gRegMu);
  PagedReg *r = regFDLocked(id, fd);
  int rc = SQLITE_IOERR_FSYNC;
  if (r) {
    int complete = 1;
    for (uint32_t p = 1; p <= r->commit; p++) if (!bitGet(r, p)) { complete = 0; break; }
    if (complete && fsync(fd) == 0) rc = SQLITE_OK;
  }
  sqlite3_mutex_leave(gRegMu);
  return rc;
}
