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

/* Implemented in Go (pagedvfs.go). Returns 0 on success; on failure the VFS
** surfaces SQLITE_IOERR_READ and SQLite poisons the read (fail closed). */
extern int goPagedFetch(int regId, unsigned int pgno, void *buf, int bufLen);
/* Informs Go that pages above keepPages were truncated away (window cache). */
extern void goPagedTruncated(int regId, unsigned int keepPages);

#define PAGED_MAX_REGS 64
#define PAGED_MAX_PATH 1024

typedef struct PagedReg {
  int used;
  int id;
  char path[PAGED_MAX_PATH];
  uint32_t pageSize;
  uint32_t commit;
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

static void bitClearFrom(PagedReg *r, uint32_t pgno) {
  for (uint32_t p = pgno; p <= r->commit; p++) {
    uint32_t i = p - 1;
    r->hydrated[i >> 3] &= (uint8_t)~(1u << (i & 7));
  }
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
  for (uint32_t pgno = first; pgno <= last; pgno++) {
    if (bitGet(r, pgno)) continue;
    sqlite3_mutex_leave(gRegMu);
    /* Fault one page at a time on the calling thread (the Rust original does
    ** the same: a synchronous read that touches no runtime). */
    unsigned char *page = (unsigned char *)sqlite3_malloc64(ps);
    if (!page) return SQLITE_NOMEM;
    int rc = goPagedFetch(pf->regId, pgno, page, (int)ps);
    if (rc == 0) {
      rc = pf->inner->pMethods->xWrite(pf->inner, page, (int)ps,
                                       (sqlite3_int64)(pgno - 1) * (sqlite3_int64)ps);
    }
    sqlite3_free(page);
    if (rc != 0) return SQLITE_IOERR_READ;
    sqlite3_mutex_enter(gRegMu);
    bitSet(r, pgno);
  }
  sqlite3_mutex_leave(gRegMu);
  return pf->inner->pMethods->xRead(pf->inner, buf, iAmt, iOfst);
}

static int pagedWrite(sqlite3_file *file, const void *buf, int iAmt, sqlite3_int64 iOfst) {
  PagedFile *pf = (PagedFile *)file;
  if (pf->reg && pf->pageSize > 0 && iAmt > 0) {
    uint32_t first = (uint32_t)(iOfst / pf->pageSize) + 1;
    uint32_t last = (uint32_t)((iOfst + iAmt - 1) / pf->pageSize) + 1;
    sqlite3_mutex_enter(gRegMu);
    for (uint32_t pgno = first; pgno <= last; pgno++) bitSet(pf->reg, pgno);
    sqlite3_mutex_leave(gRegMu);
  }
  return pf->inner->pMethods->xWrite(pf->inner, buf, iAmt, iOfst);
}

static int pagedTruncate(sqlite3_file *file, sqlite3_int64 size) {
  PagedFile *pf = (PagedFile *)file;
  if (pf->reg && pf->pageSize > 0) {
    uint32_t keep = (size <= 0) ? 0 : (uint32_t)(size / pf->pageSize);
    sqlite3_mutex_enter(gRegMu);
    bitClearFrom(pf->reg, keep + 1);
    sqlite3_mutex_leave(gRegMu);
    goPagedTruncated(pf->regId, keep); /* drop stale window pages */
  }
  return pf->inner->pMethods->xTruncate(pf->inner, size);
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

static int pagedOpen(sqlite3_vfs *vfs, sqlite3_filename zName, sqlite3_file *file,
                     int flags, int *pOutFlags) {
  int rc = gReal->xOpen(gReal, zName, file, flags, pOutFlags);
  if (rc != SQLITE_OK) return rc;
  if (!(flags & SQLITE_OPEN_MAIN_DB) || zName == 0) return SQLITE_OK; /* untouched */
  const char *path = (const char *)zName;
  sqlite3_mutex_enter(gRegMu);
  PagedReg *r = regFindLocked(path);
  sqlite3_mutex_leave(gRegMu);
  if (!r) return SQLITE_OK;

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
  sqlite3_mutex_enter(gRegMu);
  PagedReg *slot = 0;
  PagedReg *existing = regFindLocked(path);
  if (existing) {
    slot = existing;
    sqlite3_free(slot->hydrated);
  } else {
    for (int i = 0; i < PAGED_MAX_REGS; i++) {
      if (!gRegs[i].used) { slot = &gRegs[i]; break; }
    }
    if (!slot) { sqlite3_mutex_leave(gRegMu); return -2; }
  }
  size_t bytes = (size_t)((commit + 7) / 8);
  uint8_t *bits = (uint8_t *)sqlite3_malloc64(bytes);
  if (!bits) { sqlite3_mutex_leave(gRegMu); return -3; }
  memset(bits, 0, bytes);
  memset(slot, 0, sizeof(*slot));
  slot->used = 1;
  slot->id = id;
  memcpy(slot->path, path, plen + 1);
  slot->pageSize = pageSize;
  slot->commit = commit;
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
    if (gRegs[i].used && gRegs[i].id == id) { out = bitGet(&gRegs[i], pgno); break; }
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
