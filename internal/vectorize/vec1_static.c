/*
** Static registration of SQLite's official vec1 ANN extension (ADR-159).
**
** vec1.c is vendored from https://sqlite.org/vec1 (version 0.7, public domain)
** and compiled into the cell-agent binary with -DSQLITE_CORE so its API calls
** bind directly to the SQLite amalgamation that mattn/go-sqlite3 links. The
** extension is then registered process-wide via sqlite3_auto_extension, so
** every connection (including D1 tenant cells) can CREATE VIRTUAL TABLE ...
** USING vec1.
*/
#include "vec1/vec1.c"

void cellhive_vec1_register(void) {
  sqlite3_auto_extension((void (*)(void))sqlite3_extension_init);
}
