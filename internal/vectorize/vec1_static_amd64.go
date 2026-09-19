//go:build amd64 && !cellhive_vec1_portable

package vectorize

/*
#cgo CFLAGS: -O3 -DNDEBUG -DSQLITE_CORE -march=x86-64-v3 -I${SRCDIR}/../sqliteheaders -I${SRCDIR}/vec1
#cgo LDFLAGS: -lm
void cellhive_vec1_register(void);
*/
import "C"

func init() { C.cellhive_vec1_register() }
