//go:build unix

package cellstore

import (
	"os"
	"syscall"
)

// allocatedBytes reports the blocks actually allocated to a file (st_blocks is
// in 512-byte units), or -1 when unavailable. Sparse files (paged cells)
// allocate far fewer blocks than their apparent size.
func allocatedBytes(fi os.FileInfo) int64 {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return -1
	}
	return st.Blocks * 512
}
