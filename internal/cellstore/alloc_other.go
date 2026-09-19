//go:build !unix

package cellstore

import "os"

// allocatedBytes is unavailable off unix; callers fall back to apparent size.
func allocatedBytes(os.FileInfo) int64 { return -1 }
