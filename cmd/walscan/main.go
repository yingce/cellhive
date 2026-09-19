// Command walscan parses a SQLite WAL file (read-only) and prints its committed
// state. Used to validate workerd actor SQLite observability (P0.7).
package main

import (
	"fmt"
	"os"

	"cellhive/internal/wal"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: walscan <path-to-wal>")
		os.Exit(2)
	}
	s, err := wal.Read(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "walscan error:", err)
		os.Exit(1)
	}
	fmt.Printf("page_size=%d salt=%d/%d total_frames=%d commits=%d committed_frames=%d\n",
		s.Header.PageSize, s.Header.Salt1, s.Header.Salt2, s.TotalFrames, s.CommitCount, len(s.Frames))
}
