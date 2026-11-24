//go:build linux

package main

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func processUsage() (time.Duration, int64, bool) {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0, 0, false
	}
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, 0, false
	}
	f := strings.Fields(string(raw))
	if len(f) < 2 {
		return 0, 0, false
	}
	pages, err := strconv.ParseInt(f[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	cpu := time.Duration(r.Utime.Sec+r.Stime.Sec)*time.Second + time.Duration(r.Utime.Usec+r.Stime.Usec)*time.Microsecond
	return cpu, pages * int64(os.Getpagesize()), true
}
