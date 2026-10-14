package workerdpool

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type ProcessSample struct {
	RSSBytes int64
	CPUTicks uint64
	FDs      int
}

// CPU is deliberately returned in USER_HZ ticks, not incorrectly assumed to be
// milliseconds. Attribution is process-wide, not per invocation/isolate.
func ReadProcess(pid int) (ProcessSample, error) {
	if pid <= 0 {
		return ProcessSample{}, fmt.Errorf("workerdpool: invalid PID")
	}
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return ProcessSample{}, err
	}
	var sample ProcessSample
	for _, line := range strings.Split(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmRSS:" {
			n, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil || n < 0 {
				return sample, fmt.Errorf("workerdpool: invalid RSS")
			}
			sample.RSSBytes = n * 1024
		}
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return sample, err
	}
	ticks, err := processTicks(string(b))
	if err != nil {
		return sample, err
	}
	sample.CPUTicks = ticks
	files, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return sample, err
	}
	sample.FDs = len(files)
	return sample, nil
}
func processTicks(text string) (uint64, error) {
	i := strings.LastIndexByte(text, ')')
	if i < 0 {
		return 0, fmt.Errorf("workerdpool: invalid process stat")
	}
	f := strings.Fields(text[i+1:])
	if len(f) < 13 {
		return 0, fmt.Errorf("workerdpool: truncated process stat")
	}
	u, err := strconv.ParseUint(f[11], 10, 64)
	if err != nil {
		return 0, err
	}
	s, err := strconv.ParseUint(f[12], 10, 64)
	if err != nil || s > ^uint64(0)-u {
		return 0, fmt.Errorf("workerdpool: invalid CPU ticks")
	}
	return u + s, nil
}
