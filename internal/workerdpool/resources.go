package workerdpool

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Resources struct {
	MemoryCurrent, MemoryLimit          int64
	CPUUsageUS, CPUQuotaUS, CPUPeriodUS int64
}

type ResourceReader struct{ Root string }

func (r ResourceReader) Read() (Resources, error) {
	root := r.Root
	if root == "" {
		root = "/sys/fs/cgroup"
	}
	memoryCurrent, err := readIntFile(root + "/memory.current")
	if err != nil {
		return Resources{}, err
	}
	memoryLimit, err := readLimit(root + "/memory.max")
	if err != nil {
		return Resources{}, err
	}
	cpuUsage, err := readIntFile(root+"/cpu.stat", "usage_usec")
	if err != nil {
		return Resources{}, err
	}
	quota, period, err := readCPU(root + "/cpu.max")
	if err != nil {
		return Resources{}, err
	}
	return Resources{MemoryCurrent: memoryCurrent, MemoryLimit: memoryLimit, CPUUsageUS: cpuUsage, CPUQuotaUS: quota, CPUPeriodUS: period}, nil
}

func readLimit(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	v := strings.TrimSpace(string(b))
	if v == "max" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("workerdpool: parse %s: %w", path, err)
	}
	return n, nil
}
func readIntFile(path string, key ...string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	text := strings.TrimSpace(string(b))
	if len(key) == 0 {
		return strconv.ParseInt(text, 10, 64)
	}
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == key[0] {
			return strconv.ParseInt(f[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("workerdpool: %s missing in %s", key[0], path)
}
func readCPU(path string) (int64, int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		return 0, 0, fmt.Errorf("workerdpool: invalid cpu.max")
	}
	q := int64(0)
	if f[0] != "max" {
		var e error
		q, e = strconv.ParseInt(f[0], 10, 64)
		if e != nil {
			return 0, 0, e
		}
	}
	p, e := strconv.ParseInt(f[1], 10, 64)
	return q, p, e
}
