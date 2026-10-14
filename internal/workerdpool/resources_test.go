package workerdpool

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResourceReaderCgroupV2(t *testing.T) {
	d := t.TempDir()
	for n, v := range map[string]string{"memory.current": "100\n", "memory.max": "536870912\n", "cpu.stat": "usage_usec 42\n", "cpu.max": "100000 100000\n"} {
		if err := os.WriteFile(filepath.Join(d, n), []byte(v), 0644); err != nil {
			t.Fatal(err)
		}
	}
	r, err := ResourceReader{Root: d}.Read()
	if err != nil {
		t.Fatal(err)
	}
	if r.MemoryCurrent != 100 || r.MemoryLimit != 536870912 || r.CPUUsageUS != 42 || r.CPUQuotaUS != 100000 || r.CPUPeriodUS != 100000 {
		t.Fatalf("resources=%+v", r)
	}
}
