package main

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"cellhive/internal/lease"
)

type nodeLoadSampler struct {
	mu   sync.Mutex
	last time.Time
	cpu  time.Duration
}

func newNodeLoadSampler() *nodeLoadSampler { return &nodeLoadSampler{} }
func (s *nodeLoadSampler) Sample() lease.Load {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	v := lease.Load{SampledMs: now.UnixMilli()}
	cpu, rss, ok := processUsage()
	if ok {
		v.RSSBytes = rss
		if !s.last.IsZero() && cpu >= s.cpu {
			v.CPUPercentX100 = int(float64(cpu-s.cpu) / float64(now.Sub(s.last)) * 10000)
			v.ProcessSampleValid = true
		}
		s.last = now
		s.cpu = cpu
	}
	// cgroup v2: memory headroom is container-wide, RSS and CPU are process-wide.
	limit, le := os.ReadFile("/sys/fs/cgroup/memory.max")
	used, ue := os.ReadFile("/sys/fs/cgroup/memory.current")
	if le == nil && ue == nil {
		n, ne := strconv.ParseInt(strings.TrimSpace(string(limit)), 10, 64)
		u, e := strconv.ParseInt(strings.TrimSpace(string(used)), 10, 64)
		if ne == nil && e == nil && n > 0 && u >= 0 {
			v.MemoryLimit = n
			v.MemoryHeadroom = max(0, n-u)
			v.MemorySampleValid = true
		}
	}
	return v
}
