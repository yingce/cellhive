package server

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// This view deliberately renders only tenant-labelled metric families. It does
// not reuse the platform scrape, whose unlabelled series cannot be tenant scoped.
func (s *Server) handleNamespaceMetrics(w http.ResponseWriter, r *http.Request) {
	p, ok := principalFrom(r.Context())
	if !ok {
		writeErr(w, 403, "principal_required", "authenticated principal required")
		return
	}
	allow := func(ns string) bool {
		return ns != "platform" && ns != "other" && !strings.HasPrefix(ns, "__") && principalAllows(p, ns)
	}
	w.Header().Set("content-type", "text/plain; version=0.0.4")
	keys := []string{}
	s.bindingCalls.Range(func(k, v any) bool { keys = append(keys, k.(string)); return true })
	sort.Strings(keys)
	writeFmt(w, "# TYPE cellhive_binding_calls_total counter\n")
	for _, key := range keys {
		ns, rest, _ := strings.Cut(key, "|")
		if !allow(ns) {
			continue
		}
		kind, outcome, _ := strings.Cut(rest, "|")
		v, _ := s.bindingCalls.Load(key)
		writeFmt(w, "cellhive_binding_calls_total{ns=\"%s\",kind=\"%s\",outcome=\"%s\"} %d\n", escapeLabel(ns), escapeLabel(kind), escapeLabel(outcome), v.(*atomic.Int64).Load())
	}
	for _, family := range []struct {
		name   string
		values *sync.Map
	}{{"cellhive_durability_proof_seconds", &s.proofByNS}, {"cellhive_capture_ensure_seconds", &s.ensureByNS}} {
		writeFmt(w, "# TYPE %s histogram\n", family.name)
		keys = nil
		family.values.Range(func(k, v any) bool { keys = append(keys, k.(string)); return true })
		sort.Strings(keys)
		for _, ns := range keys {
			if !allow(ns) {
				continue
			}
			v, ok := family.values.Load(ns)
			if !ok {
				continue
			}
			h := v.(*proofHist)
			for i, b := range proofBucketBounds {
				writeFmt(w, "%s_bucket{ns=\"%s\",le=\"%s\"} %d\n", family.name, escapeLabel(ns), strconv.FormatFloat(b, 'g', -1, 64), h.buckets[i].Load())
			}
			writeFmt(w, "%s_bucket{ns=\"%s\",le=\"+Inf\"} %d\n", family.name, escapeLabel(ns), h.count.Load())
			writeFmt(w, "%s_sum{ns=\"%s\"} %g\n", family.name, escapeLabel(ns), float64(h.micros.Load())/1e6)
			writeFmt(w, "%s_count{ns=\"%s\"} %d\n", family.name, escapeLabel(ns), h.count.Load())
		}
	}
	writeFmt(w, "# TYPE cellhive_capture_ensure_total counter\n")
	keys = nil
	s.ensureModes.Range(func(k, v any) bool { keys = append(keys, k.(string)); return true })
	sort.Strings(keys)
	for _, key := range keys {
		ns, mode, _ := strings.Cut(key, "|")
		if !allow(ns) {
			continue
		}
		v, _ := s.ensureModes.Load(key)
		writeFmt(w, "cellhive_capture_ensure_total{ns=\"%s\",mode=\"%s\"} %d\n", escapeLabel(ns), escapeLabel(mode), v.(*atomic.Int64).Load())
	}
}
