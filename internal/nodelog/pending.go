package nodelog

import (
	"context"
	"strings"

	"cellhive/internal/objectstore"
)

// DiscoverPending excludes only sessions this process already read as sealed.
// Session keys are never reused and sealed is terminal. Explicit Get and the
// ordinary diagnostic listing always read authority, regardless of this cache.
func (m *Manager) DiscoverPending(ctx context.Context) (*Listing, error) {
	keys, values, err := m.pendingListing(ctx, objectstore.PrefixNodeLogs)
	if err != nil {
		return nil, err
	}
	l := &Listing{manager: m, keys: make(map[string][]string), values: values, pending: true}
	for _, k := range keys {
		rest, ok := strings.CutPrefix(k, objectstore.PrefixNodeLogs)
		if !ok {
			continue
		}
		node, _, ok := strings.Cut(rest, "/")
		if ok && node != "" {
			l.keys[node] = append(l.keys[node], k)
		}
	}
	return l, nil
}

func (m *Manager) pendingListing(ctx context.Context, prefix string) ([]string, map[string][]byte, error) {
	m.terminalMu.Lock()
	terminal := make(map[string]bool, len(m.terminal))
	for k := range m.terminal {
		terminal[k] = true
	}
	m.terminalMu.Unlock()
	objs := m.Objs
	if objs == nil {
		objs = objectstore.NewOwned(m.B, objectstore.PrefixNodeLogs, objectstore.OwnerNodeLog)
	}
	return objs.ListWithResolvedValuesExcept(ctx, prefix, terminal)
}

func (m *Manager) ListPendingNode(ctx context.Context, node string) ([]Record, error) {
	keys, values, err := m.pendingListing(ctx, objectstore.PrefixNodeLogs+node+"/")
	if err != nil {
		return nil, err
	}
	return m.pendingRecords(ctx, keys, values)
}

func (m *Manager) pendingRecords(ctx context.Context, keys []string, values map[string][]byte) ([]Record, error) {
	records, err := m.readRecords(ctx, keys, values)
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(records))
	m.terminalMu.Lock()
	defer m.terminalMu.Unlock()
	if m.terminal == nil {
		m.terminal = make(map[string]bool)
	}
	for _, r := range records {
		if r.Status == StatusSealed {
			if len(m.terminal) < 10000 {
				m.terminal[Key(r.Node, r.Session)] = true
			}
		} else {
			out = append(out, r)
		}
	}
	return out, nil
}
