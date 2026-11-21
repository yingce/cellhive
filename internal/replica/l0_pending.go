package replica

import (
	"strconv"
	"strings"

	"cellhive/internal/cell"
)

// l0Pending is the owner's record of L0 objects it has published for one
// scope and epoch and has not yet folded into L1. The promotion gate reads
// this tally. It does not list the bucket.
type l0Pending struct {
	files map[string]int64
}

func (m *Manager) initL0() {
	if m.l0 == nil {
		m.l0 = map[string]*l0Pending{}
	}
}

func l0PendingKey(s cell.Scope, epoch uint64) string {
	return s.String() + "\x00" + u64(epoch)
}

func u64(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// l0KeySize reports the stored size when key is one canonical L0 object.
func l0KeySize(key string, size int64) (int64, bool) {
	const marker = "/0000/"
	i := strings.LastIndex(key, marker)
	if i < 0 || size < 0 {
		return 0, false
	}
	name := key[i+len(marker):]
	if strings.Contains(name, "/") || !strings.HasSuffix(name, ".ltx") {
		return 0, false
	}
	return size, true
}

func (m *Manager) noteL0Object(s cell.Scope, epoch uint64, key string, size int64) {
	n, ok := l0KeySize(key, size)
	if !ok {
		return
	}
	m.l0Mu.Lock()
	defer m.l0Mu.Unlock()
	if m.l0Lost != nil && epoch <= m.l0Lost[s.String()] && m.l0Lost[s.String()] != 0 {
		return
	}
	m.initL0()
	slot := l0PendingKey(s, epoch)
	p := m.l0[slot]
	if p == nil {
		p = &l0Pending{files: map[string]int64{}}
		m.l0[slot] = p
	}
	if _, exists := p.files[key]; exists {
		return
	}
	p.files[key] = n
}

// L0Pending returns the L0 file count and stored bytes this owner has
// published for the scope and epoch and not yet dropped after an L1 fold.
func (m *Manager) L0Pending(s cell.Scope, epoch uint64) (int, int64) {
	m.l0Mu.Lock()
	defer m.l0Mu.Unlock()
	p := m.l0[l0PendingKey(s, epoch)]
	if p == nil {
		return 0, 0
	}
	var bytes int64
	for _, n := range p.files {
		bytes += n
	}
	return len(p.files), bytes
}

// SnapshotL0Keys copies the L0 keys currently in the owner's tally.
func (m *Manager) SnapshotL0Keys(s cell.Scope, epoch uint64) []string {
	m.l0Mu.Lock()
	defer m.l0Mu.Unlock()
	p := m.l0[l0PendingKey(s, epoch)]
	if p == nil {
		return nil
	}
	out := make([]string, 0, len(p.files))
	for key := range p.files {
		out = append(out, key)
	}
	return out
}

// LoseL0 drops one epoch's tally and ignores later uploads of that same epoch.
// The caller has confirmed this process no longer owns it. A newer epoch can
// still be recorded.
func (m *Manager) LoseL0(s cell.Scope, epoch uint64) {
	if m == nil || epoch == 0 {
		return
	}
	m.l0Mu.Lock()
	defer m.l0Mu.Unlock()
	if m.l0 != nil {
		delete(m.l0, l0PendingKey(s, epoch))
	}
	m.markL0Lost(s, epoch)
}

func (m *Manager) markL0Lost(s cell.Scope, epoch uint64) {
	if epoch == 0 {
		return
	}
	if m.l0Lost == nil {
		m.l0Lost = map[string]uint64{}
	}
	if epoch > m.l0Lost[s.String()] {
		m.l0Lost[s.String()] = epoch
	}
}

// ForgetL0 drops one epoch's tally without treating the epoch as lost.
// Compaction still uses DropL0 so a later upload of the same epoch counts.
func (m *Manager) ForgetL0(s cell.Scope, epoch uint64) {
	if m == nil {
		return
	}
	m.l0Mu.Lock()
	defer m.l0Mu.Unlock()
	if m.l0 == nil {
		return
	}
	delete(m.l0, l0PendingKey(s, epoch))
}

// ForgetScopeL0 drops every epoch recorded for a scope. Resource retirement
// calls it after the scope can no longer be written.
func (m *Manager) ForgetScopeL0(s cell.Scope) {
	if m == nil {
		return
	}
	prefix := s.String() + "\x00"
	m.l0Mu.Lock()
	defer m.l0Mu.Unlock()
	var max uint64
	for key := range m.l0 {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if e, err := strconv.ParseUint(key[len(prefix):], 10, 64); err == nil && e > max {
			max = e
		}
		delete(m.l0, key)
	}
	m.markL0Lost(s, max)
}

// DropL0 removes keys folded into L1, or keys already covered when a
// promotion finds nothing left to read. Keys published after the snapshot
// stay in the tally.
func (m *Manager) DropL0(s cell.Scope, epoch uint64, keys []string) {
	if len(keys) == 0 {
		return
	}
	m.l0Mu.Lock()
	defer m.l0Mu.Unlock()
	p := m.l0[l0PendingKey(s, epoch)]
	if p == nil {
		return
	}
	for _, key := range keys {
		delete(p.files, key)
	}
	if len(p.files) == 0 {
		delete(m.l0, l0PendingKey(s, epoch))
	}
}
