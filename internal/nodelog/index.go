package nodelog

import "context"

// PendingIndex stores discovery hints, never recovery authority. Registration
// must precede COS publication; removal requires an authoritative sealed record.
// Implementations must fail closed when uninitialized or unavailable.
type PendingIndex interface {
	RegisterRecoverySession(context.Context, string, []byte) error
	RemoveRecoverySession(context.Context, string) error
	PendingRecoverySessions(context.Context, string) ([]string, map[string][]byte, error)
}

func (m *Manager) removeSealed(ctx context.Context, r Record) error {
	if m.Index != nil && r.Status == StatusSealed {
		return m.Index.RemoveRecoverySession(ctx, Key(r.Node, r.Session))
	}
	return nil
}
