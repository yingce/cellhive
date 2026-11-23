package control

import (
	"cellhive/internal/cell"
	"fmt"
)

// QueueResourceScope resolves a business name without inventing a storage ID.
func QueueResourceScope(resources []ResourceRef, ns, name string) (cell.Scope, error) {
	for _, r := range resources {
		if r.Namespace != ns || r.Kind != "queue" || r.Name != name {
			continue
		}
		sc, err := cell.ParseScope(r.Scope)
		if err != nil || sc.Namespace != ns || sc.Class != "__queue__" {
			return cell.Scope{}, fmt.Errorf("queue %s/%s has invalid storage scope %q", ns, name, r.Scope)
		}
		return sc, nil
	}
	return cell.Scope{}, fmt.Errorf("queue %s/%s: %w", ns, name, ErrNotFound)
}
