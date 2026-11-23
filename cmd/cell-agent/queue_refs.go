package main

import (
	"cellhive/internal/control"
	"cellhive/internal/queue"
	"errors"
)

// Resolve before owner gating: every data operation uses the registered scope,
// while the worker receives the configured business queue name.
func resolveQueueRefs(resources []control.ResourceRef, targets []control.QueueTarget) ([]queue.Ref, error) {
	var out []queue.Ref
	var failures []error
	for _, t := range targets {
		sc, err := control.QueueResourceScope(resources, t.Namespace, t.Queue)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		dlq := ""
		if t.Consumer.DeadLetterQueue != "" {
			dest, err := control.QueueResourceScope(resources, t.Namespace, t.Consumer.DeadLetterQueue)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			dlq = dest.ID
		}
		out = append(out, t.ResolvedRef(sc, dlq))
	}
	return out, errors.Join(failures...)
}
