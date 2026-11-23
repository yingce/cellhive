package control

import (
	"cellhive/internal/cell"
	"cellhive/internal/queue"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// ResolveQueueConsumer reads a physical queue's active consumer and DLQ
// mapping from the same SQLite snapshot. Worker/name ordering matches
// QueueTargets followed by the runner's first-physical-queue deduplication.
func (s *Store) ResolveQueueConsumer(ctx context.Context, ns, id string) (queue.Ref, bool, error) {
	scope := queue.Scope(ns, id)
	if err := scope.Validate(); err != nil {
		return queue.Ref{}, false, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return queue.Ref{}, false, err
	}
	var t QueueTarget
	var config string
	var dlq sql.NullString
	err = c.DB.QueryRowContext(ctx, `SELECT src.name,w.name,v.bundle_sha,v.number,j.value,dst.scope
		FROM resources src
		JOIN workers w ON w.ns=src.ns AND w.deleted_ms=0
		JOIN versions v ON v.ns=w.ns AND v.worker=w.name AND v.number=w.active
		JOIN json_each(v.consumers) j ON json_extract(j.value,'$.queue')=src.name
		LEFT JOIN resources dst ON dst.ns=src.ns AND dst.kind='queue'
			AND dst.name=json_extract(j.value,'$.dead_letter_queue')
		WHERE src.ns=? AND src.kind='queue' AND src.scope=?
		ORDER BY w.name,j.key LIMIT 1`, ns, scope.String()).Scan(&t.Queue, &t.Worker, &t.BundleSHA, &t.Version, &config, &dlq)
	if errors.Is(err, sql.ErrNoRows) {
		return queue.Ref{}, false, nil
	}
	if err != nil {
		return queue.Ref{}, false, err
	}
	if err := json.Unmarshal([]byte(config), &t.Consumer); err != nil {
		return queue.Ref{}, false, err
	}
	dest := ""
	if t.Consumer.DeadLetterQueue != "" {
		sc, err := cell.ParseScope(dlq.String)
		if err != nil || sc.Namespace != ns || sc.Class != "__queue__" {
			return queue.Ref{}, false, fmt.Errorf("invalid dead-letter queue mapping for %s", scope)
		}
		dest = sc.ID
	}
	return t.ResolvedRef(scope, dest), true, nil
}

func (t QueueTarget) ResolvedRef(sc cell.Scope, dlq string) queue.Ref {
	return queue.Ref{Namespace: sc.Namespace, Name: sc.ID, BindingName: t.Queue,
		Worker: t.Worker, BundleSHA: t.BundleSHA, Version: t.Version,
		MaxRetries: t.Consumer.MaxRetries, RetryDelaySeconds: t.Consumer.RetryDelaySeconds,
		DeadLetterQueue: dlq, MaxBatchSize: t.Consumer.MaxBatchSize,
		MaxBatchTimeoutSeconds: t.Consumer.MaxBatchTimeoutSeconds, MaxConcurrency: t.Consumer.MaxConcurrency}
}
