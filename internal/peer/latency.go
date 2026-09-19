package peer

import (
	"context"
	"fmt"
	"time"

	"cellhive/internal/cell"
)

// LatencyTransport wraps a Transport and injects a fixed one-way delay on both
// the outbound request and the returned ack. It emulates a non-loopback network
// (same-AZ or cross-region RTT) without needing two physical hosts, so the
// fleet benchmark can report a network overhead curve. Total added latency per
// call is 2*oneWay (request + ack).
type LatencyTransport struct {
	inner  Transport
	oneWay time.Duration
}

func NewLatencyTransport(inner Transport, oneWay time.Duration) *LatencyTransport {
	return &LatencyTransport{inner: inner, oneWay: oneWay}
}

func (t *LatencyTransport) wait(ctx context.Context) error {
	if t.oneWay <= 0 {
		return nil
	}
	timer := time.NewTimer(t.oneWay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (t *LatencyTransport) Append(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segment []byte) error {
	if err := t.wait(ctx); err != nil {
		return err
	}
	err := t.inner.Append(ctx, baseURL, scope, epoch, segment)
	if err == nil {
		err = t.wait(ctx)
	}
	return err
}

func (t *LatencyTransport) AppendBatch(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) error {
	if err := t.wait(ctx); err != nil {
		return err
	}
	err := t.inner.AppendBatch(ctx, baseURL, scope, epoch, segments)
	if err == nil {
		err = t.wait(ctx)
	}
	return err
}

func (t *LatencyTransport) Held(ctx context.Context, baseURL string) ([]HeldSegment, error) {
	if err := t.wait(ctx); err != nil {
		return nil, err
	}
	held, err := t.inner.Held(ctx, baseURL)
	if err != nil {
		return nil, err
	}
	if err := t.wait(ctx); err != nil {
		return nil, err
	}
	return held, err
}

// AppendBatchAsync forwards to the inner async transport (when present). The
// frame is written immediately so an ordered dispatcher can pipeline sends;
// the injected RTT (2x one-way) is applied to the ack instead of blocking the
// write, which is what a real network does.
func (t *LatencyTransport) AppendBatchAsync(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) (<-chan error, error) {
	inner, ok := t.inner.(AsyncTransport)
	if !ok {
		return nil, errNotAsync
	}
	ch, err := inner.AppendBatchAsync(ctx, baseURL, scope, epoch, segments)
	if err != nil {
		return nil, err
	}
	out := make(chan error, 1)
	go func() {
		ackErr := <-ch
		if ackErr == nil && t.oneWay > 0 {
			timer := time.NewTimer(2 * t.oneWay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				ackErr = ctx.Err()
			}
			timer.Stop()
		}
		out <- ackErr
	}()
	return out, nil
}

var errNotAsync = fmt.Errorf("peer: inner transport does not support async append")
