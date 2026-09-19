// Package peer implements node-to-node replication for the fleet durability
// posture (P0.4).
//
// The owner sends a committed segment to one or more followers and acknowledges
// only after at least one follower has fsynced it to its local spool. The
// object-store upload happens afterwards, asynchronously. If no follower is
// reachable the owner falls back to the bucket (single-node) posture.
//
// P0 transport is HTTP; ADR-026 plans gRPC/raw frames for production. The
// Transport interface isolates that choice.
package peer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"cellhive/internal/cell"
)

// Transport sends a segment to a follower and reads a follower's held segments.
type Transport interface {
	Append(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segment []byte) error
	// AppendBatch sends several segments for one scope/epoch in a single framed
	// request (fleet group commit).
	AppendBatch(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) error
	Held(ctx context.Context, baseURL string) ([]HeldSegment, error)
}

// HeldSegment is a segment a follower currently holds in its spool.
type HeldSegment struct {
	Scope   string
	Epoch   uint64
	Segment []byte
}

// HTTPTransport is the P0 peer transport.
type HTTPTransport struct {
	Token  string
	Client *http.Client
}

// NewHTTPTransport builds an HTTP transport.
func NewHTTPTransport(token string, client *http.Client) *HTTPTransport {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPTransport{Token: token, Client: client}
}

// Append sends a segment to the follower at baseURL.
func (t *HTTPTransport) Append(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segment []byte) error {
	u := fmt.Sprintf("%s/v1/peer/append?scope=%s&epoch=%d", trimSlash(baseURL), url.QueryEscape(scope.String()), epoch)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(segment))
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	req.Header.Set("content-type", "application/octet-stream")
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("peer append %s: status %d: %s", baseURL, resp.StatusCode, string(b))
	}
	return nil
}

// AppendBatch sends several segments to the follower in one framed request.
func (t *HTTPTransport) AppendBatch(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) error {
	u := fmt.Sprintf("%s/v1/peer/append_batch?scope=%s&epoch=%d", trimSlash(baseURL), url.QueryEscape(scope.String()), epoch)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(EncodeFrames(segments)))
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	req.Header.Set("content-type", "application/octet-stream")
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("peer append_batch %s: status %d: %s", baseURL, resp.StatusCode, string(b))
	}
	return nil
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

// Held fetches the segments a follower holds (recovery cold path).
func (t *HTTPTransport) Held(ctx context.Context, baseURL string) ([]HeldSegment, error) {
	u := trimSlash(baseURL) + "/v1/peer/held"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	resp, err := t.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("peer held %s: status %d: %s", baseURL, resp.StatusCode, string(b))
	}
	var wire struct {
		Segments []struct {
			Scope   string `json:"scope"`
			Epoch   uint64 `json:"epoch"`
			Segment string `json:"segment"`
		} `json:"segments"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&wire); err != nil {
		return nil, err
	}
	out := make([]HeldSegment, 0, len(wire.Segments))
	for _, s := range wire.Segments {
		seg, err := base64.StdEncoding.DecodeString(s.Segment)
		if err != nil {
			return nil, err
		}
		out = append(out, HeldSegment{Scope: s.Scope, Epoch: s.Epoch, Segment: seg})
	}
	return out, nil
}

// Manager replicates segments to followers with quorum-of-one semantics.
type Manager struct {
	T Transport
}

// NewManager builds a replication manager.
func NewManager(t Transport) *Manager { return &Manager{T: t} }

// Replicate sends the segment to every follower concurrently and returns the
// address of the first follower that fsynced it. If no follower succeeds it
// returns an error (caller falls back to the bucket posture).
func (m *Manager) Replicate(ctx context.Context, followers []string, scope cell.Scope, epoch uint64, segment []byte) (string, error) {
	if len(followers) == 0 {
		return "", fmt.Errorf("peer: no followers")
	}
	type result struct {
		addr string
		err  error
	}
	ch := make(chan result, len(followers))
	for _, f := range followers {
		go func(addr string) {
			ch <- result{addr: addr, err: m.T.Append(ctx, addr, scope, epoch, segment)}
		}(f)
	}
	var lastErr error
	for i := 0; i < len(followers); i++ {
		r := <-ch
		if r.err == nil {
			return r.addr, nil
		}
		lastErr = r.err
	}
	return "", fmt.Errorf("peer: all followers failed: %w", lastErr)
}
