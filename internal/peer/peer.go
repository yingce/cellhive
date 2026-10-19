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
	"cellhive/internal/nodelog"
)

// Transport sends a segment to a follower and reads a follower's held segments.
type Transport interface {
	Append(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segment []byte) error
	// AppendBatch sends several segments for one scope/epoch in a single framed
	// request (fleet group commit).
	AppendBatch(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) error
	Held(ctx context.Context, baseURL string) ([]HeldSegment, error)
}

// RecoveryTransport seals an owner's follower session and reads its immutable
// sequenced tail.
type RecoveryTransport interface {
	Seal(ctx context.Context, baseURL, session string) (uint64, error)
	Tail(ctx context.Context, baseURL, session string, after uint64) ([]SessionFragment, error)
	Reclaim(ctx context.Context, baseURL, session string) error
}

// LiveRecoveryTransport snapshots an active process session without sealing it.
// The caller must fence the target cell before taking the snapshot.
type LiveRecoveryTransport interface {
	Head(ctx context.Context, baseURL, session string) (uint64, error)
	TailOpen(ctx context.Context, baseURL, session, scope string, after, final uint64, offset int64) ([]SessionFragment, uint64, int64, error)
}

// HeldSegment is a segment a follower currently holds in its spool.
type HeldSegment struct {
	Scope   string
	Epoch   uint64
	Segment []byte
}

// HTTPTransport is the P0 peer transport.
type HTTPTransport struct {
	Token   string
	Session string
	Client  *http.Client
}

// NewHTTPTransport builds an HTTP transport.
func NewHTTPTransport(token string, client *http.Client) *HTTPTransport {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPTransport{Token: token, Client: client}
}

// WithSession binds replicated fragments to the owner's process session.
func (t *HTTPTransport) WithSession(session string) *HTTPTransport {
	t.Session = session
	return t
}

const SessionProtocolHeader = "X-Cellhive-Session-Protocol"

func (t *HTTPTransport) verifySessionProof(resp *http.Response) error {
	if t.Session != "" && resp.Header.Get(SessionProtocolHeader) != nodelog.ProtoVersionSession {
		return fmt.Errorf("peer session protocol unavailable: missing %s=%s", SessionProtocolHeader, nodelog.ProtoVersionSession)
	}
	return nil
}

// Append sends a segment to the follower at baseURL.
func (t *HTTPTransport) Append(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segment []byte) error {
	u := fmt.Sprintf("%s/v1/peer/append?scope=%s&epoch=%d", trimSlash(baseURL), url.QueryEscape(scope.String()), epoch)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(segment))
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	if t.Session != "" {
		req.Header.Set("x-cellhive-owner-session", t.Session)
	}
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
	return t.verifySessionProof(resp)
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
	if t.Session != "" {
		req.Header.Set("x-cellhive-owner-session", t.Session)
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("peer append_batch %s: status %d: %s", baseURL, resp.StatusCode, string(b))
	}
	return t.verifySessionProof(resp)
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

// Seal closes an owner session on a follower and returns its final sequence.
func (t *HTTPTransport) Seal(ctx context.Context, baseURL, session string) (uint64, error) {
	u := fmt.Sprintf("%s/v1/peer/seal?session=%s", trimSlash(baseURL), url.QueryEscape(session))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	resp, err := t.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return 0, fmt.Errorf("peer seal %s: status %d: %s", baseURL, resp.StatusCode, string(b))
	}
	var out struct {
		Final uint64 `json:"final"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Final, nil
}

// Tail reads sealed fragments after sequence.
func (t *HTTPTransport) Tail(ctx context.Context, baseURL, session string, after uint64) ([]SessionFragment, error) {
	u := fmt.Sprintf("%s/v1/peer/tail?session=%s&after=%d", trimSlash(baseURL), url.QueryEscape(session), after)
	return t.readTail(ctx, u, baseURL)
}

func (t *HTTPTransport) Head(ctx context.Context, baseURL, session string) (uint64, error) {
	u := fmt.Sprintf("%s/v1/peer/head?session=%s", trimSlash(baseURL), url.QueryEscape(session))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	resp, err := t.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("peer head %s: status %d", baseURL, resp.StatusCode)
	}
	var out struct {
		Final uint64 `json:"final"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Final, nil
}

func (t *HTTPTransport) TailOpen(ctx context.Context, baseURL, session, scope string, after, final uint64, offset int64) ([]SessionFragment, uint64, int64, error) {
	u := fmt.Sprintf("%s/v1/peer/tail_open?session=%s&scope=%s&after=%d&final=%d&offset=%d", trimSlash(baseURL), url.QueryEscape(session), url.QueryEscape(scope), after, final, offset)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, after, offset, err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	resp, err := t.Client.Do(req)
	if err != nil {
		return nil, after, offset, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, after, offset, fmt.Errorf("peer live tail %s: status %d", baseURL, resp.StatusCode)
	}
	var wire struct {
		Next      uint64 `json:"next"`
		Offset    int64  `json:"offset"`
		Fragments []struct {
			Sequence uint64   `json:"sequence"`
			Scope    string   `json:"scope"`
			Epoch    uint64   `json:"epoch"`
			Segments []string `json:"segments"`
		} `json:"fragments"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 96<<20)).Decode(&wire); err != nil {
		return nil, after, offset, err
	}
	out := make([]SessionFragment, 0, len(wire.Fragments))
	for _, f := range wire.Fragments {
		fragment := SessionFragment{Sequence: f.Sequence, Scope: f.Scope, Epoch: f.Epoch}
		for _, raw := range f.Segments {
			seg, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				return nil, after, offset, err
			}
			fragment.Segments = append(fragment.Segments, seg)
		}
		out = append(out, fragment)
	}
	return out, wire.Next, wire.Offset, nil
}

func (t *HTTPTransport) readTail(ctx context.Context, u, baseURL string) ([]SessionFragment, error) {
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
		return nil, fmt.Errorf("peer tail %s: status %d: %s", baseURL, resp.StatusCode, string(b))
	}
	var wire struct {
		Fragments []struct {
			Sequence uint64   `json:"sequence"`
			Scope    string   `json:"scope"`
			Epoch    uint64   `json:"epoch"`
			Segments []string `json:"segments"`
		} `json:"fragments"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 96<<20)).Decode(&wire); err != nil {
		return nil, err
	}
	out := make([]SessionFragment, 0, len(wire.Fragments))
	for _, f := range wire.Fragments {
		fragment := SessionFragment{Sequence: f.Sequence, Scope: f.Scope, Epoch: f.Epoch}
		for _, raw := range f.Segments {
			segment, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				return nil, err
			}
			fragment.Segments = append(fragment.Segments, segment)
		}
		out = append(out, fragment)
	}
	return out, nil
}

// Reclaim deletes a sealed follower session after its coverage is durable.
func (t *HTTPTransport) Reclaim(ctx context.Context, baseURL, session string) error {
	u := fmt.Sprintf("%s/v1/peer/session?session=%s", trimSlash(baseURL), url.QueryEscape(session))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("peer reclaim %s: status %d: %s", baseURL, resp.StatusCode, string(b))
	}
	return nil
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
