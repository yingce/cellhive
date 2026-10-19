package sqlcapture

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/cell"
)

// HTTPCommitter submits captured LTX segments to a cell-agent owner and requires
// an actual RPO=0 durability proof: a fleet acknowledgement or a waited bucket
// commit. Async bucket acks (RPO>0) are rejected.
type HTTPCommitter struct {
	owner     string
	token     string
	followers []string
	client    *http.Client
	// RequireBucket waits for the authoritative bucket before ack. DO cold
	// restore reads bucket objects, so a fleet-only proof is insufficient.
	RequireBucket bool
	// RequireDOOwner keeps administrative whole-directory captures on bucket
	// proof when no claimed shard identity is attached to the gate context.
	RequireDOOwner bool
}

type doOwnerContextKey struct{}

type DOOwnerIdentity struct {
	Scope cell.Scope
	Node  string
	Epoch uint64
}

// WithDOOwner attaches the shard lease proven by the do-runtime router to one
// supervisor output gate. The cell-agent checks it before fleet confirmation.
func WithDOOwner(ctx context.Context, owner DOOwnerIdentity) context.Context {
	return context.WithValue(ctx, doOwnerContextKey{}, owner)
}

func NewHTTPCommitter(owner, token string, followers []string, client *http.Client) *HTTPCommitter {
	if client == nil {
		client = &http.Client{
			Transport: &http.Transport{MaxIdleConns: 128, MaxIdleConnsPerHost: 128},
			Timeout:   30 * time.Second,
		}
	}
	return &HTTPCommitter{
		owner: strings.TrimRight(owner, "/"), token: token,
		followers: append([]string(nil), followers...), client: client,
	}
}

func (c *HTTPCommitter) Claim(ctx context.Context, scope cell.Scope) error {
	body, _ := json.Marshal(map[string]string{"scope": scope.String()})
	status, response, err := c.post(ctx, "/v1/internal/claim", body)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusConflict {
		return fmt.Errorf("sqlcapture: claim status %d: %s", status, response)
	}
	return nil
}

// AsyncCommitter is an optional Committer extension: CommitAsync writes the
// commit request and returns a channel resolved when the fleet proof completes.
// Concurrent calls may be in flight; the caller is responsible for ordering by
// delivering them in txid order.
type AsyncCommitter interface {
	CommitAsync(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte, base uint64) <-chan error
}

// CommitAsync submits a pipelined commit and resolves the channel on ack. base
// is the first txid of this pipeline epoch; the owner initializes the scope's
// ordering baseline from it on the first request.
func (c *HTTPCommitter) CommitAsync(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte, base uint64) <-chan error {
	out := make(chan error, 1)
	go func() { out <- c.commit(ctx, scope, epoch, segment, true, base) }()
	return out
}

func (c *HTTPCommitter) Commit(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte) error {
	return c.commit(ctx, scope, epoch, segment, false, 0)
}

func (c *HTTPCommitter) commit(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte, pipelined bool, base uint64) error {
	path := fmt.Sprintf("/v1/internal/commit_binary?scope=%s&epoch=%d", url.QueryEscape(scope.String()), epoch)
	if pipelined {
		path += "&pipelined=1&base=" + strconv.FormatUint(base, 10)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.owner+path, bytes.NewReader(segment))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/octet-stream")
	req.Header.Set("x-cellhive-internal-token", c.token)
	req.Header.Set("x-cellhive-followers", strings.Join(c.followers, ","))
	doOwner, hasDOOwner := ctx.Value(doOwnerContextKey{}).(DOOwnerIdentity)
	if hasDOOwner && doOwner.Epoch != 0 {
		req.Header.Set("x-cellhive-do-owner-scope", doOwner.Scope.String())
		req.Header.Set("x-cellhive-do-owner-node", doOwner.Node)
		req.Header.Set("x-cellhive-do-owner-epoch", strconv.FormatUint(doOwner.Epoch, 10))
	}
	if c.RequireBucket || (c.RequireDOOwner && (!hasDOOwner || doOwner.Epoch == 0)) {
		req.Header.Set("x-cellhive-bucket-proof", "1")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	status := resp.StatusCode
	if status != http.StatusOK {
		return fmt.Errorf("sqlcapture: commit status %d: %s", status, response)
	}
	var result struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return fmt.Errorf("sqlcapture: decode commit response: %w", err)
	}
	switch result.Mode {
	case "fleet", "bucket", "bucket-batch":
		// All three are RPO=0 proofs: fleet = follower fsync ack; bucket = the
		// object store acked the write; bucket-batch = the group-commit block was
		// acked. Only async uploads are not durable.
		return nil
	case "bucket-async":
		return fmt.Errorf("sqlcapture: durability not proven (mode %q uploads in background, RPO>0)", result.Mode)
	default:
		return fmt.Errorf("sqlcapture: unexpected commit proof %q", result.Mode)
	}
}

func (c *HTTPCommitter) post(ctx context.Context, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.owner+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, response, err
}
