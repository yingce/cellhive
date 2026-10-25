package peer

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"cellhive/internal/cell"
)

const StreamProtocol = "cellhive-peer-v2"

func encodeBatchFrame(scope cell.Scope, epoch uint64, segments [][]byte) []byte {
	scopeText := scope.String()
	segmentsData := EncodeFrames(segments)
	total := 2 + len(scopeText) + 8 + len(segmentsData)
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf, uint32(total))
	binary.BigEndian.PutUint16(buf[4:], uint16(len(scopeText)))
	copy(buf[6:], scopeText)
	off := 6 + len(scopeText)
	binary.BigEndian.PutUint64(buf[off:], epoch)
	copy(buf[off+8:], segmentsData)
	return buf
}

// DecodeBatchFrame reads one length-prefixed stream batch.
func DecodeBatchFrame(r io.Reader) (string, uint64, [][]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return "", 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(size[:]))
	if n < 10 || n > 64<<20 {
		return "", 0, nil, fmt.Errorf("peer stream: invalid batch size %d", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return "", 0, nil, err
	}
	scopeLen := int(binary.BigEndian.Uint16(payload))
	if scopeLen+10 > len(payload) {
		return "", 0, nil, fmt.Errorf("peer stream: invalid scope length %d", scopeLen)
	}
	scopeText := string(payload[2 : 2+scopeLen])
	epoch := binary.BigEndian.Uint64(payload[2+scopeLen:])
	segments := DecodeFrames(payload[2+scopeLen+8:])
	if len(segments) == 0 {
		return "", 0, nil, fmt.Errorf("peer stream: empty batch")
	}
	if len(EncodeFrames(segments)) != len(payload[2+scopeLen+8:]) {
		return "", 0, nil, fmt.Errorf("peer stream: malformed segment frames")
	}
	return scopeText, epoch, segments, nil
}

// BatchFrame is one decoded persistent-stream append.
type BatchFrame struct {
	Scope    cell.Scope
	Epoch    uint64
	Segments [][]byte
}

// DecodeBatchFrames blocks for one frame, then drains only complete frames
// already buffered by the connection. It never waits for another frame solely
// to improve batching and never consumes a partial buffered frame. maxBytes
// bounds the encoded group size; the blocking first frame is always accepted.
func DecodeBatchFrames(r *bufio.Reader, maxFrames, maxBytes int) ([]BatchFrame, error) {
	if maxFrames <= 0 {
		maxFrames = 1
	}
	decode := func() (BatchFrame, error) {
		scopeText, epoch, segments, err := DecodeBatchFrame(r)
		if err != nil {
			return BatchFrame{}, err
		}
		scope, err := cell.ParseScope(scopeText)
		if err != nil {
			return BatchFrame{}, err
		}
		return BatchFrame{Scope: scope, Epoch: epoch, Segments: segments}, nil
	}
	header, err := r.Peek(4)
	if err != nil {
		return nil, err
	}
	firstBytes := 4 + int(binary.BigEndian.Uint32(header))
	first, err := decode()
	if err != nil {
		return nil, err
	}
	frames := []BatchFrame{first}
	groupBytes := firstBytes
	for len(frames) < maxFrames && r.Buffered() >= 4 {
		header, err := r.Peek(4)
		if err != nil {
			break
		}
		n := int(binary.BigEndian.Uint32(header))
		if n < 10 || n > 64<<20 {
			return nil, fmt.Errorf("peer stream: invalid batch size %d", n)
		}
		frameBytes := 4 + n
		if r.Buffered() < frameBytes || maxBytes > 0 && groupBytes+frameBytes > maxBytes {
			break
		}
		frame, err := decode()
		if err != nil {
			return nil, err
		}
		frames = append(frames, frame)
		groupBytes += frameBytes
	}
	return frames, nil
}

// streamIOTimeout bounds each handshake, frame write and outstanding ACK.
// It is deliberately independent of request cancellation on shared lanes.
const streamIOTimeout = 5 * time.Second

type streamPending struct {
	done      chan error
	timer     *time.Timer
	completed bool // protected by rawStream.stateMu
}

type rawStream struct {
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	writeGate  chan struct{}
	stateMu    sync.Mutex
	pending    []*streamPending
	slots      chan struct{}
	closed     chan struct{}
	failed     bool
	ackTimeout time.Duration
}

func dialRawStream(ctx context.Context, baseURL, token, session string, pipeline int, identities ...cell.PeerIdentity) (*rawStream, error) {
	ctx, cancel := context.WithTimeout(ctx, streamIOTimeout)
	defer cancel()
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" {
		return nil, fmt.Errorf("peer stream: only http is supported, got %q", u.Scheme)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}
	// DialContext only bounds TCP establishment, not the HTTP upgrade.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return nil, err
	}
	path := strings.TrimSuffix(u.Path, "/") + "/v1/peer/stream"
	req := &http.Request{
		Method: http.MethodPost,
		URL:    &url.URL{Path: path},
		Host:   u.Host,
		Header: http.Header{
			"Connection":                []string{"Upgrade"},
			"Upgrade":                   []string{StreamProtocol},
			"X-Cellhive-Internal-Token": []string{token},
		},
	}
	if session != "" {
		req.Header.Set("X-Cellhive-Owner-Session", session)
	}
	var id cell.PeerIdentity
	if len(identities) > 0 {
		id = identities[0]
	}
	setIdentity(req.Header, id)
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols || !strings.EqualFold(resp.Header.Get("Upgrade"), StreamProtocol) {
		resp.Body.Close()
		conn.Close()
		return nil, fmt.Errorf("peer stream: upgrade returned %s", resp.Status)
	}
	if err := verifyIdentity(resp.Header, id); err != nil {
		resp.Body.Close()
		conn.Close()
		return nil, err
	}
	if !stop() || ctx.Err() != nil {
		conn.Close()
		return nil, ctx.Err()
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, err
	}
	if pipeline <= 0 {
		pipeline = 4
	}
	s := &rawStream{
		conn:       conn,
		reader:     reader,
		writer:     bufio.NewWriterSize(conn, 64<<10),
		writeGate:  make(chan struct{}, 1),
		ackTimeout: streamIOTimeout,
		slots:      make(chan struct{}, pipeline),
		closed:     make(chan struct{}),
	}
	go s.readAcks()
	return s, nil
}

// finishLocked owns both completion and slot release, including timer/ACK races.
func (s *rawStream) finishLocked(p *streamPending, err error) {
	if p.completed {
		return
	}
	p.completed = true
	if p.timer != nil {
		p.timer.Stop()
	}
	<-s.slots
	p.done <- err
}

func (s *rawStream) failLocked(err error) {
	if s.failed {
		return
	}
	s.failed = true
	close(s.closed)
	s.conn.Close()
	for _, p := range s.pending {
		s.finishLocked(p, err)
	}
	s.pending = nil
}

func (s *rawStream) readAcks() {
	var ack [1]byte
	for {
		if _, err := io.ReadFull(s.reader, ack[:]); err != nil {
			s.fail(fmt.Errorf("peer stream: read ack: %w", err))
			return
		}
		s.stateMu.Lock()
		if s.failed {
			s.stateMu.Unlock()
			return
		}
		if len(s.pending) == 0 {
			s.failLocked(fmt.Errorf("peer stream: unsolicited ack"))
			s.stateMu.Unlock()
			return
		}
		p := s.pending[0]
		s.pending[0] = nil
		s.pending = s.pending[1:]
		var err error
		if ack[0] != 1 {
			err = fmt.Errorf("peer stream: follower rejected batch")
		}
		s.finishLocked(p, err)
		s.stateMu.Unlock()
	}
}

// Gate acquisition is cancellable. Pending ACK order and wire order are both
// registered inside this gate, so concurrent senders cannot swap their ACKs.
func (s *rawStream) submit(ctx context.Context, frame []byte) (*streamPending, error) {
	select {
	case s.slots <- struct{}{}:
	case <-s.closed:
		return nil, fmt.Errorf("peer stream: closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case s.writeGate <- struct{}{}:
	case <-s.closed:
		<-s.slots
		return nil, fmt.Errorf("peer stream: closed")
	case <-ctx.Done():
		<-s.slots
		return nil, ctx.Err()
	}
	defer func() { <-s.writeGate }()
	if err := ctx.Err(); err != nil {
		<-s.slots
		return nil, err
	}
	s.stateMu.Lock()
	if s.failed {
		s.stateMu.Unlock()
		<-s.slots
		return nil, fmt.Errorf("peer stream: closed")
	}
	p := &streamPending{done: make(chan error, 1)}
	s.pending = append(s.pending, p)
	timeout := s.ackTimeout
	if timeout <= 0 {
		timeout = streamIOTimeout
	}
	p.timer = time.AfterFunc(timeout, func() {
		s.stateMu.Lock()
		defer s.stateMu.Unlock()
		if !p.completed {
			s.failLocked(fmt.Errorf("peer stream: ack timeout"))
		}
	})
	s.stateMu.Unlock()

	// Cancellation after a possible partial write retires the entire stream:
	// continuing would lose frame boundaries and ACK correspondence.
	writeCtx, cancel := context.WithTimeout(ctx, streamIOTimeout)
	defer cancel()
	stop := context.AfterFunc(writeCtx, func() { s.fail(writeCtx.Err()) })
	deadline, _ := writeCtx.Deadline()
	err := s.conn.SetWriteDeadline(deadline)
	if err == nil {
		_, err = s.writer.Write(frame)
	}
	if err == nil {
		err = s.writer.Flush()
	}
	stopped := stop()
	if err == nil && (!stopped || writeCtx.Err() != nil) {
		err = writeCtx.Err()
	}
	if err == nil {
		err = s.conn.SetWriteDeadline(time.Time{})
	}
	if err != nil {
		s.fail(err)
	}
	return p, err
}

func (s *rawStream) send(ctx context.Context, frame []byte) error {
	p, err := s.submit(ctx, frame)
	if err != nil {
		return err
	}
	select {
	case err := <-p.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *rawStream) sendAsync(ctx context.Context, frame []byte) <-chan error {
	out := make(chan error, 1)
	p, err := s.submit(ctx, frame)
	if err != nil {
		out <- err
		return out
	}
	go func() {
		select {
		case err := <-p.done:
			out <- err
		case <-ctx.Done():
			out <- ctx.Err()
		}
	}()
	return out
}

func (s *rawStream) fail(err error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.failLocked(err)
}

func (s *rawStream) isFailed() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.failed
}

// StreamTransport keeps several persistent, scope-hashed lanes per follower.
// Each lane preserves FIFO order and pipelines up to pipeline batches.
type StreamTransport struct {
	Targets  *Targets
	token    string
	session  string
	lanes    int
	pipeline int
	fallback *HTTPTransport
	mu       sync.Mutex
	streams  map[string]*rawStream
	dialing  map[string]*streamDial
	closed   bool
}

func (t *StreamTransport) Head(ctx context.Context, baseURL, session string) (uint64, error) {
	return t.fallback.Head(ctx, baseURL, session)
}

func (t *StreamTransport) TailOpen(ctx context.Context, baseURL, session, scope string, after, final uint64, offset int64) ([]SessionFragment, uint64, int64, error) {
	return t.fallback.TailOpen(ctx, baseURL, session, scope, after, final, offset)
}

func NewStreamTransport(token string, lanes, pipeline int) *StreamTransport {
	return NewSessionStreamTransport(token, "", lanes, pipeline)
}

func NewSessionStreamTransport(token, session string, lanes, pipeline int) *StreamTransport {
	if lanes <= 0 {
		lanes = 4
	}
	if pipeline <= 0 {
		pipeline = 4
	}
	fallback := NewHTTPTransport(token, nil).WithSession(session)
	return &StreamTransport{
		token:    token,
		lanes:    lanes,
		session:  session,
		pipeline: pipeline,
		fallback: fallback,
		streams:  make(map[string]*rawStream),
	}
}

type streamDial struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error // published by closing done
}

func (t *StreamTransport) stream(ctx context.Context, baseURL string, scope cell.Scope) (*rawStream, error) {
	key := fmt.Sprintf("%s#%d", baseURL, shardIndex(scope.String(), t.lanes))
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return nil, fmt.Errorf("peer stream transport closed")
		}
		if s := t.streams[key]; s != nil && !s.isFailed() {
			t.mu.Unlock()
			return s, nil
		}
		if d := t.dialing[key]; d != nil {
			t.mu.Unlock()
			select {
			case <-d.done:
				if d.err != nil {
					return nil, d.err
				}
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		dialCtx, cancel := context.WithCancel(ctx)
		d := &streamDial{done: make(chan struct{}), cancel: cancel}
		if t.dialing == nil {
			t.dialing = make(map[string]*streamDial)
		}
		t.dialing[key] = d
		t.mu.Unlock()
		s, err := dialRawStream(dialCtx, baseURL, t.token, t.session, t.pipeline, targetIdentity(ctx, t.Targets, baseURL))
		cancel()
		t.mu.Lock()
		delete(t.dialing, key)
		if t.closed && s != nil {
			s.fail(fmt.Errorf("peer stream transport closed"))
			s = nil
			err = fmt.Errorf("peer stream transport closed")
		}
		if err == nil {
			t.streams[key] = s
		}
		d.err = err
		close(d.done)
		t.mu.Unlock()
		return s, err
	}
}

// AsyncTransport is an optional Transport extension: AppendBatchAsync writes
// the frame in call order and resolves the follower ack through the returned
// channel, which lets a single lane pipeline several batches. Implementations
// must preserve lane order across sequential calls.
type AsyncTransport interface {
	AppendBatchAsync(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) (<-chan error, error)
}

func (t *StreamTransport) Append(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segment []byte) error {
	return t.AppendBatch(ctx, baseURL, scope, epoch, [][]byte{segment})
}

func (t *StreamTransport) AppendBatch(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) error {
	s, err := t.stream(ctx, baseURL, scope)
	if err != nil {
		if t.session != "" {
			return fmt.Errorf("peer stream: session protocol unavailable: %w", err)
		}
		return t.fallback.AppendBatch(ctx, baseURL, scope, epoch, segments)
	}
	// Do not retry after a stream write: if the ack was lost the outcome is
	// uncertain. The caller safely falls back to bucket durability instead.
	return s.send(ctx, encodeBatchFrame(scope, epoch, segments))
}

func (t *StreamTransport) AppendBatchAsync(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) (<-chan error, error) {
	s, err := t.stream(ctx, baseURL, scope)
	if err != nil {
		out := make(chan error, 1)
		if t.session != "" {
			out <- fmt.Errorf("peer stream: session protocol unavailable: %w", err)
			return out, nil
		}
		go func() { out <- t.fallback.AppendBatch(ctx, baseURL, scope, epoch, segments) }()
		return out, nil
	}
	return s.sendAsync(ctx, encodeBatchFrame(scope, epoch, segments)), nil
}

func (t *StreamTransport) Held(ctx context.Context, baseURL string) ([]HeldSegment, error) {
	return t.fallback.Held(ctx, baseURL)
}

func (t *StreamTransport) Seal(ctx context.Context, baseURL, session string) (uint64, error) {
	return t.fallback.Seal(ctx, baseURL, session)
}

func (t *StreamTransport) Tail(ctx context.Context, baseURL, session string, after uint64) ([]SessionFragment, error) {
	return t.fallback.Tail(ctx, baseURL, session, after)
}

func (t *StreamTransport) Reclaim(ctx context.Context, baseURL, session string) error {
	return t.fallback.Reclaim(ctx, baseURL, session)
}

// Close releases persistent lanes on shutdown; hijacked connections are not
// closed by the HTTP server's Shutdown method.
func (t *StreamTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for _, d := range t.dialing {
		d.cancel()
	}
	for key, s := range t.streams {
		s.fail(fmt.Errorf("peer stream transport closed"))
		delete(t.streams, key)
	}
}
