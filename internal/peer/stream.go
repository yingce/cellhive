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

	"cellhive/internal/cell"
)

const StreamProtocol = "cellhive-peer-v1"

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

type streamPending struct {
	done chan error
}

type rawStream struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	writeMu sync.Mutex
	stateMu sync.Mutex
	pending chan *streamPending
	slots   chan struct{}
	closed  chan struct{}
	failed  bool
	failOne sync.Once
}

func dialRawStream(ctx context.Context, baseURL, token string, pipeline int) (*rawStream, error) {
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
	if pipeline <= 0 {
		pipeline = 4
	}
	s := &rawStream{
		conn:    conn,
		reader:  reader,
		writer:  bufio.NewWriterSize(conn, 64<<10),
		pending: make(chan *streamPending, pipeline),
		slots:   make(chan struct{}, pipeline),
		closed:  make(chan struct{}),
	}
	go s.readAcks()
	return s, nil
}

func (s *rawStream) readAcks() {
	var ack [1]byte
	for {
		if _, err := io.ReadFull(s.reader, ack[:]); err != nil {
			s.fail(fmt.Errorf("peer stream: read ack: %w", err))
			return
		}
		p := <-s.pending
		<-s.slots
		if ack[0] == 1 {
			p.done <- nil
		} else {
			p.done <- fmt.Errorf("peer stream: follower rejected batch")
		}
	}
}

// submit acquires a pipeline slot, registers the pending ack, and writes the
// frame. The frame is on the wire (ordered by writeMu) when submit returns, so
// callers that issue frames sequentially preserve lane order while resolving
// acks asynchronously.
func (s *rawStream) submit(ctx context.Context, frame []byte) (*streamPending, error) {
	select {
	case s.slots <- struct{}{}:
	case <-s.closed:
		return nil, fmt.Errorf("peer stream: closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p := &streamPending{done: make(chan error, 1)}
	s.stateMu.Lock()
	if s.failed {
		s.stateMu.Unlock()
		<-s.slots
		return nil, fmt.Errorf("peer stream: closed")
	}
	s.pending <- p
	s.stateMu.Unlock()

	s.writeMu.Lock()
	_, err := s.writer.Write(frame)
	if err == nil {
		err = s.writer.Flush()
	}
	s.writeMu.Unlock()
	if err != nil {
		s.fail(err)
	}
	return p, nil
}

func (s *rawStream) send(ctx context.Context, frame []byte) error {
	p, err := s.submit(ctx, frame)
	if err != nil {
		return err
	}
	select {
	case err := <-p.done:
		return err
	case <-s.closed:
		select {
		case err := <-p.done:
			return err
		default:
			return fmt.Errorf("peer stream: closed")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendAsync writes the frame and returns a channel that receives the ack (or
// error) later. Pipelining the caller over one lane is safe because this lane
// is a single reader on the follower and applies frames in arrival order.
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
		case <-s.closed:
			select {
			case err := <-p.done:
				out <- err
			default:
				out <- fmt.Errorf("peer stream: closed")
			}
		}
	}()
	return out
}

func (s *rawStream) fail(err error) {
	s.failOne.Do(func() {
		s.stateMu.Lock()
		s.failed = true
		close(s.closed)
		s.conn.Close()
		for {
			select {
			case p := <-s.pending:
				select {
				case <-s.slots:
				default:
				}
				p.done <- err
			default:
				s.stateMu.Unlock()
				return
			}
		}
	})
}

func (s *rawStream) isFailed() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.failed
}

// StreamTransport keeps several persistent, scope-hashed lanes per follower.
// Each lane preserves FIFO order and pipelines up to pipeline batches.
type StreamTransport struct {
	token    string
	lanes    int
	pipeline int
	fallback *HTTPTransport
	mu       sync.Mutex
	streams  map[string]*rawStream
}

func NewStreamTransport(token string, lanes, pipeline int) *StreamTransport {
	if lanes <= 0 {
		lanes = 4
	}
	if pipeline <= 0 {
		pipeline = 4
	}
	return &StreamTransport{
		token:    token,
		lanes:    lanes,
		pipeline: pipeline,
		fallback: NewHTTPTransport(token, nil),
		streams:  make(map[string]*rawStream),
	}
}

func (t *StreamTransport) stream(ctx context.Context, baseURL string, scope cell.Scope) (*rawStream, error) {
	lane := shardIndex(scope.String(), t.lanes)
	key := fmt.Sprintf("%s#%d", baseURL, lane)
	t.mu.Lock()
	defer t.mu.Unlock()
	if s := t.streams[key]; s != nil && !s.isFailed() {
		return s, nil
	}
	s, err := dialRawStream(ctx, baseURL, t.token, t.pipeline)
	if err != nil {
		return nil, err
	}
	t.streams[key] = s
	return s, nil
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
		// No stream was established, so no write can have reached the follower.
		return t.fallback.AppendBatch(ctx, baseURL, scope, epoch, segments)
	}
	// Do not retry after a stream write: if the ack was lost the outcome is
	// uncertain. The caller safely falls back to bucket durability instead.
	return s.send(ctx, encodeBatchFrame(scope, epoch, segments))
}

// AppendBatchAsync pipelines a batch on the follower's lane. The frame is
// written before returning, so sequential calls preserve order; the follower
// ack arrives on the returned channel.
func (t *StreamTransport) AppendBatchAsync(ctx context.Context, baseURL string, scope cell.Scope, epoch uint64, segments [][]byte) (<-chan error, error) {
	s, err := t.stream(ctx, baseURL, scope)
	if err != nil {
		// Fall back to a one-shot HTTP batch (its own connection) when the
		// stream is unavailable.
		out := make(chan error, 1)
		go func() { out <- t.fallback.AppendBatch(ctx, baseURL, scope, epoch, segments) }()
		return out, nil
	}
	return s.sendAsync(ctx, encodeBatchFrame(scope, epoch, segments)), nil
}

func (t *StreamTransport) Held(ctx context.Context, baseURL string) ([]HeldSegment, error) {
	return t.fallback.Held(ctx, baseURL)
}
