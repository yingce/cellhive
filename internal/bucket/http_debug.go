package bucket

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
)

var storageRequestSequence atomic.Uint64

type storageDebugTransport struct {
	base http.RoundTripper
	log  *slog.Logger
}

func debugTransport(base http.RoundTripper, log *slog.Logger) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if existing, ok := base.(*storageDebugTransport); ok {
		if log == nil {
			return base
		}
		base = existing.base
	}
	return &storageDebugTransport{base: base, log: log}
}
func (t *storageDebugTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}
func storageError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var n net.Error
	if errors.As(err, &n) && n.Timeout() {
		return "timeout"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return "syscall: " + errno.Error()
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns_error"
	}
	// Error strings can contain signed URLs, headers or provider response bodies.
	return "transport_or_body_error"
}
func storageOperation(r *http.Request) string {
	if op := awsmiddleware.GetOperationName(r.Context()); op != "" {
		return op
	}
	q := r.URL.Query()
	switch {
	case q.Has("append"):
		return "AppendObject"
	case q.Has("uploads"):
		if r.Method == "POST" {
			return "CreateMultipartUpload"
		}
		return "ListMultipartUploads"
	case q.Has("uploadId"):
		switch r.Method {
		case "PUT":
			return "UploadPart"
		case "POST":
			return "CompleteMultipartUpload"
		case "DELETE":
			return "AbortMultipartUpload"
		default:
			return "ListParts"
		}
	case q.Has("list-type") || q.Has("prefix"):
		return "ListObjects"
	}
	switch r.Method {
	case "GET":
		return "GetObject"
	case "HEAD":
		return "HeadObject"
	case "PUT":
		return "PutObject"
	case "DELETE":
		return "DeleteObject"
	}
	return r.Method
}
func (t *storageDebugTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	log := t.log
	if log == nil {
		log = slog.Default()
	}
	if !log.Enabled(r.Context(), slog.LevelDebug) {
		return t.base.RoundTrip(r)
	}
	start := time.Now()
	id := storageRequestSequence.Add(1)
	attrs := []any{"storage_request", id, "operation", storageOperation(r), "method", r.Method, "host", r.URL.Hostname(), "path", r.URL.EscapedPath(), "range", r.Header.Get("Range"), "content_length", r.ContentLength}
	// Only numeric protocol fields are allowed; never log raw query/signature.
	for _, key := range []string{"position", "partNumber"} {
		if n, err := strconv.ParseInt(r.URL.Query().Get(key), 10, 64); err == nil {
			attrs = append(attrs, key, n)
		}
	}
	if retry := r.Header.Get("X-Cos-Sdk-Retry"); retry == "true" {
		attrs = append(attrs, "sdk_retry", true)
	}
	// AWS emits "attempt=N; max=M". Parse rather than logging arbitrary headers.
	for _, field := range strings.Split(r.Header.Get("Amz-Sdk-Request"), ";") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(field), "attempt="); ok {
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				attrs = append(attrs, "sdk_attempt", n)
			}
		}
	}

	if deadline, ok := r.Context().Deadline(); ok {
		attrs = append(attrs, "deadline_remaining_ms", float64(time.Until(deadline))/float64(time.Millisecond))
	}
	log.DebugContext(r.Context(), "object storage request started", attrs...)
	var mu sync.Mutex
	var dnsAt, connectAt, tlsAt time.Time
	var dnsMS, connectMS, tlsMS, firstMS float64
	var reused bool
	var remote string
	trace := &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { mu.Lock(); dnsAt = time.Now(); mu.Unlock() },
		DNSDone: func(httptrace.DNSDoneInfo) {
			mu.Lock()
			dnsMS += float64(time.Since(dnsAt)) / float64(time.Millisecond)
			mu.Unlock()
		},
		ConnectStart: func(_, _ string) { mu.Lock(); connectAt = time.Now(); mu.Unlock() },
		ConnectDone: func(_, _ string, _ error) {
			mu.Lock()
			connectMS += float64(time.Since(connectAt)) / float64(time.Millisecond)
			mu.Unlock()
		},
		TLSHandshakeStart: func() { mu.Lock(); tlsAt = time.Now(); mu.Unlock() },
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			mu.Lock()
			tlsMS += float64(time.Since(tlsAt)) / float64(time.Millisecond)
			mu.Unlock()
		},
		GotConn: func(i httptrace.GotConnInfo) {
			mu.Lock()
			reused = i.Reused
			if i.Conn != nil {
				remote = i.Conn.RemoteAddr().String()
			}
			mu.Unlock()
		},
		GotFirstResponseByte: func() { mu.Lock(); firstMS = float64(time.Since(start)) / float64(time.Millisecond); mu.Unlock() },
	}
	request := r.Clone(httptrace.WithClientTrace(r.Context(), trace))
	var sent atomic.Int64
	if r.Body != nil {
		request.Body = &countedUpload{ReadCloser: r.Body, n: &sent}
	}
	response, err := t.base.RoundTrip(request)
	mu.Lock()
	timings := []any{"dns_ms", dnsMS, "connect_ms", connectMS, "tls_ms", tlsMS, "first_byte_ms", firstMS, "connection_reused", reused, "remote", remote}
	mu.Unlock()
	details := append(append([]any{}, attrs...), timings...)
	if response != nil {
		details = append(details, "status", response.StatusCode, "response_content_length", response.ContentLength, "content_range", response.Header.Get("Content-Range"))
		for _, h := range []string{"X-Cos-Request-Id", "X-Amz-Request-Id", "X-Oss-Request-Id"} {
			if v := response.Header.Get(h); v != "" {
				details = append(details, h, v)
			}
		}
	}
	if err != nil {
		log.DebugContext(r.Context(), "object storage request failed", append(details, "elapsed_ms", float64(time.Since(start))/float64(time.Millisecond), "uploaded_bytes", sent.Load(), "error_kind", storageError(err))...)
		return response, err
	}
	log.DebugContext(r.Context(), "object storage response headers", append(details, "headers_ms", float64(time.Since(start))/float64(time.Millisecond))...)
	if response != nil && response.Body != nil {
		response.Body = &debugResponseBody{ReadCloser: response.Body, finish: func(n int64, end string, e error) {
			log.DebugContext(r.Context(), "object storage request completed", append(details, "elapsed_ms", float64(time.Since(start))/float64(time.Millisecond), "uploaded_bytes", sent.Load(), "downloaded_bytes", n, "body_end", end, "error_kind", storageError(e))...)
		}}
	}
	return response, err
}

type countedUpload struct {
	io.ReadCloser
	n *atomic.Int64
}

func (b *countedUpload) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	b.n.Add(int64(n))
	return n, e
}

type debugResponseBody struct {
	io.ReadCloser
	n      atomic.Int64
	once   sync.Once
	finish func(int64, string, error)
}

func (b *debugResponseBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	b.n.Add(int64(n))
	if e != nil {
		end := "read_error"
		if e == io.EOF {
			end = "eof"
			e = nil
		}
		b.once.Do(func() { b.finish(b.n.Load(), end, e) })
		if end == "eof" {
			e = io.EOF
		}
	}
	return n, e
}
func (b *debugResponseBody) Close() error {
	e := b.ReadCloser.Close()
	b.once.Do(func() { b.finish(b.n.Load(), "closed", e) })
	return e
}
