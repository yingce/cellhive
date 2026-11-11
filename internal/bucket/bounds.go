package bucket

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"time"
)

const (
	// DefaultMaxObjectBytes bounds one whole-object read so a malformed or
	// hostile S3/COS/OSS endpoint cannot force arbitrary allocation. It covers
	// CellHive's own largest creatable object: the R2 multipart ceiling of
	// 512 MiB (direct R2 uploads are capped at 64 MiB). Larger single objects
	// can be admitted per client through S3Options.MaxReadBytes.
	DefaultMaxObjectBytes int64 = 512 << 20

	// Provider HTTP timeouts. There is deliberately no whole-client timeout:
	// large object bodies stream under the caller's context deadline, while
	// connect, TLS and response-header phases cannot stall forever.
	defaultDialTimeout           = 30 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultResponseHeaderTimeout = 30 * time.Second
	defaultIdleConnTimeout       = 90 * time.Second
)

// ErrObjectTooLarge means a response body exceeded the caller's read bound.
var ErrObjectTooLarge = errors.New("bucket: object exceeds read limit")

func newBoundedHTTPTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: defaultDialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       defaultIdleConnTimeout,
		TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		ResponseHeaderTimeout: defaultResponseHeaderTimeout,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

func newBoundedHTTPClient() *http.Client {
	return &http.Client{Transport: debugTransport(newBoundedHTTPTransport(), nil)}
}

func readContentLength(v *int64) int64 {
	if v == nil {
		return -1
	}
	return *v
}

func objectTooLarge(key string, size, max int64) error {
	if size < 0 {
		return fmt.Errorf("%w: key %q exceeds %d bytes", ErrObjectTooLarge, key, max)
	}
	return fmt.Errorf("%w: key %q size %d exceeds %d bytes", ErrObjectTooLarge, key, size, max)
}

func readAllBounded(key string, body io.Reader, contentLength, max int64) ([]byte, error) {
	if max <= 0 {
		max = DefaultMaxObjectBytes
	}
	if contentLength > max {
		return nil, objectTooLarge(key, contentLength, max)
	}
	limit := max
	if limit < math.MaxInt64 {
		limit++
	}
	data, err := io.ReadAll(io.LimitReader(body, limit))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, objectTooLarge(key, int64(len(data)), max)
	}
	return data, nil
}

// readTailBounded keeps only the last n bytes while consuming a possibly full
// response (some providers ignore suffix ranges), so a full-object response
// does not allocate the full object.
func readTailBounded(key string, body io.Reader, n int, contentLength, max int64) ([]byte, int64, error) {
	if max <= 0 {
		max = DefaultMaxObjectBytes
	}
	if contentLength > max {
		return nil, 0, objectTooLarge(key, contentLength, max)
	}
	if n <= 0 {
		n = segProbeBytes
	}
	if int64(n) > max {
		n = int(max)
	}
	limit := max
	if limit < math.MaxInt64 {
		limit++
	}
	src := io.LimitReader(body, limit)
	buf := make([]byte, 32<<10)
	var tail []byte
	var read int64
	for {
		nr, err := src.Read(buf)
		if nr > 0 {
			read += int64(nr)
			if read > max {
				return nil, 0, objectTooLarge(key, read, max)
			}
			tail = append(tail, buf[:nr]...)
			if len(tail) > 2*n {
				copy(tail, tail[len(tail)-n:])
				tail = tail[:n]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, err
		}
	}
	if len(tail) > n {
		tail = tail[len(tail)-n:]
	}
	return tail, read, nil
}
