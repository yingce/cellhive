package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
)

// Error contains only controlled diagnostic strings, never response messages or
// raw transport errors (which may embed credentials and URL query parameters).
type Error struct {
	StatusCode                      int
	Code, Phase, Host, Path, Worker string
	ErrorName, ErrorFrame           string
	cause                           error
}

func (e *Error) Error() string {
	return fmt.Sprintf("dispatch: %s (%s), status %d", e.Code, e.Phase, e.StatusCode)
}

func (e *Error) Unwrap() error { return e.cause }

func (e *Error) DiagnosticAttrs() []slog.Attr {
	a := []slog.Attr{slog.String("error_code", e.Code), slog.String("phase", e.Phase), slog.String("target_host", e.Host), slog.String("target_path", e.Path)}
	if e.StatusCode != 0 {
		a = append(a, slog.Int("http_status", e.StatusCode))
	}
	if e.Worker != "" {
		a = append(a, slog.String("worker", e.Worker))
	}
	if e.ErrorName != "" {
		a = append(a, slog.String("error_name", e.ErrorName))
	}
	if e.ErrorFrame != "" {
		a = append(a, slog.String("error_frame", e.ErrorFrame))
	}
	return a
}

// route is a fixed endpoint name, not the potentially sensitive configured path.
func dispatchFailure(rawURL, route, worker, phase string, cause error, resp *http.Response) error {
	e := &Error{Code: "dispatch_error", Phase: phase, Path: route, Worker: worker, cause: cause}
	if u, err := url.Parse(rawURL); err == nil {
		e.Host = u.Host
	}
	if cause != nil {
		var ne net.Error
		var dns *net.DNSError
		var op *net.OpError
		switch {
		case errors.Is(cause, context.Canceled):
			e.Code = "canceled"
		case errors.Is(cause, context.DeadlineExceeded):
			e.Code = "timeout"
		case errors.As(cause, &ne) && ne.Timeout():
			e.Code = "timeout"
		default:
			e.Code = "transport_error"
		}
		if errors.As(cause, &dns) {
			e.Phase = "dns"
		} else if errors.As(cause, &op) {
			switch op.Op {
			case "dial", "read", "write":
				e.Phase = op.Op
			}
		}
	}
	if resp != nil {
		e.StatusCode, e.Code, e.Phase = resp.StatusCode, "http_error", "response"
		// A bounded read also drains small errors for connection reuse. Large or
		// malformed bodies are not drained further; the caller always closes.
		b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var body struct {
			Error      string `json:"error"`
			Code       string `json:"code"`
			ErrorName  string `json:"error_name"`
			ErrorFrame string `json:"error_frame"`
		}
		if err == nil && json.Unmarshal(b, &body) == nil {
			code := body.Error
			if code == "" {
				code = body.Code
			}
			switch code {
			case "handler_failed", "workflow_failed", "bundle_fetch_failed", "unauthorized", "bad_json", "bad_request", "dedicated_target_mismatch":
				e.Code = code
				if code == "handler_failed" {
					e.ErrorName = allowlistedErrorName(body.ErrorName)
					e.ErrorFrame = allowlistedErrorFrame(body.ErrorFrame)
				}
			}
		}
	}
	return e
}

var (
	errorNamePattern  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,63}$`)
	errorFramePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,80}:[1-9][0-9]{0,5}:[1-9][0-9]{0,5}$`)
)

func allowlistedErrorName(name string) string {
	if errorNamePattern.MatchString(name) {
		return name
	}
	return ""
}

func allowlistedErrorFrame(frame string) string {
	if errorFramePattern.MatchString(frame) {
		return frame
	}
	return ""
}
