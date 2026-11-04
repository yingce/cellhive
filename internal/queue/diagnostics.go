package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
)

const dispatchErrorBodyLimit = 4096

const queueDispatchPath = "/v1/queues/dispatch"

var dispatchErrorCodes = map[string]struct{}{
	"handler_failed":            {},
	"workflow_failed":           {},
	"bundle_fetch_failed":       {},
	"unauthorized":              {},
	"bad_json":                  {},
	"bad_request":               {},
	"dedicated_target_mismatch": {},
}

type DispatchError struct {
	StatusCode int
	Code       string
	Phase      string
	Host       string
	Path       string
	Worker     string
	ErrorName  string
	ErrorFrame string
}

func (e *DispatchError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("queue dispatch: %s (%s), status %d", e.Code, e.Phase, e.StatusCode)
	}
	return fmt.Sprintf("queue dispatch: %s (%s)", e.Code, e.Phase)
}

func (e *DispatchError) DiagnosticAttrs() []slog.Attr {
	attrs := []slog.Attr{
		slog.String("error_code", e.Code),
		slog.String("phase", e.Phase),
		slog.String("target_host", e.Host),
		slog.String("target_path", e.Path),
	}
	if e.StatusCode != 0 {
		attrs = append(attrs, slog.Int("http_status", e.StatusCode))
	}
	if e.Worker != "" {
		attrs = append(attrs, slog.String("worker", e.Worker))
	}
	if e.ErrorName != "" {
		attrs = append(attrs, slog.String("error_name", e.ErrorName))
	}
	if e.ErrorFrame != "" {
		attrs = append(attrs, slog.String("error_frame", e.ErrorFrame))
	}
	return attrs
}

func dispatchFailureAttrs(err error, worker string) []slog.Attr {
	var diagnostic interface{ DiagnosticAttrs() []slog.Attr }
	if errors.As(err, &diagnostic) {
		return diagnostic.DiagnosticAttrs()
	}
	code := "dispatch_error"
	if errors.Is(err, context.Canceled) {
		code = "canceled"
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = "timeout"
	}
	attrs := []slog.Attr{slog.String("error_code", code), slog.String("phase", "dispatch")}
	if worker != "" {
		attrs = append(attrs, slog.String("worker", worker))
	}
	return attrs
}

func dispatchResponseError(rawURL string, ref Ref, status int, body []byte) error {
	e := &DispatchError{
		StatusCode: status,
		Code:       "http_error",
		Phase:      "response",
		Host:       "",
		Path:       queueDispatchPath,
		Worker:     ref.Worker,
	}
	if u, err := url.Parse(rawURL); err == nil {
		e.Host = u.Host
	}
	var payload struct {
		Error      string `json:"error"`
		Code       string `json:"code"`
		ErrorName  string `json:"error_name"`
		ErrorFrame string `json:"error_frame"`
	}
	if json.Unmarshal(body, &payload) == nil {
		code := payload.Error
		if code == "" {
			code = payload.Code
		}
		if _, ok := dispatchErrorCodes[code]; ok {
			e.Code = code
			if code == "handler_failed" {
				e.ErrorName = allowlistedErrorName(payload.ErrorName)
				e.ErrorFrame = allowlistedErrorFrame(payload.ErrorFrame)
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
