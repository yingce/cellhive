package runtimelog

// The consumer is deliberately independent of the pipe/writer goroutine: neither
// a stalled collector nor a cursor fsync can hold workerd's output reader.
import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	collectlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collecttrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type exportCursor struct {
	File   string
	Offset int64
}

func (s *Spool) cursorPath() string { return filepath.Join(s.cfg.Dir, "."+s.cfg.Stream+"-cursor.json") }
func (s *Spool) saveCursor(c exportCursor) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.cursorPath()+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err != nil {
		return err
	}
	if ce != nil {
		return ce
	}
	if err = os.Rename(s.cursorPath()+".tmp", s.cursorPath()); err != nil {
		return err
	}
	d, err := os.Open(s.cfg.Dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func exportBase(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("runtimelog: export endpoint must be an HTTP base URL without credentials or query")
	}
	u.Path = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(u.Path, "/"), "/v1/logs"), "/v1/traces")
	return strings.TrimRight(u.String(), "/"), nil
}
func (s *Spool) export(ctx context.Context) {
	defer close(s.exportDone)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 1
	transport.MaxConnsPerHost = 1
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: 3 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var cursor exportCursor
	if b, err := os.ReadFile(s.cursorPath()); err == nil {
		if json.Unmarshal(b, &cursor) != nil || cursor.Offset < 0 || filepath.Base(cursor.File) != cursor.File {
			cursor = exportCursor{}
			s.failures.Add(1)
		}
	}
	delay := time.Duration(0)
	backoff := time.Second
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		next, records, err := s.readBatch(cursor)
		if err == nil && len(records) > 0 {
			err = s.sendBatch(ctx, client, records)
		}
		if err == nil && next != cursor {
			err = s.saveCursor(next)
			if err == nil {
				cursor = next
			}
		}
		if err != nil {
			s.failures.Add(1)
			delay = backoff
			backoff = min(30*time.Second, backoff*2)
			continue
		}
		backoff = time.Second
		delay = time.Second
		if len(records) > 0 {
			delay = 0
		}
	}
}

// Only newline-terminated records are consumed. A crash-truncated final line
// in a sealed older file is skipped; an active partial line is retried later.
func (s *Spool) readBatch(c exportCursor) (exportCursor, [][]byte, error) {
	paths, err := filepath.Glob(filepath.Join(s.cfg.Dir, s.cfg.Stream+"-*.ndjson"))
	if err != nil {
		return c, nil, err
	}
	sort.Strings(paths)
	var records [][]byte
	total := 0
	for i, path := range paths {
		name := filepath.Base(path)
		if name < c.File {
			continue
		}
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return c, nil, err
		}
		offset := int64(0)
		if name == c.File {
			offset = c.Offset
		}
		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return c, nil, err
		}
		reader := bufio.NewReaderSize(f, maxLogLine+1)
		c = exportCursor{name, offset}
		for {
			line, err := reader.ReadSlice('\n')
			if err != nil {
				if err == io.EOF {
					if len(line) > 0 && i < len(paths)-1 {
						c.Offset += int64(len(line))
						s.dropped.Add(1)
					}
					break
				}
				f.Close()
				return c, nil, err
			}
			c.Offset += int64(len(line))
			records = append(records, bytes.Clone(line))
			total += len(line)
			if len(records) >= 128 || total >= 256<<10 {
				f.Close()
				return c, records, nil
			}
		}
		f.Close()
	}
	return c, records, nil
}
func (s *Spool) sendBatch(ctx context.Context, client *http.Client, records [][]byte) error {
	logs, traces, invalid := encodeRecords(records, s.cfg.Stream)
	s.dropped.Add(invalid)
	if len(logs.ResourceLogs) > 0 {
		if err := s.post(ctx, client, "logs", logs); err != nil {
			return err
		}
	}
	if len(traces.ResourceSpans) > 0 {
		if err := s.post(ctx, client, "traces", traces); err != nil {
			return err
		}
	}
	return nil
}
func (s *Spool) post(ctx context.Context, client *http.Client, signal string, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", s.cfg.Endpoint+"/v1/"+signal, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return err
	}
	if len(b) > 65536 {
		return fmt.Errorf("runtimelog: oversized export response")
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("runtimelog: export HTTP %d", resp.StatusCode)
	}
	var rejected int64
	if signal == "logs" {
		var r collectlogs.ExportLogsServiceResponse
		if err = proto.Unmarshal(b, &r); err != nil {
			return err
		}
		rejected = r.GetPartialSuccess().GetRejectedLogRecords()
	} else {
		var r collecttrace.ExportTraceServiceResponse
		if err = proto.Unmarshal(b, &r); err != nil {
			return err
		}
		rejected = r.GetPartialSuccess().GetRejectedSpans()
	}
	// OTLP explicitly forbids retrying partial success, which duplicates accepted records.
	if rejected > 0 {
		s.dropped.Add(uint64(rejected))
	}
	return nil
}
