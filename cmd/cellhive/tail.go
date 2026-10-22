package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func newTailCommand() *cobra.Command {
	var worker string
	cmd := &cobra.Command{
		Use:   "tail [namespace]",
		Short: "follow the audit trail or a worker log buffer",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if worker != "" {
				parts := strings.SplitN(worker, "/", 2)
				if len(parts) != 2 {
					return fmt.Errorf("--worker must be <namespace>/<worker>")
				}
				return tailWorkerLogs(parts[0], parts[1])
			}
			if len(args) != 1 {
				return fmt.Errorf("usage: cellhive tail <namespace> | cellhive tail --worker <ns>/<worker>")
			}
			return tailAudit(args[0])
		},
	}
	cmd.Flags().StringVar(&worker, "worker", "", "follow worker logs as <namespace>/<worker>")
	return cmd
}

func tailAudit(ns string) error {
	fmt.Fprintf(os.Stderr, "tailing audit for %s (ctrl-c to stop)\n", ns)
	seen := map[string]bool{}
	for {
		q := url.Values{"namespace": {ns}, "limit": {"100"}}
		data, err := adminCall(http.MethodGet, "/v1/control/audit?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		var out struct {
			Audit []struct {
				AtMs   int64  `json:"at_ms"`
				Actor  string `json:"actor"`
				Action string `json:"action"`
				Target string `json:"target"`
			} `json:"audit"`
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		for _, a := range out.Audit {
			key := fmt.Sprintf("%d/%s/%s", a.AtMs, a.Actor, a.Target)
			if seen[key] {
				continue
			}
			seen[key] = true
			fmt.Printf("%s %s %s %s\n", time.UnixMilli(a.AtMs).Format(time.RFC3339), a.Actor, a.Action, a.Target)
		}
		time.Sleep(time.Second)
	}
}

// tailWorkerLogs polls the bounded per-worker log buffer (non-durable).
func tailWorkerLogs(ns, worker string) error {
	fmt.Fprintf(os.Stderr, "tailing logs for %s/%s (ctrl-c to stop)\n", ns, worker)
	var cursor uint64
	mode := ""
	for {
		// Renew the OTLP log-export subscription while this tail is running, so
		// backend export stops shortly after it exits (ADR-172).
		if data, err := adminCall(http.MethodPost, "/v1/control/logs/subscribe", map[string]any{
			"namespace": ns, "worker": worker, "ttl_ms": 60000,
		}); err == nil {
			var sub struct {
				Mode string `json:"mode"`
			}
			if json.Unmarshal(data, &sub) == nil && sub.Mode != mode {
				mode = sub.Mode
				if mode == "tail" || mode == "all" {
					fmt.Fprintf(os.Stderr, "otlp log export: %s\n", mode)
				}
			}
		}
		q := url.Values{"namespace": {ns}, "worker": {worker}, "since": {fmt.Sprint(cursor)}}
		data, err := adminCall(http.MethodGet, "/v1/control/logs?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		var out struct {
			Next    uint64 `json:"next"`
			Entries []struct {
				AtMs    int64  `json:"at_ms"`
				Level   string `json:"level"`
				Message string `json:"message"`
			} `json:"entries"`
		}
		if err := json.Unmarshal(data, &out); err != nil {
			return err
		}
		for _, e := range out.Entries {
			fmt.Printf("%s [%s] %s\n", time.UnixMilli(e.AtMs).Format(time.RFC3339), e.Level, e.Message)
		}
		cursor = out.Next
		time.Sleep(time.Second)
	}
}
