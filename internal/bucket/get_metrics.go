package bucket

import (
	"context"
	"maps"
	"net/http"
	"strings"
	"sync"
)

// ReadSource is a bounded attribution label, never an object or tenant name.
type ReadSource string

const (
	RouteProjection   ReadSource = "route_projection"
	ResourceCleanup   ReadSource = "resource_cleanup"
	TransferCleanup   ReadSource = "transfer_cleanup"
	CompactionRead    ReadSource = "compaction"
	UploadRead        ReadSource = "upload"
	WorkflowRetention ReadSource = "workflow_retention"
	MaintenanceRead   ReadSource = "maintenance"
	PeerHTTP          ReadSource = "peer_http"
	ControlHTTP       ReadSource = "control_http"
	DOHTTP            ReadSource = "do_http"
	BindingHTTP       ReadSource = "binding_http"
	HealthHTTP        ReadSource = "health_http"
	OtherHTTP         ReadSource = "other_http"

	QueueDispatch     ReadSource = "queue_dispatch"
	QueueReconcile    ReadSource = "queue_reconcile"
	CronPlan          ReadSource = "cron_plan"
	CronDispatch      ReadSource = "cron_dispatch"
	CronRetry         ReadSource = "cron_retry"
	WakeScan          ReadSource = "wake_scan"
	WakeRepair        ReadSource = "wake_repair"
	TimerDispatch     ReadSource = "timer"
	RecoveryRead      ReadSource = "recovery"
	OwnerRead         ReadSource = "owner"
	LeaseRead         ReadSource = "lease"
	PurgeRead         ReadSource = "purge"
	KVRefresh         ReadSource = "kv_auth_refresh"
	RebalanceRead     ReadSource = "rebalance"
	EpochGCRead       ReadSource = "epoch_gc"
	EpochFoldRead     ReadSource = "epoch_fold"
)

type sourceTag struct{}
type listTag struct{}
type objectClassTag struct{}

func WithReadSource(ctx context.Context, s ReadSource) context.Context {
	switch s {
	case RouteProjection, ResourceCleanup, TransferCleanup, CompactionRead, UploadRead, WorkflowRetention, MaintenanceRead, PeerHTTP, ControlHTTP, DOHTTP, BindingHTTP, HealthHTTP, OtherHTTP, SharedSchedule,
		QueueDispatch, QueueReconcile, CronPlan, CronDispatch, CronRetry, WakeScan, WakeRepair, TimerDispatch, RecoveryRead, OwnerRead, LeaseRead, PurgeRead, KVRefresh, RebalanceRead, EpochGCRead, EpochFoldRead:
		return context.WithValue(ctx, sourceTag{}, s)
	}
	return ctx
}
func GetReadSource(ctx context.Context) ReadSource {
	s, _ := ctx.Value(sourceTag{}).(ReadSource)
	return s
}

func objectGetContext(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, objectClassTag{}, objectKeyClass(key))
}

func objectKeyClass(key string) string {
	if base, _, ok := ParsePhysicalAuthorityKey(key); ok {
		key = base
	}
	prefix, _, _ := strings.Cut(key, "/")
	class := "other"
	switch prefix {
	case "cells":
		switch {
		case key == "cells/__platform__/__control__/main/own.json":
			class = "control_owner"
		case ownKey(key), strings.HasSuffix(key, "/owner-gen"):
			class = "owner"
		case strings.Contains(key, "/ltx/"):
			class = "ltx"
		default:
			class = "cell_data"
		}
	case "nodes":
		class = "lease"
		if strings.Contains(key, "/recovery/") {
			class = "recovery_address"
		}
	case "node-logs":
		class = "node_log"
	case "fleet", "wake", "r2", "r2meta", "r2usage", "assets", "bundles", "dosupervisor", "deploys":
		class = prefix
	}
	return class
}

type GetLabels struct {
	Source        ReadSource
	KeyClass, Via string
}

var getCounters = struct {
	sync.Mutex
	counts map[GetLabels]uint64
}{counts: make(map[GetLabels]uint64)}

func countObjectGet(ctx context.Context) {
	class, ok := ctx.Value(objectClassTag{}).(string)
	if !ok {
		return
	}
	source := GetReadSource(ctx)
	if source == "" {
		source = "other"
	}
	via := "direct"
	if ctx.Value(listTag{}) != nil {
		via = "list"
	}
	getCounters.Lock()
	getCounters.counts[GetLabels{source, class, via}]++
	getCounters.Unlock()
}

func ObjectGetAttempts() map[GetLabels]uint64 {
	getCounters.Lock()
	defer getCounters.Unlock()
	out := make(map[GetLabels]uint64, len(getCounters.counts))
	for labels, n := range getCounters.counts {
		out[labels] = n
	}
	return out
}

type ResponseLabels struct {
	Source           ReadSource
	KeyClass, Method string
	Status           int
}

var responseCounters = struct {
	sync.Mutex
	counts map[ResponseLabels]uint64
}{counts: make(map[ResponseLabels]uint64)}

func countStorageResponse(r *http.Request, response *http.Response, pathBucket string) {
	if response == nil {
		return
	}
	ctx := r.Context()
	source := GetReadSource(ctx)
	if source == "" {
		source = "other"
	}
	class, ok := ctx.Value(objectClassTag{}).(string)
	if !ok {
		key := strings.TrimPrefix(r.URL.Path, "/")
		if pathBucket != "" {
			if key == pathBucket {
				key = ""
			} else {
				key = strings.TrimPrefix(key, pathBucket+"/")
			}
		}
		if key == "" {
			class = "bucket"
		} else {
			class = objectKeyClass(key)
		}
	}
	method := r.Method
	switch method {
	case "GET", "HEAD", "PUT", "POST", "DELETE", "PATCH", "OPTIONS":
	default:
		method = "other"
	}
	label := ResponseLabels{source, class, method, response.StatusCode}
	responseCounters.Lock()
	responseCounters.counts[label]++
	responseCounters.Unlock()
}

func StorageResponses() map[ResponseLabels]uint64 {
	responseCounters.Lock()
	defer responseCounters.Unlock()
	return maps.Clone(responseCounters.counts)
}
