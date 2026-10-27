package server

import (
	"context"
	"fmt"
	"sort"

	"cellhive/internal/control"
)

// cloudflareVersionBindings describes one immutable version; never expose secret values.
func (s *Server) cloudflareVersionBindings(ctx context.Context, view control.WorkerView) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(view.Bindings)+len(view.Vars))
	for _, b := range view.Bindings {
		item := map[string]any{"name": b.Name}
		switch b.Type {
		case "kv":
			item["type"], item["namespace_id"] = "kv_namespace", b.ID
		case "d1":
			item["type"], item["id"] = "d1", b.ID
		case "r2":
			item["type"], item["bucket_name"] = "r2_bucket", b.ID
		case "queue":
			item["type"], item["queue_name"] = "queue", b.ID
		case "do":
			item["type"], item["class_name"] = "durable_object_namespace", b.ID
		case "service":
			item["type"], item["service"] = "service", b.ID
			if b.Entrypoint != "" {
				item["entrypoint"] = b.Entrypoint
			}
		case "workflow":
			item["type"], item["workflow_name"], item["class_name"] = "workflow", b.ID, b.ClassName
		case "hyperdrive":
			item["type"], item["id"] = "hyperdrive", b.ID
		case "vectorize":
			item["type"], item["index_name"] = "vectorize", b.ID
		case "ai", "assets":
			item["type"] = b.Type
		default:
			return nil, fmt.Errorf("unsupported binding type %q", b.Type)
		}
		out = append(out, item)
	}
	for key, value := range view.Vars {
		out = append(out, map[string]any{"type": "plain_text", "name": key, "text": value})
	}
	keys, err := s.Control.VersionSecretNames(ctx, view.Namespace, view.Worker, view.Version)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		out = append(out, map[string]any{"type": "secret_text", "name": key})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["name"].(string) < out[j]["name"].(string) })
	return out, nil
}
