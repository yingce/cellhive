package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func vectorizeCreateCore(namespace, name string, dimensions int, metric, description string) error {
	payload := map[string]any{
		"namespace": namespace, "kind": "vectorize", "name": name,
		"scope":  fmt.Sprintf("%s/__vectorize__/%s", namespace, name),
		"config": map[string]any{"dimensions": dimensions, "metric": metric, "description": description},
	}
	data, err := adminCall(http.MethodPost, "/v1/vectorize/resources", payload)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeListCore(namespace string) error {
	q := url.Values{"namespace": {namespace}}
	data, err := adminCall(http.MethodGet, "/v1/vectorize/resources?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeDeleteCore(namespace, name string, force bool) error {
	q := url.Values{"namespace": {namespace}, "name": {name}}
	if force {
		q.Set("force", "1")
	}
	data, err := adminCall(http.MethodDelete, "/v1/vectorize/resources?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeInfoCore(namespace, name string) error {
	data, err := adminCall(http.MethodGet,
		"/v1/vectorize/stats?"+url.Values{"namespace": {namespace}, "name": {name}}.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeInsertCore(verb, namespace, name, file string, batch int) error {
	if file == "" {
		return fmt.Errorf("--file is required (ndjson)")
	}
	vectors, err := readVectorFile(file)
	if err != nil {
		return err
	}
	if batch <= 0 || batch > 1000 {
		batch = 1000
	}
	var last map[string]any
	for i := 0; i < len(vectors); i += batch {
		end := i + batch
		if end > len(vectors) {
			end = len(vectors)
		}
		data, err := scopeCall(http.MethodPost, "/v1/vectorize/"+verb, namespace, name,
			map[string]any{"vectors": vectors[i:end]})
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &last); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(last)
	if err != nil {
		return err
	}
	printJSON(raw)
	return nil
}

type vectorQueryOptions struct {
	vector         string
	vectorID       string
	topK           int
	returnValues   bool
	returnMetadata string
	namespace      string
	filter         string
}

func vectorizeQueryCore(namespace, name string, o vectorQueryOptions) error {
	body := map[string]any{"topK": o.topK, "returnValues": o.returnValues, "returnMetadata": o.returnMetadata}
	if o.vector != "" {
		vals, err := parseVector(strings.Split(o.vector, ","))
		if err != nil {
			return err
		}
		body["vector"] = vals
	} else if o.vectorID != "" {
		body["id"] = o.vectorID
	} else {
		return fmt.Errorf("--vector or --vector-id is required")
	}
	if o.namespace != "" {
		body["namespace"] = o.namespace
	}
	if o.filter != "" {
		var f any
		if err := json.Unmarshal([]byte(o.filter), &f); err != nil {
			return fmt.Errorf("--filter must be JSON: %w", err)
		}
		body["filter"] = f
	}
	data, err := scopeCall(http.MethodPost, "/v1/vectorize/query", namespace, name, body)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeIDsCore(verb, namespace, name string, ids []string) error {
	if len(ids) == 0 {
		return fmt.Errorf("--ids is required")
	}
	path := "/v1/vectorize/get"
	if verb == "delete-vectors" {
		path = "/v1/vectorize/delete"
	}
	data, err := scopeCall(http.MethodPost, path, namespace, name, map[string]any{"ids": ids})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeListVectorsCore(namespace, name string, count int, cursor string) error {
	data, err := scopeCall(http.MethodGet, "/v1/vectorize/list", namespace, name, nil,
		url.Values{"count": {strconv.Itoa(count)}, "cursor": {cursor}})
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeMetadataIndexCore(verb, namespace, name, property, typ string) error {
	if property == "" {
		return fmt.Errorf("--property-name is required")
	}
	method := http.MethodPost
	if verb == "delete-metadata-index" {
		method = http.MethodDelete
	}
	payload := map[string]string{"namespace": namespace, "name": name, "property": property, "type": typ}
	data, err := adminCall(method, "/v1/vectorize/metadata-index", payload)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeListMetadataIndexCore(namespace, name string) error {
	data, err := adminCall(http.MethodGet,
		"/v1/vectorize/metadata-indexes?"+url.Values{"namespace": {namespace}, "name": {name}}.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeRebuildCore(namespace, name string, buckets int, quantizer string, codesize int, nprobe float64) error {
	payload := map[string]any{
		"namespace": namespace, "name": name,
		"buckets": buckets, "quantizer": quantizer, "codesize": codesize, "nprobe": nprobe,
	}
	data, err := adminCall(http.MethodPost, "/v1/vectorize/rebuild", payload)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func vectorizeDropANNCore(namespace, name string) error {
	q := url.Values{"namespace": {namespace}, "name": {name}}
	data, err := adminCall(http.MethodDelete, "/v1/vectorize/ann?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}
