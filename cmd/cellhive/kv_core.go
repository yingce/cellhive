package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
)

type kvPutOptions struct {
	value      string
	path       string
	ttl        string
	expiration string
	metadata   string
}

func kvPutCore(namespace, binding, key string, o kvPutOptions) error {
	value := o.value
	if o.path != "" {
		if value != "" {
			return fmt.Errorf("kv key put accepts either a value or --path")
		}
		raw, err := os.ReadFile(o.path)
		if err != nil {
			return err
		}
		value = string(raw)
	}
	if value == "" && o.path == "" {
		return fmt.Errorf("kv key put requires a value or --path")
	}
	q := url.Values{"key": {key}}
	if o.ttl != "" {
		q.Set("expiration_ttl", o.ttl)
	}
	if o.expiration != "" {
		q.Set("expiration", o.expiration)
	}
	if o.metadata != "" {
		if !json.Valid([]byte(o.metadata)) {
			return fmt.Errorf("kv metadata must be JSON")
		}
		q.Set("metadata", base64.StdEncoding.EncodeToString([]byte(o.metadata)))
	}
	out, err := scopeRawCall(http.MethodPost, "/v1/kv/put", namespace, "kv", binding, []byte(value), q)
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func kvGetCore(namespace, binding, key string) error {
	out, err := scopeRawCall(http.MethodGet, "/v1/kv/get", namespace, "kv", binding, nil, url.Values{"key": {key}})
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(out)
	return err
}

func kvDeleteCore(namespace, binding, key string) error {
	out, err := scopeRawCall(http.MethodDelete, "/v1/kv/delete", namespace, "kv", binding, nil, url.Values{"key": {key}})
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func kvListCore(namespace, binding, prefix string) error {
	q := url.Values{}
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	out, err := scopeRawCall(http.MethodGet, "/v1/kv/list", namespace, "kv", binding, nil, q)
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func kvBulkCore(verb, namespace, binding, file string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	if len(raw) > 8<<20 || !json.Valid(raw) {
		return fmt.Errorf("KV bulk file must contain valid JSON within 8 MiB")
	}
	method := http.MethodPut
	if verb == "delete" {
		method = http.MethodDelete
	}
	path := "/client/v4/accounts/" + url.PathEscape(namespace) + "/storage/kv/namespaces/" + url.PathEscape(binding) + "/bulk"
	out, err := adminCall(method, path, json.RawMessage(raw))
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}
