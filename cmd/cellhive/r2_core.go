package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
)

type r2Metadata struct {
	contentType        string
	contentDisposition string
	contentEncoding    string
	contentLanguage    string
	cacheControl       string
	expires            string
}

func (m r2Metadata) mapValue() map[string]string {
	out := map[string]string{}
	for _, pair := range []struct{ field, value string }{
		{"contentType", m.contentType},
		{"contentDisposition", m.contentDisposition},
		{"contentEncoding", m.contentEncoding},
		{"contentLanguage", m.contentLanguage},
		{"cacheControl", m.cacheControl},
		{"cacheExpiry", m.expires},
	} {
		if pair.value != "" {
			out[pair.field] = pair.value
		}
	}
	return out
}

func r2ObjectPutCore(namespace, bucket, key, file string, m r2Metadata) error {
	if file == "" {
		return fmt.Errorf("r2 object put requires a file")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	headers := http.Header{}
	meta := m.mapValue()
	if len(meta) != 0 {
		encoded, _ := json.Marshal(meta)
		headers.Set("x-cellhive-r2-http-metadata", base64.StdEncoding.EncodeToString(encoded))
	}
	out, err := scopeRawCallHeaders(http.MethodPut, "/v1/r2/object", namespace, "r2", bucket, data,
		url.Values{"bucket": {bucket}, "key": {key}}, headers)
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func r2ObjectGetCore(namespace, bucket, key, file string) error {
	out, err := scopeRawCall(http.MethodGet, "/v1/r2/object", namespace, "r2", bucket, nil,
		url.Values{"bucket": {bucket}, "key": {key}})
	if err != nil {
		return err
	}
	output := key
	if file != "" {
		output = file
	}
	return os.WriteFile(output, out, 0600)
}

func r2ObjectDeleteCore(namespace, bucket, key string) error {
	out, err := scopeRawCall(http.MethodDelete, "/v1/r2/object", namespace, "r2", bucket, nil,
		url.Values{"bucket": {bucket}, "key": {key}})
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func r2ObjectListCore(namespace, bucket, prefix string) error {
	q := url.Values{"bucket": {bucket}}
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	out, err := scopeRawCall(http.MethodGet, "/v1/r2/list", namespace, "r2", bucket, nil, q)
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}
