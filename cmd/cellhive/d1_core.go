package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func d1DataCore(verb, namespace, database string, sqlArgs, commands []string, file string) error {
	sqlArgs = append(append([]string{}, sqlArgs...), commands...)
	if file != "" && len(sqlArgs) != 0 {
		return fmt.Errorf("d1 %s accepts either SQL or --file", verb)
	}
	if file != "" {
		raw, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		if verb == "execute" {
			digest := sha256.Sum256(raw)
			out, err := scopeCallKind(http.MethodPost, "/v1/d1/import", namespace, "d1", database,
				map[string]any{"digest": hex.EncodeToString(digest[:]), "sql": string(raw)},
				url.Values{"db": {database}})
			if err != nil {
				return err
			}
			printJSON(out)
			return nil
		}
		sqlArgs = []string{string(raw)}
	}
	sqlText := strings.Join(sqlArgs, " ")
	if strings.TrimSpace(sqlText) == "" {
		return fmt.Errorf("usage: cellhive d1 %s <namespace> <database> <sql>|--command <sql>|--file <path> [--remote]", verb)
	}
	path := "/v1/d1/query"
	if verb == "execute" {
		path = "/v1/d1/exec"
	}
	out, err := scopeCallKind(http.MethodPost, path, namespace, "d1", database,
		map[string]any{"sql": sqlText}, url.Values{"db": {database}})
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func d1ExportCore(namespace, database, output string, noSchema, noData bool, tables []string) error {
	if output == "" || noSchema && noData {
		return fmt.Errorf("d1 export requires --output and at least schema or data")
	}
	path := "/client/v4/accounts/" + url.PathEscape(namespace) + "/d1/database/" + url.PathEscape(database) + "/export"
	body := map[string]any{
		"output_format": "polling",
		"dump_options":  map[string]any{"no_schema": noSchema, "no_data": noData, "tables": tables},
	}
	response, err := adminCall(http.MethodPost, path, body)
	if err != nil {
		return err
	}
	var envelope struct {
		Result struct {
			Result struct {
				SignedURL string `json:"signed_url"`
			} `json:"result"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil || envelope.Result.Result.SignedURL == "" {
		return fmt.Errorf("invalid D1 export response: %v", err)
	}
	resp, err := httpClient.Get(envelope.Result.Result.SignedURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("D1 export download: %s", resp.Status)
	}
	tmp, err := os.CreateTemp(filepath.Dir(output), ".cellhive-export-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	written, err := io.Copy(tmp, io.LimitReader(resp.Body, (32<<20)+1))
	if err != nil {
		tmp.Close()
		return err
	}
	if written > 32<<20 {
		tmp.Close()
		return fmt.Errorf("D1 export exceeds 32 MiB")
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), output)
}

type d1MigrationsOptions struct {
	directory string
	table     string
	remote    bool
	local     bool
	applied   bool
}

func d1MigrationsCore(verb, namespace, database, message string, o d1MigrationsOptions) error {
	if o.directory == "" {
		o.directory = "migrations"
	}
	if o.table == "" {
		o.table = "d1_migrations"
	}
	if o.local || !o.remote {
		return fmt.Errorf("local migrations use `cellhive dev`; this command targets the CellHive database")
	}
	switch verb {
	case "create":
		if message == "" {
			return fmt.Errorf("usage: cellhive d1 migrations create <namespace> <database> <message> [--directory dir]")
		}
		if err := os.MkdirAll(o.directory, 0755); err != nil {
			return err
		}
		files, err := filepath.Glob(filepath.Join(o.directory, "*.sql"))
		if err != nil {
			return err
		}
		next := 1
		for _, file := range files {
			n, _ := strconv.Atoi(strings.SplitN(filepath.Base(file), "_", 2)[0])
			if n >= next {
				next = n + 1
			}
		}
		slug := strings.ToLower(strings.Trim(strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
				return r
			}
			return '_'
		}, message), "_"))
		if slug == "" {
			return fmt.Errorf("migration message must contain letters or digits")
		}
		path := filepath.Join(o.directory, fmt.Sprintf("%04d_%s.sql", next, slug))
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return err
		}
		_ = f.Close()
		fmt.Println(path)
		return nil
	case "list", "apply":
		out, err := scopeRawCall(http.MethodGet, "/v1/d1/migrations", namespace, "d1", database, nil,
			url.Values{"db": {database}, "table": {o.table}})
		if err != nil {
			return err
		}
		if verb == "list" && o.applied {
			printJSON(out)
			return nil
		}
		var state struct {
			Migrations []struct {
				Name string `json:"name"`
			} `json:"migrations"`
		}
		if err := json.Unmarshal(out, &state); err != nil {
			return err
		}
		applied := map[string]bool{}
		for _, item := range state.Migrations {
			applied[item.Name] = true
		}
		files, err := filepath.Glob(filepath.Join(o.directory, "*.sql"))
		if err != nil {
			return err
		}
		if _, err := os.Stat(o.directory); err != nil {
			return err
		}
		sort.Strings(files)
		pending := []string{}
		for _, file := range files {
			if !applied[filepath.Base(file)] {
				pending = append(pending, filepath.Base(file))
			}
		}
		if verb == "list" {
			result, _ := json.Marshal(map[string]any{"pending": pending})
			printJSON(result)
			return nil
		}
		for _, file := range files {
			name := filepath.Base(file)
			raw, err := os.ReadFile(filepath.Join(o.directory, name))
			if err != nil {
				return err
			}
			if _, err := scopeCallKind(http.MethodPost, "/v1/d1/migrations", namespace, "d1", database,
				map[string]any{"table": o.table, "name": name, "sql": string(raw)},
				url.Values{"db": {database}}); err != nil {
				return fmt.Errorf("migration %s: %w", name, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown d1 migrations verb %q (create|list|apply)", verb)
	}
}
