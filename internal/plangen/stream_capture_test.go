package plangen

import (
	"cellhive/internal/ingress"
	"cellhive/internal/staticgen"
	"cellhive/internal/workerdbin"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStaticR2ModuleScopeCapture(t *testing.T) {
	bin, err := workerdbin.Find()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	base := fakeCell(t)
	defer base.Close()
	cell := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/worker/bindings":
			_ = json.NewEncoder(w).Encode(map[string]any{"bindings": map[string]any{"BUCKET": map[string]any{"kind": "r2", "ns": "acme", "name": "bucket", "authorization": map[string]any{"ns": "acme", "kind": "r2", "name": "bucket"}}}, "compat_date": "2026-06-15"})
		case "/v1/internal/bundle":
			fmt.Fprint(w, `import {env} from "cloudflare:workers"; const bucket=env.BUCKET; export default {async fetch(){ const obj=await bucket.get("k"); return new Response(await obj.text()); }};`)
		case "/v1/r2/object":
			fmt.Fprint(w, "streamed")
		default:
			req, _ := http.NewRequest(r.Method, base.URL+r.URL.RequestURI(), nil)
			req.Header = r.Header.Clone()
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
				w.WriteHeader(502)
				return
			}
			defer res.Body.Close()
			w.WriteHeader(res.StatusCode)
			io.Copy(w, res.Body)
		}
	}))
	defer cell.Close()
	b := New(Config{
		CellURL: cell.URL, CellToken: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", BindingSigningKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
		LogToken: "logtok", MinimalMode: "off",
	})
	plan, _, err := b.Build(context.Background(), []string{"bindings.test"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	tablePath := filepath.Join(root, "routes.json")
	ports := freePortRange(t, len(plan.Sockets)+4)
	mgr, err := staticgen.NewManager(staticgen.Config{
		WorkerdBin: bin, Root: root, PortBase: ports, TablePath: tablePath,
		DrainGrace: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	if err := mgr.Apply(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatal(err)
	}
	var table ingress.Table
	if err := json.Unmarshal(data, &table); err != nil {
		t.Fatal(err)
	}
	addr, err := ingress.BackendAddr(table.Routes["bindings.test"])
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || string(body) != "streamed" {
		t.Fatalf("static bound worker = %d %q", res.StatusCode, body)
	}
}
