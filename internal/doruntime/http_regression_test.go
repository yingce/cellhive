package doruntime

import "testing"

const httpRegressionTenant = `import { DurableObject } from "cloudflare:workers";
 export class Review extends DurableObject {
 async fetch(req) {
 const p=new URL(req.url).pathname;
 if(p==="/headers"){const h=new Headers({"X-Review":"kept","Location":"/next","X-Cellhive-Internal-Token":"forged"});h.append("Set-Cookie","a=b");h.append("Set-Cookie","c=d");return new Response("ok",{headers:h});}
 if(p==="/binary")return new Response(new Uint8Array([0,255,128,65]));
 if(p==="/echo")return new Response(await req.arrayBuffer());
 if(p==="/redirect")return new Response(null,{status:302,headers:{Location:"/must-not-follow"}});
 if(p==="/failed")return new Response(new Uint8Array([0,255,128,65]),{status:418,headers:{"X-Review":"error"}});
 if(p==="/empty")return new Response(null,{status:204});
 if(p==="/delete"){
 for(let i=0;i<300;i++)await this.ctx.storage.put("k"+String(i).padStart(4,"0"),i);
 await this.ctx.storage.deleteAll();
 return new Response(String((await this.ctx.storage.list()).size));
 }
 return new Response("ok");
 }};`

func TestDOHTTPBytesHeadersAndDeleteAll(t *testing.T) {
	base, stop := startRuntimeWithBundle(t, &ownerStub{live: map[string]bool{}}, httpRegressionTenant)
	defer stop()
	for _, path := range []string{"/headers", "/binary", "/empty", "/delete", "/echo", "/redirect"} {
		t.Run(path, func(t *testing.T) {
			request := map[string]any{"path": path}
			if path == "/echo" {
				request["method"] = "POST"
				request["body_base64"] = "AP+AQQ=="
			}
			r, b := invokeSpec(t, base, map[string]any{"namespace": "demo", "worker": "review", "bundle_sha": "sha1", "class": "Review", "id": path, "request": request})
			t.Logf("status=%d body=%q headers=%v", r.StatusCode, b, r.Header)
			switch path {
			case "/headers":
				if r.Header.Get("X-Review") != "kept" || len(r.Header.Values("Set-Cookie")) != 2 || r.Header.Get("X-Cellhive-Internal-Token") != "" {
					t.Error("tenant headers lost")
				}
			case "/binary", "/echo":
				if b != string([]byte{0, 255, 128, 65}) {
					t.Errorf("binary changed: %x", []byte(b))
				}
			case "/redirect":
				if r.StatusCode != 302 {
					t.Fatal("redirect followed")
				}
			case "/empty":
				if r.StatusCode != 204 {
					t.Error("204 response failed")
				}
			case "/delete":
				if b != "0" {
					t.Error("deleteAll left keys")
				}
			}
		})
	}
}
