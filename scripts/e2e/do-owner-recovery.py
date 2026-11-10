"""Force only a disposable shard's owner loss; never restart its runtime."""
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import urllib.parse
import urllib.request

ns = os.environ["E2E_OWNER_NAMESPACE"]
agent = "http://127.0.0.1:" + os.environ["E2E_OWNER_AGENT"]
gate = "http://127.0.0.1:" + os.environ["E2E_OWNER_GATE"]
runtime = "http://127.0.0.1:" + os.environ["E2E_OWNER_RUNTIME"]
token = os.environ["E2E_OWNER_TOKEN"]
binding = os.environ["E2E_OWNER_BINDING"]
pid = os.environ["E2E_OWNER_PID"]

def request(url, data=None, auth=token):
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = "Bearer " + auth
    raw = None if data is None else json.dumps(data).encode()
    with urllib.request.urlopen(urllib.request.Request(url, raw, headers), timeout=25) as r:
        return r.read()

def invoke(name, path="/read"):
    return request(agent+"/v1/do/invoke", {
        "namespace": ns, "worker": "paging", "class": "Bench", "id": name,
        "request": {"path": path}}, binding)

def shard(name):
    h = 2166136261
    for c in (ns+"/paging/Bench/"+name).encode():
        h = ((h ^ c) * 16777619) & 0xffffffff
    return h % 16

target = shard("single")
other = next("unrelated"+str(i) for i in range(100) if shard("unrelated"+str(i)) != target)
assert invoke(other, "/seed?rows=2") == b"seeded"
scope = ns+"/__do__/paging~Bench~shard"+str(target)
def resolve():
    return json.loads(request(agent+"/v1/internal/resolve?scope="+urllib.parse.quote(scope, safe="")))["owner"]

def native_children():
    return sorted({child for entry in Path("/proc/"+pid+"/task").glob("*/children")
                   for child in entry.read_text().split()})
before = native_children()
assert before, "native child missing"
proof = hashlib.sha256(invoke("single", "/proof")).hexdigest()
errors = []
stop = threading.Event()
def unrelated():
    try:
        while not stop.is_set():
            assert json.loads(invoke(other))["v"] == 0
            stop.wait(.03)
    except Exception as exc:
        errors.append(str(exc))
reader = threading.Thread(target=unrelated)
reader.start()
try:
    for cycle in range(3):
        owner = resolve()
        identity = {"scope": scope, "node": owner["node"], "epoch": owner["epoch"], "forget": True}
        request(gate+"/internal/do/lease", identity, "")
        request(agent+"/v1/internal/do/release", {
            "namespace":ns,"worker":"paging","class":"Bench","shard":target,
            "node":owner["node"],"epoch":owner["epoch"]})
        request(runtime+"/v1/do/renew", {})
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            values = list(pool.map(lambda _: json.loads(invoke("single"))["v"], range(16)))
        assert values == [0]*16, values
        after = resolve()
        assert after["epoch"] > owner["epoch"]
        assert hashlib.sha256(invoke("single","/proof")).hexdigest() == proof
        assert native_children() == before
        assert not errors, errors
        print("owner recovery cycle=%d epoch=%d->%d reads=16 data=PASS native_pid=unchanged unrelated=PASS" %
              (cycle+1,owner["epoch"],after["epoch"]), flush=True)
    # Another live runtime must be able to change the authoritative data before
    # the original runtime reclaims. Its old cache must not win over that write.
    if os.environ.get("E2E_OWNER_OTHER_RUNTIME"):
        other_runtime = "http://127.0.0.1:" + os.environ["E2E_OWNER_OTHER_RUNTIME"]
        other_gate = "http://127.0.0.1:" + os.environ["E2E_OWNER_OTHER_GATE"]
        deployed = json.loads(Path(os.environ["E2E_OWNER_DEPLOY"]).read_text())
        def relinquish(g, rt):
            current = resolve()
            request(g+"/internal/do/lease", {"scope":scope,"node":current["node"],"epoch":current["epoch"],"forget":True}, "")
            request(agent+"/v1/internal/do/release", {
                "namespace":ns,"worker":"paging","class":"Bench","shard":target,
                "node":current["node"],"epoch":current["epoch"]})
            request(rt+"/v1/do/renew",{})
            return current
        old = relinquish(gate,runtime)
        changed = json.loads(request(other_runtime+"/v1/do/invoke", {
            "namespace":ns,"worker":"paging","class":"Bench","storage_class":"Bench",
            "id":"single","storage_id":deployed["storage_id"],"bundle_sha":deployed["bundle_sha"],
            "request":{"path":"/inc"}}))
        assert changed["v"] == 1, changed
        middle = relinquish(other_gate,other_runtime)
        assert middle["node"] != old["node"] and middle["epoch"] > old["epoch"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            values = list(pool.map(lambda _: json.loads(invoke("single"))["v"], range(16)))
        assert values == [1]*16, values
        newest = resolve()
        assert newest["node"] == old["node"] and newest["epoch"] > middle["epoch"]
        assert hashlib.sha256(invoke("single","/proof")).hexdigest() == proof
        assert native_children() == before
        assert not errors, errors
        print("cross-node owner recovery epoch=%d->%d->%d remote_write=1 reads=16 data=PASS native_pid=unchanged unrelated=PASS" %
              (old["epoch"],middle["epoch"],newest["epoch"]),flush=True)
finally:
    stop.set()
    reader.join()
assert not errors, errors
