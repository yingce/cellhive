#!/usr/bin/env python3
"""Sample CellHive workerd RSS in SG/USW and print deltas/slopes.

Read-only: kubectl get/exec only. Output CSV at --csv (default /tmp/workerd-memory.csv).
"""
import argparse, csv, json, os, subprocess, sys, time
from datetime import datetime, timezone

REGIONS = [
    ("sg", "/root/.kube/config_tke", "cellhive-sg"),
    ("usw", "/root/.kube/config_tke_us", "cellhive-usw"),
]
NAMES = ("do-runtime", "user-runtime", "do-canary")
SUM_RSS = r'''
for p in $(pgrep -x workerd 2>/dev/null); do
  awk '/VmRSS/{print $2}' /proc/$p/status 2>/dev/null
done
'''

def kubectl(kube, ns, args, timeout=60):
    env = os.environ.copy(); env["KUBECONFIG"] = kube
    return subprocess.run(["kubectl", "-n", ns] + args, env=env, text=True,
                          capture_output=True, timeout=timeout)

def pods(kube, ns):
    r = kubectl(kube, ns, ["get", "pods", "-o", "json"], timeout=30)
    if r.returncode != 0:
        return [], r.stderr.strip()
    out = []
    for p in json.loads(r.stdout)["items"]:
        name = p["metadata"]["name"]
        if not any(n in name for n in NAMES):
            continue
        restarts = sum(c.get("restartCount", 0) for c in p.get("status", {}).get("containerStatuses", []))
        out.append((name, restarts))
    return out, ""

def sample(kube, ns, pod):
    r = kubectl(kube, ns, ["exec", pod, "--", "sh", "-c", SUM_RSS], timeout=20)
    if r.returncode != 0:
        return None, 0, r.stderr.strip().replace("\n", " ")[:200]
    vals = []
    for line in r.stdout.splitlines():
        try: vals.append(int(line.strip()))
        except ValueError: pass
    return sum(vals) / 1024.0, len(vals), ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=60)
    ap.add_argument("--count", type=int, default=0, help="0 = forever")
    ap.add_argument("--csv", default="/tmp/workerd-memory.csv")
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()
    if a.once: a.count = 1
    prev = {}
    fields = ["ts","region","pod","workerd_pids","rss_mib","delta_mib","rate_mib_min","restarts","error"]
    new = not os.path.exists(a.csv)
    with open(a.csv, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new: w.writeheader(); f.flush()
        n = 0
        while a.count == 0 or n < a.count:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"\n=== {now} ===", flush=True)
            print(f"{'region':5} {'pod':42} {'pids':>4} {'RSS MiB':>9} {'delta':>8} {'MiB/min':>8} {'rst':>4}", flush=True)
            for region, kube, ns in REGIONS:
                plist, err = pods(kube, ns)
                if err:
                    print(f"{region:5} list error: {err[:120]}", flush=True)
                for pod, restarts in plist:
                    rss, npids, e = sample(kube, ns, pod)
                    key = (region, pod)
                    d = ""; rate = ""
                    if rss is not None and key in prev:
                        dt = a.interval / 60.0
                        d = rss - prev[key]
                        rate = d / dt if dt else ""
                        d = f"{d:+.1f}"; rate = f"{rate:+.1f}"
                    if rss is not None: prev[key] = rss
                    rss_s = f"{rss:.1f}" if rss is not None else "ERR"
                    print(f"{region:5} {pod:42} {npids:4d} {rss_s:>9} {str(d):>8} {str(rate):>8} {restarts:4d}", flush=True)
                    w.writerow({"ts":now,"region":region,"pod":pod,"workerd_pids":npids,
                                "rss_mib":"" if rss is None else f"{rss:.1f}",
                                "delta_mib":d,"rate_mib_min":rate,"restarts":restarts,"error":e})
                f.flush()
            n += 1
            if a.count == 0 or n < a.count: time.sleep(a.interval)

if __name__ == "__main__":
    main()
