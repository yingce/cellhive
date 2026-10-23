#!/usr/bin/env bash
# Cold-path KV write latency E2E.
#
# Real cell-agent process(es) and a few-MiB KV cell. Five situations where a
# put can hit a cold path: steady state, cell-handle eviction, SIGKILL+restart
# in the same data dir, cross-node takeover, and local wipe + re-hydrate.
# Each scenario prints one RESULT line, then reads back every acknowledged key
# from the harness ledger (key -> sha256) and counts missing/wrong.
#
# KV access: scoped token (x-cellhive-scope-token, minted by cmd/kvbench
# -print-token from CELLHIVE_ROOT_KEY) on POST /v1/kv/put and GET /v1/kv/get of
# the cell-agent REST listener. The app and kv resource are registered through
# the admin listener (x-cellhive-admin-token), as scripts/bench-capture-cells.sh.
#
# Env (defaults): BIN (prebuilt cell-agent; default builds ./cmd/cell-agent),
#   CELLHIVE_E2E_BUCKET=fs|cos, CELLHIVE_COS_ENV_FILE (cos only; defines
#   AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY), STEADY_SECONDS=20,
#   PRELOAD_BYTES=3145728, PRELOAD_VALUE_BYTES=65536, PUT_BYTES=4096,
#   LEASE_TTL=10s, P99_LIMIT_MS=1000, TAKEOVER_SLACK_MS=10000,
#   PORT_BASE=17211 (A_REST_PORT A_ADMIN_PORT B_REST_PORT B_ADMIN_PORT),
#   CELLHIVE_E2E_DELETE_APP=0|1 (default 1 for cos, 0 for fs).
# Credentials are never printed, logged or written; keep `set -x` off.
set -uo pipefail
set +x
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
for d in /usr/local/go/bin; do [[ -d $d && ":$PATH:" != *":$d:"* ]] && PATH="$PATH:$d"; done
export PATH CGO_ENABLED=1
export HOME=${HOME:-/root}
export GOPATH=${GOPATH:-$HOME/go}
export GOCACHE=${GOCACHE:-$HOME/.cache/go-build}
mkdir -p "$GOCACHE"
TAGS="sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"
STEADY_SECONDS=${STEADY_SECONDS:-20}
PRELOAD_BYTES=${PRELOAD_BYTES:-$((3 * 1024 * 1024))}
PRELOAD_VALUE_BYTES=${PRELOAD_VALUE_BYTES:-65536}
PUT_BYTES=${PUT_BYTES:-4096}
LEASE_TTL=${LEASE_TTL:-10s}
P99_LIMIT_MS=${P99_LIMIT_MS:-1000}
TAKEOVER_SLACK_MS=${TAKEOVER_SLACK_MS:-10000}
BUCKET_MODE=${CELLHIVE_E2E_BUCKET:-fs}
PORT_BASE=${PORT_BASE:-17211}
A_REST_PORT=${A_REST_PORT:-$PORT_BASE}
A_ADMIN_PORT=${A_ADMIN_PORT:-$((PORT_BASE + 1))}
B_REST_PORT=${B_REST_PORT:-$((PORT_BASE + 2))}
B_ADMIN_PORT=${B_ADMIN_PORT:-$((PORT_BASE + 3))}
COS_BUCKET_URL=cos://vwork-e2e-hk-1376795072
COS_BUCKET_ENDPOINT=https://vwork-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com
TMP=$(mktemp -d /tmp/cellhive-cold-e2e.XXXXXX)
RUN_ID="e2e-cold-$(date +%s)-$(openssl rand -hex 3)"
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
FAILED=0
A_PID=""; B_PID=""
NODE_A="$RUN_ID-a"; NODE_B="$RUN_ID-b"
NS=$RUN_ID
NS_ALL=("$NS")
LEDGER=$TMP/ledger-main.tsv
: >"$LEDGER"
TOKEN_ADMIN=$(openssl rand -hex 16)
_CK=""; _CS=""
DELETE_APP=${CELLHIVE_E2E_DELETE_APP:-$([[ $BUCKET_MODE == cos ]] && echo 1 || echo 0)}

# mask redacts the COS credential values (if any) from agent logs.
mask(){ MASK_A="$_CK" MASK_B="$_CS" python3 -c '
import os,sys
vals=[v for v in (os.environ.get("MASK_A"),os.environ.get("MASK_B")) if v]
for line in sys.stdin:
    for v in vals: line=line.replace(v,"***")
    sys.stdout.write(line)'; }
show_logs(){
  local f
  for f in "$TMP"/agent-*.log; do
    [[ -f $f ]] || continue
    echo "--- tail $(basename "$f") ---" >&2
    tail -15 "$f" | mask >&2
  done
}
fail(){ echo "COLD-E2E: FAIL: $*" >&2; show_logs; FAILED=1; }
die(){ fail "$*"; exit 1; }
write_client(){
cat >"$TMP/kvc.py" <<'PY'
import argparse, hashlib, http.client, math, os, sys, time, urllib.parse
TOK = {}
def tok(a, name):
    if name not in TOK:
        with open(os.path.join(a.tokdir, "tok-%s-%s" % (a.ns, name))) as f:
            TOK[name] = f.read().strip()
    return TOK[name]
def call(a, method, name, key, body=None, timeout=20):
    op = "put" if method == "POST" else "get"
    path = "/v1/kv/%s?ns=%s&key=%s" % (op, urllib.parse.quote(a.ns, safe=""), urllib.parse.quote(key, safe=""))
    t0 = time.perf_counter()
    try:
        c = http.client.HTTPConnection("127.0.0.1", a.port, timeout=timeout)
        c.request(method, path, body=body, headers={"x-cellhive-scope-token": tok(a, name)})
        r = c.getresponse(); data = r.read(); code = r.status; c.close()
    except Exception as e:
        return 0, str(e).encode(), (time.perf_counter() - t0) * 1000
    return code, data, (time.perf_counter() - t0) * 1000
def ack(a, name, key, v):
    with open(a.ledger, "a") as f:
        f.write("%s\t%s\t%s\n" % (name, key, hashlib.sha256(v).hexdigest()))
def lat(a, ms):
    if a.lat:
        with open(a.lat, "a") as f: f.write("%.3f\n" % ms)
def put_retry(a, name, key, v, timeout, t0=None):
    start = t0 if t0 else time.time()
    n = 0
    while True:
        n += 1
        code, data, ms = call(a, "POST", name, key, v)
        if 200 <= code < 300:
            ack(a, name, key, v)
            return True, n, ms, (time.time() - start) * 1000, code
        if time.time() - start > timeout:
            return False, n, ms, (time.time() - start) * 1000, code
        time.sleep(0.02)
def cmd_preload(a):
    n = max(1, a.bytes // a.size); bad = 0
    for i in range(n):
        ok = put_retry(a, a.name, "p-%05d" % i, os.urandom(a.size), 60)[0]
        bad += 0 if ok else 1
    print("keys=%d failed=%d" % (n, bad)); return bad
def cmd_steady(a):
    end = time.time() + a.seconds; i = errs = getbad = puts = 0
    while time.time() < end:
        key = "w-%d" % (i % 32); i += 1
        v = os.urandom(a.size)
        code, data, ms = call(a, "POST", a.name, key, v)
        if not 200 <= code < 300:
            errs += 1; sys.stderr.write("steady put %s -> %d %s\n" % (key, code, data[:120])); continue
        ack(a, a.name, key, v); lat(a, ms); puts += 1
        code, data, _ = call(a, "GET", a.name, key)
        if code != 200 or data != v: getbad += 1
    print("puts=%d errs=%d getbad=%d" % (puts, errs, getbad)); return 1 if errs or getbad else 0
def cmd_once(a):
    ok, n, ms, ttfs, code = put_retry(a, a.name, a.key, os.urandom(a.size), a.timeout, a.t0 or None)
    if ok: lat(a, ttfs)
    print("ok=%d ttfs_ms=%.1f attempts=%d put_ms=%.1f last_code=%d" % (ok, ttfs, n, ms, code)); return 0 if ok else 1
def cmd_seq(a):
    errs = 0
    for i in range(a.n):
        ok, _, ms, _, code = put_retry(a, a.name, "sq-%d" % (i % 50), os.urandom(a.size), 0)
        if ok: lat(a, ms)
        else: errs += 1; sys.stderr.write("seq put -> %d\n" % code)
    print("puts=%d errs=%d" % (a.n - errs, errs)); return 1 if errs else 0
def cmd_verify(a):
    want = {}
    with open(a.ledger) as f:
        for line in f:
            name, key, h = line.rstrip("\n").split("\t")
            want[(name, key)] = h
    miss = wrong = 0
    for (name, key), h in want.items():
        deadline = time.time() + a.timeout
        while True:
            code, data, _ = call(a, "GET", name, key)
            if code in (200, 404) or time.time() > deadline: break
            time.sleep(0.2)
        if code != 200: miss += 1
        elif hashlib.sha256(data).hexdigest() != h: wrong += 1
    print("acked=%d missing=%d wrong=%d" % (len(want), miss, wrong)); return 0
def cmd_pct(a):
    xs = sorted(float(x) for x in open(a.lat) if x.strip()) if os.path.exists(a.lat) else []
    def p(q): return xs[min(len(xs) - 1, int(math.ceil(q * len(xs))) - 1)] if xs else 0.0
    print("puts=%d p50_ms=%.1f p99_ms=%.1f max_ms=%.1f" % (len(xs), p(0.5), p(0.99), xs[-1] if xs else 0.0)); return 0
def cmd_mint(a):
    import base64, hmac, json
    b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
    root = bytes.fromhex(os.environ["CELLHIVE_ROOT_KEY"].strip())
    prk = hmac.new(b"\0" * 32, root, hashlib.sha256).digest()
    okm = hmac.new(prk, b"cellhive/token/scope\x01", hashlib.sha256).digest()
    secret = b64(okm).encode()
    payload = json.dumps({"ns": a.ns, "kind": "kv", "name": a.name}, separators=(",", ":"), ensure_ascii=False).encode()
    t = b64(payload) + "." + b64(hmac.new(secret, payload, hashlib.sha256).digest())
    with open(os.path.join(a.tokdir, "tok-%s-%s" % (a.ns, a.name)), "w") as f: f.write(t)
    return 0
def cmd_ttl(a):
    import re
    m = re.fullmatch(r"([0-9.]+)(ms|s|m)", a.key)
    print(int(float(m.group(1)) * {"ms": 1, "s": 1000, "m": 60000}[m.group(2)])); return 0
ap = argparse.ArgumentParser()
ap.add_argument("cmd"); ap.add_argument("--port", type=int, default=0); ap.add_argument("--ns", default="")
ap.add_argument("--name", default="KV"); ap.add_argument("--key", default="once"); ap.add_argument("--tokdir", default="")
ap.add_argument("--ledger", default=""); ap.add_argument("--lat", default=""); ap.add_argument("--size", type=int, default=4096)
ap.add_argument("--bytes", type=int, default=0); ap.add_argument("--seconds", type=float, default=0)
ap.add_argument("--timeout", type=float, default=30); ap.add_argument("--n", type=int, default=50)
ap.add_argument("--t0", type=float, default=0)
a = ap.parse_args()
sys.exit({"preload": cmd_preload, "steady": cmd_steady, "once": cmd_once, "seq": cmd_seq,
          "verify": cmd_verify, "pct": cmd_pct, "mint": cmd_mint, "ttl": cmd_ttl}[a.cmd](a))
PY
}
write_client
kv(){ python3 "$TMP/kvc.py" "$1" --ns "$NS" --tokdir "$TMP/tok" --ledger "$LEDGER" "${@:2}"; }
LEASE_MS=$(python3 "$TMP/kvc.py" ttl --key "$LEASE_TTL") || die "bad LEASE_TTL=$LEASE_TTL (want e.g. 3s, 500ms)"
SESS=0; CUR=a
rest_port(){ [[ $1 == a ]] && echo "$A_REST_PORT" || echo "$B_REST_PORT"; }
admin_port(){ [[ $1 == a ]] && echo "$A_ADMIN_PORT" || echo "$B_ADMIN_PORT"; }
data_dir(){ echo "$TMP/data-$1"; }
cleanup(){
  set +e
  if [[ ${DELETE_APP:-0} == 1 && -n ${CUR_ALIVE:-} ]]; then delete_app; fi
  [[ -n ${A_PID:-} ]] && { kill -9 "$A_PID" 2>/dev/null; wait "$A_PID" 2>/dev/null; }
  [[ -n ${B_PID:-} ]] && { kill -9 "$B_PID" 2>/dev/null; wait "$B_PID" 2>/dev/null; }
  rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
# delete_app removes only cells/<NS>/ and assets/<NS>/ objects (the product's
# own app-delete path, which lists those exact prefixes) plus the registry rows.
delete_app(){
  local n code
  for n in "${NS_ALL[@]}"; do
    [[ $n == e2e-cold-* ]] || { echo "cleanup: refusing to delete namespace $n"; continue; }
    code=$(curl -sS -m 180 -o "$TMP/del.out" -w '%{http_code}' -X DELETE -H "x-cellhive-admin-token: $TOKEN_ADMIN" \
      "http://127.0.0.1:$(admin_port "$CUR")/v1/control/app?namespace=$n" 2>/dev/null)
    echo "cleanup: delete app $n http=$code $(head -c 300 "$TMP/del.out" 2>/dev/null | mask)"
  done
}
start_agent(){ # TAG MAXOPEN IDLE
  local tag=$1 maxopen=$2 idle=$3 rest admin pid
  rest=$(rest_port "$tag"); admin=$(admin_port "$tag"); SESS=$((SESS + 1))
  if curl -s -m 1 -o /dev/null "http://127.0.0.1:$rest/healthz"; then fail "port $rest already in use"; return 1; fi
  mkdir -p "$(data_dir "$tag")"
  (
    export CELLHIVE_NODE_ID="$RUN_ID-$tag" CELLHIVE_SESSION_ID="$RUN_ID-$tag-s$SESS" CELLHIVE_DATA_DIR="$(data_dir "$tag")"
    export CELLHIVE_REST_ADDR=127.0.0.1:$rest CELLHIVE_ADMIN_ADDR=127.0.0.1:$admin CELLHIVE_ADVERTISE=127.0.0.1:$rest CELLHIVE_PEER_URL=http://127.0.0.1:$rest
    export CELLHIVE_LEASE_TTL="$LEASE_TTL" CELLHIVE_MAX_OPEN_CELLS="$maxopen" CELLHIVE_CELL_IDLE="$idle"
    export CELLHIVE_COMPACTION_INTERVAL=1m CELLHIVE_EPOCH_GC_INTERVAL=1m CELLHIVE_EPOCH_GC_INITIAL=1m
    if [[ $BUCKET_MODE == cos ]]; then
      export CELLHIVE_BUCKET="$COS_BUCKET_URL" COS_ENDPOINT="$COS_BUCKET_ENDPOINT" COS_SECRET_ID="$_CK" COS_SECRET_KEY="$_CS"
    else
      export CELLHIVE_BUCKET_DIR="$BUCKET_DIR"
    fi
    exec "$BIN"
  ) >>"$TMP/agent-$tag.log" 2>&1 &
  pid=$!
  if [[ $tag == a ]]; then A_PID=$pid; else B_PID=$pid; fi
  CUR=$tag; CUR_ALIVE=1
  local i
  for i in $(seq 1 400); do
    curl -sf -m 1 "http://127.0.0.1:$rest/healthz" >/dev/null 2>&1 && return 0
    kill -0 "$pid" 2>/dev/null || { fail "agent $tag exited during startup"; return 1; }
    sleep 0.05
  done
  fail "agent $tag not healthy"; return 1
}
kill_agent(){ # TAG
  local pid
  if [[ $1 == a ]]; then pid=${A_PID:-}; A_PID=""; else pid=${B_PID:-}; B_PID=""; fi
  [[ -n $pid ]] && { kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; }
  return 0
}
stop_agent(){ # TAG: graceful SIGTERM (falls back to SIGKILL after 15s)
  local pid i
  if [[ $1 == a ]]; then pid=${A_PID:-}; A_PID=""; else pid=${B_PID:-}; B_PID=""; fi
  [[ -n $pid ]] || return 0
  kill -TERM "$pid" 2>/dev/null
  for i in $(seq 1 150); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; return 0
}
# snap PORT -> "list get put epoch evicted open_cells" (absolute counters)
snap(){
  curl -sS -m 5 "http://127.0.0.1:$1/metrics" 2>/dev/null | awk '
    /^cellhive_bucket_ops_total\{op="list"\}/{l=$NF}
    /^cellhive_bucket_ops_total\{op="get"\}/{g=$NF}
    /^cellhive_bucket_ops_total\{op="put"\}/{p=$NF}
    /^cellhive_owner_epoch_changes_total\{role="owner"\}/{e=$NF}
    /^cellhive_cellstore_evicted_total /{v=$NF}
    /^cellhive_cellstore_open_cells /{o=$NF}
    END{print l+0, g+0, p+0, e+0, v+0, o+0}'
}
register_resource(){ # NAME SCOPE
  local code
  code=$(curl -sS -m 60 -o "$TMP/reg.out" -w '%{http_code}' -X POST -H "x-cellhive-admin-token: $TOKEN_ADMIN" \
    -H 'content-type: application/json' --data-binary "{\"namespace\":\"$NS\",\"kind\":\"kv\",\"name\":\"$1\",\"scope\":\"$2\"}" \
    "http://127.0.0.1:$(admin_port "$CUR")/v1/control/resource")
  [[ $code == 200 ]] || { fail "register kv resource $1: http=$code $(head -c 300 "$TMP/reg.out")"; return 1; }
  python3 "$TMP/kvc.py" mint --ns "$NS" --name "$1" --tokdir "$TMP/tok" || { fail "mint token $1"; return 1; }
}
# finish NAME LATFILE "SNAP_BEFORE" "SNAP_AFTER" EXTRA [LIMIT_MS]
# Prints the RESULT line after reading back every acked key; sets FAILED on
# missing/wrong keys or p99 above the limit.
finish(){
  local name=$1 lat=$2 b=($3) a=($4) extra=${5:-} limit=${6:-$P99_LIMIT_MS}
  local pc vr port; port=$(rest_port "$CUR")
  pc=$(python3 "$TMP/kvc.py" pct --lat "$lat")
  vr=$(kv verify --port "$port" --timeout 60)
  local p99 missing wrong
  p99=$(sed -n 's/.*p99_ms=\([0-9.]*\).*/\1/p' <<<"$pc")
  missing=$(sed -n 's/.*missing=\([0-9]*\).*/\1/p' <<<"$vr"); wrong=$(sed -n 's/.*wrong=\([0-9]*\).*/\1/p' <<<"$vr")
  echo "RESULT scenario=$name $pc list_ops=$((a[0]-b[0])) get_ops=$((a[1]-b[1])) put_ops=$((a[2]-b[2])) epoch_changes=$((a[3]-b[3])) $vr${extra:+ $extra}"
  [[ ${missing:-1} -eq 0 && ${wrong:-1} -eq 0 ]] || fail "$name: acknowledged data missing/wrong ($vr)"
  python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) else 1)' "${p99:-999999}" "$limit" \
    || fail "$name: p99 ${p99}ms > limit ${limit}ms"
}
field(){ sed -n "s/.*\\(^\\| \\)$1=\\([^ ]*\\).*/\\2/p" <<<"$2"; }
# Scrub any ambient storage config; the agent gets only what start_agent sets.
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_BUCKET AWS_ENDPOINT AWS_ENDPOINT_URL AWS_REGION AWS_PROFILE \
  COS_ENDPOINT COS_SECRET_ID COS_SECRET_KEY OSS_ENDPOINT OSS_ACCESS_KEY_ID OSS_ACCESS_KEY_SECRET CELLHIVE_BUCKET CELLHIVE_BUCKET_DIR \
  CELLHIVE_S3_PATH_STYLE CELLHIVE_ROOT_KEY_FILE
case $BUCKET_MODE in
  fs) BUCKET_DIR=$TMP/bucket; mkdir -p "$BUCKET_DIR" ;;
  cos)
    [[ -n ${CELLHIVE_COS_ENV_FILE:-} && -r $CELLHIVE_COS_ENV_FILE ]] || die "CELLHIVE_E2E_BUCKET=cos needs CELLHIVE_COS_ENV_FILE (readable)"
    # Parse KEY=VALUE lines without executing the file; only the two COS
    # credentials are taken (AWS_BUCKET/AWS_ENDPOINT in the file are ignored).
    _CREDS=$(python3 - "$CELLHIVE_COS_ENV_FILE" <<'PY'
import sys
want = {"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": ""}
for line in open(sys.argv[1]):
    line = line.strip()
    if line.startswith("export "): line = line[7:].strip()
    if not line or line.startswith("#") or "=" not in line: continue
    k, v = line.split("=", 1); k = k.strip(); v = v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"": v = v[1:-1]
    if k in want: want[k] = v
sys.stdout.write(want["AWS_ACCESS_KEY_ID"] + "\n" + want["AWS_SECRET_ACCESS_KEY"])
PY
    ) || die "cannot read CELLHIVE_COS_ENV_FILE"
    _CK=${_CREDS%%$'\n'*}; _CS=${_CREDS#*$'\n'}; unset _CREDS
    [[ -n $_CK && -n $_CS ]] || die "CELLHIVE_COS_ENV_FILE lacks AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"
    ;;
  *) die "CELLHIVE_E2E_BUCKET must be fs or cos (got $BUCKET_MODE)" ;;
esac
[[ $RUN_ID == e2e-cold-* ]] || die "bad run id"

if [[ -z ${BIN:-} ]]; then
  BIN=$ROOT/bin/cell-agent-cold
  mkdir -p "$(dirname "$BIN")"
  go build -tags "$TAGS" -o "$BIN" ./cmd/cell-agent || die "build"
fi
[[ -x $BIN ]] || die "BIN=$BIN is not executable"
export CELLHIVE_ROOT_KEY=$(openssl rand -hex 32)
export CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 CELLHIVE_ADMIN_TOKEN=$TOKEN_ADMIN
mkdir -p "$TMP/tok"
echo "COLD-E2E: bucket=$BUCKET_MODE ns=$NS lease_ttl=$LEASE_TTL steady=${STEADY_SECONDS}s preload=${PRELOAD_BYTES}B p99_limit=${P99_LIMIT_MS}ms bin=$BIN"
PORT_A=$A_REST_PORT; PORT_B=$B_REST_PORT
TARGET_DB_REL="cells/$NS/__kv__/main.db"

# ---- setup: agent A, app, target KV cell ------------------------------------
start_agent a 32 5m || exit 1
code=$(curl -sS -m 60 -o "$TMP/app.out" -w '%{http_code}' -X POST -H "x-cellhive-admin-token: $TOKEN_ADMIN" \
  -H 'content-type: application/json' --data-binary "{\"namespace\":\"$NS\"}" "http://127.0.0.1:$A_ADMIN_PORT/v1/control/app")
[[ $code == 200 ]] || die "create app: http=$code $(head -c 300 "$TMP/app.out")"
register_resource KV main || exit 1

# ---- scenario 1: steady -------------------------------------------------------
LAT=$TMP/lat-steady; : >"$LAT"
OUT=$(kv preload --port "$PORT_A" --name KV --size "$PRELOAD_VALUE_BYTES" --bytes "$PRELOAD_BYTES") || die "preload failed: $OUT"
echo "preload: $OUT (${PRELOAD_BYTES}B in ${PRELOAD_VALUE_BYTES}B values)"
B=$(snap "$PORT_A")
OUT=$(kv steady --port "$PORT_A" --name KV --size "$PUT_BYTES" --seconds "$STEADY_SECONDS" --lat "$LAT") || fail "steady: $OUT"
A=$(snap "$PORT_A")
finish steady "$LAT" "$B" "$A" "client=\"$OUT\""
read -r -a _b <<<"$B"; read -r -a _a <<<"$A"
[[ $((_a[3] - _b[3])) -eq 0 ]] || fail "steady: epoch changed during the window (${_b[3]} -> ${_a[3]})"

# ---- scenario 2: eviction -----------------------------------------------------
# Graceful restart with a tiny open-cell cap / short idle TTL, touch the target
# first so it is the LRU cell, then open several filler KV cells.
stop_agent a
start_agent a 3 3s || exit 1
OUT=$(kv once --port "$PORT_A" --name KV --key warm --size "$PUT_BYTES" --lat /dev/null --timeout 60) || fail "eviction warmup: $OUT"
EV0=$(snap "$PORT_A" | awk '{print $5}')
for i in 1 2 3 4 5; do
  register_resource "F$i" "fill$i" || exit 1
  OUT=$(kv once --port "$PORT_A" --name "F$i" --key f --size 1024 --lat /dev/null --timeout 60) || fail "filler F$i put: $OUT"
done
EVICTED=0
for _ in $(seq 1 60); do
  read -r -a _s <<<"$(snap "$PORT_A")"
  EVICTED=$((_s[4] - EV0))
  [[ $EVICTED -ge 1 && ${_s[5]} -le 3 ]] && break
  sleep 0.5
done
[[ $EVICTED -ge 1 ]] || fail "eviction: no cell handle was evicted (cellhive_cellstore_evicted_total unchanged)"
sleep 4 # idle TTL (3s) + sweep interval: make sure the target handle is gone too
read -r -a _s <<<"$(snap "$PORT_A")"
echo "eviction: evicted_delta=$((_s[4] - EV0)) open_cells=${_s[5]} (cap 3)"
LAT=$TMP/lat-evict; : >"$LAT"
B=$(snap "$PORT_A")
OUT=$(kv once --port "$PORT_A" --name KV --key evict-probe --size "$PUT_BYTES" --lat "$LAT" --timeout 60) || fail "eviction put: $OUT"
A=$(snap "$PORT_A")
finish eviction "$LAT" "$B" "$A" "single_op_ms=$(field put_ms "$OUT") $OUT"

# ---- scenario 3: SIGKILL + restart, same node id/data dir, new session --------
kill_agent a
start_agent a 32 5m || exit 1
LAT=$TMP/lat-restart; : >"$LAT"
B=$(snap "$PORT_A")
OUT=$(kv once --port "$PORT_A" --name KV --key restart-probe --size "$PUT_BYTES" --lat "$LAT" --timeout 60) || fail "restart put: $OUT"
A=$(snap "$PORT_A")
TTFS=$(field ttfs_ms "$OUT")
finish restart "$LAT" "$B" "$A" "single_op_ms=$(field put_ms "$OUT") $OUT"
python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) < float(sys.argv[2]) else 1)' "${TTFS:-999999}" "$LEASE_MS" \
  || fail "restart: first put took ${TTFS}ms, not below LEASE_TTL (${LEASE_MS}ms): it waited for the lease"

# ---- scenario 4: takeover by a second node -----------------------------------
OUT=$(kv seq --port "$PORT_A" --name KV --size "$PUT_BYTES" --n 20 --lat /dev/null) || fail "pre-takeover puts via A: $OUT"
kill_agent a
T0=$(python3 -c 'import time; print("%.6f" % time.time())')
start_agent b 32 5m || exit 1
LAT=$TMP/lat-takeover-first; : >"$LAT"
B=$(snap "$PORT_B")
TO=$(( LEASE_MS / 1000 * 3 + 60 ))
OUT=$(kv once --port "$PORT_B" --name KV --key takeover-probe --size "$PUT_BYTES" --lat "$LAT" --timeout "$TO" --t0 "$T0") || fail "takeover: B never accepted a put: $OUT"
A=$(snap "$PORT_B")
TTFS=$(field ttfs_ms "$OUT")
finish takeover_first "$LAT" "$B" "$A" "single_op_ms=$(field put_ms "$OUT") $OUT lease_ttl_ms=$LEASE_MS" 1000000000
python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) + float(sys.argv[3]) else 1)' "${TTFS:-999999999}" "$LEASE_MS" "$TAKEOVER_SLACK_MS" \
  || fail "takeover: time-to-first-success ${TTFS}ms > LEASE_TTL ${LEASE_MS}ms + slack ${TAKEOVER_SLACK_MS}ms"
LAT=$TMP/lat-takeover-next; : >"$LAT"
B=$(snap "$PORT_B")
OUT=$(kv seq --port "$PORT_B" --name KV --size "$PUT_BYTES" --n 50 --lat "$LAT") || fail "takeover: puts after first success: $OUT"
A=$(snap "$PORT_B")
finish takeover_next50 "$LAT" "$B" "$A" "client=\"$OUT\""

# ---- scenario 5: cold hydrate (local cells dir wiped) -------------------------
kill_agent b
rm -rf "$(data_dir b)/cells"
start_agent b 32 5m || exit 1
LAT=$TMP/lat-cold; : >"$LAT"
B=$(snap "$PORT_B")
OUT=$(kv once --port "$PORT_B" --name KV --key cold-probe --size "$PUT_BYTES" --lat "$LAT" --timeout "$TO") || fail "cold put: $OUT"
A=$(snap "$PORT_B")
finish cold "$LAT" "$B" "$A" "single_op_ms=$(field put_ms "$OUT") $OUT"

# ---- final: local SQLite integrity of the target cell -------------------------
DB="$(data_dir b)/$TARGET_DB_REL"
if ! command -v sqlite3 >/dev/null; then
  echo "integrity_check: skipped (sqlite3 not installed)"
elif [[ ! -f $DB ]]; then
  fail "target cell DB missing: $DB"
else
  INTEG=$(sqlite3 -readonly "$DB" 'PRAGMA integrity_check;' 2>&1)
  [[ $INTEG == ok ]] && echo "integrity_check: ok ($TARGET_DB_REL)" || fail "integrity_check = $INTEG"
fi

if [[ $FAILED -eq 0 ]]; then echo "COLD-E2E: PASS"; else echo "COLD-E2E: FAIL"; exit 1; fi
