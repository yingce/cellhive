#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
NS=e2etransfer
e2e_start_agent transfer "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" '' "$E2E_BASE/agent"
e2e_cli app create "$NS" >/dev/null
e2e_cli resource create "$NS" kv SOURCE >/dev/null
e2e_cli resource create "$NS" kv TARGET >/dev/null
e2e_cli resource create "$NS" d1 LEGACY >/dev/null
TOKEN=$(e2e_token scope "$NS" kv SOURCE)
for i in $(seq 1 100); do
 curl -fsS --max-time 30 -H "Authorization: Bearer $TOKEN" -X POST --data-binary "value-$i" "http://127.0.0.1:$E2E_AGENT_PORT/v1/kv/put?ns=$NS&resource=SOURCE&key=k$i" >/dev/null
done
e2e_cli cell export "$NS/__kv__/SOURCE" --output "$E2E_BASE/export.sqlite"
python3 - "$E2E_BASE/export.sqlite" <<'PY'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
 assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
 rows=dict(db.execute('SELECT key,value FROM kv'))
 assert all(rows.get('k'+str(i))==('value-'+str(i)).encode() for i in range(1,101))
print('PASS SQLite export integrity and100 values')
PY
e2e_cli cell import "$NS/__kv__/TARGET" "$E2E_BASE/export.sqlite" > "$E2E_BASE/import.json"
if e2e_cli cell import "$NS/__kv__/TARGET" "$E2E_BASE/export.sqlite" > "$E2E_BASE/duplicate.log" 2>&1; then e2e_fail 'existing target overwritten'; fi
TOKEN=$(e2e_token scope "$NS" kv TARGET)
for i in $(seq 1 100); do
 got=$(curl -fsS --max-time 30 -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$E2E_AGENT_PORT/v1/kv/get?ns=$NS&resource=TARGET&key=k$i")
 [[ "$got" = "value-$i" ]] || e2e_fail "import mismatch at$i"
done
# Read after a post-import write and force another object-store export to ensure
# native txid continuation remains usable after migration.
curl -fsS --max-time 30 -H "Authorization: Bearer $TOKEN" -X POST --data-binary changed "http://127.0.0.1:$E2E_AGENT_PORT/v1/kv/put?ns=$NS&resource=TARGET&key=k100" >/dev/null
# Remove only this test node's local database cache after a clean drain.
pid=$(cat "$E2E_BASE/transfer.pid")
kill -TERM "$pid"
for i in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
kill -0 "$pid" 2>/dev/null && e2e_fail 'transfer agent did not drain'
rm "$E2E_BASE/transfer.pid"
rm -rf "$E2E_BASE/agent/cells"
mv "$E2E_BASE/transfer.log" "$E2E_BASE/transfer-before-cold.log"
e2e_start_agent transfer "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" '' "$E2E_BASE/agent"
TOKEN=$(e2e_token scope "$NS" kv TARGET)
for i in $(seq 1 100); do
 expected="value-$i"; [[ $i = 100 ]] && expected=changed
 got=$(curl -fsS --max-time 60 -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$E2E_AGENT_PORT/v1/kv/get?ns=$NS&key=k$i")
 [[ "$got" = "$expected" ]] || e2e_fail "cold import mismatch at$i"
done
echo 'PASS100 imported values after local cache removal (including post-import update)'
e2e_cli cell export "$NS/__kv__/TARGET" --output "$E2E_BASE/reexport.sqlite"
python3 - "$E2E_BASE/reexport.sqlite" <<'PY'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
 assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
 assert db.execute("SELECT value FROM kv WHERE key='k100'").fetchone()[0]==b'changed'
print('PASS post-import write and SQLite reexport')
PY
# A plain SQLite file models migration input independent of the native LTX
# version. It intentionally has no CellHive metadata tables.
python3 - "$E2E_BASE/legacy.sqlite" <<'PYCODE'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
 db.executescript("CREATE TABLE legacy_data(id INTEGER PRIMARY KEY AUTOINCREMENT, value BLOB); CREATE INDEX legacy_value ON legacy_data(value);")
 db.execute("INSERT INTO legacy_data(value) VALUES(?)",(bytes(range(256)),))
PYCODE
e2e_cli cell import "$NS/__d1__/LEGACY" "$E2E_BASE/legacy.sqlite" > "$E2E_BASE/legacy-import.json"
e2e_cli cell export "$NS/__d1__/LEGACY" --output "$E2E_BASE/legacy-restored.sqlite"
python3 - "$E2E_BASE/legacy-restored.sqlite" <<'PYCODE'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
 assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
 assert db.execute('SELECT value FROM legacy_data WHERE id=1').fetchone()[0]==bytes(range(256))
 assert db.execute("SELECT count(*) FROM sqlite_schema WHERE name='legacy_value'").fetchone()[0]==1
print('PASS plain SQLite migration preserves BLOB and index')
PYCODE
echo 'E2E-CELL-TRANSFER: PASS'
