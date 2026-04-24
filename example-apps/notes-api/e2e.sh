#!/usr/bin/env bash
set -euo pipefail

# ── Notes API end-to-end test ──
# Starts the data-plane, runs setup, starts the app, exercises every endpoint,
# then tears everything down. Exit 0 = all passed, non-zero = failure.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR=$(mktemp -d)
DP_PORT=3355
APP_PORT=8185
DP_PID=""
APP_PID=""
PASS=0
FAIL=0
TOKEN=""

cleanup() {
  [ -n "$APP_PID" ] && kill "$APP_PID" 2>/dev/null || true
  [ -n "$DP_PID" ]  && kill "$DP_PID"  2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }
pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then pass "$label"; else fail "$label (expected '$expected', got '$actual')"; fi
}

assert_contains() {
  local label="$1" needle="$2" haystack="$3"
  if echo "$haystack" | grep -q "$needle"; then pass "$label"; else fail "$label (expected to contain '$needle')"; fi
}

assert_status() {
  local label="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then pass "$label"; else fail "$label (expected HTTP $expected, got $actual)"; fi
}

json_field() { echo "$1" | python3 -c "import sys,json; print(json.load(sys.stdin)$2)"; }

# ──────────────────────────────────────────────
echo "==> Building data-plane..."
cd "$ROOT"
cargo build -p freq-data-plane --quiet 2>&1

# ──────────────────────────────────────────────
echo "==> Starting data-plane on :$DP_PORT (data_dir=$DATA_DIR)..."
DATA_DIR="$DATA_DIR" PORT=$DP_PORT \
  cargo run -p freq-data-plane --quiet 2>/dev/null &
DP_PID=$!

# Wait for health
for i in $(seq 1 20); do
  if curl -sf http://localhost:$DP_PORT/health >/dev/null 2>&1; then break; fi
  sleep 0.3
done
curl -sf http://localhost:$DP_PORT/health >/dev/null || { echo "Data-plane failed to start"; exit 1; }
echo "    Data-plane ready (pid $DP_PID)"

# ──────────────────────────────────────────────
echo ""
echo "── Phase 1: Data-plane health & token ──"

HEALTH=$(curl -s http://localhost:$DP_PORT/health)
assert_eq "health status" "ok" "$(json_field "$HEALTH" "['status']")"

# Login and create token
curl -s -X POST "http://localhost:$DP_PORT/login" \
  -d 'username=admin&password=admin' \
  -c /tmp/e2e-cookies -b /tmp/e2e-cookies >/dev/null

TOKEN_RESP=$(curl -s -X POST "http://localhost:$DP_PORT/api/tokens" \
  -b /tmp/e2e-cookies \
  -H 'Content-Type: application/json' \
  -d '{"label":"e2e-test"}')
TOKEN=$(json_field "$TOKEN_RESP" "['raw_token']")
assert_contains "token created" "e2e-test" "$TOKEN_RESP"

AUTH="Authorization: Bearer $TOKEN"

# ──────────────────────────────────────────────
echo ""
echo "── Phase 2: OpenAPI spec served ──"

SPEC_STATUS=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$DP_PORT/openapi.yaml)
assert_status "openapi.yaml served" "200" "$SPEC_STATUS"

SPEC_BODY=$(curl -s http://localhost:$DP_PORT/openapi.yaml | head -3)
assert_contains "openapi version in spec" "openapi: 3.1" "$SPEC_BODY"

# ──────────────────────────────────────────────
echo ""
echo "── Phase 3: Run setup.ts (provision + migrate + bind) ──"

cd "$APP_DIR"
SETUP_OUT=$(DATA_PLANE_URL=http://localhost:$DP_PORT \
  DATA_PLANE_TOKEN=$TOKEN \
  deno run -A setup.ts 2>&1)
echo "$SETUP_OUT" | sed 's/^/    /'

assert_contains "setup provisions database" "Databases provisioned: 1" "$SETUP_OUT"
assert_contains "setup creates binding" "NOTES_DB" "$SETUP_OUT"
assert_contains "setup applies migrations" "Migrations applied: 2" "$SETUP_OUT"

# Verify database file exists
DB_FILE="$DATA_DIR/default/databases/notes-api-notes_db.duckdb"
if [ -f "$DB_FILE" ]; then pass "duckdb file created"; else fail "duckdb file missing at $DB_FILE"; fi

# ──────────────────────────────────────────────
echo ""
echo "── Phase 4: Idempotent re-setup ──"

SETUP2_OUT=$(DATA_PLANE_URL=http://localhost:$DP_PORT \
  DATA_PLANE_TOKEN=$TOKEN \
  deno run -A setup.ts 2>&1)

assert_contains "re-setup: no new bindings" "Bindings created: 0" "$SETUP2_OUT"
assert_contains "re-setup: no new migrations" "Migrations applied: 0" "$SETUP2_OUT"

# ──────────────────────────────────────────────
echo ""
echo "── Phase 5: Verify data-plane state via API ──"

# List databases
DBS=$(curl -s -H "$AUTH" http://localhost:$DP_PORT/databases)
DB_COUNT=$(echo "$DBS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
assert_eq "one database provisioned" "1" "$DB_COUNT"

DB_KIND=$(echo "$DBS" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['database_kind'])")
assert_eq "database kind is duckdb_file" "duckdb_file" "$DB_KIND"

# List migrations
MIGS=$(curl -s -H "$AUTH" http://localhost:$DP_PORT/databases/notes-api-notes_db/migrations)
MIG_COUNT=$(echo "$MIGS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
assert_eq "two migrations applied" "2" "$MIG_COUNT"

MIG_V0=$(echo "$MIGS" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['version'])")
MIG_V1=$(echo "$MIGS" | python3 -c "import sys,json; print(json.load(sys.stdin)[1]['version'])")
assert_eq "migration v0.0" "v0.0" "$MIG_V0"
assert_eq "migration v0.1" "v0.1" "$MIG_V1"

# List bindings
BINDINGS=$(curl -s -H "$AUTH" http://localhost:$DP_PORT/deployments/notes-api/bindings)
BIND_COUNT=$(echo "$BINDINGS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
assert_eq "one binding" "1" "$BIND_COUNT"

BIND_NAME=$(echo "$BINDINGS" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['binding_name'])")
assert_eq "binding name" "NOTES_DB" "$BIND_NAME"

# ──────────────────────────────────────────────
echo ""
echo "── Phase 6: Start notes-api and exercise endpoints ──"

DATA_PLANE_URL=http://localhost:$DP_PORT \
  DATA_PLANE_TOKEN=$TOKEN \
  PORT=$APP_PORT \
  deno run -A main.ts 2>/dev/null &
APP_PID=$!

for i in $(seq 1 20); do
  if curl -sf http://localhost:$APP_PORT/health >/dev/null 2>&1; then break; fi
  sleep 0.3
done
curl -sf http://localhost:$APP_PORT/health >/dev/null || { echo "Notes API failed to start"; exit 1; }
echo "    Notes API ready (pid $APP_PID)"

# Health
APP_HEALTH=$(curl -s http://localhost:$APP_PORT/health)
assert_eq "app health" "ok" "$(json_field "$APP_HEALTH" "['status']")"
assert_eq "app service" "notes-api" "$(json_field "$APP_HEALTH" "['service']")"
assert_eq "app binding" "NOTES_DB" "$(json_field "$APP_HEALTH" "['binding']")"
assert_eq "app migrations count" "2" "$(json_field "$APP_HEALTH" "['migrations']")"

# Bindings introspection
APP_BINDINGS=$(curl -s http://localhost:$APP_PORT/bindings)
assert_contains "app sees NOTES_DB binding" "NOTES_DB" "$APP_BINDINGS"
assert_contains "app sees duckdb_file" "duckdb_file" "$APP_BINDINGS"

# Empty list
NOTES=$(curl -s http://localhost:$APP_PORT/notes)
NOTE_COUNT=$(echo "$NOTES" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
assert_eq "initially empty" "0" "$NOTE_COUNT"

# Create note 1
N1=$(curl -s -X POST http://localhost:$APP_PORT/notes \
  -H 'Content-Type: application/json' \
  -d '{"title":"Test note","body":"Hello world","tags":"e2e,test"}')
N1_STATUS=$(echo "$N1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('title',''))")
assert_eq "create note 1" "Test note" "$N1_STATUS"
N1_ID=$(json_field "$N1" "['id']")

# Create note 2
N2=$(curl -s -X POST http://localhost:$APP_PORT/notes \
  -H 'Content-Type: application/json' \
  -d '{"title":"Second note","body":"More content"}')
N2_ID=$(json_field "$N2" "['id']")
assert_eq "create note 2" "Second note" "$(json_field "$N2" "['title']")"

# List should have 2
NOTES=$(curl -s http://localhost:$APP_PORT/notes)
NOTE_COUNT=$(echo "$NOTES" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
assert_eq "list has 2 notes" "2" "$NOTE_COUNT"

# Get by ID
GOT=$(curl -s http://localhost:$APP_PORT/notes/$N1_ID)
assert_eq "get note by id" "Test note" "$(json_field "$GOT" "['title']")"
assert_eq "get note tags" "e2e,test" "$(json_field "$GOT" "['tags']")"

# Patch
PATCHED=$(curl -s -X PATCH http://localhost:$APP_PORT/notes/$N1_ID \
  -H 'Content-Type: application/json' \
  -d '{"title":"Updated title","tags":"e2e,updated"}')
assert_eq "patch title" "Updated title" "$(json_field "$PATCHED" "['title']")"
assert_eq "patch tags" "e2e,updated" "$(json_field "$PATCHED" "['tags']")"

# Delete
DEL=$(curl -s -X DELETE http://localhost:$APP_PORT/notes/$N2_ID)
assert_contains "delete note" "true" "$DEL"

# List should have 1
NOTES=$(curl -s http://localhost:$APP_PORT/notes)
NOTE_COUNT=$(echo "$NOTES" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
assert_eq "list has 1 after delete" "1" "$NOTE_COUNT"

# 404 on missing note
NOT_FOUND_STATUS=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$APP_PORT/notes/00000000-0000-0000-0000-000000000000)
assert_status "404 on missing note" "404" "$NOT_FOUND_STATUS"

# 404 on unknown route
UNKNOWN_STATUS=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$APP_PORT/nope)
assert_status "404 on unknown route" "404" "$UNKNOWN_STATUS"

# ──────────────────────────────────────────────
echo ""
echo "── Phase 7: Data-plane CRUD extras ──"

# Update database status
UPDATED=$(curl -s -X PUT "http://localhost:$DP_PORT/databases/notes-api-notes_db/status" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"status":"archived"}')
assert_eq "update db status" "archived" "$(json_field "$UPDATED" "['status']")"

# Restore
curl -s -X PUT "http://localhost:$DP_PORT/databases/notes-api-notes_db/status" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"status":"active"}' >/dev/null

# Delete binding
BIND_ID=$(echo "$BINDINGS" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['binding_id'])")
DEL_STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "http://localhost:$DP_PORT/bindings/$BIND_ID" -H "$AUTH")
assert_status "delete binding" "204" "$DEL_STATUS"

# Verify empty bindings
EMPTY_BINDS=$(curl -s -H "$AUTH" http://localhost:$DP_PORT/deployments/notes-api/bindings)
EMPTY_COUNT=$(echo "$EMPTY_BINDS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
assert_eq "bindings empty after delete" "0" "$EMPTY_COUNT"

# Archive database
ARCH_STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "http://localhost:$DP_PORT/databases/notes-api-notes_db" -H "$AUTH")
assert_status "archive database" "204" "$ARCH_STATUS"

# ──────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "════════════════════════════════════════"

rm -f /tmp/e2e-cookies

if [ "$FAIL" -gt 0 ]; then exit 1; fi
