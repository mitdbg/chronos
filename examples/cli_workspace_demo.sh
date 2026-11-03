#!/usr/bin/env bash
# Run from any directory after installing chronos-core with Qdrant support.
set -euo pipefail

case "${1:---postgres}" in
    --postgres)
        : "${CHRONOS_DEMO_POSTGRES_URL:?Set CHRONOS_DEMO_POSTGRES_URL to a fresh PostgreSQL demo database}"
        sql_kind="postgresql"
        sql_url='${CHRONOS_DEMO_POSTGRES_URL}'
        ;;
    --sqlite)
        sql_kind="sqlite"
        sql_url="sqlite:///app.sqlite"
        ;;
    --help)
        printf 'Usage: bash examples/cli_workspace_demo.sh [--postgres|--sqlite]\n'
        printf 'Default: PostgreSQL (CHRONOS_DEMO_POSTGRES_URL) + embedded Qdrant.\n'
        printf 'Use --sqlite for SQLite + embedded Qdrant with no servers.\n'
        exit 0
        ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
esac

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
mkdir -p .chronos
demo_dir="$(mktemp -d "$repo_root/.chronos/workspace-demo.XXXXXX")"
export CHRONOS_WORKSPACE_CONFIG="$demo_dir/chronos.toml"
cli=(uv run --no-sync --package chronos-core chronos workspace)
python=(uv run --no-sync --package chronos-core python)

# Python only compares JSON output; every Chronos operation uses the CLI.
check() {
    local label="$1" field="$2" expected="$3" actual
    shift 3
    printf '\n%s\n' "$label"
    actual="$("${cli[@]}" "$@")"
    printf '%s\n' "$actual"
    "${python[@]}" -c '
import json, sys
value = json.loads(sys.argv[1] or "null")
for field in filter(None, sys.argv[2].split(".")):
    value = value[int(field)] if isinstance(value, list) else value[field]
expected = json.loads(sys.argv[3])
if value != expected:
    raise SystemExit(f"FAIL: expected {expected!r}, got {value!r}")
print("PASS")
' "$actual" "$field" "$expected"
}

printf 'Persistent demo directory: %s\n' "$demo_dir"
cat > "$CHRONOS_WORKSPACE_CONFIG" <<TOML
[stores.app]
kind = "$sql_kind"
database_url = "$sql_url"
TOML

printf '\nPrepare %s through the CLI\n' "$sql_kind"
"${cli[@]}" table create --store app 'CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)'
"${cli[@]}" table register --store app items --primary-key id
"${cli[@]}" execute --store app "INSERT INTO items (id, value) VALUES (1, 'base')"
check 'SQL alone works' '' '[{"id":1,"value":"base"}]' query --store app 'SELECT * FROM items'

cat >> "$CHRONOS_WORKSPACE_CONFIG" <<TOML

[workspace]
shared_metadata_url = "$sql_url"

[stores.vectors]
kind = "qdrant"
metadata_url = "$sql_url"
path = "vectors"
TOML

printf '\nAdd embedded Qdrant; no server is needed\n'
"${cli[@]}" qdrant --store vectors collection register documents --dimensions 3
check 'Existing SQL data survives adding Qdrant' '' '[{"id":1,"value":"base"}]' query --store app 'SELECT * FROM items'
"${cli[@]}" branch create experiment
"${cli[@]}" execute --store app --branch experiment "UPDATE items SET value = 'candidate' WHERE id = 1"
"${cli[@]}" qdrant --store vectors point upsert documents one \
    '{"vector":[1,0,0],"payload":{"value":"candidate"}}' --branch experiment

check 'Main still has the original SQL row' '' '[{"id":1,"value":"base"}]' query --store app 'SELECT * FROM items'
check 'Main cannot see the branch vector' '' 'null' qdrant --store vectors point get documents one
check 'Branch SQL sees its own change' '' '[{"id":1,"value":"candidate"}]' query --store app --branch experiment 'SELECT * FROM items'
check 'Branch vector is searchable' '0.id' '"one"' qdrant --store vectors search documents '[1,0,0]' --branch experiment

printf '\nCheckpoint and restore both stores\n'
"${cli[@]}" checkpoint create saved --branch experiment
"${cli[@]}" branch create-from-checkpoint restored saved
check 'Checkpoint restores the SQL row' '' '[{"id":1,"value":"candidate"}]' query --store app --branch restored 'SELECT * FROM items'
check 'Checkpoint restores the vector' 'payload.value' '"candidate"' qdrant --store vectors point get documents one --branch restored

printf '\nInspect both stores before merging\n'
"${cli[@]}" diff main experiment
preview="$("${cli[@]}" merge atomic-preview experiment main)"
printf '%s\n' "$preview"
preview_token="$("${python[@]}" -c 'import json, sys; print(json.loads(sys.argv[1])["preview_token"])' "$preview")"
check 'Atomic merge commits' 'status' '"committed"' merge atomic-apply experiment main --operation-id demo-merge --preview-token "$preview_token"
check 'Main now has the SQL change' '' '[{"id":1,"value":"candidate"}]' query --store app 'SELECT * FROM items'
check 'Main now has the vector change' 'payload.value' '"candidate"' qdrant --store vectors point get documents one

printf '\nRemove demo branches; retain data and checkpoint\n'
"${cli[@]}" branch delete restored
"${cli[@]}" branch delete experiment
check 'Only main remains' '' '["main"]' branch list
printf '\nAll checks passed. Local files remain in %s\n' "$demo_dir"
printf 'SQL data remains in the configured %s database.\n' "$sql_kind"
printf 'To keep exploring:\nexport CHRONOS_WORKSPACE_CONFIG=%q\n' "$CHRONOS_WORKSPACE_CONFIG"
