#!/usr/bin/env bash
set -euo pipefail

repo=/home/ubuntu/TAR-OS/chronos
log="$repo/.enterprise-knowledge/enterprise-rag-curated-v2/native-priority-polls.log"
service=chronos-enterprise-native-priority.service
timer=chronos-enterprise-native-poll.timer
python="$repo/.venv/bin/python"

mkdir -p "$(dirname "$log")"
state=$(systemctl --user show "$service" -p ActiveState --value)
timestamp=$(date -u +%FT%TZ)

if [[ "$state" == active || "$state" == activating ]]; then
  counts=$(
    "$python" - <<'PY'
import json
import psycopg

with psycopg.connect(
    "postgresql://postgres:password@127.0.0.1:55439/"
    "chronos_enterprise_curated_v2"
) as connection:
    documents, chunks = connection.execute(
        """
        SELECT COUNT(*),
               (SELECT COUNT(*) FROM knowledge_chunks)
        FROM knowledge_documents
        """
    ).fetchone()
print(json.dumps({"documents": documents, "chunks": chunks}))
PY
  )
  printf '%s state=%s counts=%s\n' \
    "$timestamp" "$state" "$counts" >>"$log"
else
  result=$(systemctl --user show "$service" -p Result --value)
  printf '%s state=%s result=%s\n' \
    "$timestamp" "$state" "$result" >>"$log"
  systemctl --user stop --no-block "$timer"
fi
