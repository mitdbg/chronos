#!/usr/bin/env bash
set -euo pipefail

pipeline_log=${1:?usage: guard_minilm_during_workflows.sh PIPELINE_LOG}
pipeline_service=${2:-chronos-enterprise-curated-v2.service}
minilm_service=${3:-chronos-enterprise-minilm-backfill.service}
poll_seconds=${POLL_SECONDS:-5}
paused=${MINILM_PREPAUSED:-false}

resume_minilm() {
  if [[ "$paused" == true ]]; then
    systemctl --user kill \
      --kill-whom=main \
      --signal=SIGCONT \
      "$minilm_service" \
      >/dev/null 2>&1 || true
    paused=false
  fi
}

trap resume_minilm EXIT

while systemctl --user is-active "$pipeline_service" >/dev/null 2>&1; do
  phase=$(
    sed -n 's/.*phase=//p' "$pipeline_log" 2>/dev/null |
      tail -n 1
  )
  case "$phase" in
    benchmark-recorded-main-suite | pause-minilm-for-timed-workflows)
      if systemctl --user is-active "$minilm_service" >/dev/null 2>&1; then
        systemctl --user kill \
          --kill-whom=main \
          --signal=SIGSTOP \
          "$minilm_service"
        paused=true
      fi
      break
      ;;
  esac
  sleep "$poll_seconds"
done

while [[ "$paused" == true ]] &&
    systemctl --user is-active "$pipeline_service" >/dev/null 2>&1; do
  phase=$(
    sed -n 's/.*phase=//p' "$pipeline_log" 2>/dev/null |
      tail -n 1
  )
  case "$phase" in
    resume-minilm-after-timed-workflows | complete)
      break
      ;;
  esac
  sleep "$poll_seconds"
done

resume_minilm
trap - EXIT
