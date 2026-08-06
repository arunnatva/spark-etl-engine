#!/usr/bin/env bash
#
# run_mapping.sh — execute a SINGLE session (one mapping) from a workflow.
#
# Runs exactly one session via run_workflow.py --only-session, so the mapping
# still gets its workflow-derived target overrides (physical table names, load
# modes, lookup connections, session attributes). Use this for testing or
# reprocessing one mapping without running the whole workflow.
#
# Usage:
#   ./run_mapping.sh <workflow.xml|workflow.json> <mappings_dir> <session_name> [options]
#
# Options / environment: same as run_workflow.sh
#   --config-dir DIR    per-mapping override files
#   --plan-only         print plan and exit
#   CONNECTIONS, VARS   shared base config files (defaults: ./connections.json, ./vars.json)
#
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <workflow.xml|workflow.json> <mappings_dir> <session_name> [options]" >&2
  exit 1
fi

WORKFLOW_INPUT="$1"; shift
MAPPINGS_DIR="$1";   shift
SESSION="$1";        shift

CONNECTIONS="${CONNECTIONS:-./connections.json}"
VARS="${VARS:-./vars.json}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$WORKFLOW_INPUT" in
  *.xml|*.XML)
    WORKFLOW_JSON="${WORKFLOW_INPUT%.*}.json"
    echo "[run_mapping] converting workflow XML -> $WORKFLOW_JSON"
    python3 "$HERE/workflow_xml_to_json.py" --input "$WORKFLOW_INPUT" --output "$WORKFLOW_JSON"
    ;;
  *.json|*.JSON) WORKFLOW_JSON="$WORKFLOW_INPUT" ;;
  *) echo "[run_mapping] unrecognized workflow file type: $WORKFLOW_INPUT" >&2; exit 1 ;;
esac

EXTRA=()
[[ -f "$CONNECTIONS" ]] && EXTRA+=(--connections "$CONNECTIONS")
[[ -f "$VARS" ]]        && EXTRA+=(--vars "$VARS")

echo "[run_mapping] executing session '$SESSION' from $WORKFLOW_JSON"
"$SPARK_SUBMIT" "$HERE/run_workflow.py" \
  --workflow "$WORKFLOW_JSON" \
  --mappings-dir "$MAPPINGS_DIR" \
  --only-session "$SESSION" \
  "${EXTRA[@]}" \
  "$@"
