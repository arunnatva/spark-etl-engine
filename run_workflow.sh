#!/usr/bin/env bash
#
# run_workflow.sh — execute an entire Informatica workflow on Spark.
#
# Converts the workflow XML to JSON (if needed), then runs every session in the
# workflow's dependency order via run_workflow.py. Connections/vars use a single
# shared base file, with optional per-mapping overrides in a config dir.
#
# Usage:
#   ./run_workflow.sh <workflow.xml|workflow.json> <mappings_dir> [options]
#
# Options (passed through to run_workflow.py):
#   --config-dir DIR        per-mapping override files (<mapping>.connections.json)
#   --honor-conditions      gate each session on predecessor success
#   --continue-on-error     keep going after a failure (with --honor-conditions)
#   --plan-only             print the resolved plan and exit
#
# Environment:
#   CONNECTIONS   shared base connections JSON   (default: ./connections.json)
#   VARS          shared base vars JSON          (default: ./vars.json)
#   SPARK_SUBMIT  spark-submit binary            (default: spark-submit)
#
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <workflow.xml|workflow.json> <mappings_dir> [options]" >&2
  exit 1
fi

WORKFLOW_INPUT="$1"; shift
MAPPINGS_DIR="$1";   shift

CONNECTIONS="${CONNECTIONS:-./connections.json}"
VARS="${VARS:-./vars.json}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1) Ensure we have a workflow JSON (convert XML if an XML was passed)
case "$WORKFLOW_INPUT" in
  *.xml|*.XML)
    WORKFLOW_JSON="${WORKFLOW_INPUT%.*}.json"
    echo "[run_workflow] converting workflow XML -> $WORKFLOW_JSON"
    python3 "$HERE/workflow_xml_to_json.py" --input "$WORKFLOW_INPUT" --output "$WORKFLOW_JSON"
    ;;
  *.json|*.JSON)
    WORKFLOW_JSON="$WORKFLOW_INPUT"
    ;;
  *)
    echo "[run_workflow] unrecognized workflow file type: $WORKFLOW_INPUT" >&2
    exit 1
    ;;
esac

# 2) Assemble optional flags
EXTRA=()
[[ -f "$CONNECTIONS" ]] && EXTRA+=(--connections "$CONNECTIONS")
[[ -f "$VARS" ]]        && EXTRA+=(--vars "$VARS")

echo "[run_workflow] executing workflow $WORKFLOW_JSON"
"$SPARK_SUBMIT" "$HERE/run_workflow.py" \
  --workflow "$WORKFLOW_JSON" \
  --mappings-dir "$MAPPINGS_DIR" \
  "${EXTRA[@]}" \
  "$@"
