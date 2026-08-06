#!/usr/bin/env bash
#
# gen_configs.sh — scaffold connection/vars configs in either mode.
#
# Mode 1 (workflow, layered): shared base + per-mapping override tree
#   ./gen_configs.sh workflow <workflow.json> <mappings_dir> [out_dir]
#
# Mode 2 (single mapping, flat): one combined connections/vars pair
#   ./gen_configs.sh mapping <mapping.json> [out_connections] [out_vars]
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-}"; shift || true

case "$MODE" in
  workflow)
    WORKFLOW="${1:-}"; MAPPINGS_DIR="${2:-}"; OUT_DIR="${3:-./cfg}"
    if [[ -z "$WORKFLOW" || -z "$MAPPINGS_DIR" ]]; then
      echo "Usage: $0 workflow <workflow.json> <mappings_dir> [out_dir]" >&2
      exit 1
    fi
    python3 "$HERE/gen_configs.py" \
      --workflow "$WORKFLOW" \
      --mappings-dir "$MAPPINGS_DIR" \
      --out-dir "$OUT_DIR"
    ;;

  mapping)
    MAPPING="${1:-}"; OUT_CONN="${2:-connections.json}"; OUT_VARS="${3:-vars.json}"
    if [[ -z "$MAPPING" ]]; then
      echo "Usage: $0 mapping <mapping.json> [out_connections] [out_vars]" >&2
      exit 1
    fi
    python3 "$HERE/gen_configs.py" \
      --mapping "$MAPPING" \
      --out-connections "$OUT_CONN" \
      --out-vars "$OUT_VARS"
    ;;

  *)
    echo "Usage:" >&2
    echo "  $0 workflow <workflow.json> <mappings_dir> [out_dir]" >&2
    echo "  $0 mapping  <mapping.json> [out_connections] [out_vars]" >&2
    exit 1
    ;;
esac
