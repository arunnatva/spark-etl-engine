#!/usr/bin/env python3
"""
gen_configs.py
==============
Scaffold connections.json and vars.json from an Informatica mapping JSON.

It does NOT invent credentials or paths — it emits the correct *keys* (every
source name, every target name, and every workflow/mapping variable referenced
in expressions) with placeholder values you fill in. Run once per mapping, then
edit the placeholders for your environment.

Usage:
    python gen_configs.py --mapping your_mapping.json \
        --out-connections connections.json \
        --out-vars vars.json
"""

import argparse
import json
import re


PLACEHOLDER = "__FILL_ME__"


def scaffold_connections(doc):
    conns = {}

    # Shared JDBC block (referenced by any Oracle source/target)
    conns["oracle"] = {
        "url": "jdbc:oracle:thin:@//HOST:1521/SERVICE",
        "user": PLACEHOLDER,
        "password": "${ORACLE_PWD}",
        "driver": "oracle.jdbc.OracleDriver",
        "schema": PLACEHOLDER,
        "fetchsize": 10000,
        "batchsize": 10000,
    }
    # Default target behavior (used when a target has no explicit entry)
    conns["target_default"] = {"format": "jdbc", "mode": "append"}

    def entry_for(spec, is_target):
        db = (spec.get("database_type") or "").lower()
        if "flat file" in db or "file" in db:
            base = {
                "format": "csv",
                "path": f"{PLACEHOLDER}/{spec['name']}.csv",
                "header": "true",
            }
            if is_target:
                base["mode"] = "overwrite"
            else:
                base["inferSchema"] = "true"
            return base
        if "oracle" in db:
            # JDBC details come from the shared "oracle" block; just declare intent
            e = {"format": "jdbc"}
            if is_target:
                e["mode"] = "append"
            return e
        # unknown / other db: leave a generic stub
        e = {"format": PLACEHOLDER, "path_or_table": PLACEHOLDER}
        if is_target:
            e["mode"] = "append"
        return e

    for s in doc.get("sources", []):
        conns[s["name"]] = entry_for(s, is_target=False)
    for t in doc.get("targets", []):
        conns[t["name"]] = entry_for(t, is_target=True)

    return conns


# Match Informatica variable tokens: $PMFoo, $$MappingVar, $SessionVar
VAR_RE = re.compile(r"\$\$?[A-Za-z_][A-Za-z0-9_]*")

# System vars the engine resolves on its own — no need to supply these
BUILTIN = {"$$SESSSTARTTIME"}


def scaffold_vars(doc):
    found = set()
    m = doc.get("mapping", {})

    def scan_expr(expr):
        if not expr:
            return
        for tok in VAR_RE.findall(str(expr)):
            if tok not in BUILTIN:
                found.add(tok)

    for t in m.get("transformations", []):
        for f in t.get("fields", []):
            scan_expr(f.get("expression"))
        for v in (t.get("table_attributes") or {}).values():
            scan_expr(v)

    # Emit with sensible typed placeholders for the well-known ones
    known_defaults = {
        "$PMWorkflowRunId": 0,
        "$PMWorkflowName": PLACEHOLDER,
        "$PMRepositoryUserName": PLACEHOLDER,
        "$PMMappingName": m.get("name", PLACEHOLDER),
        "$PMIntegrationServiceName": PLACEHOLDER,
        "$PMSessionName": PLACEHOLDER,
    }
    out = {}
    for v in sorted(found):
        out[v] = known_defaults.get(v, PLACEHOLDER)
    return out, sorted(found)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mapping", required=True)
    ap.add_argument("--out-connections", default="connections.json")
    ap.add_argument("--out-vars", default="vars.json")
    args = ap.parse_args()

    doc = json.load(open(args.mapping))

    conns = scaffold_connections(doc)
    json.dump(conns, open(args.out_connections, "w"), indent=2)

    vars_out, found = scaffold_vars(doc)
    json.dump(vars_out, open(args.out_vars, "w"), indent=2)

    print(f"Wrote {args.out_connections}:")
    print(f"  {len(doc.get('sources', []))} sources, "
          f"{len(doc.get('targets', []))} targets + shared 'oracle' block")
    print(f"Wrote {args.out_vars}:")
    if found:
        print(f"  variables referenced in expressions: {', '.join(found)}")
    else:
        print("  (no workflow/mapping variables referenced — file is empty {})")
    print(f"\nNext: replace every \"{PLACEHOLDER}\" with real values.")


if __name__ == "__main__":
    main()
