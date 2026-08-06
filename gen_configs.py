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
import os
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


def _split_shared_and_per_mapping(mapping_doc):
    """Split a mapping's connection scaffold into (shared, per_mapping).

    Shared = physical infrastructure common across mappings (the 'oracle' block,
    'target_default'). Per-mapping = source/target file paths and table specifics.
    """
    full = scaffold_connections(mapping_doc)
    shared_keys = {"oracle", "target_default"}
    shared = {k: v for k, v in full.items() if k in shared_keys}
    per_mapping = {k: v for k, v in full.items() if k not in shared_keys}
    return shared, per_mapping


def generate_layered(workflow_path, mappings_dir, out_dir):
    """From a workflow JSON + mappings dir, emit a layered config tree:
        <out_dir>/connections.json                     (shared base)
        <out_dir>/vars.json                            (shared base)
        <out_dir>/configs/<mapping>.connections.json   (per-mapping overrides)
        <out_dir>/configs/<mapping>.vars.json          (per-mapping, if any vars)
    """
    import glob
    wf = json.load(open(workflow_path))
    os.makedirs(os.path.join(out_dir, "configs"), exist_ok=True)

    # index mapping JSONs by their mapping.name
    mapping_files = {}
    for fp in glob.glob(os.path.join(mappings_dir, "*.json")):
        try:
            d = json.load(open(fp))
            nm = d.get("mapping", {}).get("name")
            if nm:
                mapping_files[nm] = (fp, d)
        except Exception:
            continue

    shared_conn = {
        "oracle": {
            "url": "jdbc:oracle:thin:@//HOST:1521/SERVICE",
            "user": PLACEHOLDER, "password": "${ORACLE_PWD}",
            "driver": "oracle.jdbc.OracleDriver", "schema": PLACEHOLDER,
            "fetchsize": 10000, "batchsize": 10000,
        },
        "target_default": {"format": "jdbc", "mode": "append"},
    }
    # connection variables referenced by the workflow become shared keys too
    for cvar, ctype in (wf.get("connection_variables") or {}).items():
        shared_conn.setdefault(cvar, {
            "format": "jdbc" if ctype == "Relational" else PLACEHOLDER,
            "url": PLACEHOLDER, "user": PLACEHOLDER, "password": "${PWD}",
        })

    shared_vars = {}
    used_mappings = set()
    for sname, sess in wf.get("sessions", {}).items():
        mp = sess.get("mapping_name")
        if mp:
            used_mappings.add(mp)

    per_written = 0
    for mp in sorted(used_mappings):
        entry = mapping_files.get(mp)
        if not entry:
            continue
        _, mdoc = entry
        _, per_conn = _split_shared_and_per_mapping(mdoc)
        json.dump(per_conn,
                  open(os.path.join(out_dir, "configs", f"{mp}.connections.json"), "w"),
                  indent=2)
        mvars, _ = scaffold_vars(mdoc)
        if mvars:
            for k in mvars:
                shared_vars.setdefault(k, mvars[k])
        per_written += 1

    # workflow assignment variables are shared
    for vn in (wf.get("assignments") or {}):
        shared_vars.setdefault(vn, PLACEHOLDER)

    json.dump(shared_conn, open(os.path.join(out_dir, "connections.json"), "w"), indent=2)
    json.dump(shared_vars, open(os.path.join(out_dir, "vars.json"), "w"), indent=2)

    print(f"Layered config generated under {out_dir}/")
    print(f"  connections.json   (shared: oracle, target_default, "
          f"{len(wf.get('connection_variables') or {})} connection vars)")
    print(f"  vars.json          ({len(shared_vars)} variables)")
    print(f"  configs/           ({per_written} per-mapping connection files)")
    print(f"\nNext: replace every \"{PLACEHOLDER}\" with real values, and set "
          f"source/target paths in the per-mapping files.")


def main():
    ap = argparse.ArgumentParser(
        description="Scaffold connection/vars configs. Single-mapping mode emits "
                    "one flat pair; workflow mode emits a layered shared+per-mapping tree.")
    ap.add_argument("--mapping", help="single mapping JSON (flat mode)")
    ap.add_argument("--out-connections", default="connections.json")
    ap.add_argument("--out-vars", default="vars.json")
    # workflow (layered) mode
    ap.add_argument("--workflow", help="workflow JSON (layered mode)")
    ap.add_argument("--mappings-dir", help="dir of mapping JSONs (layered mode)")
    ap.add_argument("--out-dir", default=".", help="output dir for layered mode")
    args = ap.parse_args()

    if args.workflow:
        if not args.mappings_dir:
            ap.error("--workflow requires --mappings-dir")
        generate_layered(args.workflow, args.mappings_dir, args.out_dir)
        return

    if not args.mapping:
        ap.error("provide --mapping (flat mode) or --workflow + --mappings-dir (layered)")

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
