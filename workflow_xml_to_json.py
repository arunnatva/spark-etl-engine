#!/usr/bin/env python3
"""
workflow_xml_to_json.py
=======================
Convert an Informatica PowerCenter *workflow* XML export into a JSON that the
orchestration layer (run_workflow.py) consumes.

The workflow layer supplies what a mapping XML cannot:
  * which physical table each mapping target actually writes to
    (SESSTRANSFORMATIONINST 'Target Table Name' override)
  * per-target load semantics: Insert / Update as Update / Update else Insert /
    Delete / Truncate  (SESSIONEXTENSION 'Relational Writer' attributes)
  * the connection each session/target binds to (CONNECTIONREFERENCE VARIABLE)
  * the session -> mapping binding
  * the execution DAG across sessions with success/failure conditions
    (TASKINSTANCE + WORKFLOWLINK)

Usage:
    python workflow_xml_to_json.py --input wf_X.XML --output wf_X.json
"""
import argparse
import json
import xml.etree.ElementTree as ET


def attr(e, name, default=None):
    return e.attrib.get(name, default) if e is not None else default


# Informatica writer checkbox flags -> normalized load spec
def _yn(v):
    return str(v).strip().upper() == "YES"


def parse_target_load(session_elem):
    """Return {target_instance_name: {table, load_flags, connection}}."""
    targets = {}

    # 1) physical table-name override lives in SESSTRANSFORMATIONINST
    for sti in session_elem.findall(".//SESSTRANSFORMATIONINST"):
        if "Target" not in (attr(sti, "TRANSFORMATIONTYPE") or ""):
            continue
        inst = attr(sti, "SINSTANCENAME") or attr(sti, "TRANSFORMATIONNAME")
        tbl = None
        for a in sti.findall("ATTRIBUTE"):
            if attr(a, "NAME") == "Target Table Name":
                tbl = attr(a, "VALUE")
        targets.setdefault(inst, {})["table"] = tbl

    # 2) load flags + connection live in the WRITER SESSIONEXTENSION
    for se in session_elem.findall(".//SESSIONEXTENSION"):
        if attr(se, "TYPE") != "WRITER":
            continue
        inst = attr(se, "SINSTANCENAME")
        flags = {}
        for a in se.findall("ATTRIBUTE"):
            n, v = attr(a, "NAME"), attr(a, "VALUE")
            flags[n] = v
        conn = None
        for cr in se.findall("CONNECTIONREFERENCE"):
            conn = attr(cr, "VARIABLE") or attr(cr, "CONNECTIONNAME")
        entry = targets.setdefault(inst, {})
        entry["connection"] = conn
        entry["load"] = {
            "insert": _yn(flags.get("Insert")),
            "update_as_update": _yn(flags.get("Update as Update")),
            "update_as_insert": _yn(flags.get("Update as Insert")),
            "update_else_insert": _yn(flags.get("Update else Insert")),
            "delete": _yn(flags.get("Delete")),
            "truncate": _yn(flags.get("Truncate target table option")),
            "target_load_type": flags.get("Target load type", "Normal"),
        }
        entry["reject_file"] = flags.get("Reject filename")
    return targets


def parse_source_connections(session_elem):
    """Return {source_instance_name: {subtype, connection}} from READER extensions."""
    sources = {}
    for se in session_elem.findall(".//SESSIONEXTENSION"):
        if attr(se, "TYPE") != "READER":
            continue
        inst = attr(se, "SINSTANCENAME")
        conn = None
        for cr in se.findall("CONNECTIONREFERENCE"):
            conn = attr(cr, "VARIABLE") or attr(cr, "CONNECTIONNAME")
        sources[inst] = {
            "subtype": attr(se, "SUBTYPE"),
            "connection": conn,
        }
    return sources


# Session-level attributes we care about for execution fidelity. Everything else
# (log paths, DTM buffer, perf flags, recovery, pushdown) is Informatica-runtime
# concern with no Spark equivalent and is deliberately dropped.
SESSION_ATTRS_OF_INTEREST = {
    "Treat source rows as": "treat_source_rows_as",
    "Commit Interval": "commit_interval",
    "Commit Type": "commit_type",
    "Commit On End Of File": "commit_on_eof",
    "Rollback Transactions on Errors": "rollback_on_error",
    "Enable high precision": "high_precision",
    "Enable Test Load": "test_load",
}


def parse_session_attributes(session_elem):
    out = {}
    for a in session_elem.findall("ATTRIBUTE"):
        n = attr(a, "NAME")
        if n in SESSION_ATTRS_OF_INTEREST:
            out[SESSION_ATTRS_OF_INTEREST[n]] = attr(a, "VALUE")
    return out


def parse_transform_connections(session_elem):
    """Return {transform_instance: connection_variable} for lookups and any
    transformation instance that declares 'Connection Information'."""
    conns = {}
    for sti in session_elem.findall(".//SESSTRANSFORMATIONINST"):
        inst = attr(sti, "SINSTANCENAME")
        for a in sti.findall("ATTRIBUTE"):
            if attr(a, "NAME") == "Connection Information":
                v = attr(a, "VALUE")
                if v:
                    conns[inst] = v
    return conns


def derive_load_mode(load):
    """Collapse Informatica writer flags into a single engine write mode.

    Returns one of: 'append', 'upsert', 'update', 'delete', 'truncate_insert'.
    Precedence mirrors Informatica session behavior:
      - Truncate + Insert           -> truncate_insert (full refresh)
      - Update else Insert          -> upsert (merge; insert when no match)
      - Update as Update (only)     -> update (merge; no insert)
      - Delete                      -> delete
      - Insert only                 -> append
    Note: Informatica ultimately routes rows by the mapping's Update Strategy
    (DD_INSERT/UPDATE/DELETE). These flags are the *session gate* on what the
    writer is allowed to do. The engine combines both: a row tagged DD_UPDATE
    is only applied if the session permits update, etc.
    """
    if not load:
        return "append"
    if load.get("truncate") and load.get("insert"):
        return "truncate_insert"
    if load.get("update_else_insert"):
        return "upsert"
    if load.get("update_as_update") and not load.get("insert"):
        return "update"
    if load.get("delete") and not load.get("insert"):
        return "delete"
    if load.get("insert"):
        return "append"
    return "append"


def parse_assignments(root):
    """Return {variable_name: expression} set by Assignment tasks.

    These are workflow-level variable assignments (e.g. $$CaptureRunTime) that
    mappings may reference in expressions. They are computed at runtime; we
    surface the expression so the orchestrator can evaluate/pass them as vars."""
    assigns = {}
    for t in root.findall(".//TASK"):
        if attr(t, "TYPE") != "Assignment":
            continue
        for vp in t.findall(".//VALUEPAIR"):
            name, val = attr(vp, "NAME"), attr(vp, "VALUE")
            if name:
                assigns[name] = val
    return assigns


def parse_workflow(xml_path):
    root = ET.parse(xml_path).getroot()
    wf = root.find(".//WORKFLOW")
    wf_name = attr(wf, "NAME")

    # sessions -> mapping + target/source overrides
    sessions = {}
    for s in root.findall(".//SESSION"):
        sname = attr(s, "NAME")
        targets = parse_target_load(s)
        for tinst, tspec in targets.items():
            tspec["load_mode"] = derive_load_mode(tspec.get("load"))
        sessions[sname] = {
            "session_name": sname,
            "mapping_name": attr(s, "MAPPINGNAME"),
            "targets": targets,
            "sources": parse_source_connections(s),
            "transform_connections": parse_transform_connections(s),
            "session_attributes": parse_session_attributes(s),
        }

    # task instances (nodes in the workflow DAG)
    task_types = {}
    for ti in root.findall(".//TASKINSTANCE"):
        task_types[attr(ti, "NAME")] = attr(ti, "TASKTYPE")

    # workflow links (edges) with conditions
    links = []
    for wl in root.findall(".//WORKFLOWLINK"):
        links.append({
            "from": attr(wl, "FROMTASK"),
            "to": attr(wl, "TOTASK"),
            "condition": (attr(wl, "CONDITION") or "").strip(),
        })

    # unique connection variables referenced anywhere (for connections.json keys)
    conn_vars = {}
    for cr in root.findall(".//CONNECTIONREFERENCE"):
        v = attr(cr, "VARIABLE")
        if v:
            conn_vars[v] = attr(cr, "CONNECTIONTYPE")

    return {
        "workflow_name": wf_name,
        "sessions": sessions,
        "tasks": task_types,
        "links": links,
        "assignments": parse_assignments(root),
        "connection_variables": conn_vars,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    doc = parse_workflow(args.input)
    json.dump(doc, open(args.output, "w"), indent=2)

    n_sess = len(doc["sessions"])
    n_sess_tasks = sum(1 for t in doc["tasks"].values() if t == "Session")
    print(f"Workflow: {doc['workflow_name']}")
    print(f"  sessions={n_sess}  tasks={len(doc['tasks'])} "
          f"({n_sess_tasks} session tasks)  links={len(doc['links'])}")
    print(f"  session -> mapping:")
    for sname, s in doc["sessions"].items():
        nt = len(s["targets"])
        print(f"    {sname:38s} -> {s['mapping_name']}  ({nt} targets)")


if __name__ == "__main__":
    main()
