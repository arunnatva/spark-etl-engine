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


        inputs = parse_session_input_files(s)
        outputs = parse_session_output_files(s)

        sources = parse_source_connections(s)
        sources = enrich_sources_with_paths(sources, inputs)

        targets = enrich_targets_with_paths(targets, outputs)

        sessions[sname] = {
            "session_name": sname,
            "mapping_name": attr(s, "MAPPINGNAME"),

            "sources": sources,
            "targets": targets,
            "transform_connections": parse_transform_connections(s), 
            "session_attributes": parse_session_attributes(s)
        }



    # task instances (nodes in the workflow DAG)
    #task_types = {}
    #for ti in root.findall(".//TASKINSTANCE"):
    #    task_types[attr(ti, "NAME")] = attr(ti, "TASKTYPE")


    # task instances (nodes in the workflow DAG)
    task_types = {}

    for ti in root.findall(".//TASKINSTANCE"):

        ti_name = attr(ti, "NAME")
        ti_type = attr(ti, "TASKTYPE")

        task_types[ti_name] = ti_type

        # Informatica reusable sessions:
        #
        # TASKINSTANCE.NAME     = workflow node name
        # TASKINSTANCE.TASKNAME = actual SESSION.NAME
        #
        # Example:
        #   NAME     = s_CSS_STG_TO_WRK
        #   TASKNAME = s_m_CSS_STG_TO_WRK
        #
        # run_workflow.py uses TASKINSTANCE names from the DAG,
        # so duplicate the session metadata under the workflow task name.
        if ti_type == "Session":

            session_name = attr(ti, "TASKNAME")

            if (
                session_name
                and session_name in sessions
                and ti_name not in sessions
            ):
                sessions[ti_name] = dict(sessions[session_name])
                sessions[ti_name]["session_name"] = ti_name


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



def normalize_file_path(path):
    """
    Convert Informatica file paths to runtime S3 paths.

    Example:
        /prod/edl/consumer/vfeth/str/vflh_wrk/tg_res_wrk/tg_res_wrk.txt

    becomes:

        s3a://edl-cdp-dev/consumer/vfeth/str/vflh_wrk/tg_res_wrk
    """

    if not path:
        return path

    path = path.strip()

    # Replace Informatica HDFS root with S3 root
    if path.startswith("/prod/edl/"):
        path = path.replace(
            "/prod/edl/",
            "s3a://edl-cdp-dev/",
            1
        )

    path = path.rstrip("/")

    # Remove filename if it looks like a .txt file
    if path.endswith(".txt"):
        path = path[:path.rfind("/")]

    return path


def parse_session_input_files(session_elem):
    """
    Extract input file metadata from HDFS readers.
    """

    inputs = []

    for se in session_elem.findall(".//SESSIONEXTENSION"):

        if attr(se, "TYPE") != "READER":
            continue

        file_path = None

        for a in se.findall("ATTRIBUTE"):

            if attr(a, "NAME") == "File Path":
                file_path = normalize_file_path(
                    attr(a, "VALUE")
                )

        if not file_path:
            continue

        conn = None

        for cr in se.findall("CONNECTIONREFERENCE"):
            conn = (
                attr(cr, "VARIABLE")
                or attr(cr, "CONNECTIONNAME")
            )

        inputs.append({
            "instance": attr(se, "SINSTANCENAME"),
            "path": file_path,
            "connection": conn,
            "subtype": attr(se, "SUBTYPE"),
            "type": "file"
        })

    return inputs



def parse_session_output_files(session_elem):
    """
    Extract output file metadata from HDFS writers.
    """

    outputs = []

    for se in session_elem.findall(".//SESSIONEXTENSION"):

        if attr(se, "TYPE") != "WRITER":
            continue

        output_path = None
        hive_table = None

        for a in se.findall("ATTRIBUTE"):

            name = attr(a, "NAME")

            if name == "Output File Path":
                output_path = normalize_file_path(
                    attr(a, "VALUE")
                )

            elif name == "Hive Table Name":
                hive_table = attr(a, "VALUE")

        if not output_path:
            continue

        conn = None

        for cr in se.findall("CONNECTIONREFERENCE"):
            conn = (
                attr(cr, "VARIABLE")
                or attr(cr, "CONNECTIONNAME")
            )

        outputs.append({
            "instance": attr(se, "SINSTANCENAME"),
            "path": output_path,
            "hive_table": hive_table,
            "connection": conn,
            "subtype": attr(se, "SUBTYPE"),
            "type": "file"
        })

    return outputs




def enrich_sources_with_paths(sources, inputs):
    """
    Merge file source path information into source metadata, and type every
    source (file vs jdbc) by its reader subtype.

    The reader binding (path/connection) is attached to the Source Qualifier
    instance in the workflow, so file paths land on the SQ_* keys. Relational
    readers have no File Path attribute, so they never appear in `inputs`;
    we still must type them as 'jdbc' from their subtype.
    """

    input_map = {
        x["instance"]: x
        for x in inputs
    }

    for src_name, src_meta in sources.items():
        subtype = (src_meta.get("subtype") or "").lower()

        # 1) merge any file-path metadata collected from HDFS/flat-file readers
        if src_name in input_map:
            inp = input_map[src_name]
            if ("hdfs" in subtype or "flat file" in subtype):
                src_meta["type"] = "file"
                src_meta["path"] = inp.get("path")
            if inp.get("subtype"):
                src_meta["subtype"] = inp["subtype"]

        # 2) type by subtype regardless of whether a file path was present.
        #    This is what makes relational (Oracle) sources resolve to jdbc.
        if src_meta.get("type") in (None, "null"):
            if "relational" in subtype:
                src_meta["type"] = "jdbc"
            elif "hdfs" in subtype or "flat file" in subtype:
                # a flat-file reader with no path yet: still a file source; the
                # path may sit on the paired SQ_* entry
                src_meta["type"] = "file"
            else:
                src_meta.setdefault("type", "null")

    return sources


def enrich_targets_with_paths(targets, outputs):
    """
    Merge file target path information into target metadata.
    """

    output_map = {
        x["instance"]: x
        for x in outputs
    }

    for tgt_name, tgt_meta in targets.items():

        if tgt_name in output_map:

            out = output_map[tgt_name]

            tgt_meta["type"] = "file"
            tgt_meta["path"] = out.get("path")

            if out.get("hive_table"):
                tgt_meta["hive_table"] = out["hive_table"]

        else:
            tgt_meta.setdefault("type", "jdbc")

    return targets



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
