#!/usr/bin/env python3
"""
run_workflow.py
===============
Orchestration layer for the config-driven Spark ETL engine.

Reads a workflow JSON (from workflow_xml_to_json.py), resolves the session
execution DAG, and runs each session in dependency order by invoking the
generic mapping engine (mapping_engine.py) with that session's target
overrides (physical table name + load mode) applied.

It ties the two config layers together:
  * workflow JSON  -> orchestration: order, per-session target load semantics
  * mapping JSON   -> dataflow: the transformations for each session's mapping

Usage:
    spark-submit run_workflow.py \
        --workflow wf_run_strategy.json \
        --mappings-dir ./mappings \
        --connections connections.json \
        --vars vars.json

    # preview the resolved plan without running Spark:
    python run_workflow.py --workflow wf_run_strategy.json \
        --mappings-dir ./mappings --plan-only
"""
import argparse
import json
import os
from collections import defaultdict, deque

from mapping_engine import (
    MappingModel, MappingEngine, ConnectionRegistry, build_spark,
)


class WorkflowPlan:
    """Resolves the session-level execution DAG from the workflow JSON."""

    def __init__(self, wf_doc):
        self.wf = wf_doc
        self.name = wf_doc.get("workflow_name")
        self.sessions = wf_doc.get("sessions", {})
        self.tasks = wf_doc.get("tasks", {})
        self.links = wf_doc.get("links", [])
        self.session_names = set(
            n for n, t in self.tasks.items() if t == "Session"
        )
        # some sessions may not be registered as tasks; include keys too
        self.session_names |= set(self.sessions.keys())
        self.adj = self._collapse_to_sessions()
        self.order = self._topo_sort()

    def _raw_adj(self):
        adj = defaultdict(list)
        for l in self.links:
            adj[l["from"]].append(l["to"])
        return adj

    def _collapse_to_sessions(self):
        """Collapse non-session tasks (Start/Assignment/etc) so edges connect
        sessions directly, walking through intermediate control tasks."""
        raw = self._raw_adj()

        def next_sessions(start):
            out, seen = [], set()
            q = deque(raw[start])
            while q:
                n = q.popleft()
                if n in seen:
                    continue
                seen.add(n)
                if n in self.session_names:
                    out.append(n)
                else:
                    q.extend(raw[n])
            return out

        return {s: next_sessions(s) for s in self.session_names}

    def _topo_sort(self):
        indeg = {s: 0 for s in self.session_names}
        for s, nxts in self.adj.items():
            for n in nxts:
                indeg[n] = indeg.get(n, 0) + 1
        q = deque(sorted([s for s in self.session_names if indeg[s] == 0]))
        order = []
        while q:
            n = q.popleft()
            order.append(n)
            for m in sorted(self.adj.get(n, [])):
                indeg[m] -= 1
                if indeg[m] == 0:
                    q.append(m)
        if len(order) != len(self.session_names):
            missing = [s for s in self.session_names if s not in order]
            order.extend(sorted(missing))  # break cycles deterministically
        return order

    def predecessors(self):
        preds = defaultdict(list)
        for s, nxts in self.adj.items():
            for n in nxts:
                preds[n].append(s)
        return preds


def _deep_merge(base, override):
    """Recursively merge override into a copy of base (override wins)."""
    out = dict(base)
    for k, v in (override or {}).items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _load_json(path):
    with open(path) as f:
        return json.load(f)


def load_layered_config(base_path, per_mapping_dir, mapping_name, kind):
    """Build a config dict for one mapping by layering:
        1. shared base file (base_path), applied to every mapping
        2. optional per-mapping file  <per_mapping_dir>/<mapping_name>.<kind>.json
           (or <mapping_name>.json inside a kind-named subdir)
    Per-mapping values override the shared base. Either layer may be absent.

    kind is 'connections' or 'vars' (used only to locate the per-mapping file).
    """
    merged = {}
    if base_path and os.path.exists(base_path):
        merged = _load_json(base_path)

    if per_mapping_dir and mapping_name:
        candidates = [
            os.path.join(per_mapping_dir, f"{mapping_name}.{kind}.json"),
            os.path.join(per_mapping_dir, kind, f"{mapping_name}.json"),
            os.path.join(per_mapping_dir, f"{mapping_name}.json"),
        ]
        for c in candidates:
            if os.path.exists(c):
                merged = _deep_merge(merged, _load_json(c))
                break
    return merged


def find_mapping_file(mappings_dir, mapping_name):
    """Locate the mapping JSON for a mapping name in the mappings dir."""
    candidates = [
        os.path.join(mappings_dir, f"{mapping_name}.json"),
        os.path.join(mappings_dir, f"{mapping_name}.JSON"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    # fall back: scan for a file whose JSON 'mapping.name' matches
    for fn in os.listdir(mappings_dir):
        if not fn.lower().endswith(".json"):
            continue
        path = os.path.join(mappings_dir, fn)
        try:
            with open(path) as f:
                doc = json.load(f)
            if doc.get("mapping", {}).get("name") == mapping_name:
                return path
        except Exception:
            continue
    return None


def run(args):
    with open(args.workflow) as f:
        wf_doc = json.load(f)
    plan = WorkflowPlan(wf_doc)

    print(f"=== Workflow: {plan.name} ===")
    print(f"Sessions ({len(plan.order)}) in execution order:")
    preds = plan.predecessors()
    for i, s in enumerate(plan.order, 1):
        sess = plan.sessions.get(s, {})
        mp = sess.get("mapping_name", "?")
        dep = preds.get(s, [])
        dep_s = f"  after: {', '.join(dep)}" if dep else "  (no deps)"
        print(f"  {i}. {s:36s} -> {mp}{dep_s}")

    # Resolve mapping files up front so plan-only can report gaps
    resolved = {}
    missing = []
    for s in plan.order:
        mp = plan.sessions.get(s, {}).get("mapping_name")
        mf = find_mapping_file(args.mappings_dir, mp) if mp else None
        resolved[s] = mf
        if mf is None:
            missing.append((s, mp))

    if missing:
        print("\n[!] Missing mapping JSONs for:")
        for s, mp in missing:
            print(f"    session {s} -> mapping {mp}")

    if args.plan_only:
        print("\n(plan-only: not executing)")
        return

    # Shared base configs (apply to every mapping). Per-mapping override files,
    # if present in --config-dir, are layered on top inside the loop.
    base_connections = {}
    if args.connections and os.path.exists(args.connections):
        base_connections = _load_json(args.connections)
    base_vars = {}
    if args.vars and os.path.exists(args.vars):
        base_vars = _load_json(args.vars)
    # workflow-level assignment variables (e.g. $$CaptureRunTime) apply to all
    wf_assignments = wf_doc.get("assignments") or {}

    spark = build_spark(plan.name or "WorkflowRunner")
    spark.sparkContext.setLogLevel("WARN")

    status = {}   # session -> 'succeeded' | 'failed' | 'skipped'
    for s in plan.order:
        sess = plan.sessions.get(s, {})
        mp = sess.get("mapping_name")
        mf = resolved.get(s)

        # Single-session mode: run just the named session, skip the rest.
        if args.only_session and s != args.only_session:
            continue

        # Gate on predecessors if requested
        if args.honor_conditions:
            dep_status = [status.get(d) for d in preds.get(s, [])]
            if any(st in ("failed", "skipped") for st in dep_status):
                print(f"\n[SKIP] {s} (upstream not succeeded)")
                status[s] = "skipped"
                continue

        if mf is None:
            print(f"\n[SKIP] {s}: no mapping JSON for {mp}")
            status[s] = "skipped"
            continue

        # Build this session's effective config: shared base + per-mapping override
        conn_dict = _deep_merge(
            base_connections,
            load_layered_config(None, args.config_dir, mp, "connections")
            if args.config_dir else {})
        var_dict = _deep_merge(
            base_vars,
            load_layered_config(None, args.config_dir, mp, "vars")
            if args.config_dir else {})
        # workflow assignments fill in only where the mapping/base didn't set them
        for vn, vexpr in wf_assignments.items():
            var_dict.setdefault(vn, vexpr)

        print(f"\n{'='*60}\n[SESSION] {s}  (mapping {mp})\n{'='*60}")
        try:
            doc = _load_json(mf)
            model = MappingModel(doc)
            overrides = sess.get("targets", {})
            engine = MappingEngine(
                spark, model, ConnectionRegistry(conns=conn_dict), var_dict,
                session_overrides=overrides,
                session_attributes=sess.get("session_attributes", {}),
                transform_connections=sess.get("transform_connections", {}),
            )
            engine.run()
            status[s] = "succeeded"
        except Exception as e:
            print(f"[ERROR] session {s} failed: {e}")
            status[s] = "failed"
            if not args.continue_on_error and args.honor_conditions:
                print("Stopping workflow (use --continue-on-error to proceed).")
                break

    spark.stop()

    print(f"\n=== Workflow {plan.name} summary ===")
    for s in plan.order:
        print(f"  {status.get(s, 'not-run'):10s}  {s}")


def main():
    ap = argparse.ArgumentParser(description="Run an Informatica workflow JSON on Spark")
    ap.add_argument("--workflow", required=True, help="workflow JSON file")
    ap.add_argument("--mappings-dir", required=True,
                    help="directory containing mapping JSON files")
    ap.add_argument("--connections", help="shared base connections JSON (applies to all mappings)")
    ap.add_argument("--vars", help="shared base vars JSON (applies to all mappings)")
    ap.add_argument("--config-dir",
                    help="optional dir of per-mapping override files "
                         "(<mapping>.connections.json / <mapping>.vars.json); "
                         "these layer on top of the shared base")
    ap.add_argument("--only-session",
                    help="run just this one session (skips the rest of the DAG)")
    ap.add_argument("--plan-only", action="store_true",
                    help="print the resolved execution plan and exit")
    ap.add_argument("--honor-conditions", action="store_true",
                    help="gate each session on predecessor success (Status=succeeded)")
    ap.add_argument("--continue-on-error", action="store_true",
                    help="with --honor-conditions, keep going after a failure")
    args = ap.parse_args()
    run(args)


if __name__ == "__main__":
    main()
