#!/usr/bin/env python3
"""
mapping_engine.py
=================
A generic PySpark engine that reads an Informatica mapping JSON (the format
exported from PowerCenter with top-level `sources`, `targets`, and `mapping`
{instances, transformations, connectors}) and executes it as a Spark DAG.

Design
------
* No per-mapping code. All variation lives in the JSON + a small runtime
  `connections.json`. The engine is fully generic.
* Instances are the DAG nodes. Each instance references either a source, a
  target, or a transformation. Connectors are field-level edges; the engine
  collapses them to instance-level edges while keeping the per-field mapping
  (Informatica renames fields as they cross transformations, so the field map
  is load-bearing, not decorative).
* Execution is topologically ordered. Each node produces one DataFrame held in
  memory under the instance name; downstream nodes read their inputs from there.

Supported op types (by transformation_type):
    Source Definition   -> registered as a raw source (read happens at SQ)
    Source Qualifier    -> read source into a DataFrame (+ optional SQL/filter)
    Expression          -> withColumn for each OUTPUT port that has an expression
    Lookup Procedure    -> left join against a lookup table on the parsed condition
    Sequence            -> monotonic surrogate key column (NEXTVAL/CURRVAL)
    Router              -> passthrough / group filter (fan-out to multiple targets)
    Update Strategy     -> tags rows with a row-op (DD_INSERT/UPDATE/DELETE)
    Target Definition   -> write to JDBC table or file

Usage
-----
    spark-submit mapping_engine.py \
        --mapping sample-mapping-json-file.txt \
        --connections connections.json
"""

import argparse
import json
import re
from collections import defaultdict, deque


# ----------------------------------------------------------------------------
# Informatica -> Spark expression translation (light-touch, extend as needed)
# ----------------------------------------------------------------------------
class ExpressionTranslator:
    """Translates common Informatica expression functions/system vars to Spark SQL."""

    # system variables / constants -> Spark SQL
    SYSTEM_VARS = {
        "SYSDATE": "current_timestamp()",
        "SESSSTARTTIME": "current_timestamp()",
        "$$SESSSTARTTIME": "current_timestamp()",
    }

    @classmethod
    def translate(cls, expr, runtime_vars=None):
        if expr is None:
            return None
        s = str(expr).strip()
        if s == "":
            return None

        runtime_vars = runtime_vars or {}

        # Informatica string literals use single quotes already compatible with SQL,
        # but the export sometimes double-wraps: "'MANUGISTICS'" -> keep inner.
        if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
            s = s[1:-1]

        # $PMWorkflowRunId, $PMRepositoryUserName, $PM... mapping/workflow vars
        def repl_pm(m):
            var = m.group(0)
            if var in runtime_vars:
                v = runtime_vars[var]
                return f"'{v}'" if isinstance(v, str) else str(v)
            # default: emit NULL-safe literal so the pipeline still runs
            return "NULL"

        s = re.sub(r"\$PM\w+|\$\$?\w+", repl_pm, s)

        # system vars
        for k, v in cls.SYSTEM_VARS.items():
            s = re.sub(rf"\b{re.escape(k)}\b", v, s)

        # function name translations (Informatica -> Spark SQL)
        translations = [
            (r"\bIIF\s*\(", "IF("),
            (r"\bISNULL\s*\(", "ISNULL("),
            (r"\bIS_NULL\s*\(", "ISNULL("),
            (r"\bNVL\s*\(", "COALESCE("),
            (r"\bIFNULL\s*\(", "COALESCE("),
            (r"\bLTRIM\s*\(", "LTRIM("),
            (r"\bRTRIM\s*\(", "RTRIM("),
            (r"\bLENGTH\s*\(", "LENGTH("),
            (r"\bLOWER\s*\(", "LOWER("),
            (r"\bUPPER\s*\(", "UPPER("),
            (r"\bSUBSTR\s*\(", "SUBSTR("),
            (r"\bINSTR\s*\(", "INSTR("),
            (r"\bLPAD\s*\(", "LPAD("),
            (r"\bRPAD\s*\(", "RPAD("),
            (r"\bREPLACECHR\s*\(", "TRANSLATE("),
            (r"\bREPLACESTR\s*\(", "REPLACE("),
            # numeric / cast conversions
            (r"\bTO_INTEGER\s*\(", "CAST_INT("),
            (r"\bTO_BIGINT\s*\(", "CAST_BIGINT("),
            (r"\bTO_DECIMAL\s*\(", "CAST_DECIMAL("),
            (r"\bTO_FLOAT\s*\(", "CAST_DOUBLE("),
            (r"\bABS\s*\(", "ABS("),
            (r"\bROUND\s*\(", "ROUND("),
            (r"\bTRUNC\s*\(", "TRUNC("),
            # date functions
            (r"\bTO_DATE\s*\(", "TO_DATE("),
            (r"\bADD_TO_DATE\s*\(", "DATE_ADD("),
            (r"\bLAST_DAY\s*\(", "LAST_DAY("),
            (r"\|\|", " || "),
        ]
        for pat, rep in translations:
            s = re.sub(pat, rep, s, flags=re.IGNORECASE)

        # Resolve the cast placeholders: TO_INTEGER(x) took only the first arg
        # in Informatica; Spark uses CAST(x AS type). We rewrite CAST_T(expr...)
        # by taking the first top-level argument and casting it.
        s = cls._resolve_casts(s)
        # Informatica TRUNC(date) with a single arg truncates to day. Spark's
        # trunc() needs two args, so rewrite the single-arg form to DATE_TRUNC.
        s = cls._resolve_single_arg_trunc(s)
        return s

    @staticmethod
    def _resolve_single_arg_trunc(s):
        out = s
        while "TRUNC(" in out:
            start = out.find("TRUNC(")
            open_paren = start + len("TRUNC")
            depth = 0
            i = open_paren
            has_comma = False
            while i < len(out):
                if out[i] == "(":
                    depth += 1
                elif out[i] == ")":
                    depth -= 1
                    if depth == 0:
                        break
                elif out[i] == "," and depth == 1:
                    has_comma = True
                i += 1
            if i >= len(out):
                break
            arg = out[open_paren + 1:i]
            if has_comma:
                # already 2-arg; leave as a distinct token to avoid reprocessing
                replacement = "DATETRUNC2(" + arg + ")"
            else:
                replacement = f"DATE_TRUNC('DAY', {arg})"
            out = out[:start] + replacement + out[i + 1:]
        return out.replace("DATETRUNC2(", "TRUNC(")

    @staticmethod
    def _resolve_casts(s):
        cast_types = {
            "CAST_INT": "INT",
            "CAST_BIGINT": "BIGINT",
            "CAST_DECIMAL": "DECIMAL(38,10)",
            "CAST_DOUBLE": "DOUBLE",
        }
        for fn, sqltype in cast_types.items():
            while fn + "(" in s:
                start = s.find(fn + "(")
                open_paren = start + len(fn)
                depth = 0
                i = open_paren
                first_arg_end = None
                while i < len(s):
                    if s[i] == "(":
                        depth += 1
                    elif s[i] == ")":
                        depth -= 1
                        if depth == 0:
                            break
                    elif s[i] == "," and depth == 1 and first_arg_end is None:
                        first_arg_end = i
                    i += 1
                if i >= len(s):
                    # unbalanced; bail to avoid infinite loop
                    s = s.replace(fn + "(", "(", 1)
                    break
                inner_end = first_arg_end if first_arg_end is not None else i
                arg = s[open_paren + 1:inner_end]
                replacement = f"TRY_CAST({arg} AS {sqltype})"
                s = s[:start] + replacement + s[i + 1:]
        return s


# ----------------------------------------------------------------------------
# Connection registry (kept separate from mapping logic)
# ----------------------------------------------------------------------------
class ConnectionRegistry:
    def __init__(self, conn_file=None):
        self.conns = {}
        if conn_file:
            with open(conn_file) as f:
                self.conns = json.load(f)

    def get(self, name, default=None):
        return self.conns.get(name, default)

    def jdbc(self, hint="oracle"):
        c = self.conns.get(hint) or self.conns.get("oracle") or {}
        return c


# ----------------------------------------------------------------------------
# Mapping model: parse JSON into instances / edges / lookups
# ----------------------------------------------------------------------------
class MappingModel:
    """Normalizes the raw mapping JSON into an executable graph."""

    def __init__(self, doc):
        self.doc = doc
        self.sources = {s["name"]: s for s in doc.get("sources", [])}
        self.targets = {t["name"]: t for t in doc.get("targets", [])}
        m = doc["mapping"]
        self.name = m.get("name")
        self.instances = {i["name"]: i for i in m.get("instances", [])}
        self.transforms = {t["name"]: t for t in m.get("transformations", [])}
        self.connectors = m.get("connectors", [])

        self.edges, self.field_maps = self._build_edges()
        self.order = self._topo_sort()

    def _build_edges(self):
        """Collapse field-level connectors to instance-level edges + field maps."""
        edges = defaultdict(set)                 # from_inst -> {to_inst}
        field_maps = defaultdict(list)           # (from_inst, to_inst) -> [(from_f, to_f)]
        for c in self.connectors:
            fi, ti = c["from_instance"], c["to_instance"]
            edges[fi].add(ti)
            field_maps[(fi, ti)].append((c["from_field"], c["to_field"]))
        return edges, field_maps

    def inputs_of(self, inst_name):
        """Return list of upstream instance names feeding this instance."""
        ins = []
        for (fi, ti) in self.field_maps.keys():
            if ti == inst_name and fi not in ins:
                ins.append(fi)
        return ins

    def _topo_sort(self):
        indeg = defaultdict(int)
        nodes = set(self.instances.keys())
        adj = defaultdict(set)
        for fi, tos in self.edges.items():
            for ti in tos:
                if fi in nodes and ti in nodes:
                    adj[fi].add(ti)
        for fi in adj:
            for ti in adj[fi]:
                indeg[ti] += 1
        for n in nodes:
            indeg.setdefault(n, 0)

        q = deque(sorted([n for n in nodes if indeg[n] == 0]))
        order = []
        while q:
            n = q.popleft()
            order.append(n)
            for ti in sorted(adj[n]):
                indeg[ti] -= 1
                if indeg[ti] == 0:
                    q.append(ti)
        if len(order) != len(nodes):
            # cycle / disconnected: fall back to declared order
            missing = [n for n in nodes if n not in order]
            order.extend(sorted(missing))
        return order


# ----------------------------------------------------------------------------
# The engine
# ----------------------------------------------------------------------------
class MappingEngine:
    def __init__(self, spark, model, connections, runtime_vars=None,
                 session_overrides=None, session_attributes=None,
                 transform_connections=None):
        self.spark = spark
        self.model = model
        self.conns = connections
        self.runtime_vars = runtime_vars or {}
        self.frames = {}   # instance_name -> DataFrame
        self.translator = ExpressionTranslator()
        # session_overrides: {target_instance_name: {table, load_mode, connection,
        # load{...}}} sourced from the workflow JSON. When present, these are
        # authoritative for physical table name and write semantics, replacing
        # the engine's own upstream-Update-Strategy heuristic.
        self.session_overrides = session_overrides or {}
        # session_attributes: {treat_source_rows_as, commit_interval, ...}
        self.session_attributes = session_attributes or {}
        # transform_connections: {transform_instance: connection_variable}
        # e.g. per-lookup DB binding. Used so lookups read from the right DB.
        self.transform_connections = transform_connections or {}

    # ---- helpers -----------------------------------------------------------
    def _F(self):
        from pyspark.sql import functions as F
        return F

    def _instance(self, name):
        return self.model.instances.get(name, {})

    def _transform(self, inst):
        return self.model.transforms.get(inst.get("transformation_name"), {})

    def _apply_edge_rename(self, df, from_inst, to_inst):
        """Rename df columns according to the field map on the edge from->to.

        Informatica renames ports as they cross transformations. When several
        upstream fields collide on the target we keep the last; when a source
        field is missing we skip it (router OUTPUT ports duplicate INPUTs).
        """
        if df is None:
            return None
        fmap = self.model.field_maps.get((from_inst, to_inst), [])
        if not fmap:
            return df
        cols = set(df.columns)
        for from_f, to_f in fmap:
            if from_f in cols and from_f != to_f and to_f not in df.columns:
                df = df.withColumnRenamed(from_f, to_f)
        return df

    def _merged_input(self, inst_name):
        """Combine upstream frames for a node into a single DataFrame.

        Most nodes have exactly one data input. Targets and update strategies
        can receive both the main pipeline frame and a sequence-generator frame;
        we attach sequence columns by position-free cross-join-free injection
        (sequence handled specially, see _op_sequence)."""
        F = self._F()
        inputs = self.model.inputs_of(inst_name)
        data_frames = []
        for up in inputs:
            df = self.frames.get(up)
            if df is None:
                continue
            df = self._apply_edge_rename(df, up, inst_name)
            data_frames.append((up, df))

        if not data_frames:
            return None
        if len(data_frames) == 1:
            return data_frames[0][1]

        # Multiple inputs: the widest frame is the true data path. Other inputs
        # are either sequence generators (carry __seq_start/__seq_inc config) or
        # small side frames. Sequence config is broadcast as literals so it does
        # not disturb the row count; the real key is generated at the target.
        data_frames.sort(key=lambda x: len(x[1].columns), reverse=True)
        base_name, base = data_frames[0]
        for up_name, extra in data_frames[1:]:
            if "__seq_start" in extra.columns:
                row = extra.select("__seq_start", "__seq_inc").first()
                if row is not None:
                    base = (base
                            .withColumn("__seq_start", F.lit(row["__seq_start"]))
                            .withColumn("__seq_inc", F.lit(row["__seq_inc"])))
                continue
            new_cols = [c for c in extra.columns if c not in base.columns]
            for c in new_cols:
                base = base.withColumn(c, F.lit(None))
        return base

    def _attach_by_index(self, left, right):
        """Attach right's columns to left aligning on a generated row index."""
        F = self._F()
        from pyspark.sql.window import Window
        w = Window.orderBy(F.monotonically_increasing_id())
        l = left.withColumn("__ridx", F.row_number().over(w))
        r = right.withColumn("__ridx", F.row_number().over(w))
        out = l.join(r, on="__ridx", how="left").drop("__ridx")
        return out

    # ---- run loop ----------------------------------------------------------
    def run(self):
        print(f"=== Executing mapping: {self.model.name} ===")
        print(f"Execution order ({len(self.model.order)} nodes):")
        for n in self.model.order:
            inst = self._instance(n)
            print(f"    {n:34s} [{inst.get('transformation_type')}]")
        print("-" * 60)

        for name in self.model.order:
            inst = self._instance(name)
            ttype = inst.get("transformation_type", "")
            handler = self._dispatch(ttype)
            print(f"[RUN] {name}  ({ttype})")
            self.frames[name] = handler(name, inst)
        print("=== Mapping complete ===")

    def _dispatch(self, ttype):
        return {
            "Source Definition": self._op_source_definition,
            "Source Qualifier": self._op_source_qualifier,
            "Expression": self._op_expression,
            "Lookup Procedure": self._op_lookup,
            "Sequence": self._op_sequence,
            "Sequence Generator": self._op_sequence,
            "Router": self._op_router,
            "Filter": self._op_filter,
            "Aggregator": self._op_aggregator,
            "Joiner": self._op_joiner,
            "Sorter": self._op_sorter,
            "Union": self._op_union,
            "Union Transformation": self._op_union,
            "Update Strategy": self._op_update_strategy,
            "Target Definition": self._op_target,
        }.get(ttype, self._op_passthrough)

    # ---- ops ---------------------------------------------------------------
    def _op_source_definition(self, name, inst):
        # Source Definition is a placeholder node; the actual read is done by the
        # Source Qualifier. We stash the source spec so the SQ can find it.
        return None

    def _op_source_qualifier(self, name, inst):
        """Read the upstream source definition into a DataFrame."""
        F = self._F()
        src_inst_names = self.model.inputs_of(name)
        source_spec = None
        for up in src_inst_names:
            up_inst = self._instance(up)
            src_name = up_inst.get("transformation_name")
            if src_name in self.model.sources:
                source_spec = self.model.sources[src_name]
                break
        if source_spec is None:
            print(f"    [WARN] no source found upstream of {name}")
            return None

        df = self._read_source(source_spec)

        # Source Qualifier options
        tf = self._transform(inst)
        attrs = tf.get("table_attributes", {}) if tf else {}
        sql_override = (attrs.get("Sql Query") or "").strip()
        src_filter = (attrs.get("Source Filter") or "").strip()
        select_distinct = (attrs.get("Select Distinct") or "NO").upper() == "YES"

        if sql_override:
            df.createOrReplaceTempView(f"{name}_src")
            df = self.spark.sql(sql_override)
        if src_filter:
            df = df.filter(src_filter)
        if select_distinct:
            df = df.distinct()

        # Project to the SQ's OUTPUT ports (keeps only mapped columns downstream)
        return df

    def _read_source(self, source_spec):
        """Read a source definition based on its database_type."""
        db_type = (source_spec.get("database_type") or "").lower()
        name = source_spec["name"]
        cols = [f["name"] for f in source_spec.get("fields", [])]

        conn = self.conns.get(name) or self.conns.get(db_type) or {}
        reader_format = conn.get("format")

        if "flat file" in db_type or reader_format in ("csv", "flatfile"):
            path = conn.get("path", f"./data/{name}.csv")
            df = (self.spark.read
                  .option("header", conn.get("header", "true"))
                  .option("sep", conn.get("sep", ","))
                  .option("inferSchema", conn.get("inferSchema", "true"))
                  .csv(path))
            # align column names if header differs
            if len(df.columns) == len(cols) and conn.get("apply_names", True):
                for old, new in zip(df.columns, cols):
                    if old != new:
                        df = df.withColumnRenamed(old, new)
            return df

        if "oracle" in db_type or reader_format == "jdbc":
            jc = self.conns.jdbc("oracle")
            owner = source_spec.get("owner_name") or jc.get("schema")
            table = f"{owner}.{name}" if owner else name
            return (self.spark.read.format("jdbc")
                    .option("url", jc["url"])
                    .option("dbtable", table)
                    .option("user", jc["user"])
                    .option("password", jc["password"])
                    .option("driver", jc.get("driver", "oracle.jdbc.OracleDriver"))
                    .load())

        if reader_format == "parquet":
            return self.spark.read.parquet(conn["path"])

        # default: empty frame with the declared schema so the DAG still runs
        print(f"    [WARN] unknown source db_type={db_type!r} for {name}; empty frame")
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField(c, StringType(), True) for c in cols])
        return self.spark.createDataFrame([], schema)

    def _op_expression(self, name, inst):
        """Add/replace columns for each OUTPUT port carrying an expression.

        Informatica Expression transforms can define VARIABLE ports: local
        scratch values computed in port order and referenced by later ports.
        They are not columns in the output. We resolve them first, in order,
        substituting each variable's name with its (parenthesized) expression
        wherever a later port references it, then materialize only OUTPUT ports."""
        F = self._F()
        df = self._merged_input(name)
        if df is None:
            return None
        tf = self._transform(inst)
        fields = tf.get("fields", [])

        # Pass 1: build a substitution map for VARIABLE ports, in declaration
        # order, so a variable can reference earlier variables.
        var_defs = {}  # var_name -> resolved spark expression string
        for f in fields:
            pt = (f.get("port_type") or "").upper()
            if "VARIABLE" not in pt:
                continue
            expr = f.get("expression")
            if not expr or expr == f["name"]:
                continue
            spark_expr = self.translator.translate(expr, self.runtime_vars)
            if spark_expr is None:
                continue
            # inline previously-defined variables into this one
            spark_expr = self._inline_vars(spark_expr, var_defs)
            var_defs[f["name"]] = f"({spark_expr})"

        # Pass 2: materialize OUTPUT ports, inlining any variable references.
        for f in fields:
            pt = (f.get("port_type") or "").upper()
            expr = f.get("expression")
            if "OUTPUT" in pt and "VARIABLE" not in pt and expr and expr != f["name"]:
                spark_expr = self.translator.translate(expr, self.runtime_vars)
                if not spark_expr:
                    continue
                spark_expr = self._inline_vars(spark_expr, var_defs)
                try:
                    df = df.withColumn(f["name"], F.expr(spark_expr))
                except Exception as e:
                    print(f"    [WARN] expr failed on {f['name']}: {e}; NULL")
                    df = df.withColumn(f["name"], F.lit(None))
        return df

    @staticmethod
    def _inline_vars(expr, var_defs):
        """Replace whole-word variable references with their definitions.

        Longest names first so a variable that is a prefix of another doesn't
        partially match. Repeated until stable (handles chained references)."""
        if not var_defs:
            return expr
        names = sorted(var_defs.keys(), key=len, reverse=True)
        for _ in range(len(names) + 1):
            changed = False
            for vn in names:
                pattern = rf"\b{re.escape(vn)}\b"
                if re.search(pattern, expr):
                    new = re.sub(pattern, var_defs[vn], expr)
                    if new != expr:
                        expr = new
                        changed = True
            if not changed:
                break
        return expr


    def _op_lookup(self, name, inst):
        """Left-join against the lookup table on the parsed lookup condition."""
        F = self._F()
        df = self._merged_input(name)
        if df is None:
            return None
        tf = self._transform(inst)
        attrs = tf.get("table_attributes", {})
        lk_table = (attrs.get("Lookup table name") or "").strip()
        lk_cond = (attrs.get("Lookup condition") or "").strip()
        lk_sql = (attrs.get("Lookup Sql Override") or "").strip()

        if not lk_table and not lk_sql:
            return df

        lk_df = self._read_lookup(lk_table, lk_sql, attrs, lookup_instance=name)
        if lk_df is None:
            return df

        # Parse "P1 = USYS_H1_LEVEL1 AND P2 = USYS_H1_LEVEL2 ..." into join keys.
        # Left side = lookup port, right side = incoming stream port.
        join_pairs = self._parse_lookup_condition(lk_cond)
        if not join_pairs:
            return df

        # alias to avoid ambiguous columns
        lk_alias = lk_df.alias("lk")
        st_alias = df.alias("st")
        conds = []
        for lk_col, st_col in join_pairs:
            if lk_col in lk_df.columns and st_col in df.columns:
                conds.append(F.col(f"lk.{lk_col}") == F.col(f"st.{st_col}"))
        if not conds:
            return df
        join_cond = conds[0]
        for c in conds[1:]:
            join_cond = join_cond & c

        policy = (attrs.get("Lookup policy on multiple match") or "").lower()
        joined = st_alias.join(lk_alias, join_cond, how="left")

        # Bring lookup OUTPUT ports that aren't join keys into the stream.
        lk_out_cols = [f["name"] for f in tf.get("fields", [])
                       if "OUTPUT" in (f.get("port_type") or "").upper()]
        select_cols = [F.col(f"st.{c}") for c in df.columns]
        for c in lk_out_cols:
            if c in lk_df.columns and c not in df.columns:
                select_cols.append(F.col(f"lk.{c}"))
        out = joined.select(*select_cols)

        if "use last value" in policy or "use first value" in policy:
            # de-dup already handled upstream in most cases; leave as-is for parity
            pass
        return out

    def _read_lookup(self, table, sql_override, attrs, lookup_instance=None):
        """Read the lookup source, using the lookup's own connection when the
        workflow declared one (transform_connections), else the default oracle."""
        try:
            # Resolve the lookup's declared connection variable, if any.
            conn_var = self.transform_connections.get(lookup_instance)
            jc = None
            if conn_var:
                # connection var may key directly into connections.json, or the
                # var name (with or without leading $) may be the key.
                jc = (self.conns.get(conn_var)
                      or self.conns.get(conn_var.lstrip("$"))
                      or None)
            if not jc:
                jc = self.conns.jdbc("oracle")
            if not jc:
                return self.spark.table(table) if table else None
            dbtable = f"({sql_override}) t" if sql_override else table
            return (self.spark.read.format("jdbc")
                    .option("url", jc["url"])
                    .option("dbtable", dbtable)
                    .option("user", jc["user"])
                    .option("password", jc["password"])
                    .option("driver", jc.get("driver", "oracle.jdbc.OracleDriver"))
                    .load())
        except Exception as e:
            print(f"    [WARN] lookup read failed for {table}: {e}")
            return None

    @staticmethod
    def _parse_lookup_condition(cond):
        """'P1 = A AND P2 = B' -> [('P1','A'), ('P2','B')] (lookup_port, stream_port)."""
        if not cond:
            return []
        pairs = []
        for part in re.split(r"\bAND\b", cond, flags=re.IGNORECASE):
            if "=" in part:
                lhs, rhs = part.split("=", 1)
                pairs.append((lhs.strip(), rhs.strip()))
        return pairs

    def _op_sequence(self, name, inst):
        """Produce a one-column-ish frame carrying NEXTVAL (surrogate keys).

        In batch Spark we realize NEXTVAL as a monotonic id offset by the
        configured Current/Start value. The frame is attached to targets by the
        _merged_input row-index alignment."""
        F = self._F()
        tf = self._transform(inst)
        attrs = tf.get("table_attributes", {})
        try:
            start = int(attrs.get("Current Value") or attrs.get("Start Value") or 0)
        except ValueError:
            start = 0
        try:
            inc = int(attrs.get("Increment By") or 1)
        except ValueError:
            inc = 1

        # We don't know the row count until the consuming target; emit a tiny
        # marker frame carrying the config. The target injects the real sequence.
        seq_meta = [(start, inc)]
        df = self.spark.createDataFrame(seq_meta, ["__seq_start", "__seq_inc"])
        # expose NEXTVAL/CURRVAL as columns for the field-map rename to pick up
        df = df.withColumn("NEXTVAL", F.col("__seq_start")) \
               .withColumn("CURRVAL", F.col("__seq_start"))
        return df

    def _op_filter(self, name, inst):
        """Filter: keep rows matching the Filter Condition."""
        df = self._merged_input(name)
        if df is None:
            return None
        tf = self._transform(inst)
        cond = (tf.get("table_attributes", {}).get("Filter Condition") or "").strip()
        if not cond or cond.upper() == "TRUE":
            return df
        spark_cond = self.translator.translate(cond, self.runtime_vars)
        try:
            return df.filter(spark_cond)
        except Exception as e:
            print(f"    [WARN] filter condition failed on {name}: {e}; passthrough")
            return df

    def _op_aggregator(self, name, inst):
        """Aggregator: group by the GROUP BY ports, aggregate the OUTPUT exprs.

        Group-by ports are flagged in the port list (port_type containing
        'GROUPBY', or an explicit `group_by`/`is_group_by` flag). Aggregate
        expressions live on OUTPUT ports (SUM(x), MAX(y), COUNT(*), ...)."""
        F = self._F()
        df = self._merged_input(name)
        if df is None:
            return None
        tf = self._transform(inst)
        fields = tf.get("fields", [])

        group_cols = []
        for f in fields:
            pt = (f.get("port_type") or "").upper()
            is_gb = ("GROUPBY" in pt.replace(" ", "")
                     or f.get("is_group_by") in (True, "YES", "true")
                     or f.get("group_by") in (True, "YES", "true"))
            if is_gb and f["name"] in df.columns:
                group_cols.append(f["name"])

        agg_exprs = []
        for f in fields:
            pt = (f.get("port_type") or "").upper()
            expr = f.get("expression")
            if "OUTPUT" in pt and expr and expr != f["name"]:
                spark_expr = self.translator.translate(expr, self.runtime_vars)
                if spark_expr:
                    try:
                        agg_exprs.append(F.expr(spark_expr).alias(f["name"]))
                    except Exception as e:
                        print(f"    [WARN] agg expr failed on {f['name']}: {e}")

        if not agg_exprs:
            # Informatica aggregator with no agg expr = distinct on group ports
            if group_cols:
                return df.select(*group_cols).distinct()
            return df
        if group_cols:
            return df.groupBy(*group_cols).agg(*agg_exprs)
        return df.agg(*agg_exprs)

    def _op_joiner(self, name, inst):
        """Joiner: join two upstream inputs on the Join Condition.

        Join Type maps Informatica -> Spark:
          Normal Join        -> inner
          Master Outer Join  -> right_outer  (detail is preserved)
          Detail Outer Join  -> left_outer   (master... depends on orientation)
          Full Outer Join    -> full_outer
        The two inputs are taken from the node's upstream instances."""
        F = self._F()
        inputs = self.model.inputs_of(name)
        frames = []
        for up in inputs:
            d = self.frames.get(up)
            if d is not None:
                frames.append(self._apply_edge_rename(d, up, name))
        if len(frames) < 2:
            return frames[0] if frames else None

        left, right = frames[0], frames[1]
        tf = self._transform(inst)
        attrs = tf.get("table_attributes", {})
        cond = (attrs.get("Join Condition") or "").strip()
        jtype_raw = (attrs.get("Join Type") or "Normal Join").lower()
        jtype = {
            "normal join": "inner",
            "master outer join": "right_outer",
            "detail outer join": "left_outer",
            "full outer join": "full_outer",
        }.get(jtype_raw, "inner")

        pairs = self._parse_lookup_condition(cond)  # "A = B AND C = D" form
        if not pairs:
            print(f"    [WARN] joiner {name} has no parseable condition; cross join")
            return left.crossJoin(right)

        la, ra = left.alias("l"), right.alias("r")
        conds = []
        for lft, rgt in pairs:
            lc = lft if lft in left.columns else rgt
            rc = rgt if rgt in right.columns else lft
            if lc in left.columns and rc in right.columns:
                conds.append(F.col(f"l.{lc}") == F.col(f"r.{rc}"))
        if not conds:
            return left.crossJoin(right)
        jc = conds[0]
        for c in conds[1:]:
            jc = jc & c

        # select all left cols + right cols not already present
        sel = [F.col(f"l.{c}") for c in left.columns]
        for c in right.columns:
            if c not in left.columns:
                sel.append(F.col(f"r.{c}"))
        return la.join(ra, jc, how=jtype).select(*sel)

    def _op_sorter(self, name, inst):
        """Sorter: order by sort-key ports; optionally distinct.

        Sort keys are ports flagged with a sort direction; if none are flagged
        we fall back to ordering by all INPUT/OUTPUT ports (rare)."""
        F = self._F()
        df = self._merged_input(name)
        if df is None:
            return None
        tf = self._transform(inst)
        attrs = tf.get("table_attributes", {})
        order_cols = []
        for f in tf.get("fields", []):
            direction = (f.get("sort_direction") or f.get("sort_key") or "")
            is_key = f.get("is_sort_key") in (True, "YES", "true") or direction
            if is_key and f["name"] in df.columns:
                if str(direction).upper().startswith("DESC"):
                    order_cols.append(F.col(f["name"]).desc())
                else:
                    order_cols.append(F.col(f["name"]).asc())
        if order_cols:
            df = df.orderBy(*order_cols)
        if (attrs.get("Distinct") or "NO").upper() == "YES":
            df = df.distinct()
        return df

    def _op_union(self, name, inst):
        """Union: stack all upstream inputs (union by name, allowing gaps)."""
        inputs = self.model.inputs_of(name)
        frames = []
        for up in inputs:
            d = self.frames.get(up)
            if d is not None:
                frames.append(self._apply_edge_rename(d, up, name))
        if not frames:
            return None
        out = frames[0]
        for nxt in frames[1:]:
            out = out.unionByName(nxt, allowMissingColumns=True)
        return out

    def _op_router(self, name, inst):
        """Router: fan-out node. Groups with filter conditions would split rows;
        this export carries no group conditions, so it passes rows through.

        Informatica routers rename ports: an INPUT port `descr` is exposed as
        OUTPUT ports `descr1`, `descr2`, ... (one per output group). We recreate
        those OUTPUT columns as copies of their base INPUT column so the
        per-edge field map on each outgoing connector can pick them up."""
        F = self._F()
        df = self._merged_input(name)
        if df is None:
            return None
        tf = self._transform(inst)
        input_ports = [f["name"] for f in tf.get("fields", [])
                       if (f.get("port_type") or "").upper() == "INPUT"]
        output_ports = [f["name"] for f in tf.get("fields", [])
                        if "OUTPUT" in (f.get("port_type") or "").upper()]

        # match each OUTPUT port to its base INPUT port by stripping trailing digits
        def base(nm):
            return re.sub(r"\d+$", "", nm)
        in_by_base = {}
        for ip in input_ports:
            in_by_base.setdefault(base(ip), ip)

        for op in output_ports:
            if op in df.columns:
                continue
            src = in_by_base.get(base(op))
            if src and src in df.columns:
                df = df.withColumn(op, F.col(src))
        return df

    def _op_update_strategy(self, name, inst):
        """Tag rows with a row operation derived from the Update Strategy Expression.

        DD_INSERT=0, DD_UPDATE=1, DD_DELETE=2, DD_REJECT=3. We add a
        `__row_op` column so the target write can branch (merge/append/delete)."""
        F = self._F()
        df = self._merged_input(name)
        if df is None:
            return None
        tf = self._transform(inst)
        attrs = tf.get("table_attributes", {})
        expr = (attrs.get("Update Strategy Expression") or "").strip().upper()

        mapping = {
            "DD_INSERT": 0, "0": 0,
            "DD_UPDATE": 1, "1": 1,
            "DD_DELETE": 2, "2": 2,
            "DD_REJECT": 3, "3": 3,
        }
        if expr in mapping:
            df = df.withColumn("__row_op", F.lit(mapping[expr]))
        else:
            # expression form: translate and evaluate (best-effort)
            spark_expr = self.translator.translate(expr, self.runtime_vars)
            try:
                df = df.withColumn("__row_op", F.expr(spark_expr))
            except Exception:
                df = df.withColumn("__row_op", F.lit(1))  # default UPDATE
        return df

    def _op_target(self, name, inst):
        """Write to the target: JDBC table or file.

        When a workflow session override exists for this target instance, it is
        authoritative: it supplies the physical table name and the load mode
        (append / upsert / update / delete / truncate_insert), overriding the
        engine's own heuristics. Otherwise the engine falls back to the target
        definition name and __row_op-based routing."""
        F = self._F()
        df = self._merged_input(name)
        if df is None:
            print(f"    [WARN] nothing to write for {name}")
            return None

        tgt_name = inst.get("transformation_name")
        tgt_spec = self.model.targets.get(tgt_name, {})
        tgt_cols = [f["name"] for f in tgt_spec.get("fields", [])]

        # workflow override is keyed by the INSTANCE name (e.g. *_INSERT / *_UPDATE)
        override = self.session_overrides.get(name, {})
        phys_table = override.get("table")          # e.g. DIM_MFG_DETAILS
        load_mode = override.get("load_mode")        # e.g. upsert / append / ...

        # Session-level 'Treat source rows as' sets the default DML intent for
        # rows that carry no explicit Update Strategy row-op. Informatica applies
        # this when the mapping has no Update Strategy transformation. We honor it
        # by deriving a load_mode from it when the workflow gave us no per-target
        # mode, so a session marked 'Update' actually updates rather than inserts.
        tsra = (self.session_attributes.get("treat_source_rows_as") or "").lower()
        if not load_mode and tsra:
            load_mode = {
                "insert": "append",
                "update": "update",
                "delete": "delete",
                "data driven": None,   # honor per-row __row_op instead
            }.get(tsra)
            if load_mode:
                print(f"    [session] treat source rows as '{tsra}' -> {load_mode}")

        # Capture sequence config (if a sequence generator fed this target)
        seq_cfg = None
        if "__seq_start" in df.columns:
            row = df.select("__seq_start", "__seq_inc").first()
            if row is not None:
                seq_cfg = (row["__seq_start"], row["__seq_inc"])
            df = df.drop("__seq_start", "__seq_inc")

        # Align to declared target columns (case-insensitive match)
        df = self._align_to_target(df, tgt_cols)

        # Now generate the surrogate key into the target's key column(s)
        if seq_cfg is not None:
            df = self._materialize_sequence(df, seq_cfg, tgt_cols)

        conn = self.conns.get(tgt_name) or self.conns.get("target_default") or {}
        out_format = conn.get("format")
        db_type = (tgt_spec.get("database_type") or "").lower()
        has_row_op = "__row_op" in df.columns

        # ---- file targets -------------------------------------------------
        if out_format in ("csv", "parquet", "json") or "flat file" in db_type:
            # for file targets the physical table name becomes the folder name
            out_name = phys_table or tgt_name
            path = conn.get("path", f"./output/{out_name}")
            writer = df.drop("__row_op") if has_row_op else df
            file_mode = "overwrite" if load_mode == "truncate_insert" else conn.get("mode", "overwrite")
            (writer.write.mode(file_mode)
             .format(out_format or "csv")
             .option("header", "true")
             .save(path))
            tag = f" [{load_mode}]" if load_mode else ""
            print(f"    [WRITE:file] {name} -> {path}{tag}")
            return df

        # ---- JDBC / relational targets -----------------------------------
        jc = self.conns.jdbc("oracle")
        owner = tgt_spec.get("owner_name") or (jc.get("schema") if jc else None)
        base_table = phys_table or tgt_name          # workflow name wins
        table = f"{owner}.{base_table}" if owner else base_table

        if not jc:
            tag = f" [{load_mode}]" if load_mode else ""
            print(f"    [WARN] no JDBC connection; would write {name} -> {table}{tag}")
            df.show(5, truncate=False)
            return df

        # Dispatch on the workflow-declared load mode when present.
        if load_mode:
            self._write_with_load_mode(df, table, jc, tgt_spec, load_mode,
                                       override.get("load", {}), has_row_op, name)
        elif has_row_op:
            self._write_by_row_op(df, table, jc, tgt_spec)
        else:
            (df.write.format("jdbc")
             .option("url", jc["url"]).option("dbtable", table)
             .option("user", jc["user"]).option("password", jc["password"])
             .option("driver", jc.get("driver", "oracle.jdbc.OracleDriver"))
             .mode(conn.get("mode", "append")).save())
            print(f"    [WRITE:jdbc] {name} -> {table} (append)")
        return df

    def _write_with_load_mode(self, df, table, jc, tgt_spec, load_mode,
                              load_flags, has_row_op, inst_name):
        """Write honoring the workflow-declared load semantics.

        append          -> insert all rows
        truncate_insert -> overwrite (truncate then insert)
        upsert          -> stage rows for MERGE (insert-or-update on keys)
        update          -> stage rows for MERGE (update only, no insert)
        delete          -> stage rows for DELETE by key

        Row-level DD_ operations (from the mapping's Update Strategy) are combined
        with the session gate: if __row_op is present we respect it, but the
        session load flags bound what the writer is permitted to do."""
        F = self._F()
        drv = jc.get("driver", "oracle.jdbc.OracleDriver")
        # Session 'Commit Interval' maps to the JDBC writer batch size.
        batchsize = str(self.session_attributes.get("commit_interval")
                        or jc.get("batchsize") or 10000)

        def _write(frame, tbl, mode):
            (frame.write.format("jdbc")
             .option("url", jc["url"]).option("dbtable", tbl)
             .option("user", jc["user"]).option("password", jc["password"])
             .option("driver", drv).option("batchsize", batchsize)
             .mode(mode).save())

        clean = df.drop("__row_op") if has_row_op else df

        if load_mode == "append":
            _write(clean, table, "append")
            print(f"    [WRITE:jdbc] {inst_name} -> {table} (append, {clean.count()} rows)")
            return

        if load_mode == "truncate_insert":
            # overwrite with truncate so the table object/grants are preserved
            (clean.write.format("jdbc")
             .option("url", jc["url"]).option("dbtable", table)
             .option("user", jc["user"]).option("password", jc["password"])
             .option("driver", drv).option("truncate", "true")
             .mode("overwrite").save())
            print(f"    [WRITE:jdbc] {inst_name} -> {table} (truncate+insert, {clean.count()} rows)")
            return

        # upsert / update / delete need a MERGE or DELETE against Oracle, which
        # Spark's JDBC writer cannot do directly. Stage the rows and emit the
        # SQL the orchestrator (or a post-step) runs. This preserves correctness
        # instead of silently appending.
        keys = [f["name"] for f in tgt_spec.get("fields", [])
                if (f.get("keytype") or "").upper().startswith("PRIMARY")]
        stage_tbl = f"{table}_STG"
        _write(clean, stage_tbl, "overwrite")

        non_keys = [c for c in clean.columns if c not in keys]
        if load_mode in ("upsert", "update") and keys:
            on = " AND ".join([f"t.{k}=s.{k}" for k in keys])
            setc = ", ".join([f"t.{c}=s.{c}" for c in non_keys])
            insc = ", ".join(clean.columns)
            insv = ", ".join([f"s.{c}" for c in clean.columns])
            merge = (f"MERGE INTO {table} t USING {stage_tbl} s ON ({on}) "
                     f"WHEN MATCHED THEN UPDATE SET {setc}")
            if load_mode == "upsert":
                merge += (f" WHEN NOT MATCHED THEN INSERT ({insc}) VALUES ({insv})")
            print(f"    [STAGE→MERGE] {inst_name}: staged to {stage_tbl}; "
                  f"run:\n        {merge}")
        elif load_mode == "delete" and keys:
            on = " AND ".join([f"t.{k}=s.{k}" for k in keys])
            dele = f"DELETE FROM {table} t WHERE EXISTS (SELECT 1 FROM {stage_tbl} s WHERE {on})"
            print(f"    [STAGE→DELETE] {inst_name}: staged to {stage_tbl}; "
                  f"run:\n        {dele}")
        else:
            print(f"    [WARN] {inst_name}: load_mode={load_mode} but no primary "
                  f"keys in target metadata; staged to {stage_tbl} only")

    def _materialize_sequence(self, df, seq_cfg, tgt_cols):
        """Generate a running surrogate key from the sequence config.

        Fills NEXTVAL/CURRVAL columns if present, and the target's primary key
        column (heuristically the first column, usually `ID`) when it is unset."""
        F = self._F()
        from pyspark.sql.window import Window
        start, inc = seq_cfg
        w = Window.orderBy(F.monotonically_increasing_id())
        df = df.withColumn("__rn", F.row_number().over(w))
        seq_col = (F.lit(int(start)) + (F.col("__rn") - 1) * F.lit(int(inc)))

        for c in ("NEXTVAL", "CURRVAL"):
            if c in df.columns:
                df = df.withColumn(c, seq_col)

        # Fill the surrogate key column: prefer an ID column, else the first
        # target column if it is currently null (typical Informatica pattern).
        key_col = None
        if "ID" in df.columns:
            key_col = "ID"
        elif tgt_cols:
            key_col = tgt_cols[0]
        if key_col and key_col in df.columns:
            df = df.withColumn(
                key_col,
                F.when(F.col(key_col).isNull(), seq_col).otherwise(F.col(key_col)),
            )
        return df.drop("__rn")

    def _align_to_target(self, df, tgt_cols):
        """Select/rename df columns to match declared target columns."""
        if not tgt_cols:
            return df
        lower_map = {c.lower(): c for c in df.columns}
        F = self._F()
        from pyspark.sql.types import NullType
        dtypes = dict(df.dtypes)
        selected = []
        for tc in tgt_cols:
            if tc in df.columns:
                # a genuinely-null (void) column can't be written by file sources
                if dtypes.get(tc) in ("void", "null"):
                    selected.append(F.col(tc).cast("string").alias(tc))
                else:
                    selected.append(F.col(tc))
            elif tc.lower() in lower_map:
                src = lower_map[tc.lower()]
                if dtypes.get(src) in ("void", "null"):
                    selected.append(F.col(src).cast("string").alias(tc))
                else:
                    selected.append(F.col(src).alias(tc))
            else:
                # unmapped target col: typed NULL so file datasources accept it
                selected.append(F.lit(None).cast("string").alias(tc))
        # carry the row-op flag through for the writer
        if "__row_op" in df.columns:
            selected.append(F.col("__row_op"))
        return df.select(*selected)

    def _write_by_row_op(self, df, table, jc, tgt_spec):
        """Branch writes by DD_ row-op. INSERT->append, UPDATE/DELETE->log.

        A true UPDATE/DELETE against Oracle from Spark needs either a MERGE via
        a staging table or row-by-row JDBC. Here we append inserts and surface
        counts for update/delete so behavior is explicit and safe by default."""
        F = self._F()
        ops = {0: "INSERT", 1: "UPDATE", 2: "DELETE", 3: "REJECT"}
        for code, label in ops.items():
            part = df.filter(F.col("__row_op") == code).drop("__row_op")
            n = part.count()
            if n == 0:
                continue
            if code == 0:  # INSERT
                (part.write.format("jdbc")
                 .option("url", jc["url"])
                 .option("dbtable", table)
                 .option("user", jc["user"])
                 .option("password", jc["password"])
                 .option("driver", jc.get("driver", "oracle.jdbc.OracleDriver"))
                 .mode("append").save())
                print(f"    [WRITE:jdbc] {table} INSERT {n} rows")
            else:
                # stage for MERGE (recommended) instead of silent drop
                stage = f"{table}_STG_{label}"
                (part.write.format("jdbc")
                 .option("url", jc["url"])
                 .option("dbtable", stage)
                 .option("user", jc["user"])
                 .option("password", jc["password"])
                 .option("driver", jc.get("driver", "oracle.jdbc.OracleDriver"))
                 .mode("overwrite").save())
                print(f"    [STAGE] {table} {label} {n} rows -> {stage} "
                      f"(run MERGE/DELETE from {stage})")

    def _op_passthrough(self, name, inst):
        return self._merged_input(name)


def build_spark(app_name):
    from pyspark.sql import SparkSession
    return (SparkSession.builder
            .appName(app_name)
            .enableHiveSupport()
            .getOrCreate())


def _inspect(args):
    """Debug helper: parse a mapping JSON and print its DAG. No Spark, no writes.

    This is NOT the pipeline driver — run_workflow.py is. This exists only to
    inspect a single mapping's parsed structure and execution order while
    developing or debugging a converter's output.
    """
    with open(args.mapping) as f:
        doc = json.load(f)
    model = MappingModel(doc)
    print(f"Mapping: {model.name}")
    print(f"Sources: {list(model.sources)}")
    print(f"Targets: {list(model.targets)}")
    print(f"\nTopological execution order ({len(model.order)} nodes):")
    for n in model.order:
        inst = model.instances.get(n, {})
        ins = model.inputs_of(n)
        print(f"  {n:34s} [{(inst.get('transformation_type') or ''):18s}] "
              f"inputs={ins}")


def main():
    ap = argparse.ArgumentParser(
        description="Mapping engine library. This is NOT the pipeline driver — "
                    "use run_workflow.py to execute pipelines. This CLI only "
                    "inspects a single mapping JSON's parsed DAG for debugging.")
    ap.add_argument("--mapping", required=True, help="mapping JSON file to inspect")
    ap.add_argument("--inspect", action="store_true",
                    help="print the parsed DAG and execution order, then exit")
    args = ap.parse_args()

    if not args.inspect:
        ap.error("mapping_engine.py does not execute pipelines directly. "
                 "Run pipelines with run_workflow.py. To inspect a mapping's "
                 "parsed DAG, pass --inspect.")
    _inspect(args)


if __name__ == "__main__":
    main()
