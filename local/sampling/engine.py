"""The sampling engine: config-driven stratified sampling on Teradata (no Airflow imports).

Like the GRE rules engine, everything a sample selects is configuration rows holding SQL - no field name or
value is known to this code (sql/schema.sql describes every table):

    gre_sampling_config      universe, scope_sql, key columns, size, method, diversity, output table
    gre_sampling_param       run parameters looked up by SQL ({batch_id} = the latest batch, ...)
    gre_sampling_exclusion   exclusion rules, in order (SQL condition, or "selected by an earlier run")
    gre_sampling_priority    priority criteria, in order (SQL condition / expression, or history based)
    gre_sampling_strata      stratification levels (VALUE: by an expression's value, CONDITION: by bucket SQL),
                             each optionally only under some parent buckets (applies_to)
    gre_sampling_bucket      a level's buckets: FRACTION of the parent's target, ALL there is, or the REMAINDER
                             the others leave; a different mix per parent bucket (parent_path)
    gre_sampling_output_col  the columns of the output snapshot of the selected cases

One run = one gre_sampling_config row:

    look up parameters        gre_sampling_param
    audit row (RUNNING)       gre_sampling_audit
    pull                      one SELECT of the scope: key columns + one flag / value column per bucket condition,
                              level, exclusion rule and priority criterion (all evaluated by Teradata)
    exclude                   first matching exclusion rule is the reason; history rules read earlier runs
    order                     priority criteria in order, then priority_rank_sql, then the key columns
    stratify                  per level and parent: FRACTION buckets, then ALL buckets, then REMAINDER buckets
                              take what is left (unused quota flows to them); equal shares are water-filled
    select per leaf           RANKED: in priority order | RANDOM: seeded shuffle | SYSTEMATIC: seeded start,
                              every k-th; rotating across diversity_expr values (diversity_cap per value)
    top up                    shortfall_policy TOPUP: from the rest of the candidates, in priority order
    output snapshot           output_table: the selected cases' gre_sampling_output_col values
    persist (one transaction) selections (+ excluded rows), bucket per level, counts per rule and bucket, output
                              snapshot; earlier runs of the same (config_id, run_key) -> etl_is_curr_ind = 'N';
                              audit row -> COMPLETED
    on failure                gre_sampling_errors row, audit row -> ERROR (nothing half written)

Reproducible: one seed per run (on the audit row); every leaf draws from its own generator seeded with
(seed, bucket path), so a draw never depends on the order buckets are visited.
"""
from __future__ import annotations

import json
import logging
import math
import random
import re
import traceback
from dataclasses import dataclass, field
from datetime import date, timedelta
from fnmatch import fnmatchcase
from typing import Optional

from sampling.db import Connection, error_text, generate_run_id, substitute_params
from sampling.settings import ConfigError, Settings

log = logging.getLogger(__name__)

VALID_METHODS = ("RANKED", "RANDOM", "SYSTEMATIC")
VALID_ROUNDING = ("FLOOR", "ROUND", "CEIL")
QUOTA_MODES = ("FRACTION", "ALL", "REMAINDER")
UNLISTED_POLICIES = ("SHARE", "REMAINDER", "SKIP")
EXCLUSION_TYPES = ("SQL", "PRIOR_SELECTION")
PRIORITY_TYPES = ("CONDITION", "EXPRESSION", "PRIOR_CANDIDATE", "PRIOR_SELECTION")
HISTORY_TYPES = ("PRIOR_CANDIDATE", "PRIOR_SELECTION")
HISTORY_SCOPES = ("CONFIG", "PROJECT")
SHORTFALL_POLICIES = ("TOPUP", "NONE")
OTHER_BUCKET = "OTHER"
NULL_BUCKET = "NULL"
OUTPUT_META_COLS = ("sample_run_id", "config_id", "run_key", "sample_cycle", "case_key", "selection_order",
                    "priority_rank", "stratum_path", "etl_is_curr_ind", "load_datetime")
OUTPUT_KEY_CHUNK = 50
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]{0,127}$")
_NEEDS_ORDER = ("RANKED", "SYSTEMATIC")

BASE_RANK = "smp_base_rank"
DIVERSITY = "smp_div"


def _up(value, default: str = "") -> str:
    return str(value if value not in (None, "") else default).strip().upper()


def _norm(value) -> str:
    """A bucket value as Teradata compares it: trailing blanks ignored, not case specific."""
    return NULL_BUCKET if value is None else str(value).rstrip().upper()


def _label(value) -> str:
    return NULL_BUCKET if value is None else str(value).rstrip()


def _matches(path: str, pattern) -> bool:
    return fnmatchcase(path.upper(), _up(pattern, "*"))


def _specificity(pattern: str) -> tuple:
    p = _up(pattern, "*")
    return (p.count("*") + p.count("?"), -len(p))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Sample:
    """One gre_sampling_config row and its rule rows."""
    config: dict
    params: list = field(default_factory=list)
    exclusions: list = field(default_factory=list)
    priorities: list = field(default_factory=list)
    levels: list = field(default_factory=list)       # each: strata row + "buckets": [bucket rows]
    output_cols: list = field(default_factory=list)

    @property
    def config_id(self):
        return self.config.get("config_id")

    @property
    def method(self) -> str:
        return _up(self.config.get("sampling_method"), "RANKED")

    @property
    def keys(self) -> list:
        return key_columns(self.config)


def key_columns(config: dict) -> list:
    return [c.strip() for c in str(config.get("key_columns") or "").split(",") if c.strip()]


def load_config(conn: Connection, s: Settings, config_id) -> Sample:
    """The active gre_sampling_config row of config_id with its active rule rows, each list in its order."""
    db = s.meta_db
    rows = conn.query(f"SELECT * FROM {db}.gre_sampling_config WHERE config_id = ? AND act_ind = 1", [config_id])
    if not rows:
        raise ConfigError(f"no active gre_sampling_config row for config_id={config_id}")

    def rules(table, order, id_col):
        return conn.query(f"SELECT * FROM {db}.{table} WHERE config_id = ? AND act_ind = 1 "
                          f"ORDER BY {order}, {id_col}", [config_id])

    sample = Sample(config=rows[0],
                    params=rules("gre_sampling_param", "seq_no", "param_id"),
                    exclusions=rules("gre_sampling_exclusion", "seq_no", "exclusion_id"),
                    priorities=rules("gre_sampling_priority", "seq_no", "priority_id"),
                    output_cols=rules("gre_sampling_output_col", "seq_no", "output_col_id"))
    strata = rules("gre_sampling_strata", "level_order", "strata_id")
    buckets = conn.query(f"SELECT b.* FROM {db}.gre_sampling_bucket b "
                         f"JOIN {db}.gre_sampling_strata t ON t.strata_id = b.strata_id "
                         f"WHERE t.config_id = ? AND t.act_ind = 1 AND b.act_ind = 1 "
                         f"ORDER BY b.seq_no, b.bucket_id", [config_id])
    for level in strata:
        level["buckets"] = [b for b in buckets if b["strata_id"] == level["strata_id"]]
        level["value_level"] = bool(str(level.get("stratify_expr") or "").strip())
    sample.levels = strata
    return sample


def config_problems(sample: Sample) -> list:
    """Everything wrong with one sample's rows (no database access); empty = valid."""
    c, problems = sample.config, []
    method = sample.method
    if method not in VALID_METHODS:
        problems.append(f"sampling_method {method!r} must be one of {', '.join(VALID_METHODS)}")
    ordered = any(p for p in sample.priorities if _up(p.get("rule_type"), "CONDITION") in PRIORITY_TYPES) \
        or str(c.get("priority_rank_sql") or "").strip()
    if method in _NEEDS_ORDER and not ordered:
        problems.append(f"sampling_method {method} needs gre_sampling_priority rows or priority_rank_sql")
    if _up(c.get("rounding_mode"), "FLOOR") not in VALID_ROUNDING:
        problems.append(f"rounding_mode must be one of {', '.join(VALID_ROUNDING)}")
    if _up(c.get("shortfall_policy"), "TOPUP") not in SHORTFALL_POLICIES:
        problems.append(f"shortfall_policy must be one of {', '.join(SHORTFALL_POLICIES)}")
    try:
        if int(c.get("target_volume") or 0) < 1:
            problems.append("target_volume must be >= 1")
    except (TypeError, ValueError):
        problems.append(f"target_volume {c.get('target_volume')!r} is not a whole number")
    if not sample.keys:
        problems.append("key_columns is empty")
    if not str(c.get("universe_table") or "").strip():
        problems.append("universe_table is empty")
    if c.get("diversity_cap") not in (None, ""):
        if not str(c.get("diversity_expr") or "").strip():
            problems.append("diversity_cap needs diversity_expr")
        elif int(c["diversity_cap"]) < 1:
            problems.append("diversity_cap must be >= 1")
    if str(c.get("diversity_expr") or "").strip() and method == "SYSTEMATIC":
        problems.append("diversity_expr cannot be combined with SYSTEMATIC (it steps through a fixed order)")

    for p in sample.params:
        if not _IDENT.match(str(p.get("param_name") or "")):
            problems.append(f"param {p.get('param_id')}: param_name must be a plain name")
        if not str(p.get("param_sql") or "").strip():
            problems.append(f"param {p.get('param_name')}: param_sql is empty")

    for x in sample.exclusions:
        name, kind = x.get("exclusion_name") or x.get("exclusion_id"), _up(x.get("rule_type"), "SQL")
        if kind not in EXCLUSION_TYPES:
            problems.append(f"exclusion {name}: rule_type must be one of {', '.join(EXCLUSION_TYPES)}")
        elif kind == "SQL" and not str(x.get("exclusion_sql") or "").strip():
            problems.append(f"exclusion {name}: exclusion_sql is empty")
        elif kind in HISTORY_TYPES:
            problems += _history_problems(f"exclusion {name}", x)

    for p in sample.priorities:
        name, kind = p.get("criterion_name") or p.get("priority_id"), _up(p.get("rule_type"), "CONDITION")
        if kind not in PRIORITY_TYPES:
            problems.append(f"priority {name}: rule_type must be one of {', '.join(PRIORITY_TYPES)}")
        elif kind in ("CONDITION", "EXPRESSION") and not str(p.get("criterion_sql") or "").strip():
            problems.append(f"priority {name}: criterion_sql is empty")
        elif kind in HISTORY_TYPES:
            problems += _history_problems(f"priority {name}", p)
        if kind == "EXPRESSION" and _up(p.get("sort_dir"), "ASC") not in ("ASC", "DESC"):
            problems.append(f"priority {name}: sort_dir must be ASC or DESC")

    orders = [lvl["level_order"] for lvl in sample.levels]
    if len(orders) != len(set(orders)):
        problems.append(f"gre_sampling_strata level_order values repeat: {orders}")
    for lvl in sample.levels:
        problems += _level_problems(lvl)

    if str(c.get("output_table") or "").strip():
        if not sample.output_cols:
            problems.append("output_table needs gre_sampling_output_col rows")
        names = [_up(o.get("column_name")) for o in sample.output_cols]
        for o in sample.output_cols:
            n = str(o.get("column_name") or "")
            if not _IDENT.match(n):
                problems.append(f"output column {n!r} must be a plain column name")
            elif n.lower() in (*OUTPUT_META_COLS, "last_updated_datetime"):
                problems.append(f"output column {n!r} is one the engine writes itself")
            if not str(o.get("expr_sql") or "").strip():
                problems.append(f"output column {n}: expr_sql is empty")
        if len(names) != len(set(names)):
            problems.append("output column names repeat")
    elif sample.output_cols:
        problems.append("gre_sampling_output_col rows need gre_sampling_config.output_table")
    return problems


def _history_problems(what: str, row: dict) -> list:
    out = []
    try:
        if int(row.get("lookback_days") or 0) < 1:
            out.append(f"{what}: lookback_days must be >= 1")
    except (TypeError, ValueError):
        out.append(f"{what}: lookback_days must be a whole number")
    if _up(row.get("history_scope"), "CONFIG") not in HISTORY_SCOPES:
        out.append(f"{what}: history_scope must be one of {', '.join(HISTORY_SCOPES)}")
    return out


def _level_problems(lvl: dict) -> list:
    name, out = lvl.get("level_name") or lvl["strata_id"], []
    if _up(lvl.get("unlisted_policy"), "SHARE") not in UNLISTED_POLICIES:
        out.append(f"level {name}: unlisted_policy must be one of {', '.join(UNLISTED_POLICIES)}")
    if not lvl["value_level"] and not lvl["buckets"]:
        out.append(f"level {name}: no stratify_expr and no gre_sampling_bucket rows")
    groups = {}
    for b in lvl["buckets"]:
        bname, mode = str(b.get("bucket_name") or ""), _up(b.get("quota_mode"), "FRACTION")
        what = f"level {name} bucket {bname!r}"
        if not bname.strip() or "/" in bname:
            out.append(f"{what}: bucket_name must be set and must not contain '/'")
        if lvl["value_level"] and str(b.get("bucket_sql") or "").strip():
            out.append(f"{what}: bucket_sql is only used on a level without stratify_expr")
        if not lvl["value_level"] and not str(b.get("bucket_sql") or "").strip():
            out.append(f"{what}: bucket_sql is empty")
        if mode not in QUOTA_MODES:
            out.append(f"{what}: quota_mode must be one of {', '.join(QUOTA_MODES)}")
        fraction = b.get("target_fraction")
        if mode == "FRACTION":
            if fraction is None or not 0 <= float(fraction) <= 1:
                out.append(f"{what}: FRACTION needs target_fraction between 0 and 1")
        elif fraction is not None:
            out.append(f"{what}: target_fraction is not used with quota_mode {mode}")
        if b.get("max_count") is not None and int(b["max_count"]) < 0:
            out.append(f"{what}: max_count must be >= 0")
        group = groups.setdefault(_up(b.get("parent_path"), "*"), {})
        if _norm(bname) in group:
            out.append(f"{what}: appears more than once under parent_path {b.get('parent_path') or '*'!r}")
        group[_norm(bname)] = b
    for parent, group in groups.items():
        total = sum(float(b.get("target_fraction") or 0) for b in group.values()
                    if _up(b.get("quota_mode"), "FRACTION") == "FRACTION")
        if total > 1 + 1e-6:
            out.append(f"level {name} under {parent!r}: target_fraction values add up to {total:.4f}, more than 1")
    return out


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def _col(prefix: str, row_id) -> str:
    return f"smp_{prefix}_{row_id}"


def _flag(sql: str) -> str:
    return f"CASE WHEN ({sql}) THEN 1 ELSE 0 END"


def build_queries(sample: Sample, s: Settings, params: dict) -> dict:
    """{universe, scope, count (or None), pull, output (or None)}: the SQL of a run, $env and parameters
    resolved. Every bucket condition, exclusion rule and priority criterion is a column Teradata computes."""
    def sql(text, identifier=False):
        return substitute_params(s.resolve_env(str(text or "").strip(), identifier=identifier), params)

    c = sample.config
    universe = sql(c["universe_table"], identifier=True)
    keys = sample.keys
    scope = sql(c.get("scope_sql")) or "1=1"
    quick_exclusion = sql(c.get("exclusion_sql"))
    columns = list(keys)
    for lvl in sample.levels:
        if lvl["value_level"]:
            columns.append(f"{sql(lvl['stratify_expr'])} AS {_col('strata', lvl['strata_id'])}")
        else:
            columns += [f"{_flag(sql(b['bucket_sql']))} AS {_col('b', b['bucket_id'])}" for b in lvl["buckets"]]
    for x in sample.exclusions:
        if _up(x.get("rule_type"), "SQL") == "SQL":
            columns.append(f"{_flag(sql(x['exclusion_sql']))} AS {_col('x', x['exclusion_id'])}")
    for p in sample.priorities:
        kind = _up(p.get("rule_type"), "CONDITION")
        if kind == "CONDITION":
            columns.append(f"{_flag(sql(p['criterion_sql']))} AS {_col('p', p['priority_id'])}")
        elif kind == "EXPRESSION":
            columns.append(f"({sql(p['criterion_sql'])}) AS {_col('p', p['priority_id'])}")
    if str(c.get("diversity_expr") or "").strip():
        columns.append(f"({sql(c['diversity_expr'])}) AS {DIVERSITY}")
    priority = sql(c.get("priority_rank_sql"))
    if priority:
        columns.append(f"ROW_NUMBER() OVER (ORDER BY {priority}, {', '.join(keys)}) AS {BASE_RANK}")

    where = f"({scope})"
    if quick_exclusion:
        # CASE, not NOT(...): a row whose test is unknown (a NULL column) is not excluded
        where += f" AND {_flag(quick_exclusion)} = 0"
    out = {"universe": universe, "scope": scope,
           "count": f"SELECT COUNT(*) AS cnt FROM {universe} WHERE ({scope})" if quick_exclusion else None,
           "pull": f"SELECT {', '.join(columns)} FROM {universe} WHERE {where}", "output": None}
    if str(c.get("output_table") or "").strip():
        exprs = [f"{sql(o['expr_sql'])} AS {o['column_name']}" for o in sample.output_cols]
        out["output_table"] = sql(c["output_table"], identifier=True)
        aliases = [f"{k} AS smp_key_{i}" for i, k in enumerate(keys)]
        out["output"] = f"SELECT {', '.join([*aliases, *exprs])} FROM {universe} WHERE ({scope})"
    return out


def resolve_params(conn: Connection, s: Settings, sample: Sample, given: dict, as_of: date) -> dict:
    """{as_of_date} + the run's parameters + every gre_sampling_param lookup (a given value wins).
    A lookup with no value raises LookupError (often: the data is not loaded yet - retrying can help)."""
    params = {"as_of_date": as_of.isoformat(), **(given or {})}
    given_names = {str(k).lower() for k in (given or {})}
    for p in sample.params:
        name = str(p["param_name"])
        if name.lower() in given_names:
            continue
        text = substitute_params(s.resolve_env(str(p["param_sql"]).strip(), identifier=False), params)
        rows = conn.query(text)
        value = next(iter(rows[0].values()), None) if rows else None
        if value is None:
            raise LookupError(f"run parameter {name}: its param_sql returned no value")
        params[name] = value.isoformat() if isinstance(value, date) else value
    return params


def case_key(row: dict, keys: list) -> str:
    """'col1=val1|col2=val2' - the case's identity in gre_sample_selections."""
    parts = []
    for c in keys:
        k = c.lower()
        if k not in row:
            raise ConfigError(f"key_columns entry {c!r} is not among the pulled columns {sorted(row)}")
        parts.append(f"{c}={_label(row[k])}")
    return "|".join(parts)


def _history(conn: Connection, s: Settings, sample: Sample, rule: dict, run_key: str, as_of: date,
             cache: dict) -> dict:
    """{'selected': keys selected, 'unselected': keys considered but never selected} within the rule's
    lookback, from the current runs of earlier run_keys (this config, or the whole project)."""
    scope = _up(rule.get("history_scope"), "CONFIG")
    days = int(rule.get("lookback_days") or 0)
    if (scope, days) in cache:
        return cache[(scope, days)]
    where = ["etl_is_curr_ind = 'Y'", "excluded_flag = 0", "sample_cycle >= ?", "sample_cycle <= ?",
             "NOT (config_id = ? AND run_key = ?)"]
    params = [as_of - timedelta(days=days), as_of, sample.config_id, run_key]
    if scope == "PROJECT":
        where.append("project_name = ?")
        params.append(sample.config.get("project_name"))
    else:
        where.append("config_id = ?")
        params.append(sample.config_id)
    rows = conn.query(f"SELECT case_key, MAX(selected_flag) AS sel FROM {s.meta_db}.gre_sample_selections "
                      f"WHERE {' AND '.join(where)} GROUP BY case_key", params)
    out = {"selected": {r["case_key"] for r in rows if int(r["sel"] or 0) == 1},
           "unselected": {r["case_key"] for r in rows if int(r["sel"] or 0) == 0}}
    cache[(scope, days)] = out
    return out


# ---------------------------------------------------------------------------
# Exclusions and priority
# ---------------------------------------------------------------------------

def apply_exclusions(rows: list, sample: Sample, history: dict) -> tuple:
    """(candidates, excluded, counts): each excluded row gets row['_reason'] = the first matching rule."""
    counts = {str(x["exclusion_name"]): 0 for x in sample.exclusions}
    candidates, excluded = [], []
    for row in rows:
        reason = None
        for x in sample.exclusions:
            if _up(x.get("rule_type"), "SQL") == "SQL":
                hit = row.get(_col("x", x["exclusion_id"])) == 1
            else:
                hit = row["_key"] in history[x["exclusion_id"]]["selected"]
            if hit:
                reason = str(x["exclusion_name"])
                break
        if reason:
            row["_reason"] = reason
            counts[reason] += 1
            excluded.append(row)
        else:
            candidates.append(row)
    return candidates, excluded, counts


def _sortable(v) -> tuple:
    return (v is None, 0 if v is None else v)


def order_candidates(rows: list, sample: Sample, history: dict) -> list:
    """Priority order: criterion 1, 2, ... then priority_rank_sql, then the key columns (stable sorts from the
    last key to the first). Sets row['_order'] = 0, 1, ..."""
    keys = [k.lower() for k in sample.keys]
    rows = sorted(rows, key=lambda r: tuple(_sortable(r.get(k)) for k in keys))
    if any(BASE_RANK in r for r in rows[:1]):
        rows.sort(key=lambda r: r[BASE_RANK])
    for p in reversed(sample.priorities):
        kind, col = _up(p.get("rule_type"), "CONDITION"), _col("p", p["priority_id"])
        if kind == "CONDITION":
            rows.sort(key=lambda r: 0 if r.get(col) == 1 else 1)
        elif kind == "EXPRESSION":
            if _up(p.get("sort_dir"), "ASC") == "DESC":
                rows.sort(key=lambda r: (r.get(col) is not None, 0 if r.get(col) is None else r.get(col)),
                          reverse=True)
            else:
                rows.sort(key=lambda r: _sortable(r.get(col)))
        elif kind == "PRIOR_CANDIDATE":
            seen = history[("p", p["priority_id"])]["unselected"]
            rows.sort(key=lambda r: 0 if r["_key"] in seen else 1)
        elif kind == "PRIOR_SELECTION":
            seen = history[("p", p["priority_id"])]["selected"]
            rows.sort(key=lambda r: 1 if r["_key"] in seen else 0)
    for i, r in enumerate(rows):
        r["_order"] = i
    return rows


# ---------------------------------------------------------------------------
# Quotas and selection
# ---------------------------------------------------------------------------

def round_target(value: float, rounding_mode: str) -> int:
    mode = (rounding_mode or "FLOOR").upper()
    if mode == "CEIL":
        return max(0, math.ceil(value - 1e-9))
    if mode == "ROUND":
        return max(0, round(value))
    return max(0, math.floor(value + 1e-9))


def diversified(pool: list, n: int, cap: Optional[int], use_diversity: bool) -> list:
    """The first n of `pool`, rotating across the diversity values (in order of first appearance), at most
    `cap` per value. Without diversity: the first n."""
    if not use_diversity:
        return pool[:n]
    groups = {}
    for row in pool:
        groups.setdefault(_norm(row.get(DIVERSITY)), []).append(row)
    queues = [list(g) for g in groups.values()]
    out, depth = [], 0
    while len(out) < n:
        took = False
        for q in queues:
            if depth < len(q) and (cap is None or depth < cap):
                out.append(q[depth])
                took = True
                if len(out) == n:
                    break
        if not took:
            break
        depth += 1
    return out


def select(rows: list, n: int, method: str, rng: random.Random, cap: Optional[int] = None,
           use_diversity: bool = False) -> list:
    """Up to n of one bucket's rows (they arrive in priority order)."""
    if n <= 0 or not rows:
        return []
    method = (method or "RANKED").upper()
    if method == "RANKED":
        return diversified(rows, n, cap, use_diversity)
    if method == "RANDOM":
        pool = list(rows)
        rng.shuffle(pool)
        return diversified(pool, n, cap, use_diversity)
    if method == "SYSTEMATIC":
        if n >= len(rows):
            return list(rows)
        k = max(1, len(rows) // n)
        return rows[rng.randint(1, k) - 1::k][:n]
    raise ConfigError(f"unknown sampling_method {method!r}")


class Stratifier:
    """Splits candidates level by level and selects at the leaves (see the module docstring)."""

    def __init__(self, sample: Sample, seed, rounding_mode: str):
        self.levels = sample.levels
        self.method = sample.method
        self.seed = seed
        self.rounding = rounding_mode
        self.cap = int(sample.config["diversity_cap"]) if sample.config.get("diversity_cap") not in (None, "") \
            else None
        self.use_diversity = bool(str(sample.config.get("diversity_expr") or "").strip())
        self.leaves = {}   # leaf path -> {candidates, target, selected}
        self.nodes = {}    # every bucket path -> {candidates, target, selected}

    def rng(self, path: str) -> random.Random:
        return random.Random(f"{self.seed}|{path}")

    def pick(self, rows: list, n: int, path: str) -> list:
        return select(rows, n, self.method, self.rng(path), self.cap, self.use_diversity)

    def run(self, rows: list, target: int) -> list:
        return self._allocate(rows, target, 0, "")

    def top_up(self, rest: list, n: int, chosen: list) -> list:
        """Up to n more from `rest` (the candidates not chosen), in the method's order; diversity_cap still
        holds across the whole sample."""
        pool = self.pick(rest, len(rest), "TOPUP")
        if not (self.use_diversity and self.cap):
            return pool[:n]
        used = {}
        for r in chosen:
            used[_norm(r.get(DIVERSITY))] = used.get(_norm(r.get(DIVERSITY)), 0) + 1
        out = []
        for r in pool:
            v = _norm(r.get(DIVERSITY))
            if len(out) < n and used.get(v, 0) < self.cap:
                used[v] = used.get(v, 0) + 1
                out.append(r)
        return out

    def _group(self, level: dict, path: str) -> dict:
        """The level's bucket rows for this parent: the most specific matching parent_path wins."""
        by_pattern = {}
        for b in level["buckets"]:
            if _matches(path, b.get("parent_path")):
                by_pattern.setdefault(_up(b.get("parent_path"), "*"), []).append(b)
        if not by_pattern:
            return {}
        best = min(by_pattern, key=_specificity)
        return {_norm(b["bucket_name"]): b for b in by_pattern[best]}

    def _allocate(self, rows: list, target: int, index: int, path: str) -> list:
        while index < len(self.levels) and not _matches(path, self.levels[index].get("applies_to")):
            index += 1
        if index >= len(self.levels) or not rows:
            chosen = self.pick(rows, target, path or "ALL")
            leaf = path or "ALL"
            for r in rows:
                r["_path"] = leaf
            self.leaves[leaf] = self.nodes[leaf] = {"candidates": len(rows), "target": target,
                                                    "selected": len(chosen)}
            return chosen

        level = self.levels[index]
        group = self._group(level, path)
        if not level["value_level"] and not group:   # a CONDITION level with no buckets for this parent
            return self._allocate(rows, target, index + 1, path)
        buckets, labels = {}, {}
        for r in rows:
            if level["value_level"]:
                value = r.get(_col("strata", level["strata_id"]))
                key = _norm(value)
                label = str(group[key]["bucket_name"]).rstrip() if key in group else _label(value)
            else:
                hit = next((b for b in group.values() if r.get(_col("b", b["bucket_id"])) == 1), None)
                key, label = (_norm(hit["bucket_name"]), str(hit["bucket_name"]).rstrip()) if hit \
                    else (_norm(OTHER_BUCKET), OTHER_BUCKET)
            buckets.setdefault(key, []).append(r)
            labels.setdefault(key, label)
            r.setdefault("_buckets", {})[level["strata_id"]] = label

        unlisted = _up(level.get("unlisted_policy"), "SHARE")

        def mode(key):
            spec = group.get(key)
            return _up(spec.get("quota_mode"), "FRACTION") if spec else unlisted

        def cap(key):
            spec = group.get(key) or {}
            return int(spec["max_count"]) if spec.get("max_count") is not None else None

        def sub(key):
            return f"{path}/{labels[key]}" if path else labels[key]

        named = sum(float(b.get("target_fraction") or 0) for b in group.values()
                    if _up(b.get("quota_mode"), "FRACTION") == "FRACTION")
        sharing = [k for k in buckets if mode(k) == "SHARE"]
        chosen = {}
        targets = {}
        for key in sorted(buckets):
            m = mode(key)
            if m == "FRACTION":
                t = round_target(target * float(group[key]["target_fraction"]), self.rounding)
            elif m == "SHARE":
                t = round_target(target * max(0.0, 1.0 - named) / len(sharing), self.rounding)
            else:
                continue
            targets[key] = t if cap(key) is None else min(t, cap(key))
            chosen[key] = self._allocate(buckets[key], targets[key], index + 1, sub(key))

        def fill(keys: list, budget: int) -> None:
            """Share `budget` equally between `keys`; a bucket that cannot use its share keeps what it has and the
            others share the rest again (water-filling), so the split stays as even as the data allows."""
            fixed, active = {}, list(keys)
            while active:
                base, extra = divmod(max(0, budget - sum(fixed.values())), len(active))
                full = []
                for i, k in enumerate(active):
                    want = base + (1 if i < extra else 0)
                    if cap(k) is not None:
                        want = min(want, cap(k))
                    targets[k] = want
                    chosen[k] = self._allocate(buckets[k], want, index + 1, sub(k))
                    if len(chosen[k]) < want or (cap(k) is not None and want >= cap(k)):
                        full.append(k)
                if not full:
                    break
                for k in full:
                    fixed[k] = len(chosen[k])
                active = [k for k in active if k not in full]

        all_keys = [k for k in sorted(buckets) if mode(k) == "ALL"]
        fill(all_keys, max(0, target - sum(targets.values())))
        rest = [k for k in sorted(buckets) if mode(k) == "REMAINDER"]
        fill(rest, max(0, target - sum(len(v) for v in chosen.values())))
        for key in sorted(buckets):
            if mode(key) == "SKIP":
                targets[key] = 0
                chosen[key] = self._allocate(buckets[key], 0, index + 1, sub(key))

        picked = [r for key in sorted(buckets) for r in chosen[key]]
        if path:
            self.nodes[path] = {"candidates": len(rows), "target": target, "selected": len(picked)}
        return picked


# ---------------------------------------------------------------------------
# Audit, errors, persistence
# ---------------------------------------------------------------------------

def count_prior_attempts(conn: Connection, s: Settings, config_id, run_key: str) -> int:
    rows = conn.query(f"SELECT COUNT(*) AS cnt FROM {s.meta_db}.gre_sampling_audit "
                      f"WHERE sample_config_id = ? AND run_key = ?", [config_id, run_key])
    return int(rows[0]["cnt"]) if rows else 0


def _start_audit(conn, s, run_id, run_key, config, method, seed, target, as_of, params, triggered_by, started):
    conn.execute(f"""
        INSERT INTO {s.meta_db}.gre_sampling_audit (
            run_id, run_key, sample_config_id, project_name, process_name, sample_name, sampling_method,
            random_seed, target_volume, as_of_date, run_params_txt, started_at, status, triggered_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?)""",
        [run_id, run_key, config.get("config_id"), config.get("project_name"), config.get("process_name"),
         config.get("sample_name"), method, seed, target, as_of, json.dumps(params, default=str)[:4000],
         started, triggered_by])


def _finish_audit(conn, s, run_id, status, summary, message=None):
    conn.execute(f"""
        UPDATE {s.meta_db}.gre_sampling_audit
        SET status = ?, ended_at = ?, total_in_scope = ?, total_excluded = ?, total_candidates = ?,
            total_selected = ?, shortfall = ?, error_message = ?
        WHERE run_id = ?""",
        [status, s.now(), summary.get("in_scope"), summary.get("excluded"), summary.get("candidates"),
         summary.get("selected"), summary.get("shortfall"), (message or None) and message[:2000], run_id])


def log_error(conn: Connection, s: Settings, run_id, config: dict, run_key, error_type: str, message: str,
              detail: Optional[str] = None) -> None:
    """One gre_sampling_errors row; earlier runs' errors for the same (config_id, run_key) go inactive.
    Never raises: a failure here must not hide the real error."""
    try:
        conn.execute(f"""
            UPDATE {s.meta_db}.gre_sampling_errors
            SET active_ind = 'N', last_updated_datetime = ?
            WHERE config_id = ? AND run_key = ? AND run_id <> ? AND active_ind = 'Y'""",
            [s.now(), config.get("config_id"), run_key, run_id])
        conn.execute(f"""
            INSERT INTO {s.meta_db}.gre_sampling_errors (
                run_id, config_id, process_name, run_key, error_type, error_message, error_detail, active_ind,
                occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Y', ?)""",
            [run_id, config.get("config_id"), config.get("process_name"), run_key, error_type, message[:2000],
             detail, s.now()])
    except Exception as e:  # noqa: BLE001
        log.error("could not write gre_sampling_errors (run_id=%s): %s", run_id, e)


def fetch_output(conn: Connection, queries: dict, sample: Sample, chosen: list) -> dict:
    """{case_key: {column: value}} of the selected cases, read back from the universe with the output columns."""
    keys = sample.keys
    out = {}
    for i in range(0, len(chosen), OUTPUT_KEY_CHUNK):
        conds, params = [], []
        for row in chosen[i:i + OUTPUT_KEY_CHUNK]:
            parts = []
            for k in keys:
                value = row[k.lower()]
                if value is None:
                    parts.append(f"{k} IS NULL")
                else:
                    parts.append(f"{k} = ?")
                    params.append(value)
            conds.append("(" + " AND ".join(parts) + ")")
        for r in conn.query(f"{queries['output']} AND ({' OR '.join(conds)})", params):
            out[case_key({k.lower(): r[f"smp_key_{j}"] for j, k in enumerate(keys)}, keys)] = r
    missing = [r["_key"] for r in chosen if r["_key"] not in out]
    if missing:
        raise RuntimeError(f"{len(missing)} selected case(s) not found again for the output, e.g. {missing[0]}")
    return out


def _persist(conn, s, run_id, run_key, sample: Sample, queries, candidates, excluded, chosen, stats, output,
             as_of: date) -> int:
    """Write the run and retire earlier runs of the same (config_id, run_key); returns how many earlier runs
    were retired. Runs inside the caller's transaction."""
    now = s.now()
    c = sample.config
    cid, project, process = sample.config_id, c.get("project_name"), c.get("process_name")
    order = {r["_key"]: i + 1 for i, r in enumerate(chosen)}
    rows, attrs = [], []
    for r in candidates:
        rows.append([run_id, cid, project, process, run_key, as_of, r["_key"], r["_order"] + 1,
                     1 if r["_key"] in order else 0, order.get(r["_key"]), 0, None, r.get("_path"), now])
        for lvl in sample.levels:
            if lvl["strata_id"] in r.get("_buckets", {}):
                attrs.append([run_id, cid, run_key, r["_key"], lvl["strata_id"], lvl["level_order"],
                              r["_buckets"][lvl["strata_id"]], now])
    if _up(c.get("keep_excluded"), "1") not in ("0", "N", "FALSE"):
        rows += [[run_id, cid, project, process, run_key, as_of, r["_key"], None, 0, None, 1, r["_reason"], None,
                  now] for r in excluded]
    conn.executemany(f"""
        INSERT INTO {s.meta_db}.gre_sample_selections (
            sample_run_id, config_id, project_name, process_name, run_key, sample_cycle, case_key, priority_rank,
            selected_flag, selection_order, excluded_flag, exclusion_reason, stratum_path, etl_is_curr_ind,
            load_datetime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y', ?)""", rows, s.chunk_rows)
    conn.executemany(f"""
        INSERT INTO {s.meta_db}.gre_sample_selection_attrs (
            sample_run_id, config_id, run_key, case_key, strata_id, level_order, bucket_value, etl_is_curr_ind,
            load_datetime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Y', ?)""", attrs, s.chunk_rows)
    conn.executemany(f"""
        INSERT INTO {s.meta_db}.gre_sampling_run_stats (
            sample_run_id, config_id, run_key, sample_cycle, stat_type, stat_name, candidate_cnt, target_cnt,
            selected_cnt, etl_is_curr_ind, load_datetime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y', ?)""",
        [[run_id, cid, run_key, as_of, *st, now] for st in stats], s.chunk_rows)
    tables = [f"{s.meta_db}.gre_sample_selections", f"{s.meta_db}.gre_sample_selection_attrs",
              f"{s.meta_db}.gre_sampling_run_stats"]
    if output is not None:
        names = [str(o["column_name"]) for o in sample.output_cols]
        conn.executemany(
            f"INSERT INTO {queries['output_table']} ({', '.join([*OUTPUT_META_COLS, *names])}) "
            f"VALUES ({', '.join(['?'] * (len(OUTPUT_META_COLS) + len(names)))})",
            [[run_id, cid, run_key, as_of, r["_key"], order[r["_key"]], r["_order"] + 1, r.get("_path"), "Y", now,
              *(output[r["_key"]].get(n.lower()) for n in names)] for r in chosen], s.chunk_rows)
        tables.append(queries["output_table"])

    prior = conn.query(f"SELECT DISTINCT sample_run_id FROM {s.meta_db}.gre_sample_selections "
                       f"WHERE config_id = ? AND run_key = ? AND sample_run_id <> ? AND etl_is_curr_ind = 'Y'",
                       [cid, run_key, run_id])
    for table in tables:
        conn.execute(f"""
            UPDATE {table}
            SET etl_is_curr_ind = 'N', last_updated_datetime = ?
            WHERE config_id = ? AND run_key = ? AND sample_run_id <> ? AND etl_is_curr_ind = 'Y'""",
            [now, cid, run_key, run_id])
    return len(prior)


# ---------------------------------------------------------------------------
# One config
# ---------------------------------------------------------------------------

def run_sampling(conn: Connection, s: Settings, config_id, run_key: Optional[str] = None,
                 run_params: Optional[dict] = None, seed: Optional[int] = None, triggered_by: str = "SYSTEM",
                 as_of: Optional[date] = None, dry_run: bool = False) -> dict:
    """Run one sampling config. Returns its report (status COMPLETED / ERROR; error_kind 'config' when retrying
    cannot help, 'technical' otherwise). Raises ConfigError only when the config row itself cannot be loaded.

    run_key    : which logical run this is; may hold {name} tokens (e.g. '{batch_id}'). Default: the as-of
                 date. A rerun of the same (config_id, run_key) replaces the earlier one as the current sample.
    run_params : values for {name} / $name tokens in any configured SQL; gre_sampling_param lookups add more.
                 {as_of_date} and {run_key} are always available.
    seed       : RANDOM / SYSTEMATIC seed (default: a new one, stored on the audit row).
    as_of      : the business date of the run (default: today in Settings.timezone) -> sample_cycle.
    dry_run    : do everything but write (no audit row).
    """
    as_of = as_of or s.today()
    started = s.now()
    sample = load_config(conn, s, config_id)
    config, method = sample.config, sample.method
    rounding = _up(config.get("rounding_mode"), "FLOOR")
    try:
        target = int(config.get("target_volume") or 0)
    except (TypeError, ValueError):
        target = 0
    run_seed = None
    if method in ("RANDOM", "SYSTEMATIC"):
        run_seed = int(seed) if seed not in (None, "") else random.SystemRandom().getrandbits(63)
    template = str(run_key or "{as_of_date}")

    early = None   # (kind, error_type, exception) of a failure before the audit row exists
    try:
        params = resolve_params(conn, s, sample, run_params, as_of)
        run_key = substitute_params(template, params, escape=False)
    except ValueError as e:
        early, params = ("config", "CONFIG_ERROR", e), dict(run_params or {})
    except Exception as e:  # noqa: BLE001
        early, params = ("technical", "PARAM_FAILURE", e), dict(run_params or {})
    if early:
        run_key = template if "{" not in template and "$" not in template else as_of.isoformat()
    params["run_key"] = run_key

    label = ".".join(str(p) for p in (config.get("project_name"), config.get("sample_name")) if p)
    attempt = 0 if dry_run else count_prior_attempts(conn, s, config_id, run_key)
    run_id = generate_run_id(label or f"config-{config_id}", run_key, f"attempt-{attempt + 1}", triggered_by,
                             timestamp=started)
    summary = {"config_id": config_id, "project_name": config.get("project_name"),
               "process_name": config.get("process_name"), "sample_name": config.get("sample_name"),
               "sample_run_id": None if dry_run else run_id, "run_key": run_key, "as_of_date": as_of.isoformat(),
               "method": method, "seed": run_seed, "target_volume": target, "params": params, "in_scope": None,
               "excluded": None, "candidates": 0, "selected": 0, "shortfall": None, "exclusions": {},
               "by_stratum": {}, "buckets": {}, "retired_runs": 0, "dry_run": dry_run}
    log.info("sampling starting: config_id=%s sample=%s run_key=%s method=%s target=%s meta_db=%s host=%s%s",
             config_id, label, run_key, method, target, s.meta_db, conn.host, " (dry run)" if dry_run else "")

    def fail(kind: str, error_type: str, exc_or_text, detail=None) -> dict:
        message = exc_or_text if isinstance(exc_or_text, str) else error_text(exc_or_text)
        log.error("sampling config_id=%s %s: %s", config_id, error_type, message)
        summary.update(status="ERROR", error_kind=kind, error_type=error_type, error=message)
        if not dry_run:
            log_error(conn, s, run_id, config, run_key, error_type, message, detail)
            try:
                _finish_audit(conn, s, run_id, "ERROR", summary, f"{error_type}: {message}")
            except Exception as e:  # noqa: BLE001
                log.error("could not close the audit row of %s: %s", run_id, e)
        return summary

    if not dry_run:
        _start_audit(conn, s, run_id, run_key, config, method, run_seed, target, as_of, params, triggered_by,
                     started)
    if early:
        return fail(early[0], early[1], early[2])
    problems = config_problems(sample)
    if problems:
        return fail("config", "CONFIG_ERROR", "; ".join(problems))
    try:
        queries = build_queries(sample, s, params)
    except (ValueError, KeyError) as e:
        return fail("config", "CONFIG_ERROR", e)

    try:
        if queries["count"]:
            log.info("scope count (config_id=%s): %s", config_id, queries["count"])
        log.info("pull (config_id=%s): %s", config_id, queries["pull"])
        rows = conn.stream(queries["pull"], s.chunk_rows)
        in_scope = int(conn.query(queries["count"])[0]["cnt"]) if queries["count"] else len(rows)
        history, cache = {}, {}
        for x in sample.exclusions:
            if _up(x.get("rule_type"), "SQL") in HISTORY_TYPES:
                history[x["exclusion_id"]] = _history(conn, s, sample, x, run_key, as_of, cache)
        for p in sample.priorities:
            if _up(p.get("rule_type"), "CONDITION") in HISTORY_TYPES:
                history[("p", p["priority_id"])] = _history(conn, s, sample, p, run_key, as_of, cache)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "PULL_FAILURE", e, traceback.format_exc())

    try:
        for r in rows:
            r["_key"] = case_key(r, sample.keys)
        if len({r["_key"] for r in rows}) != len(rows):
            raise ConfigError("key_columns do not identify a case uniquely (the pull returned duplicate keys)")
        candidates, excluded, counts = apply_exclusions(rows, sample, history)
        candidates = order_candidates(candidates, sample, history)
        strat = Stratifier(sample, run_seed, rounding)
        chosen = strat.run(candidates, target)
        if len(chosen) < target and _up(config.get("shortfall_policy"), "TOPUP") == "TOPUP":
            taken = {r["_key"] for r in chosen}
            rest = [r for r in candidates if r["_key"] not in taken]
            chosen += strat.top_up(rest, target - len(chosen), chosen)
        chosen = sorted(chosen[:target], key=lambda r: r["_order"])
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "SELECT_FAILURE", e, traceback.format_exc())
    summary.update(in_scope=in_scope, excluded=in_scope - len(candidates), candidates=len(candidates),
                   selected=len(chosen), shortfall=max(0, target - len(chosen)), exclusions=counts,
                   by_stratum=strat.leaves, buckets=strat.nodes)

    output = None
    if queries["output"]:
        try:
            output = fetch_output(conn, queries, sample, chosen)
        except Exception as e:  # noqa: BLE001
            return fail("technical", "OUTPUT_FAILURE", e, traceback.format_exc())

    if dry_run:
        summary["status"] = "COMPLETED"
        return summary

    stats = [("EXCLUSION", name, n, None, None) for name, n in counts.items()]
    stats += [("BUCKET", path, v["candidates"], v["target"], v["selected"]) for path, v in sorted(strat.nodes.items())]
    try:
        with conn.transaction():
            summary["retired_runs"] = _persist(conn, s, run_id, run_key, sample, queries, candidates, excluded,
                                               chosen, stats, output, as_of)
            _finish_audit(conn, s, run_id, "COMPLETED", summary)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "PERSIST_FAILURE", e, traceback.format_exc())

    summary["status"] = "COMPLETED"
    log.info("sampling complete: %s in_scope=%s excluded=%s candidates=%d selected=%d/%d retired_runs=%d", run_id,
             in_scope, summary["excluded"], len(candidates), len(chosen), target, summary["retired_runs"])
    return summary


# ---------------------------------------------------------------------------
# A scope: one config, a project, a process
# ---------------------------------------------------------------------------

def discover_configs(conn: Connection, s: Settings, config_id=None, project: Optional[str] = None,
                     process_name: Optional[str] = None) -> list:
    """Active config_ids of a scope: a config_id, or a project and / or a process_name (at least one)."""
    if config_id in (None, "") and not project and not process_name:
        raise ConfigError("give config_id, project or process_name: a run never samples every config at once")
    where, params = ["act_ind = 1"], []
    if config_id not in (None, ""):
        where.append("config_id = ?")
        params.append(int(config_id))
    if project:
        where.append("project_name = ?")
        params.append(project)
    if process_name:
        where.append("process_name = ?")
        params.append(process_name)
    rows = conn.query(f"SELECT config_id FROM {s.meta_db}.gre_sampling_config WHERE {' AND '.join(where)} "
                      f"ORDER BY config_id", params)
    return [r["config_id"] for r in rows]


def run_scope(conn: Connection, s: Settings, config_id=None, project: Optional[str] = None,
              process_name: Optional[str] = None, **kwargs) -> dict:
    """Run every active config of a scope, one after another (kwargs go to run_sampling). A config that fails
    does not stop the others. Returns {result, scope, runs: [report, ...], failed}."""
    scope = {k: v for k, v in (("config_id", config_id), ("project", project), ("process_name", process_name))
             if v not in (None, "")}
    ids = discover_configs(conn, s, config_id, project, process_name)
    if not ids:
        raise ConfigError(f"no active gre_sampling_config matches {scope}")
    runs = []
    for cid in ids:
        try:
            runs.append(run_sampling(conn, s, cid, **kwargs))
        except ConfigError as e:
            runs.append({"config_id": cid, "status": "ERROR", "error_kind": "config", "error_type": "CONFIG_ERROR",
                         "error": str(e)})
    failed = [r for r in runs if r.get("status") != "COMPLETED"]
    result = "ALL SAMPLES COMPLETED" if not failed else f"{len(failed)} OF {len(runs)} SAMPLE(S) FAILED"
    return {"result": result, "scope": scope, "runs": runs, "failed": len(failed)}


def validate(conn: Connection, s: Settings, config_id=None, project: Optional[str] = None,
             process_name: Optional[str] = None, run_params: Optional[dict] = None,
             check_sql: bool = False, as_of: Optional[date] = None) -> dict:
    """Check the configs of a scope. With check_sql, also run the parameter lookups and EXPLAIN the pull, count
    and output SQL (proves tables, columns and expressions exist; reads no universe rows)."""
    out = []
    for cid in discover_configs(conn, s, config_id, project, process_name):
        try:
            sample = load_config(conn, s, cid)
        except ConfigError as e:
            out.append({"config_id": cid, "problems": [str(e)]})
            continue
        problems = config_problems(sample)
        if check_sql and not problems:
            day = as_of or s.today()
            try:
                params = resolve_params(conn, s, sample, run_params, day)
                params.setdefault("run_key", day.isoformat())
                queries = build_queries(sample, s, params)
                for key in ("pull", "count", "output"):
                    if queries.get(key):
                        conn.query("EXPLAIN " + queries[key] + (" AND 1=0" if key == "output" else ""))
            except Exception as e:  # noqa: BLE001
                problems.append(f"SQL check failed: {error_text(e)}")
        out.append({"config_id": cid, "sample_name": sample.config.get("sample_name"), "problems": problems})
    bad = sum(1 for c in out if c["problems"])
    return {"result": "VALID" if not bad else f"{bad} CONFIG(S) WITH PROBLEMS", "configs": out}
