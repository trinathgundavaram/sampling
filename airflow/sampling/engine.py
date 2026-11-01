"""The sampling engine: config-driven stratified sampling on Teradata (no Airflow imports).

Like the GRE rules engine, everything a sample selects is configuration rows holding SQL - no field name or
value is known to this code (sql/schema.sql describes every table):

    gre_sampling_config   universe, scope_sql, key columns, size, method, diversity, output table
    gre_sampling_rule     one row per rule, rule_class saying what it is:
        PARAM             a run parameter looked up by SQL ({batch_id} = the latest batch, ...)
        GATE              a readiness check: a SELECT that must return a row, or the run waits (is retried)
        EXCLUSION         an exclusion rule (a SELECT of the cases to leave out, or "selected by an earlier run")
        PRIORITY          a priority criterion (a SELECT of the cases to take first, a SELECT of a value to order
                          by, or history based)
        LEVEL             a stratification level (by the value a SELECT returns per case, or by its buckets'
                          SELECTs), optionally only under some parent buckets (parent_path)
        BUCKET            a bucket of a level: FRACTION of the parent's target, ALL there is, or the REMAINDER
                          the others leave; a different mix per parent bucket (parent_path)

    Every selection criterion is a whole SELECT, joins and all, as a GRE rule, returning the key_columns of the
    cases it matches (EXCLUSION, PRIORITY, BUCKET), plus one value column where a value is needed (a PRIORITY
    EXPRESSION, a LEVEL by value). Each runs on its own after the pull, so it may read any table; only the
    pulled cases count. A BUCKET with no rule_sql takes the cases no earlier bucket took.
        OUTPUT            a column of the output snapshot of the selected cases

One run = one gre_sampling_config row:

    look up parameters        PARAM rules
    run row (RUNNING)         gre_sampling_run
    gates                     every GATE rule must return a row (e.g. "the batch is closed"), else GATE_NOT_MET
    pull                      one SELECT of the scope (scope_sql AND the run's extra_filters): the key columns
    rule queries              each rule's SELECT; the pulled cases it returns are marked (or given its value)
    exclude                   first matching exclusion rule is the reason; history rules read earlier runs
    order                     priority criteria in order, then priority_rank_sql, then the key columns
    stratify                  per level and parent: FRACTION buckets, then ALL buckets, then REMAINDER buckets
                              take what is left (unused quota flows to them); equal shares are water-filled
    select per leaf           RANKED: in priority order | RANDOM: seeded shuffle | SYSTEMATIC: seeded start,
                              every k-th; rotating across diversity_expr values (diversity_cap per value)
    top up                    shortfall_policy TOPUP: from the rest of the candidates, in priority order
    output snapshot           output_table: the selected cases' OUTPUT values
    persist (one transaction) selections (+ excluded rows) with their bucket path, output snapshot; earlier runs
                              of the same (config_id, run_key) -> etl_is_curr_ind = 'N'; run row -> COMPLETED with
                              the counts per rule and bucket (run_report)
    on failure                run row -> ERROR with error_type, message and traceback (nothing half written)

Audit defensibility:

    rules snapshot            every run stores the config and rule rows it used (run_report) and their SHA-256
                              (config_hash): a sample can always be explained by the rules that made it
    finalise                  finalize_run(): the current sample of a run_key becomes FINAL; a rerun of that
                              run_key is refused (FINALIZED) unless given a force_reason, which is recorded
    replace a case            replace_case(): a selected case comes out (REPLACED, with the reason and who) and
                              the next candidate of the same bucket goes in (REPLACEMENT), in priority order
                              (RANDOM: a seeded draw); the output snapshot follows

Reproducible: one seed per run (on the run row); every leaf draws from its own generator seeded with
(seed, bucket path), so a draw never depends on the order buckets are visited.
"""
from __future__ import annotations

import hashlib
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
QUERY = "QUERY"   # rule_type: rule_sql is a SELECT returning the key_columns of the matching cases (default)
EXCLUSION_TYPES = (QUERY, "PRIOR_SELECTION")
PRIORITY_TYPES = (QUERY, "EXPRESSION", "PRIOR_CANDIDATE", "PRIOR_SELECTION")
BUCKET_TYPES = (QUERY,)
_CONDITION_TYPES = ("SQL", "CONDITION")   # the former condition form: reported, with how to rewrite it
_REWRITE = "a condition is not a rule: write rule_sql as a SELECT returning the key_columns (rule_type QUERY)"
HISTORY_TYPES = ("PRIOR_CANDIDATE", "PRIOR_SELECTION")
HISTORY_SCOPES = ("CONFIG", "PROJECT")
SHORTFALL_POLICIES = ("TOPUP", "NONE")
OTHER_BUCKET = "OTHER"
NULL_BUCKET = "NULL"
OUTPUT_META_COLS = ("sample_run_id", "config_id", "run_key", "sample_cycle", "case_key", "selection_order",
                    "priority_rank", "stratum_path", "etl_is_curr_ind", "load_datetime")
OUTPUT_KEY_CHUNK = 50
RULE_CLASSES = ("PARAM", "GATE", "EXCLUSION", "PRIORITY", "LEVEL", "BUCKET", "OUTPUT")
# the optional gre_sampling_rule columns each class reads (any other one set is reported)
_AUDIT_COLUMNS = ("load_datetime", "last_updated_datetime", "created_by", "last_updated_by")
_RULE_COLUMNS = ("rule_type", "rule_sql", "level_order", "parent_path", "unlisted_policy", "quota_mode",
                 "target_fraction", "max_count", "sort_dir", "lookback_days", "history_scope")
_USED = {"PARAM": {"rule_sql"},
         "GATE": {"rule_sql"},
         "EXCLUSION": {"rule_type", "rule_sql", "lookback_days", "history_scope"},
         "PRIORITY": {"rule_type", "rule_sql", "sort_dir", "lookback_days", "history_scope"},
         "LEVEL": {"rule_sql", "level_order", "parent_path", "unlisted_policy"},
         "BUCKET": {"rule_type", "rule_sql", "level_order", "parent_path", "quota_mode", "target_fraction",
                    "max_count"},
         "OUTPUT": {"rule_sql"}}
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]{0,127}$")
# Configured SQL only reads: a statement word anywhere (outside quotes) is refused. Rows run under the service
# account, so this guards against a pasted script or a slip, not against someone who may write the rows anyway.
# (Words that are also functions or common names - DATABASE, BEGIN, END - are left out: with ';' refused a second
# statement cannot follow, so they can only be an expression.)
_STATEMENT_WORDS = ("INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "REPLACE", "DROP", "CREATE", "ALTER",
                    "RENAME", "MODIFY", "GRANT", "REVOKE", "CALL", "EXEC", "EXECUTE", "COLLECT", "COMMENT",
                    "COMMIT", "ROLLBACK", "ABORT", "BEGIN TRANSACTION", "END TRANSACTION", "SET SESSION",
                    "SET QUERY_BAND")
_STATEMENT_RE = re.compile(r"\b(" + "|".join(w.replace(" ", r"\s+") for w in _STATEMENT_WORDS) + r")\b", re.I)
_QUOTED = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|/\*.*?\*/", re.S)
_SELECT_START = re.compile(r"^\s*(LOCKING\s+.*?\s+FOR\s+ACCESS\s+)?\(*\s*(SELECT|SEL|WITH)\b", re.I | re.S)
_CONFIG_SQL = ("scope_sql", "exclusion_sql", "priority_rank_sql", "diversity_expr")
_NEEDS_ORDER = ("RANKED", "SYSTEMATIC")

BASE_RANK = "smp_base_rank"
DIVERSITY = "smp_div"
EXTRA_FILTERS = "extra_filters"   # the run parameter holding the run's filter condition (SQL)


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
    """One gre_sampling_config row and its gre_sampling_rule rows, by class, each list in its order."""
    config: dict
    params: list = field(default_factory=list)
    gates: list = field(default_factory=list)
    exclusions: list = field(default_factory=list)
    priorities: list = field(default_factory=list)
    levels: list = field(default_factory=list)       # LEVEL rows, each with "buckets": [its BUCKET rows]
    output_cols: list = field(default_factory=list)
    stray: list = field(default_factory=list)        # rows of an unknown class, or buckets of no level
    snapshot: dict = field(default_factory=dict)     # {"config": row, "rules": [rows]} as loaded
    config_hash: str = ""

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


def _text(row: dict, col: str = "rule_sql") -> str:
    return str(row.get(col) or "").strip()


def load_config(conn: Connection, s: Settings, config_id) -> Sample:
    """The active gre_sampling_config row of config_id with its active gre_sampling_rule rows."""
    db = s.meta_db
    rows = conn.query(f"SELECT * FROM {db}.gre_sampling_config WHERE config_id = ? AND act_ind = 1", [config_id])
    if not rows:
        raise ConfigError(f"no active gre_sampling_config row for config_id={config_id}")
    rules = conn.query(f"SELECT * FROM {db}.gre_sampling_rule WHERE config_id = ? AND act_ind = 1", [config_id])
    return build_sample(rows[0], rules)


def snapshot_hash(snapshot: dict) -> str:
    """SHA-256 of a config + rules snapshot (who changed a row and when do not count, only what it says)."""
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def build_sample(config: dict, rules: list) -> Sample:
    """A Sample from a config row and its active rule rows - read from the tables, or from a run's snapshot."""
    def clean(row):
        return {k: v for k, v in row.items() if k not in _AUDIT_COLUMNS}
    snapshot = {"config": clean(config), "rules": sorted((clean(r) for r in rules), key=lambda r: r["rule_id"])}
    ordered = sorted((dict(r) for r in snapshot["rules"]),     # copies: the levels get their buckets attached
                     key=lambda r: (r.get("seq_no") is not None, r.get("seq_no") or 0, r["rule_id"]))
    by_class = {c: [] for c in RULE_CLASSES}
    sample = Sample(config=dict(config), snapshot=snapshot, config_hash=snapshot_hash(snapshot))
    for r in ordered:
        r["rule_class"] = _up(r.get("rule_class"))
        by_class.get(r["rule_class"], sample.stray).append(r)
    sample.params, sample.gates, sample.exclusions = by_class["PARAM"], by_class["GATE"], by_class["EXCLUSION"]
    sample.priorities, sample.output_cols = by_class["PRIORITY"], by_class["OUTPUT"]
    levels = sorted(by_class["LEVEL"], key=lambda r: (r.get("level_order") is None, r.get("level_order") or 0,
                                                      r["rule_id"]))
    for level in levels:
        level["buckets"] = [b for b in by_class["BUCKET"] if b.get("level_order") == level.get("level_order")]
        level["value_level"] = bool(_text(level))
    orders = {lvl.get("level_order") for lvl in levels}
    sample.stray += [b for b in by_class["BUCKET"] if b.get("level_order") not in orders]
    sample.levels = levels
    return sample


def config_problems(sample: Sample) -> list:
    """Everything wrong with one sample's rows (no database access); empty = valid."""
    c, problems = sample.config, []
    method = sample.method
    if method not in VALID_METHODS:
        problems.append(f"sampling_method {method!r} must be one of {', '.join(VALID_METHODS)}")
    ordered = any(p for p in sample.priorities if _up(p.get("rule_type"), QUERY) in PRIORITY_TYPES) \
        or str(c.get("priority_rank_sql") or "").strip()
    if method in _NEEDS_ORDER and not ordered:
        problems.append(f"sampling_method {method} needs PRIORITY rules or priority_rank_sql")
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

    for r in sample.stray:
        problems.append(f"rule {r['rule_id']}: rule_class {r['rule_class']!r} is not one of "
                        f"{', '.join(RULE_CLASSES)}" if r["rule_class"] != "BUCKET"
                        else f"rule {r['rule_id']}: BUCKET of level_order {r.get('level_order')}, which has no LEVEL")
    for cls in RULE_CLASSES:
        for r in _rules_of(sample, cls):
            for col in _RULE_COLUMNS:
                value = r.get(col)
                if col not in _USED[cls] and value not in (None, "") and not (col == "parent_path" and value == "*"):
                    problems.append(f"{cls} rule {r['rule_name']}: {col} is not used by this class")
            if not str(r.get("rule_name") or "").strip():
                problems.append(f"{cls} rule {r['rule_id']}: rule_name is empty")

    for p in sample.params:
        if not _IDENT.match(str(p.get("rule_name") or "")):
            problems.append(f"PARAM rule {p.get('rule_id')}: rule_name must be a plain name")
        if not _text(p):
            problems.append(f"PARAM rule {p.get('rule_name')}: rule_sql is empty")

    for g in sample.gates:
        if not _text(g):
            problems.append(f"GATE rule {g.get('rule_name')}: rule_sql is empty")

    for x in sample.exclusions:
        name, kind = x.get("rule_name"), _up(x.get("rule_type"), QUERY)
        if kind in _CONDITION_TYPES:
            problems.append(f"EXCLUSION rule {name}: rule_type {kind}: {_REWRITE}")
        elif kind not in EXCLUSION_TYPES:
            problems.append(f"EXCLUSION rule {name}: rule_type must be one of {', '.join(EXCLUSION_TYPES)}")
        elif kind == QUERY and not _text(x):
            problems.append(f"EXCLUSION rule {name}: rule_sql is empty")
        elif kind in HISTORY_TYPES:
            problems += _history_problems(f"EXCLUSION rule {name}", x)

    for p in sample.priorities:
        name, kind = p.get("rule_name"), _up(p.get("rule_type"), QUERY)
        if kind in _CONDITION_TYPES:
            problems.append(f"PRIORITY rule {name}: rule_type {kind}: {_REWRITE}")
        elif kind not in PRIORITY_TYPES:
            problems.append(f"PRIORITY rule {name}: rule_type must be one of {', '.join(PRIORITY_TYPES)}")
        elif kind in (QUERY, "EXPRESSION") and not _text(p):
            problems.append(f"PRIORITY rule {name}: rule_sql is empty")
        elif kind in HISTORY_TYPES:
            problems += _history_problems(f"PRIORITY rule {name}", p)
        if kind == "EXPRESSION" and _up(p.get("sort_dir"), "ASC") not in ("ASC", "DESC"):
            problems.append(f"PRIORITY rule {name}: sort_dir must be ASC or DESC")
        elif kind != "EXPRESSION" and p.get("sort_dir") not in (None, ""):
            problems.append(f"PRIORITY rule {name}: sort_dir is only used by rule_type EXPRESSION")

    orders = [lvl.get("level_order") for lvl in sample.levels]
    if None in orders:
        problems.append("every LEVEL rule needs level_order")
    if len(orders) != len(set(orders)):
        problems.append(f"LEVEL rules repeat a level_order: {orders}")
    for lvl in sample.levels:
        problems += _level_problems(lvl)

    if str(c.get("output_table") or "").strip():
        if not sample.output_cols:
            problems.append("output_table needs OUTPUT rules")
        names = [_up(o.get("rule_name")) for o in sample.output_cols]
        for o in sample.output_cols:
            n = str(o.get("rule_name") or "")
            if not _IDENT.match(n):
                problems.append(f"OUTPUT rule {n!r}: rule_name must be a plain column name")
            elif n.lower() in (*OUTPUT_META_COLS, "last_updated_datetime"):
                problems.append(f"OUTPUT rule {n!r}: a column the engine writes itself")
            if not _text(o):
                problems.append(f"OUTPUT rule {n}: rule_sql is empty")
        if len(names) != len(set(names)):
            problems.append("OUTPUT rule names repeat")
    elif sample.output_cols:
        problems.append("OUTPUT rules need gre_sampling_config.output_table")
    return problems + _sql_problems(sample)


def sql_problems(text, what: str, select: bool = False) -> list:
    """Why a configured piece of SQL is not a plain read: a statement word, ';', a -- comment (it would comment out
    the rest of the query the engine builds around it), unbalanced parentheses or quotes. select: it must be a
    SELECT (PARAM, GATE). Text in quotes and /* */ comments is not checked."""
    text = str(text or "")
    if not text.strip():
        return []
    bare = _QUOTED.sub(" ", text)
    out = []
    if bare.count("'") or bare.count('"') or "/*" in bare:
        out.append(f"{what}: a quote or /* comment is not closed")
    words = sorted({re.sub(r"\s+", " ", m.group(1)).upper() for m in _STATEMENT_RE.finditer(bare)})
    if words:
        out.append(f"{what}: only reading SQL is allowed, found {', '.join(words)}")
    if ";" in bare:
        out.append(f"{what}: ';' is not allowed (one expression or SELECT, never a script)")
    if "--" in bare:
        out.append(f"{what}: '--' comments are not allowed (use /* */)")
    depth = 0
    for ch in bare:
        depth += {"(": 1, ")": -1}.get(ch, 0)
        if depth < 0:
            break
    if depth:
        out.append(f"{what}: parentheses do not balance")
    if select and not _SELECT_START.match(bare):
        out.append(f"{what}: must be a SELECT")
    return out


def _sql_problems(sample: Sample) -> list:
    out = [p for col in _CONFIG_SQL for p in sql_problems(sample.config.get(col), col)]
    for cls in RULE_CLASSES:
        for r in _rules_of(sample, cls):
            out += sql_problems(r.get("rule_sql"), f"{cls} rule {r.get('rule_name')}",
                                select=cls in ("PARAM", "GATE") or _is_query(r))
    return out


def _is_query(rule: dict) -> bool:
    """A rule whose rule_sql is a SELECT of cases: every EXCLUSION / PRIORITY but the history ones, a LEVEL by
    value, a BUCKET of a condition level (one without rule_sql takes the rest)."""
    cls = _up(rule.get("rule_class"))
    if cls in ("EXCLUSION", "PRIORITY"):
        return _up(rule.get("rule_type"), QUERY) in (QUERY, "EXPRESSION")
    return cls in ("LEVEL", "BUCKET") and bool(_text(rule))


def _is_value(rule: dict) -> bool:
    """A SELECT returning a value per case besides the key columns: a PRIORITY EXPRESSION, a LEVEL by value."""
    cls = _up(rule.get("rule_class"))
    return cls == "LEVEL" or (cls == "PRIORITY" and _up(rule.get("rule_type")) == "EXPRESSION")


def _rules_of(sample: Sample, cls: str) -> list:
    if cls == "BUCKET":
        return [b for lvl in sample.levels for b in lvl["buckets"]]
    return {"PARAM": sample.params, "GATE": sample.gates, "EXCLUSION": sample.exclusions, "PRIORITY": sample.priorities,
            "LEVEL": sample.levels, "OUTPUT": sample.output_cols}[cls]


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
    name, out = lvl.get("rule_name"), []
    if _up(lvl.get("unlisted_policy"), "SHARE") not in UNLISTED_POLICIES:
        out.append(f"level {name}: unlisted_policy must be one of {', '.join(UNLISTED_POLICIES)}")
    if not lvl["value_level"] and not lvl["buckets"]:
        out.append(f"level {name}: no rule_sql (a SELECT of each case's value) and no BUCKET rules")
    groups = {}
    for b in lvl["buckets"]:
        bname, mode = str(b.get("rule_name") or ""), _up(b.get("quota_mode"), "FRACTION")
        what = f"level {name} bucket {bname!r}"
        if "/" in bname:
            out.append(f"{what}: rule_name must not contain '/'")
        kind = _up(b.get("rule_type"), QUERY)
        if kind in _CONDITION_TYPES:
            out.append(f"{what}: rule_type {kind}: {_REWRITE}")
        elif kind not in BUCKET_TYPES:
            out.append(f"{what}: rule_type must be one of {', '.join(BUCKET_TYPES)}")
        if lvl["value_level"] and (_text(b) or b.get("rule_type") not in (None, "")):
            out.append(f"{what}: rule_sql and rule_type are only used on a level without its own SELECT")
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
    if not lvl["value_level"]:
        for parent, group in groups.items():
            rest = [b for b in group.values() if not _text(b)]
            if len(rest) > 1 or (rest and list(group.values())[-1] is not rest[0]):
                out.append(f"level {name} under {parent!r}: a bucket without rule_sql takes every case left, "
                           f"so it must be the only one and the last (seq_no)")
    for parent, group in groups.items():
        total = sum(float(b.get("target_fraction") or 0) for b in group.values()
                    if _up(b.get("quota_mode"), "FRACTION") == "FRACTION")
        if total > 1 + 1e-6:
            out.append(f"level {name} under {parent!r}: target_fraction values add up to {total:.4f}, more than 1")
    return out


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def _col(rule: dict) -> str:
    """The pulled column holding a rule's flag or value (rule_id is unique in gre_sampling_rule)."""
    return f"smp_r_{rule['rule_id']}"


def _flag(sql: str) -> str:
    return f"CASE WHEN ({sql}) THEN 1 ELSE 0 END"


def build_queries(sample: Sample, s: Settings, params: dict) -> dict:
    """{universe, scope, count (or None), pull, output (or None)}: the SQL of a run, $env and parameters
    resolved; the run's extra_filters (params[EXTRA_FILTERS], SQL) are ANDed to the scope, so the count, the pull
    and the output snapshot all read the same rows. key_queries: (rule, SELECT) of every rule, run after the pull."""
    def sql(text, identifier=False):
        return substitute_params(s.resolve_env(str(text or "").strip(), identifier=identifier), params)

    c = sample.config
    universe = sql(c["universe_table"], identifier=True)
    keys = sample.keys
    scope = sql(c.get("scope_sql")) or "1=1"
    filters = str(params.get(EXTRA_FILTERS) or "").strip()
    if filters:
        problems = sql_problems(filters, "extra_filters")
        if problems:
            raise ValueError("; ".join(problems))
        scope = f"({scope}) AND ({sql(filters)})"
    quick_exclusion = sql(c.get("exclusion_sql"))
    columns = list(keys)
    if str(c.get("diversity_expr") or "").strip():
        columns.append(f"({sql(c['diversity_expr'])}) AS {DIVERSITY}")
    priority = sql(c.get("priority_rank_sql"))
    if priority:
        columns.append(f"ROW_NUMBER() OVER (ORDER BY {priority}, {', '.join(keys)}) AS {BASE_RANK}")

    where = f"({scope})"
    if quick_exclusion:
        # CASE, not NOT(...): a row whose test is unknown (a NULL column) is not excluded
        where += f" AND {_flag(quick_exclusion)} = 0"
    key_queries = [(r, sql(r["rule_sql"])) for r in query_rules(sample)]
    out = {"universe": universe, "scope": scope, "key_queries": key_queries,
           "count": f"SELECT COUNT(*) AS cnt FROM {universe} WHERE ({scope})" if quick_exclusion else None,
           "pull": f"SELECT {', '.join(columns)} FROM {universe} WHERE {where}", "output": None}
    if str(c.get("output_table") or "").strip():
        exprs = [f"{sql(o['rule_sql'])} AS {o['rule_name']}" for o in sample.output_cols]
        out["output_table"] = sql(c["output_table"], identifier=True)
        aliases = [f"{k} AS smp_key_{i}" for i, k in enumerate(keys)]
        out["output"] = f"SELECT {', '.join([*aliases, *exprs])} FROM {universe} WHERE ({scope})"
    return out


def query_rules(sample: Sample) -> list:
    """Every rule with a SELECT of cases: exclusions, priorities, levels by value, buckets of condition levels."""
    return [r for r in (*sample.exclusions, *sample.priorities,
                        *(lvl for lvl in sample.levels if lvl["value_level"]),
                        *(b for lvl in sample.levels if not lvl["value_level"] for b in lvl["buckets"]))
            if _is_query(r)]


def mark_query_rules(conn: Connection, s: Settings, sample: Sample, rows: list, key_queries: list) -> dict:
    """Run each rule's SELECT and mark the pulled rows: 1 / 0 (the case is in its result or not), or the value
    it returns for the case (a PRIORITY EXPRESSION, a LEVEL by value; NULL for a case it does not return).
    Returns {rule_name: cases returned}. A result without the key_columns, or a value SELECT without exactly one
    more column or returning a case twice, is a ConfigError."""
    keys_lower = [k.lower() for k in sample.keys]
    counts = {}
    for rule, text in key_queries:
        name, cls, value_rule = rule["rule_name"], rule["rule_class"], _is_value(rule)
        what = f"{cls} rule {name}"
        log.info("%s (config_id=%s): %s", what, sample.config_id, text)
        result = conn.stream(text, s.chunk_rows)
        found = {}
        try:
            for r in result:
                key = case_key(r, sample.keys)
                if value_rule:
                    extra = [c for c in r if c not in keys_lower]
                    if len(extra) != 1:
                        raise ConfigError(f"returns {len(extra)} columns besides the key_columns, it needs one: the "
                                          f"value ({', '.join(extra) or 'none'})")
                    if key in found:
                        raise ConfigError(f"returns the case {key} more than once")
                    found[key] = r[extra[0]]
                else:
                    found[key] = 1
        except ConfigError as e:
            need = " and one value column" if value_rule else ""
            raise ConfigError(f"{what} must return the key_columns {', '.join(sample.keys)}{need}: {e}") from None
        col = _col(rule)
        for row in rows:
            row[col] = found.get(row["_key"]) if value_rule else (1 if row["_key"] in found else 0)
        counts[str(name)] = len(found)
    return counts


def resolve_params(conn: Connection, s: Settings, sample: Sample, given: dict, as_of: date) -> dict:
    """{as_of_date} + the run's parameters + every PARAM rule's lookup (a given value wins).
    A lookup with no value raises LookupError (often: the data is not loaded yet - retrying can help)."""
    params = {"as_of_date": as_of.isoformat(), **(given or {})}
    given_names = {str(k).lower() for k in (given or {})}
    for p in sample.params:
        name = str(p["rule_name"])
        if name.lower() in given_names:
            continue
        text = substitute_params(s.resolve_env(_text(p), identifier=False), params)
        rows = conn.query(text)
        value = next(iter(rows[0].values()), None) if rows else None
        if value is None:
            raise LookupError(f"run parameter {name}: its rule_sql returned no value")
        params[name] = value.isoformat() if isinstance(value, date) else value
    return params


def gate_queries(sample: Sample, s: Settings, params: dict) -> list:
    """(rule, SQL) of every GATE rule, $env and parameters resolved."""
    return [(g, substitute_params(s.resolve_env(_text(g), identifier=False), params)) for g in sample.gates]


def unmet_gates(conn: Connection, s: Settings, sample: Sample, params: dict) -> list:
    """The GATE rules whose SELECT returned no row: 'name (description)' each."""
    out = []
    for gate, text in gate_queries(sample, s, params):
        if not conn.query(text):
            out.append(f"{gate['rule_name']}" + (f" ({gate['description']})" if gate.get("description") else ""))
    return out


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
    rows = conn.query(f"SELECT case_key, MAX(selected_flag) AS sel, "
                      f"MAX(CASE WHEN selection_status = 'REPLACED' THEN 1 ELSE 0 END) AS replaced "
                      f"FROM {s.meta_db}.gre_sample_selections WHERE {' AND '.join(where)} GROUP BY case_key", params)
    # a REPLACED case was not audited (not "selected"), and was taken out on purpose (no carry-over)
    out = {"selected": {r["case_key"] for r in rows if int(r["sel"] or 0) == 1},
           "unselected": {r["case_key"] for r in rows if int(r["sel"] or 0) == 0 and int(r["replaced"] or 0) == 0}}
    cache[(scope, days)] = out
    return out


# ---------------------------------------------------------------------------
# Exclusions and priority
# ---------------------------------------------------------------------------

def apply_exclusions(rows: list, sample: Sample, history: dict) -> tuple:
    """(candidates, excluded, counts): each excluded row gets row['_reason'] = the first matching rule."""
    counts = {str(x["rule_name"]): 0 for x in sample.exclusions}
    candidates, excluded = [], []
    for row in rows:
        reason = None
        for x in sample.exclusions:
            if _up(x.get("rule_type"), QUERY) == QUERY:
                hit = row.get(_col(x)) == 1
            else:
                hit = row["_key"] in history[x["rule_id"]]["selected"]
            if hit:
                reason = str(x["rule_name"])
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
        kind, col = _up(p.get("rule_type"), QUERY), _col(p)
        if kind == QUERY:
            rows.sort(key=lambda r: 0 if r.get(col) == 1 else 1)
        elif kind == "EXPRESSION":
            if _up(p.get("sort_dir"), "ASC") == "DESC":
                rows.sort(key=lambda r: (r.get(col) is not None, 0 if r.get(col) is None else r.get(col)),
                          reverse=True)
            else:
                rows.sort(key=lambda r: _sortable(r.get(col)))
        elif kind == "PRIOR_CANDIDATE":
            seen = history[p["rule_id"]]["unselected"]
            rows.sort(key=lambda r: 0 if r["_key"] in seen else 1)
        elif kind == "PRIOR_SELECTION":
            seen = history[p["rule_id"]]["selected"]
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
        return {_norm(b["rule_name"]): b for b in by_pattern[best]}

    def _allocate(self, rows: list, target: int, index: int, path: str) -> list:
        while index < len(self.levels) and not _matches(path, self.levels[index].get("parent_path")):
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
        if not level["value_level"] and not group:   # a level of bucket SELECTs with no buckets for this parent
            return self._allocate(rows, target, index + 1, path)
        buckets, labels = {}, {}
        for r in rows:
            if level["value_level"]:
                value = r.get(_col(level))
                key = _norm(value)
                label = str(group[key]["rule_name"]).rstrip() if key in group else _label(value)
            else:
                # the first bucket (seq_no) whose SELECT returned the case; one without rule_sql takes the rest
                hit = next((b for b in group.values() if not _text(b) or r.get(_col(b)) == 1), None)
                key, label = (_norm(hit["rule_name"]), str(hit["rule_name"]).rstrip()) if hit \
                    else (_norm(OTHER_BUCKET), OTHER_BUCKET)
            buckets.setdefault(key, []).append(r)
            labels.setdefault(key, label)

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
# The run row, persistence
# ---------------------------------------------------------------------------

def count_prior_attempts(conn: Connection, s: Settings, config_id, run_key: str) -> int:
    rows = conn.query(f"SELECT COUNT(*) AS cnt FROM {s.meta_db}.gre_sampling_run "
                      f"WHERE config_id = ? AND run_key = ?", [config_id, run_key])
    return int(rows[0]["cnt"]) if rows else 0


def current_run(conn: Connection, s: Settings, config_id, run_key: str) -> Optional[dict]:
    """The run row whose sample is current for (config_id, run_key), or None."""
    rows = conn.query(f"SELECT * FROM {s.meta_db}.gre_sampling_run "
                      f"WHERE config_id = ? AND run_key = ? AND etl_is_curr_ind = 'Y'", [config_id, run_key])
    return rows[0] if rows else None


def _start_run(conn, s, run_id, run_key, sample: Sample, seed, target, as_of, params, triggered_by, started,
               override_reason=None):
    config = sample.config
    conn.execute(f"""
        INSERT INTO {s.meta_db}.gre_sampling_run (
            run_id, config_id, project_name, process_name, sample_name, run_key, as_of_date, sampling_method,
            random_seed, target_volume, run_params_txt, status, etl_is_curr_ind, config_hash, override_reason,
            triggered_by, started_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', 'N', ?, ?, ?, ?)""",
        [run_id, config.get("config_id"), config.get("project_name"), config.get("process_name"),
         config.get("sample_name"), run_key, as_of, sample.method, seed, target,
         json.dumps(params, default=str)[:4000], sample.config_hash, override_reason, triggered_by, started])


def _finish_run(conn, s, run_id, summary, sample: Sample, error_type=None, message=None, detail=None):
    """Close the run row: COMPLETED (current) or ERROR (error_type, message, detail). The run_report always
    holds the config and rule rows used."""
    report = {"config_hash": sample.config_hash, "config": sample.snapshot}
    if not error_type:
        report.update(exclusions=summary.get("exclusions"), buckets=summary.get("buckets"))
    report = json.dumps(report, default=str)
    conn.execute(f"""
        UPDATE {s.meta_db}.gre_sampling_run
        SET status = ?, ended_at = ?, total_in_scope = ?, total_excluded = ?, total_candidates = ?,
            total_selected = ?, shortfall = ?, error_type = ?, error_message = ?, error_detail = ?, run_report = ?,
            etl_is_curr_ind = ?, last_updated_datetime = ?
        WHERE run_id = ?""",
        ["ERROR" if error_type else "COMPLETED", s.now(), summary.get("in_scope"), summary.get("excluded"),
         summary.get("candidates"), summary.get("selected"), summary.get("shortfall"), error_type,
         (message or None) and message[:2000], detail, report, "N" if error_type else "Y", s.now(), run_id])


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


def _write_output(conn, s, queries, sample: Sample, run_id, run_key, as_of, output: dict, cases: list, now):
    """cases: (case_key, selection_order, priority_rank, stratum_path) of each row to write."""
    names = [str(o["rule_name"]) for o in sample.output_cols]
    conn.executemany(
        f"INSERT INTO {queries['output_table']} ({', '.join([*OUTPUT_META_COLS, *names])}) "
        f"VALUES ({', '.join(['?'] * (len(OUTPUT_META_COLS) + len(names)))})",
        [[run_id, sample.config_id, run_key, as_of, key, order, rank, path, "Y", now,
          *(output[key].get(n.lower()) for n in names)] for key, order, rank, path in cases], s.chunk_rows)


def _persist(conn, s, run_id, run_key, sample: Sample, queries, candidates, excluded, chosen, output,
             as_of: date) -> int:
    """Write the run's rows and retire earlier runs of the same (config_id, run_key); returns how many earlier
    runs were retired. Runs inside the caller's transaction."""
    now = s.now()
    c = sample.config
    cid, project, process = sample.config_id, c.get("project_name"), c.get("process_name")
    order = {r["_key"]: i + 1 for i, r in enumerate(chosen)}
    rows = [[run_id, cid, project, process, run_key, as_of, r["_key"], r["_order"] + 1,
             1 if r["_key"] in order else 0, order.get(r["_key"]), 0, None, r.get("_path"),
             "SELECTED" if r["_key"] in order else None, now] for r in candidates]
    if _up(c.get("keep_excluded"), "1") not in ("0", "N", "FALSE"):
        rows += [[run_id, cid, project, process, run_key, as_of, r["_key"], None, 0, None, 1, r["_reason"], None,
                  None, now] for r in excluded]
    conn.executemany(f"""
        INSERT INTO {s.meta_db}.gre_sample_selections (
            sample_run_id, config_id, project_name, process_name, run_key, sample_cycle, case_key, priority_rank,
            selected_flag, selection_order, excluded_flag, exclusion_reason, stratum_path, selection_status,
            etl_is_curr_ind, load_datetime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y', ?)""", rows, s.chunk_rows)
    tables = [f"{s.meta_db}.gre_sample_selections"]
    if output is not None:
        _write_output(conn, s, queries, sample, run_id, run_key, as_of, output,
                      [(r["_key"], order[r["_key"]], r["_order"] + 1, r.get("_path")) for r in chosen], now)
        tables.append(queries["output_table"])

    for table in tables:
        conn.execute(f"""
            UPDATE {table}
            SET etl_is_curr_ind = 'N', last_updated_datetime = ?
            WHERE config_id = ? AND run_key = ? AND sample_run_id <> ? AND etl_is_curr_ind = 'Y'""",
            [now, cid, run_key, run_id])
    return conn.execute(f"""
        UPDATE {s.meta_db}.gre_sampling_run
        SET etl_is_curr_ind = 'N', last_updated_datetime = ?
        WHERE config_id = ? AND run_key = ? AND run_id <> ? AND etl_is_curr_ind = 'Y'""",
        [now, cid, run_key, run_id])


# ---------------------------------------------------------------------------
# One config
# ---------------------------------------------------------------------------

def run_sampling(conn: Connection, s: Settings, config_id, run_key: Optional[str] = None,
                 run_params: Optional[dict] = None, seed: Optional[int] = None, triggered_by: str = "SYSTEM",
                 as_of: Optional[date] = None, dry_run: bool = False, force_reason: Optional[str] = None,
                 extra_filters: Optional[str] = None) -> dict:
    """Run one sampling config. Returns its report (status COMPLETED / ERROR; error_kind 'config' when retrying
    cannot help, 'technical' otherwise). Raises ConfigError only when the config row itself cannot be loaded.

    run_key    : which logical run this is; may hold {name} tokens (e.g. '{batch_id}'). Default: the as-of
                 date. A rerun of the same (config_id, run_key) replaces the earlier one as the current sample.
    run_params : values for {name} / $name tokens in any configured SQL; PARAM rules' lookups add more.
                 {as_of_date} and {run_key} are always available.
    seed       : RANDOM / SYSTEMATIC seed (default: a new one, stored on the run row).
    as_of      : the business date of the run (default: today in Settings.timezone) -> sample_cycle.
    dry_run    : do everything but write (no run row).
    force_reason: replace a FINAL sample of this run_key (recorded on the new run row); without it a rerun of a
                 finalised run_key fails as FINALIZED.
    extra_filters: a SQL condition on the universe table ANDed to scope_sql for this run (as the GRE's
                 extra_filters): which load, batch or SCD2 version of an incremental table to read, e.g.
                 "BTCH_ID = 'B42' AND ETL_IS_CURR_IND = 'Y'". Kept with the run's parameters, so a later case
                 replacement reads the same rows.
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

    early = None   # (kind, error_type, exception) of a failure before the run row exists
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
    if str(extra_filters or "").strip():
        params[EXTRA_FILTERS] = str(extra_filters).strip()

    label = ".".join(str(p) for p in (config.get("project_name"), config.get("sample_name")) if p)
    attempt = 0 if dry_run else count_prior_attempts(conn, s, config_id, run_key)
    run_id = generate_run_id(label or f"config-{config_id}", run_key, f"attempt-{attempt + 1}", triggered_by,
                             timestamp=started)
    summary = {"config_id": config_id, "project_name": config.get("project_name"),
               "process_name": config.get("process_name"), "sample_name": config.get("sample_name"),
               "sample_run_id": None if dry_run else run_id, "run_key": run_key, "as_of_date": as_of.isoformat(),
               "method": method, "seed": run_seed, "target_volume": target, "params": params, "in_scope": None,
               "excluded": None, "candidates": 0, "selected": 0, "shortfall": None, "exclusions": {},
               "by_stratum": {}, "buckets": {}, "retired_runs": 0, "dry_run": dry_run,
               "config_hash": sample.config_hash}
    log.info("sampling starting: config_id=%s sample=%s run_key=%s method=%s target=%s meta_db=%s host=%s%s",
             config_id, label, run_key, method, target, s.meta_db, conn.host, " (dry run)" if dry_run else "")

    def fail(kind: str, error_type: str, exc_or_text, detail=None) -> dict:
        message = exc_or_text if isinstance(exc_or_text, str) else error_text(exc_or_text)
        log.error("sampling config_id=%s %s: %s", config_id, error_type, message)
        summary.update(status="ERROR", error_kind=kind, error_type=error_type, error=message)
        if not dry_run:
            try:
                _finish_run(conn, s, run_id, summary, sample, error_type, message, detail)
            except Exception as e:  # noqa: BLE001 - must not hide the real error
                log.error("could not close the run row of %s: %s", run_id, e)
        return summary

    final = None if early else current_run(conn, s, config_id, run_key)
    final = final if final and final.get("status") == "FINAL" else None
    force_reason = str(force_reason or "").strip() or None
    if not dry_run:
        _start_run(conn, s, run_id, run_key, sample, run_seed, target, as_of, params, triggered_by, started,
                   force_reason if final else None)
    if early:
        return fail(early[0], early[1], early[2])
    if final:
        summary["finalized_run"] = final["run_id"]
        if not force_reason and not dry_run:
            return fail("config", "FINALIZED", f"the sample of run_key {run_key} is final (run {final['run_id']}, "
                                               f"finalised by {final.get('finalized_by')} at "
                                               f"{final.get('finalized_at')}): rerun with a force reason to replace it")
        if force_reason:
            log.warning("replacing the FINAL sample %s of run_key %s: %s", final["run_id"], run_key, force_reason)
    problems = config_problems(sample)
    if problems:
        return fail("config", "CONFIG_ERROR", "; ".join(problems))
    try:
        queries = build_queries(sample, s, params)
        summary["output_table"] = queries.get("output_table")
        gate_queries(sample, s, params)
    except (ValueError, KeyError) as e:
        return fail("config", "CONFIG_ERROR", e)
    try:
        waiting = unmet_gates(conn, s, sample, params)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "GATE_FAILURE", e, traceback.format_exc())
    if waiting:
        # technical: the data may be ready on a retry
        return fail("technical", "GATE_NOT_MET", "not ready yet: " + "; ".join(waiting))

    try:
        if queries["count"]:
            log.info("scope count (config_id=%s): %s", config_id, queries["count"])
        log.info("pull (config_id=%s): %s", config_id, queries["pull"])
        rows = conn.stream(queries["pull"], s.chunk_rows)
        in_scope = int(conn.query(queries["count"])[0]["cnt"]) if queries["count"] else len(rows)
        history, cache = {}, {}
        for x in sample.exclusions:
            if _up(x.get("rule_type"), QUERY) in HISTORY_TYPES:
                history[x["rule_id"]] = _history(conn, s, sample, x, run_key, as_of, cache)
        for p in sample.priorities:
            if _up(p.get("rule_type"), QUERY) in HISTORY_TYPES:
                history[p["rule_id"]] = _history(conn, s, sample, p, run_key, as_of, cache)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "PULL_FAILURE", e, traceback.format_exc())

    try:
        for r in rows:
            r["_key"] = case_key(r, sample.keys)
        if len({r["_key"] for r in rows}) != len(rows):
            raise ConfigError("key_columns do not identify a case uniquely (the pull returned duplicate keys)")
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    try:
        summary["rule_queries"] = mark_query_rules(conn, s, sample, rows, queries["key_queries"])
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "QUERY_RULE_FAILURE", e, traceback.format_exc())

    try:
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

    try:
        with conn.transaction():
            summary["retired_runs"] = _persist(conn, s, run_id, run_key, sample, queries, candidates, excluded,
                                               chosen, output, as_of)
            _finish_run(conn, s, run_id, summary, sample)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "PERSIST_FAILURE", e, traceback.format_exc())

    summary["status"] = "COMPLETED"
    log.info("sampling complete: %s in_scope=%s excluded=%s candidates=%d selected=%d/%d retired_runs=%d", run_id,
             in_scope, summary["excluded"], len(candidates), len(chosen), target, summary["retired_runs"])
    return summary


# ---------------------------------------------------------------------------
# After a run: finalise, replace a case
# ---------------------------------------------------------------------------

def finalize_run(conn: Connection, s: Settings, config_id, run_key: str, by: str) -> dict:
    """Make the current sample of (config_id, run_key) FINAL: a later run of that run_key needs a force reason."""
    run = current_run(conn, s, config_id, run_key)
    if not run:
        raise ConfigError(f"config_id={config_id} has no current sample for run_key {run_key!r}")
    if run["status"] == "FINAL":
        raise ConfigError(f"run {run['run_id']} is already final (by {run.get('finalized_by')})")
    conn.execute(f"UPDATE {s.meta_db}.gre_sampling_run SET status = 'FINAL', finalized_by = ?, finalized_at = ?, "
                 f"last_updated_datetime = ? WHERE run_id = ? AND status = 'COMPLETED'",
                 [by, s.now(), s.now(), run["run_id"]])
    log.info("run %s (config_id=%s run_key=%s) finalised by %s", run["run_id"], config_id, run_key, by)
    return {"run_id": run["run_id"], "config_id": config_id, "run_key": run_key, "status": "FINAL",
            "finalized_by": by}


def parse_case_key(text: str, keys: list) -> dict:
    """{key column: value} of a 'col1=val1|col2=val2' case key (values as text, NULL -> None)."""
    parts = str(text).split("|")
    if len(parts) != len(keys):
        raise ConfigError(f"case_key {text!r} does not have the {len(keys)} key column(s) {keys}")
    out = {}
    for column, part in zip(keys, parts):
        name, _, value = part.partition("=")
        if name.strip().lower() != column.lower():
            raise ConfigError(f"case_key {text!r}: expected column {column}, found {name}")
        out[column.lower()] = None if value == NULL_BUCKET else value
    return out


def replace_case(conn: Connection, s: Settings, config_id, run_key: str, case_key_text: str, reason: str, by: str,
                 any_bucket: bool = False) -> dict:
    """Take one selected case out of the current sample of (config_id, run_key) and put the next candidate in:
    the same bucket (any_bucket: anywhere), never selected or replaced before, in priority order (RANDOM: a
    draw seeded by the run's seed). Allowed on a FINAL sample. The output snapshot follows. One transaction."""
    if not str(reason or "").strip():
        raise ConfigError("a reason is required to replace a case")
    run = current_run(conn, s, config_id, run_key)
    if not run or run["status"] not in ("COMPLETED", "FINAL"):
        raise ConfigError(f"config_id={config_id} has no completed current sample for run_key {run_key!r}")
    db, run_id = s.meta_db, run["run_id"]
    rows = conn.query(f"SELECT case_key, selected_flag, selection_order, priority_rank, excluded_flag, stratum_path, "
                      f"selection_status FROM {db}.gre_sample_selections WHERE sample_run_id = ?", [run_id])
    old = next((r for r in rows if r["case_key"] == case_key_text), None)
    if not old or int(old["selected_flag"] or 0) != 1:
        raise ConfigError(f"{case_key_text!r} is not in the current sample of run_key {run_key!r}")
    pool = [r for r in rows if int(r["selected_flag"] or 0) == 0 and int(r["excluded_flag"] or 0) == 0
            and r["selection_status"] is None]
    if not any_bucket:
        pool = [r for r in pool if r["stratum_path"] == old["stratum_path"]]
    if not pool:
        raise ConfigError(f"no candidate left to replace {case_key_text!r}"
                          + ("" if any_bucket else f" in bucket {old['stratum_path']!r} (allow any bucket)"))
    pool.sort(key=lambda r: (r["priority_rank"] is None, r["priority_rank"] or 0, r["case_key"]))
    if _up(run.get("sampling_method")) == "RANDOM":
        new = random.Random(f"{run.get('random_seed')}|{run_id}|{case_key_text}").choice(pool)
    else:
        new = pool[0]

    output = queries = sample = None
    report = json.loads(run.get("run_report") or "{}")
    snap = report.get("config") or {}
    if str((snap.get("config") or {}).get("output_table") or "").strip():
        sample = build_sample(snap["config"], snap.get("rules") or [])   # the rules that made this sample
        params = json.loads(run.get("run_params_txt") or "{}")
        queries = build_queries(sample, s, params)
        row = {**parse_case_key(new["case_key"], sample.keys), "_key": new["case_key"]}
        output = fetch_output(conn, queries, sample, [row])

    now = s.now()
    with conn.transaction():
        for key, flag, order, status, linked in ((old["case_key"], 0, None, "REPLACED", new["case_key"]),
                                                 (new["case_key"], 1, old["selection_order"], "REPLACEMENT",
                                                  old["case_key"])):
            conn.execute(f"""
                UPDATE {db}.gre_sample_selections
                SET selected_flag = ?, selection_order = ?, selection_status = ?, linked_case_key = ?,
                    status_reason = ?, status_by = ?, status_at = ?, last_updated_datetime = ?
                WHERE sample_run_id = ? AND case_key = ?""",
                [flag, order, status, linked, reason[:1000], by, now, now, run_id, key])
        if output is not None:
            conn.execute(f"UPDATE {queries['output_table']} SET etl_is_curr_ind = 'N', last_updated_datetime = ? "
                         f"WHERE sample_run_id = ? AND case_key = ? AND etl_is_curr_ind = 'Y'",
                         [now, run_id, old["case_key"]])
            _write_output(conn, s, queries, sample, run_id, run_key, run.get("as_of_date"), output,
                          [(new["case_key"], old["selection_order"], new["priority_rank"], new["stratum_path"])], now)
        conn.execute(f"UPDATE {db}.gre_sampling_run SET last_updated_datetime = ? WHERE run_id = ?", [now, run_id])
    log.info("run %s: %s replaced by %s (%s) by %s", run_id, old["case_key"], new["case_key"], reason, by)
    return {"run_id": run_id, "replaced": old["case_key"], "replacement": new["case_key"],
            "stratum_path": new["stratum_path"], "selection_order": old["selection_order"], "reason": reason,
            "by": by}


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
             check_sql: bool = False, as_of: Optional[date] = None, extra_filters: Optional[str] = None) -> dict:
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
                if str(extra_filters or "").strip():
                    params[EXTRA_FILTERS] = str(extra_filters).strip()
                queries = build_queries(sample, s, params)
                for key in ("pull", "count", "output"):
                    if queries.get(key):
                        conn.query("EXPLAIN " + queries[key] + (" AND 1=0" if key == "output" else ""))
                for _, text in [*gate_queries(sample, s, params), *queries["key_queries"]]:
                    conn.query("EXPLAIN " + text)
            except Exception as e:  # noqa: BLE001
                problems.append(f"SQL check failed: {error_text(e)}")
        out.append({"config_id": cid, "sample_name": sample.config.get("sample_name"), "problems": problems})
    bad = sum(1 for c in out if c["problems"])
    return {"result": "VALID" if not bad else f"{bad} CONFIG(S) WITH PROBLEMS", "configs": out}
