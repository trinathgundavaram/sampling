from __future__ import annotations

import hashlib
import json
import logging
import math
import random
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from fnmatch import fnmatchcase
from typing import Optional, Sequence

from sampling.db import ERROR_TEXT_MAX, Connection, error_text, generate_run_id, log_sql, substitute_params
from sampling.settings import ConfigError, Settings


log = logging.getLogger(__name__)

VALID_METHODS = ("RANKED", "RANDOM", "SYSTEMATIC")
VALID_ROUNDING = ("FLOOR", "ROUND", "CEIL")
QUOTA_MODES = ("PCT", "CNT", "ALL", "REMAINDER")
AMOUNT_TYPES = ("CNT", "PCT")
UNLISTED_POLICIES = ("SHARE", "REMAINDER", "SKIP")
QUERY = "QUERY"
EXCLUSION_TYPES = (QUERY, "PRIOR_SELECTION")
PRIORITY_TYPES = (QUERY, "EXPRESSION", "PRIOR_CANDIDATE", "PRIOR_SELECTION")
BUCKET_TYPES = (QUERY,)
PER_VALUE = "PER_VALUE"
CAP_TYPES = (QUERY, PER_VALUE)
HISTORY_TYPES = ("PRIOR_CANDIDATE", "PRIOR_SELECTION")
HISTORY_SCOPES = ("SAMPLE", "TABLE", "PROJECT")
SHORTFALL_POLICIES = ("TOPUP", "NONE")
OTHER_BUCKET = "OTHER"
NULL_BUCKET = "NULL"
SAMPLE_AUDIT_COLS = ("etl_is_curr_ind", "etl_is_del_ind", "etl_load_dt_tm", "etl_last_updt_dt_tm")
SAMPLE_RUN_COLS = ("smpl_run_id", "smpl_run_key", "smpl_run_ty", "smpl_cycle_dt", "smpl_order_nbr",
                   "smpl_stratum_path")
SAMPLE_REQUIRED = ("smpl_run_key", "etl_is_curr_ind")
OUTPUT_KEY_CHUNK = 50
RULE_CLASSES = ("PARAM", "GATE", "EXCLUSION", "PRIORITY", "LEVEL", "BUCKET", "CAP")
_AUDIT_COLUMNS = ("load_dtts", "loaduser_id", "lastupdt_dtts", "lastupdt_id")
SAMPLE_KEY = ("project_cd", "table_nm", "run_ty")
ALL_TABLES = "*"
_SNAPSHOT_RULE_COLUMNS = ("rule_id", "project_cd", "table_nm", "run_ty", "rule_class_cd", "seq_nbr", "rule_nm",
                          "rule_ty", "rule_sql_txt", "level_nbr", "parent_path_txt", "unlisted_cd", "quota_cd",
                          "quota_val", "max_ty", "max_val", "sort_dir_cd", "lookback_days", "history_scope_cd",
                          "rule_desc")
_RUN_CONFIG_COLUMNS = ("src_schema_nm", "src_table_nm", "key_columns_txt", "scope_sql_txt", "target_ty",
                       "target_val", "min_target_cnt", "max_target_cnt", "smpl_method_cd", "rounding_cd",
                       "diversity_sql_txt", "diversity_cap_ty", "diversity_cap_val", "shortfall_cd",
                       "keep_excluded_ind", "tgt_schema_nm", "tgt_table_nm")
STEP = "SAMPLING"
_RULE_COLUMNS = ("rule_ty", "rule_sql_txt", "level_nbr", "parent_path_txt", "unlisted_cd", "quota_cd",
                 "quota_val", "max_ty", "max_val", "sort_dir_cd", "lookback_days", "history_scope_cd")
_USED = {"PARAM": {"rule_sql_txt"},
         "GATE": {"rule_sql_txt"},
         "EXCLUSION": {"rule_ty", "rule_sql_txt", "lookback_days", "history_scope_cd"},
         "PRIORITY": {"rule_ty", "rule_sql_txt", "sort_dir_cd", "lookback_days", "history_scope_cd"},
         "LEVEL": {"rule_sql_txt", "level_nbr", "parent_path_txt", "unlisted_cd"},
         "BUCKET": {"rule_ty", "rule_sql_txt", "level_nbr", "parent_path_txt", "quota_cd", "quota_val", "max_ty",
                    "max_val"},
         "CAP": {"rule_ty", "rule_sql_txt", "parent_path_txt", "max_ty", "max_val"}}
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]{0,127}$")
_STATEMENT_WORDS = ("INSERT", "INS", "UPDATE", "UPD", "DELETE", "DEL", "MERGE", "UPSERT", "REPLACE", "DROP",
                    "CREATE", "ALTER",
                    "RENAME", "MODIFY", "GRANT", "REVOKE", "CALL", "EXEC", "EXECUTE", "COLLECT", "COMMENT",
                    "COMMIT", "ROLLBACK", "ABORT", "BEGIN TRANSACTION", "END TRANSACTION", "SET SESSION",
                    "SET QUERY_BAND")
_STATEMENT_RE = re.compile(r"\b(" + "|".join(w.replace(" ", r"\s+") for w in _STATEMENT_WORDS) + r")\b", re.I)
_LOCK_FOR_WRITE = re.compile(r"\bLOCK(ING)?\b.*?\bFOR\s+(WRITE|EXCLUSIVE|EXCL|LOAD)\b", re.I | re.S)
_QUOTED = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|/\*.*?\*/", re.S)
_SELECT_START = re.compile(r"^\s*(LOCKING\s+.*?\s+FOR\s+ACCESS\s+)?\(*\s*(SELECT|SEL|WITH)\b", re.I | re.S)
_CONFIG_SQL = ("scope_sql_txt", "diversity_sql_txt")
_NEEDS_ORDER = ("RANKED", "SYSTEMATIC")

DIVERSITY_RULE = {"rule_id": "div", "rule_class_cd": "DIVERSITY", "rule_nm": "Diversity_Sql_Txt"}
DIVERSITY = "smp_r_div"
SCOPE_ALIAS = "smp_scope"
EXTRA_FILTERS = "extra_filters"
RUN_KEY_MAX, RUN_PARAMS_MAX, USER_MAX = 250, 4000, 100
BUILT_IN_PARAMS = {"as_of_date", "project_cd", "table_nm", "run_ty", "run_key", EXTRA_FILTERS}


def _up(value, default: str = "") -> str:
    return str(value if value not in (None, "") else default).strip().upper()


def _norm(value) -> str:
    return NULL_BUCKET if value is None else str(value).rstrip().upper()


def _label(value) -> str:
    return NULL_BUCKET if value is None else str(value).rstrip()


def _matches(path: str, pattern) -> bool:
    return fnmatchcase(path.upper(), _up(pattern, "*"))


def _specificity(pattern: str) -> tuple:
    p = _up(pattern, "*")
    return (p.count("*") + p.count("?"), -len(p))


@dataclass
class Sample:
    config: dict
    params: list = field(default_factory=list)
    gates: list = field(default_factory=list)
    exclusions: list = field(default_factory=list)
    priorities: list = field(default_factory=list)
    levels: list = field(default_factory=list)
    caps: list = field(default_factory=list)
    stray: list = field(default_factory=list)
    snapshot: dict = field(default_factory=dict)
    config_hash: str = ""

    @property
    def key(self) -> tuple:
        return tuple(_up(self.config.get(k)) for k in SAMPLE_KEY)

    @property
    def label(self) -> str:
        return ".".join(self.key)

    @property
    def method(self) -> str:
        return _up(self.config.get("smpl_method_cd"), "RANKED")

    @property
    def keys(self) -> list:
        return key_columns(self.config)


def key_columns(config: dict) -> list:
    return [c.strip() for c in str(config.get("key_columns_txt") or "").split(",") if c.strip()]


def _text(row: dict, col: str = "rule_sql_txt") -> str:
    return str(row.get(col) or "").strip()


def sample_key(key) -> tuple:
    parts = key.split(".") if isinstance(key, str) else list(key)
    if len(parts) != 3 or not all(str(p or "").strip() for p in parts):
        raise ConfigError(f"a sample is (project, table, run_type), got {key!r}")
    return tuple(_up(p) for p in parts)


def resolve_rules(rules: list, table: str, run_type: str) -> list:
    table, run_type, best = _up(table), _up(run_type), {}
    for r in rules:
        t, rt = _up(r.get("table_nm"), ALL_TABLES), _up(r.get("run_ty"), ALL_TABLES)
        if t not in (table, ALL_TABLES) or rt not in (run_type, ALL_TABLES):
            continue
        rank = (t != ALL_TABLES) * 2 + (rt != ALL_TABLES)
        name = (_up(r.get("rule_class_cd")), _up(r.get("rule_nm")), r.get("level_nbr"),
                _up(r.get("parent_path_txt"), "*"))
        if name not in best or rank > best[name][0] or (rank == best[name][0] and r["rule_id"] > best[name][1]["rule_id"]):
            best[name] = (rank, r)
    return [r for _, r in best.values()]


def load_config(conn: Connection, s: Settings, key) -> Sample:
    db, (project, table, run_type) = s.meta_db, sample_key(key)
    rows = conn.query(f"SELECT * FROM {db}.GOVT_RGLTRY_SamplingConfig WHERE UPPER(Project_Cd) = ? "
                      f"AND UPPER(Table_Nm) = ? AND UPPER(Run_Ty) = ? AND Active_Ind = 1", [project, table, run_type])
    if not rows:
        raise ConfigError(f"no active GOVT_RGLTRY_SamplingConfig row for {project}.{table}.{run_type}")
    if len(rows) > 1:
        raise ConfigError(f"{len(rows)} active GOVT_RGLTRY_SamplingConfig rows for {project}.{table}.{run_type}")
    rules = conn.query(f"SELECT * FROM {db}.GOVT_RGLTRY_SamplingRule WHERE UPPER(Project_Cd) = ? "
                       f"AND UPPER(Table_Nm) IN (?, '*') AND UPPER(Run_Ty) IN (?, '*') AND Active_Ind = 1",
                       [project, table, run_type])
    return build_sample(rows[0], resolve_rules(rules, table, run_type))


def snapshot_hash(snapshot: dict) -> str:
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def build_sample(config: dict, rules: list) -> Sample:
    def clean(row):
        return {k: v for k, v in row.items() if k not in _AUDIT_COLUMNS and k != "active_ind"}
    snapshot = {"config": clean(config), "rules": sorted((clean(r) for r in rules), key=lambda r: r["rule_id"])}
    ordered = sorted((dict(r) for r in snapshot["rules"]),
                     key=lambda r: (r.get("seq_nbr") is not None, r.get("seq_nbr") or 0, r["rule_id"]))
    by_class = {c: [] for c in RULE_CLASSES}
    sample = Sample(config=dict(config), snapshot=snapshot, config_hash=snapshot_hash(snapshot))
    for r in ordered:
        r["rule_class_cd"] = _up(r.get("rule_class_cd"))
        by_class.get(r["rule_class_cd"], sample.stray).append(r)
    sample.params, sample.gates, sample.exclusions = by_class["PARAM"], by_class["GATE"], by_class["EXCLUSION"]
    sample.priorities, sample.caps = by_class["PRIORITY"], by_class["CAP"]
    levels = sorted(by_class["LEVEL"], key=lambda r: (r.get("level_nbr") is None, r.get("level_nbr") or 0,
                                                      r["rule_id"]))
    for level in levels:
        level["buckets"] = [b for b in by_class["BUCKET"] if b.get("level_nbr") == level.get("level_nbr")]
        level["value_level"] = bool(_text(level))
    orders = {lvl.get("level_nbr") for lvl in levels}
    sample.stray += [b for b in by_class["BUCKET"] if b.get("level_nbr") not in orders]
    sample.levels = levels
    return sample


def config_problems(sample: Sample) -> list:
    c, problems = sample.config, []
    method = sample.method
    if method not in VALID_METHODS:
        problems.append(f"smpl_method_cd {method!r} must be one of {', '.join(VALID_METHODS)}")
    ordered = any(p for p in sample.priorities if _up(p.get("rule_ty"), QUERY) in PRIORITY_TYPES)
    if method in _NEEDS_ORDER and not ordered:
        problems.append(f"smpl_method_cd {method} needs PRIORITY rules (an EXPRESSION rule orders by a value)")
    if _up(c.get("rounding_cd"), "FLOOR") not in VALID_ROUNDING:
        problems.append(f"rounding_cd must be one of {', '.join(VALID_ROUNDING)}")
    if _up(c.get("shortfall_cd"), "TOPUP") not in SHORTFALL_POLICIES:
        problems.append(f"shortfall_cd must be one of {', '.join(SHORTFALL_POLICIES)}")
    problems += _target_problems(c)
    if not sample.keys:
        problems.append("key_columns_txt is empty")
    for col in ("src_schema_nm", "src_table_nm"):
        name = _text(c, col)
        if not name:
            problems.append(f"{col} is empty (the source table: Src_Schema_Nm.Src_Table_Nm)")
        elif not _IDENT.match(name):
            problems.append(f"{col} {name!r} must be a plain name ($env allowed), the database and the table apart")
    cap = _amount_problems(c, "diversity_cap_ty", "diversity_cap_val", minimum=1)
    problems += cap
    if not cap and c.get("diversity_cap_val") not in (None, "") and not _text(c, "diversity_sql_txt"):
        problems.append("diversity_cap_val needs diversity_sql_txt")
    if str(c.get("diversity_sql_txt") or "").strip() and method == "SYSTEMATIC":
        problems.append("diversity_sql_txt cannot be combined with SYSTEMATIC (it steps through a fixed order)")

    for p in sample.params:
        if str(p.get("rule_nm") or "").lower() in BUILT_IN_PARAMS:
            problems.append(f"PARAM rule {p.get('rule_nm')}: the name is set by the engine "
                            f"({', '.join(sorted(BUILT_IN_PARAMS))})")
    for r in sample.stray:
        if r["rule_class_cd"] == "BUCKET":
            problems.append(f"rule {r['rule_id']}: BUCKET of level_nbr {r.get('level_nbr')}, which has no LEVEL")
        else:
            problems.append(f"rule {r['rule_id']}: rule_class_cd {r['rule_class_cd']!r} is not one of "
                            f"{', '.join(RULE_CLASSES)}")
    for cls in RULE_CLASSES:
        for r in _rules_of(sample, cls):
            for col in _RULE_COLUMNS:
                value = r.get(col)
                if col not in _USED[cls] and value not in (None, "") and not (col == "parent_path_txt" and value == "*"):
                    problems.append(f"{cls} rule {r['rule_nm']}: {col} is not used by this class")
            if not str(r.get("rule_nm") or "").strip():
                problems.append(f"{cls} rule {r['rule_id']}: rule_nm is empty")

    for p in sample.params:
        if not _IDENT.match(str(p.get("rule_nm") or "")):
            problems.append(f"PARAM rule {p.get('rule_id')}: rule_nm must be a plain name")
        if not _text(p):
            problems.append(f"PARAM rule {p.get('rule_nm')}: rule_sql_txt is empty")

    for g in sample.gates:
        if not _text(g):
            problems.append(f"GATE rule {g.get('rule_nm')}: rule_sql_txt is empty")

    for x in sample.exclusions:
        name, kind = x.get("rule_nm"), _up(x.get("rule_ty"), QUERY)
        if kind not in EXCLUSION_TYPES:
            problems.append(f"EXCLUSION rule {name}: rule_ty must be one of {', '.join(EXCLUSION_TYPES)}")
        elif kind == QUERY and not _text(x):
            problems.append(f"EXCLUSION rule {name}: rule_sql_txt is empty")
        elif kind in HISTORY_TYPES:
            problems += _history_problems(f"EXCLUSION rule {name}", x)

    for p in sample.priorities:
        name, kind = p.get("rule_nm"), _up(p.get("rule_ty"), QUERY)
        if kind not in PRIORITY_TYPES:
            problems.append(f"PRIORITY rule {name}: rule_ty must be one of {', '.join(PRIORITY_TYPES)}")
        elif kind in (QUERY, "EXPRESSION") and not _text(p):
            problems.append(f"PRIORITY rule {name}: rule_sql_txt is empty")
        elif kind in HISTORY_TYPES:
            problems += _history_problems(f"PRIORITY rule {name}", p)
        if kind == "EXPRESSION" and _up(p.get("sort_dir_cd"), "ASC") not in ("ASC", "DESC"):
            problems.append(f"PRIORITY rule {name}: sort_dir_cd must be ASC or DESC")
        elif kind != "EXPRESSION" and p.get("sort_dir_cd") not in (None, ""):
            problems.append(f"PRIORITY rule {name}: sort_dir_cd is only used by rule_ty EXPRESSION")

    orders = [lvl.get("level_nbr") for lvl in sample.levels]
    if None in orders:
        problems.append("every LEVEL rule needs level_nbr")
    if len(orders) != len(set(orders)):
        problems.append(f"LEVEL rules repeat a level_nbr: {orders}")
    for lvl in sample.levels:
        problems += _level_problems(lvl)
    for cap in sample.caps:
        name, kind = cap.get("rule_nm"), _up(cap.get("rule_ty"), QUERY)
        if kind not in CAP_TYPES:
            problems.append(f"CAP rule {name}: rule_ty must be one of {', '.join(CAP_TYPES)}")
        if not _text(cap):
            problems.append(f"CAP rule {name}: rule_sql_txt is empty (a SELECT of the cases it limits)")
        problems += [f"CAP rule {name}: {p}" for p in _amount_problems(cap, "max_ty", "max_val", required=True)]

    db, table = _text(c, "tgt_schema_nm"), _text(c, "tgt_table_nm")
    if bool(db) != bool(table):
        problems.append("tgt_schema_nm and tgt_table_nm go together: set both (the target table) or neither")
    for col, name in (("tgt_schema_nm", db), ("tgt_table_nm", table)):
        if name and not _IDENT.match(name):
            problems.append(f"{col} {name!r} must be a plain name ($env allowed), the database and the table apart")
    return problems + _sql_problems(sample)


def _number(value) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _whole(value) -> bool:
    n = _number(value)
    return n is not None and abs(n - round(n)) < 1e-9


def _amount_problems(row: dict, ty_col: str, val_col: str, minimum: float = 0, required: bool = False) -> list:
    kind, value = row.get(ty_col), row.get(val_col)
    if value in (None, "") and kind in (None, ""):
        return [f"{val_col} is empty"] if required else []
    kind = _up(kind, "CNT" if required else "")
    if kind not in AMOUNT_TYPES:
        return [f"{ty_col} must be one of {', '.join(AMOUNT_TYPES)} (with {val_col})"]
    n = _number(value)
    if n is None:
        return [f"{val_col} {value!r} must be a number ({ty_col} {kind})"]
    if kind == "CNT" and (not _whole(n) or n < minimum):
        return [f"{val_col} must be a whole number >= {minimum:g} ({ty_col} CNT)"]
    if kind == "PCT" and not 0 < n <= 100:
        return [f"{val_col} must be above 0 and at most 100 ({ty_col} PCT: a percentage)"]
    return []


def _target_problems(c: dict) -> list:
    out = _amount_problems(c, "target_ty", "target_val", minimum=1, required=True)
    limits = {}
    for col in ("min_target_cnt", "max_target_cnt"):
        if c.get(col) in (None, ""):
            continue
        if not _whole(c[col]) or _number(c[col]) < 1:
            out.append(f"{col} must be a whole number >= 1")
        elif _up(c.get("target_ty"), "CNT") != "PCT":
            out.append(f"{col} is only used with target_ty PCT")
        else:
            limits[col] = int(_number(c[col]))
    if len(limits) == 2 and limits["min_target_cnt"] > limits["max_target_cnt"]:
        out.append("min_target_cnt is above max_target_cnt")
    return out


def round_target(value: float, rounding_cd: str) -> int:
    mode = (rounding_cd or "FLOOR").upper()
    if mode == "CEIL":
        return max(0, math.ceil(value - 1e-9))
    if mode == "ROUND":
        return max(0, round(value))
    return max(0, math.floor(value + 1e-9))


def resolve_target(config: dict, candidates: int, rounding_cd: str) -> int:
    value = _number(config.get("target_val")) or 0
    if _up(config.get("target_ty"), "CNT") != "PCT":
        return int(round(value))
    target = round_target(candidates * value / 100, rounding_cd)
    if config.get("min_target_cnt") not in (None, ""):
        target = max(target, int(_number(config["min_target_cnt"])))
    if config.get("max_target_cnt") not in (None, ""):
        target = min(target, int(_number(config["max_target_cnt"])))
    return target


def amount(row: dict, ty_col: str, val_col: str, of: int, rounding_cd: str) -> Optional[int]:
    value = _number(row.get(val_col))
    if value is None:
        return None
    if _up(row.get(ty_col), "CNT") == "PCT":
        return round_target(of * value / 100, rounding_cd)
    return int(round(value))


def sql_problems(text, what: str, select: bool = False) -> list:
    text = str(text or "")
    if not text.strip():
        return []
    bare = _QUOTED.sub(" ", text)
    out = []
    if bare.count("'") or bare.count('"') or "/*" in bare:
        out.append(f"{what}: a quote or /* comment is not closed")
    words = sorted({re.sub(r"\s+", " ", m.group(1)).upper() for m in _STATEMENT_RE.finditer(bare)})
    if _LOCK_FOR_WRITE.search(bare):
        words = sorted({*words, "LOCKING FOR WRITE"})
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
    out = [p for col in _CONFIG_SQL for p in sql_problems(sample.config.get(col), col, select=True)]
    for cls in RULE_CLASSES:
        for r in _rules_of(sample, cls):
            out += sql_problems(r.get("rule_sql_txt"), f"{cls} rule {r.get('rule_nm')}",
                                select=cls in ("PARAM", "GATE") or _is_query(r))
    return out


def _is_query(rule: dict) -> bool:
    cls = _up(rule.get("rule_class_cd"))
    if cls in ("EXCLUSION", "PRIORITY"):
        return _up(rule.get("rule_ty"), QUERY) in (QUERY, "EXPRESSION")
    return cls in ("LEVEL", "BUCKET", "CAP") and bool(_text(rule))


def _is_value(rule: dict) -> bool:
    cls, kind = _up(rule.get("rule_class_cd")), _up(rule.get("rule_ty"))
    return cls in ("LEVEL", "DIVERSITY") or (cls == "PRIORITY" and kind == "EXPRESSION") \
        or (cls == "CAP" and kind == PER_VALUE)


def _rules_of(sample: Sample, cls: str) -> list:
    if cls == "BUCKET":
        return [b for lvl in sample.levels for b in lvl["buckets"]]
    return {"PARAM": sample.params, "GATE": sample.gates, "EXCLUSION": sample.exclusions, "PRIORITY": sample.priorities,
            "LEVEL": sample.levels, "CAP": sample.caps}[cls]


def _history_problems(what: str, row: dict) -> list:
    out = []
    try:
        if int(row.get("lookback_days") or 0) < 1:
            out.append(f"{what}: lookback_days must be >= 1")
    except (TypeError, ValueError):
        out.append(f"{what}: lookback_days must be a whole number")
    if _up(row.get("history_scope_cd"), "SAMPLE") not in HISTORY_SCOPES:
        out.append(f"{what}: history_scope_cd must be one of {', '.join(HISTORY_SCOPES)}")
    return out


def _level_problems(lvl: dict) -> list:
    name, out = lvl.get("rule_nm"), []
    if _up(lvl.get("unlisted_cd"), "SHARE") not in UNLISTED_POLICIES:
        out.append(f"level {name}: unlisted_cd must be one of {', '.join(UNLISTED_POLICIES)}")
    if not lvl["value_level"] and not lvl["buckets"]:
        out.append(f"level {name}: no rule_sql_txt (a SELECT of each case's value) and no BUCKET rules")
    groups = {}
    for b in lvl["buckets"]:
        bname, mode = str(b.get("rule_nm") or ""), _up(b.get("quota_cd"), "PCT")
        what = f"level {name} bucket {bname!r}"
        if "/" in bname:
            out.append(f"{what}: rule_nm must not contain '/'")
        kind = _up(b.get("rule_ty"), QUERY)
        if kind not in BUCKET_TYPES:
            out.append(f"{what}: rule_ty must be one of {', '.join(BUCKET_TYPES)}")
        if lvl["value_level"] and (_text(b) or b.get("rule_ty") not in (None, "")):
            out.append(f"{what}: rule_sql_txt and rule_ty are only used on a level without its own SELECT")
        value = _number(b.get("quota_val"))
        if mode not in QUOTA_MODES:
            out.append(f"{what}: quota_cd must be one of {', '.join(QUOTA_MODES)}")
        elif mode == "PCT" and (value is None or not 0 <= value <= 100):
            out.append(f"{what}: PCT needs quota_val between 0 and 100 (a percentage of the parent's target)")
        elif mode == "CNT" and (not _whole(value) or value < 0):
            out.append(f"{what}: CNT needs quota_val, a whole number >= 0 (cases)")
        elif mode in ("ALL", "REMAINDER") and b.get("quota_val") not in (None, ""):
            out.append(f"{what}: quota_val is not used with quota_cd {mode}")
        out += [f"{what}: {p}" for p in _amount_problems(b, "max_ty", "max_val")]
        group = groups.setdefault(_up(b.get("parent_path_txt"), "*"), {})
        if _norm(bname) in group:
            out.append(f"{what}: appears more than once under parent_path_txt {b.get('parent_path_txt') or '*'!r}")
        group[_norm(bname)] = b
    if not lvl["value_level"]:
        for parent, group in groups.items():
            rest = [b for b in group.values() if not _text(b)]
            if len(rest) > 1 or (rest and list(group.values())[-1] is not rest[0]):
                out.append(f"level {name} under {parent!r}: a bucket without rule_sql_txt takes every case left, "
                           f"so it must be the only one and the last (seq_nbr)")
    for parent, group in groups.items():
        total = sum(_number(b.get("quota_val")) or 0 for b in group.values() if _up(b.get("quota_cd"), "PCT") == "PCT")
        if total > 100 + 1e-6:
            out.append(f"level {name} under {parent!r}: the PCT quota_val values add up to {total:g}, more than 100")
    return out


def _col(rule: dict) -> str:
    return f"smp_r_{rule['rule_id']}"


def build_queries(sample: Sample, s: Settings, params: dict) -> dict:
    def sql(text, identifier=False):
        return substitute_params(s.resolve_env(str(text or "").strip(), identifier=identifier), params)

    c = sample.config
    source = f"{sql(c.get('src_schema_nm'), identifier=True)}.{sql(c.get('src_table_nm'), identifier=True)}"
    keys = sample.keys
    rows = sql(c.get("scope_sql_txt")) or f"SELECT * FROM {source}"
    condition = "1=1"
    filters = str(params.get(EXTRA_FILTERS) or "").strip()
    if filters:
        problems = sql_problems(filters, "extra_filters")
        if problems:
            raise ValueError("; ".join(problems))
        condition = sql(filters)
    scope = f"FROM ({rows}) {SCOPE_ALIAS} WHERE ({condition})"

    key_queries = [(r, sql(r["rule_sql_txt"])) for r in query_rules(sample)]
    if _text(c, "diversity_sql_txt"):
        key_queries.append((DIVERSITY_RULE, sql(c["diversity_sql_txt"])))
    configured_rows = _text(c, "scope_sql_txt") or f"SELECT * FROM {source}"
    configured = f"FROM ({configured_rows}) {SCOPE_ALIAS} WHERE ({filters or '1=1'})"
    out = {"source": source, "scope": scope, "key_queries": key_queries,
           "pull": f"SELECT {', '.join(keys)} {scope}", "output": None,
           "pull_template": f"SELECT {', '.join(keys)} {configured}"}
    if _text(c, "tgt_table_nm"):
        out["tgt_table_nm"] = f"{sql(c.get('tgt_schema_nm'), identifier=True)}.{sql(c['tgt_table_nm'], identifier=True)}"
        out["output"] = f"SELECT * {scope}"
    return out


def target_table_columns(conn: Connection, queries: dict, sample: Sample) -> list:
    table = queries["tgt_table_nm"]
    columns = conn.columns(f"SELECT * FROM {table} WHERE 1=0")
    source = {c.lower() for c in conn.columns(f"{queries['output']} AND 1=0")}
    have = {c.lower() for c in columns}
    problems = [f"needs the column {c.upper()}" for c in SAMPLE_REQUIRED if c not in have]
    problems += [f"needs the key column {k}" for k in sample.keys if k.lower() not in have]
    other = [c for c in columns if c.lower() not in (*SAMPLE_AUDIT_COLS, *SAMPLE_RUN_COLS) and c.lower() not in source]
    if other:
        problems.append(f"has columns that are neither returned by the scope (Scope_Sql_Txt) nor set by sampling: "
                        f"{', '.join(other)}")
    if problems:
        raise ConfigError(f"target table {table}: " + "; ".join(problems))
    return columns


def query_rules(sample: Sample) -> list:
    return [r for r in (*sample.exclusions, *sample.priorities,
                        *(lvl for lvl in sample.levels if lvl["value_level"]),
                        *(b for lvl in sample.levels if not lvl["value_level"] for b in lvl["buckets"]),
                        *sample.caps)
            if _is_query(r)]


def mark_query_rules(conn: Connection, s: Settings, sample: Sample, rows: list, key_queries: list,
                     params: Optional[dict] = None) -> dict:
    keys_lower = [k.lower() for k in sample.keys]
    counts = {}
    for rule, text in key_queries:
        name, cls, value_rule = rule["rule_nm"], rule["rule_class_cd"], _is_value(rule)
        what = f"{cls} rule {name}"
        configured = sample.config.get("diversity_sql_txt") if rule is DIVERSITY_RULE else rule.get("rule_sql_txt")
        log_sql(f"{STEP} | {sample.label} | {what}", configured, text, params)
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
                        raise ConfigError("returns a case more than once")
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
        log.info("%s | %s | %s returned %d case(s)", STEP, sample.label, what, len(found))
    return counts


def resolve_params(conn: Connection, s: Settings, sample: Sample, given: dict, as_of: date) -> dict:
    project, table, run_type = sample.key
    given_names = {str(k).lower() for k in (given or {})}
    reserved = sorted(given_names & BUILT_IN_PARAMS)
    if reserved:
        raise ValueError(f"run_params {reserved} are set by the engine and cannot be passed with the run")
    params = {**(given or {}), "as_of_date": as_of.isoformat(), "project_cd": project, "table_nm": table,
              "run_ty": run_type}
    for p in sample.params:
        name = str(p["rule_nm"])
        if name.lower() in BUILT_IN_PARAMS:
            raise ValueError(f"PARAM rule {name}: {name.lower()} is set by the engine, give the rule another name")
        if name.lower() in given_names:
            continue
        text = substitute_params(s.resolve_env(_text(p), identifier=False), params)
        rows = conn.query(text)
        value = next(iter(rows[0].values()), None) if rows else None
        if value is None:
            raise LookupError(f"run parameter {name}: its rule_sql_txt returned no value")
        params[name] = _param_text(value)
    return params


def _param_text(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return value.isoformat() if isinstance(value, date) else value


def gate_queries(sample: Sample, s: Settings, params: dict) -> list:
    return [(g, substitute_params(s.resolve_env(_text(g), identifier=False), params)) for g in sample.gates]


def unmet_gates(conn: Connection, s: Settings, sample: Sample, params: dict) -> list:
    out = []
    for gate, text in gate_queries(sample, s, params):
        if not conn.query(text):
            out.append(f"{gate['rule_nm']}" + (f" ({gate['rule_desc']})" if gate.get("rule_desc") else ""))
    return out


def case_key(row: dict, keys: list) -> str:
    parts = []
    for c in keys:
        k = c.lower()
        if k not in row:
            raise ConfigError(f"key_columns entry {c!r} is not among the pulled columns {sorted(row)}")
        parts.append(f"{c}={_label(row[k])}")
    return "|".join(parts)


def _history(conn: Connection, s: Settings, sample: Sample, rule: dict, run_key: str, as_of: date,
             cache: dict) -> dict:
    scope = _up(rule.get("history_scope_cd"), "SAMPLE")
    days = int(rule.get("lookback_days") or 0)
    if (scope, days) in cache:
        return cache[(scope, days)]
    project, table, run_type = sample.key
    where = ["Curr_Ind = 1", "Excluded_Ind = 0", "Smpl_Cycle_Dt_Key >= ?", "Smpl_Cycle_Dt_Key <= ?",
             f"NOT ({_same_sample()} AND Run_Key = ?)", "UPPER(Project_Cd) = ?"]
    params = [as_of - timedelta(days=days), as_of, project, table, run_type, run_key, project]
    if scope in ("SAMPLE", "TABLE"):
        where.append("UPPER(Table_Nm) = ?")
        params.append(table)
    if scope == "SAMPLE":
        where.append("UPPER(Run_Ty) = ?")
        params.append(run_type)
    rows = conn.query(f"SELECT Case_Key, MAX(Selected_Ind) AS sel, "
                      f"MAX(CASE WHEN Selection_Stat = 'REPLACED' THEN 1 ELSE 0 END) AS replaced "
                      f"FROM {s.meta_db}.GOVT_RGLTRY_SampleSelection WHERE {' AND '.join(where)} GROUP BY Case_Key",
                      params)
    out = {"selected": {r["case_key"] for r in rows if int(r["sel"] or 0) == 1},
           "unselected": {r["case_key"] for r in rows if int(r["sel"] or 0) == 0 and int(r["replaced"] or 0) == 0}}
    cache[(scope, days)] = out
    return out


def apply_exclusions(rows: list, sample: Sample, history: dict) -> tuple:
    counts = {str(x["rule_nm"]): 0 for x in sample.exclusions}
    candidates, excluded = [], []
    for row in rows:
        reason = None
        for x in sample.exclusions:
            if _up(x.get("rule_ty"), QUERY) == QUERY:
                hit = row.get(_col(x)) == 1
            else:
                hit = row["_key"] in history[x["rule_id"]]["selected"]
            if hit:
                reason = str(x["rule_nm"])
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
    keys = [k.lower() for k in sample.keys]
    rows = sorted(rows, key=lambda r: tuple(_sortable(r.get(k)) for k in keys))
    for p in reversed(sample.priorities):
        kind, col = _up(p.get("rule_ty"), QUERY), _col(p)
        if kind == QUERY:
            rows.sort(key=lambda r: 0 if r.get(col) == 1 else 1)
        elif kind == "EXPRESSION":
            if _up(p.get("sort_dir_cd"), "ASC") == "DESC":
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


def diversified(pool: list, n: int, cap: Optional[int], use_diversity: bool) -> list:
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
    raise ConfigError(f"unknown smpl_method_cd {method!r}")


class Stratifier:
    def __init__(self, sample: Sample, seed, rounding_cd: str):
        self.levels = sample.levels
        self.method = sample.method
        self.seed = seed
        self.rounding = rounding_cd
        self.config = sample.config
        self.use_diversity = bool(_text(sample.config, "diversity_sql_txt"))
        self.target = 0
        self.leaves = {}
        self.nodes = {}
        self.notes = []

    def rng(self, path: str) -> random.Random:
        return random.Random(f"{self.seed}|{path}")

    def cap_for(self, n: int) -> Optional[int]:
        if not self.use_diversity:
            return None
        cap = amount(self.config, "diversity_cap_ty", "diversity_cap_val", n, self.rounding)
        return None if cap is None else max(1, cap)

    def pick(self, rows: list, n: int, path: str) -> list:
        return select(rows, n, self.method, self.rng(path), self.cap_for(n), self.use_diversity)

    def run(self, rows: list, target: int) -> list:
        self.target = target
        return self._allocate(rows, target, 0, "")

    def top_up(self, rest: list, n: int, chosen: list) -> list:
        cap = self.cap_for(self.target)
        pool = select(rest, len(rest), self.method, self.rng("TOPUP"), cap, self.use_diversity)
        if not (self.use_diversity and cap):
            return pool[:n]
        used = {}
        for r in chosen:
            used[_norm(r.get(DIVERSITY))] = used.get(_norm(r.get(DIVERSITY)), 0) + 1
        out = []
        for r in pool:
            v = _norm(r.get(DIVERSITY))
            if len(out) < n and used.get(v, 0) < cap:
                used[v] = used.get(v, 0) + 1
                out.append(r)
        return out

    def unselect(self, path: str) -> None:
        parts = path.split("/")
        for i in range(1, len(parts) + 1):
            node = self.nodes.get("/".join(parts[:i]))
            if node:
                node["selected"] -= 1
        if path in self.leaves and self.leaves[path] is not self.nodes.get(path):
            self.leaves[path]["selected"] -= 1

    def _forget(self, path: str) -> None:
        prefix = f"{path}/"
        self.notes = [n for n in self.notes if not n.startswith(prefix)]

    def _group(self, level: dict, path: str) -> dict:
        by_pattern = {}
        for b in level["buckets"]:
            if _matches(path, b.get("parent_path_txt")):
                by_pattern.setdefault(_up(b.get("parent_path_txt"), "*"), []).append(b)
        if not by_pattern:
            return {}
        best = min(by_pattern, key=_specificity)
        return {_norm(b["rule_nm"]): b for b in by_pattern[best]}

    def _allocate(self, rows: list, target: int, index: int, path: str) -> list:
        while index < len(self.levels) and not _matches(path, self.levels[index].get("parent_path_txt")):
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
        if not level["value_level"] and not group:
            return self._allocate(rows, target, index + 1, path)
        buckets, labels = {}, {}
        for r in rows:
            if level["value_level"]:
                value = r.get(_col(level))
                key = _norm(value)
                label = str(group[key]["rule_nm"]).rstrip() if key in group else _label(value)
            else:
                hit = next((b for b in group.values() if not _text(b) or r.get(_col(b)) == 1), None)
                key, label = (_norm(hit["rule_nm"]), str(hit["rule_nm"]).rstrip()) if hit \
                    else (_norm(OTHER_BUCKET), OTHER_BUCKET)
            buckets.setdefault(key, []).append(r)
            labels.setdefault(key, label)

        unlisted = _up(level.get("unlisted_cd"), "SHARE")

        def mode(key):
            spec = group.get(key)
            return _up(spec.get("quota_cd"), "PCT") if spec else unlisted

        def cap(key):
            return amount(group.get(key) or {}, "max_ty", "max_val", len(buckets[key]), self.rounding)

        def sub(key):
            return f"{path}/{labels[key]}" if path else labels[key]

        named = sum(_number(b.get("quota_val")) or 0 for b in group.values()
                    if _up(b.get("quota_cd"), "PCT") == "PCT") / 100
        sharing = [k for k in sorted(buckets) if mode(k) == "SHARE"]
        in_order = [k for k in group if k in buckets]
        chosen = {}
        targets = {}
        left, counted = target, 0
        for kind in ("CNT", "PCT"):
            for key in (k for k in in_order if mode(k) == kind):
                value = _number(group[key].get("quota_val")) or 0
                want = int(round(value)) if kind == "CNT" else round_target(target * value / 100, self.rounding)
                t = min(want, left)
                if t < want:
                    self.notes.append(f"{sub(key)}: quota {kind} {value:g} ({want}) cut to {t}: the quotas under "
                                      f"{path or 'the sample'} exceed its target of {target}")
                if cap(key) is not None:
                    t = min(t, cap(key))
                left -= t
                counted += t if kind == "CNT" else 0
                targets[key] = t
                chosen[key] = self._allocate(buckets[key], t, index + 1, sub(key))
        for key in sharing:
            t = round_target(max(0.0, target * (1.0 - named) - counted) / len(sharing), self.rounding)
            targets[key] = t if cap(key) is None else min(t, cap(key))
            chosen[key] = self._allocate(buckets[key], targets[key], index + 1, sub(key))

        def fill(keys: list, budget: int) -> None:
            fixed, active = {}, list(keys)
            while active:
                base, extra = divmod(max(0, budget - sum(fixed.values())), len(active))
                full = []
                for i, k in enumerate(active):
                    want = base + (1 if i < extra else 0)
                    if cap(k) is not None:
                        want = min(want, cap(k))
                    targets[k] = want
                    self._forget(sub(k))
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


def _under(path: str, pattern) -> bool:
    parts = str(path or "ALL").split("/")
    return any(_matches("/".join(parts[:i]), pattern) for i in range(1, len(parts) + 1))


class Caps:
    def __init__(self, sample: Sample, target: int, rounding_cd: str):
        self.rules = []
        for c in sample.caps:
            limit = amount(c, "max_ty", "max_val", target, rounding_cd)
            self.rules.append((c, limit, _col(c), _up(c.get("rule_ty"), QUERY) == PER_VALUE))
        self.counts = [{} for _ in self.rules]
        self.report = {str(c["rule_nm"]): {"limit": limit, "over": 0, "replaced": 0} for c, limit, _, _ in self.rules}
        self._groups = {}

    def groups(self, row: dict) -> list:
        known = self._groups.get(row["_key"])
        if known is None:
            known = self._groups[row["_key"]] = self._find_groups(row)
        return known

    def _find_groups(self, row: dict) -> list:
        out = []
        for i, (cap, _, col, per_value) in enumerate(self.rules):
            if not _under(row.get("_path"), cap.get("parent_path_txt")):
                continue
            value = row.get(col)
            if per_value and value is not None:
                out.append((i, _norm(value)))
            elif not per_value and value == 1:
                out.append((i, ""))
        return out

    def blocking(self, row: dict) -> Optional[str]:
        for i, group in self.groups(row):
            if self.counts[i].get(group, 0) >= self.rules[i][1]:
                return str(self.rules[i][0]["rule_nm"])
        return None

    def take(self, row: dict) -> None:
        for i, group in self.groups(row):
            self.counts[i][group] = self.counts[i].get(group, 0) + 1


def apply_caps(caps: Caps, strat: "Stratifier", candidates: list, chosen: list) -> list:
    if not caps.rules:
        return chosen
    kept, over = [], []
    for row in sorted(chosen, key=lambda r: r["_order"]):
        name = caps.blocking(row)
        if name:
            caps.report[name]["over"] += 1
            over.append((row, name))
        else:
            caps.take(row)
            kept.append(row)
    if not over:
        return kept
    used = {r["_key"] for r in chosen}
    pools = {}
    for row in candidates:
        if row["_key"] not in used:
            pools.setdefault(row.get("_path"), []).append(row)
    if strat.method == "RANDOM":
        for path, pool in pools.items():
            strat.rng(f"CAP|{path}").shuffle(pool)
    for row, name in over:
        pool = pools.get(row.get("_path"), [])
        new = next((c for c in pool if caps.blocking(c) is None), None)
        if new:
            pool.remove(new)
            caps.take(new)
            kept.append(new)
            caps.report[name]["replaced"] += 1
        else:
            strat.unselect(row.get("_path"))
            strat.notes.append(f"{row.get('_path')}: {row['_key']} is over the cap {name} and its bucket has no "
                               f"other candidate within the caps")
    return sorted(kept, key=lambda r: r["_order"])


def _same_sample(prefix: str = "") -> str:
    return " AND ".join(f"UPPER({prefix}{c}) = ?" for c in ("Project_Cd", "Table_Nm", "Run_Ty"))


def count_prior_attempts(conn: Connection, s: Settings, key, run_key: str) -> int:
    rows = conn.query(f"SELECT COUNT(*) AS cnt FROM {s.meta_db}.GOVT_RGLTRY_SamplingRun "
                      f"WHERE {_same_sample()} AND Run_Key = ?", [*sample_key(key), run_key])
    return int(rows[0]["cnt"]) if rows else 0


def current_run(conn: Connection, s: Settings, key, run_key: str) -> Optional[dict]:
    rows = conn.query(f"SELECT * FROM {s.meta_db}.GOVT_RGLTRY_SamplingRun "
                      f"WHERE {_same_sample()} AND Run_Key = ? AND Curr_Ind = 1", [*sample_key(key), run_key])
    return rows[0] if rows else None


def _start_run(conn, s, run_id, run_key, sample: Sample, seed, as_of, params_text, triggered_by, started,
               override_reason=None):
    config, now = sample.config, s.now()
    columns = ["Smpl_Run_ID", "Project_Cd", "Table_Nm", "Run_Ty", "Run_Key", "As_Of_Dt_Key", "Run_Stat", "Curr_Ind",
               "Random_Seed_Nbr", "Run_Parm_Txt", "Config_Hash", "Override_Rsn_Txt", "Triggered_By", "Start_Dtts",
               *_RUN_CONFIG_COLUMNS, "Load_Dtts", "LastUpdt_Dtts"]
    values = [run_id, *sample.key, run_key, as_of, "RUNNING", 0, seed, params_text,
              sample.config_hash, override_reason, triggered_by, started,
              *(config.get(c) for c in _RUN_CONFIG_COLUMNS), now, now]
    conn.execute(f"INSERT INTO {s.meta_db}.GOVT_RGLTRY_SamplingRun ({', '.join(columns)}) "
                 f"VALUES ({', '.join(['?'] * len(columns))})", values)
    rules = sample.snapshot.get("rules") or []
    columns = ["Smpl_Run_ID", *_SNAPSHOT_RULE_COLUMNS, "Load_Dtts", "LastUpdt_Dtts"]
    conn.executemany(f"INSERT INTO {s.meta_db}.GOVT_RGLTRY_SamplingRunRule ({', '.join(columns)}) "
                     f"VALUES ({', '.join(['?'] * len(columns))})",
                     [[run_id, *(r.get(c) for c in _SNAPSHOT_RULE_COLUMNS), now, now] for r in rules], s.chunk_rows)


def _stored_error(message: str) -> str:
    return " ".join(str(message or "").split())[:ERROR_TEXT_MAX]


def _finish_run(conn, s, run_id, summary, error_type=None, message=None):
    conn.execute(f"""
        UPDATE {s.meta_db}.GOVT_RGLTRY_SamplingRun
        SET Run_Stat = ?, End_Dtts = ?, In_Scope_Cnt = ?, Excluded_Cnt = ?, Candidate_Cnt = ?, Selected_Cnt = ?,
            Shortfall_Cnt = ?, Target_Cnt = ?, Error_Ty = ?, Error_Txt = ?, Curr_Ind = ?, LastUpdt_Dtts = ?,
            LastUpdt_ID = USER
        WHERE Smpl_Run_ID = ?""",
        ["ERROR" if error_type else "COMPLETED", s.now(), summary.get("in_scope"), summary.get("excluded"),
         summary.get("candidates"), summary.get("selected"), summary.get("shortfall"), summary.get("target_cnt"),
         error_type, _stored_error(message) if error_type else None, 0 if error_type else 1, s.now(), run_id])


def _key_condition(keys: list, row: dict, params: list) -> str:
    parts = []
    for k in keys:
        value = row[k.lower()]
        if value is None:
            parts.append(f"{k} IS NULL")
        else:
            parts.append(f"{k} = ?")
            params.append(value)
    return "(" + " AND ".join(parts) + ")"


def fetch_output(conn: Connection, queries: dict, sample: Sample, chosen: list) -> dict:
    keys = sample.keys
    out = {}
    for i in range(0, len(chosen), OUTPUT_KEY_CHUNK):
        params = []
        conds = [_key_condition(keys, row, params) for row in chosen[i:i + OUTPUT_KEY_CHUNK]]
        for r in conn.query(f"{queries['output']} AND ({' OR '.join(conds)})", params):
            out[case_key(r, keys)] = r
    missing = [r["_key"] for r in chosen if r["_key"] not in out]
    if missing:
        raise RuntimeError(f"{len(missing)} selected case(s) not found again for the target table (the source "
                           f"changed since the pull?)")
    return out


def _retire_output(conn, queries, run_key, now, where: str = "", params: Sequence = ()):
    have = {c.lower(): c for c in queries["output_columns"]}
    sets, values = [f"{have['etl_is_curr_ind']} = 'N'"], []
    if "etl_is_del_ind" in have:
        sets.append(f"{have['etl_is_del_ind']} = 'Y'")
    if "etl_last_updt_dt_tm" in have:
        sets.append(f"{have['etl_last_updt_dt_tm']} = ?")
        values.append(now)
    conn.execute(f"UPDATE {queries['tgt_table_nm']} SET {', '.join(sets)} "
                 f"WHERE {have['smpl_run_key']} = ? AND {have['etl_is_curr_ind']} = 'Y'"
                 f"{' AND ' + where if where else ''}", [*values, run_key, *params])


def _write_output(conn, s, queries, run_id, run_key, run_type, as_of, output: dict, cases: list, now):
    columns = queries["output_columns"]

    def row(key, order, path):
        own = {"etl_is_curr_ind": "Y", "etl_is_del_ind": "N", "etl_load_dt_tm": now, "etl_last_updt_dt_tm": now,
               "smpl_run_id": run_id, "smpl_run_key": run_key, "smpl_run_ty": run_type, "smpl_cycle_dt": as_of,
               "smpl_order_nbr": order, "smpl_stratum_path": path}
        return [own[c.lower()] if c.lower() in own else output[key].get(c.lower()) for c in columns]
    conn.executemany(f"INSERT INTO {queries['tgt_table_nm']} ({', '.join(columns)}) "
                     f"VALUES ({', '.join(['?'] * len(columns))})",
                     [row(*case) for case in cases], s.chunk_rows)


class FinalizedMeanwhile(ConfigError):
    pass


def _persist(conn, s, run_id, run_key, sample: Sample, queries, candidates, excluded, chosen, output,
             as_of: date, forced: bool = False) -> int:
    now = s.now()
    c, key = sample.config, sample.key
    live = None if forced else current_run(conn, s, key, run_key)
    if live and _up(live.get("run_stat")) == "FINAL":
        raise FinalizedMeanwhile(f"the sample of run_key {run_key} was finalised while this run was selecting (run "
                                 f"{live['smpl_run_id']}, by {live.get('finalized_by')}): rerun with a force reason "
                                 f"to replace it")
    order = {r["_key"]: i + 1 for i, r in enumerate(chosen)}
    rows = [[run_id, *key, run_key, as_of, r["_key"], r["_order"] + 1,
             1 if r["_key"] in order else 0, order.get(r["_key"]), 0, None, r.get("_path"),
             "SELECTED" if r["_key"] in order else None, now, now] for r in candidates]
    if _up(c.get("keep_excluded_ind"), "1") not in ("0", "N", "FALSE"):
        rows += [[run_id, *key, run_key, as_of, r["_key"], None, 0, None, 1, r["_reason"], None,
                  None, now, now] for r in excluded]
    conn.executemany(f"""
        INSERT INTO {s.meta_db}.GOVT_RGLTRY_SampleSelection (
            Smpl_Run_ID, Project_Cd, Table_Nm, Run_Ty, Run_Key, Smpl_Cycle_Dt_Key, Case_Key, Priority_Rank_Nbr,
            Selected_Ind, Selection_Order_Nbr, Excluded_Ind, Exclusion_Rsn_Txt, Stratum_Path_Txt, Selection_Stat,
            Curr_Ind, Load_Dtts, LastUpdt_Dtts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)""", rows, s.chunk_rows)
    if output is not None:
        _retire_output(conn, queries, run_key, now)
        _write_output(conn, s, queries, run_id, run_key, key[2], as_of, output,
                      [(r["_key"], order[r["_key"]], r.get("_path")) for r in chosen], now)

    conn.execute(f"""
        UPDATE {s.meta_db}.GOVT_RGLTRY_SampleSelection
        SET Curr_Ind = 0, LastUpdt_Dtts = ?, LastUpdt_ID = USER
        WHERE {_same_sample()} AND Run_Key = ? AND Smpl_Run_ID <> ? AND Curr_Ind = 1""",
        [now, *key, run_key, run_id])
    return conn.execute(f"""
        UPDATE {s.meta_db}.GOVT_RGLTRY_SamplingRun
        SET Curr_Ind = 0, LastUpdt_Dtts = ?, LastUpdt_ID = USER
        WHERE {_same_sample()} AND Run_Key = ? AND Smpl_Run_ID <> ? AND Curr_Ind = 1""",
        [now, *key, run_key, run_id])


def _log_selection(label: str, summary: dict, topped: int) -> None:
    excluded = ", ".join(f"{k} {v}" for k, v in summary["exclusions"].items()) or "no exclusion rules"
    log.info("%s | %s | excluded %s (%s) | %d candidate(s) | target %s", STEP, label, summary["excluded"], excluded,
             summary["candidates"], summary["target_cnt"])
    for path, n in summary["by_stratum"].items():
        log.info("%s | %s | bucket %s | selected %d of %d candidate(s), target %d", STEP, label, path,
                 n["selected"], n["candidates"], n["target"])
    for name, c in (summary.get("caps") or {}).items():
        log.info("%s | %s | cap %s | limit %s | over %d | replaced %d", STEP, label, name, c["limit"], c["over"],
                 c["replaced"])
    if topped:
        log.info("%s | %s | topped up %d from the other candidates", STEP, label, topped)
    for note in summary.get("notes") or []:
        log.warning("%s | %s | %s", STEP, label, note)
    if summary["shortfall"]:
        log.warning("%s | %s | short of the target by %d", STEP, label, summary["shortfall"])


def run_sampling(conn: Connection, s: Settings, key, run_key: Optional[str] = None,
                 run_params: Optional[dict] = None, seed: Optional[int] = None, triggered_by: str = "SYSTEM",
                 as_of: Optional[date] = None, dry_run: bool = False, force_reason: Optional[str] = None,
                 extra_filters: Optional[str] = None) -> dict:
    as_of = as_of or s.today()
    started = s.now()
    sample = load_config(conn, s, key)
    config, method = sample.config, sample.method
    project, table, run_type = sample.key
    rounding = _up(config.get("rounding_cd"), "FLOOR")
    percent = _up(config.get("target_ty"), "CNT") == "PCT"
    target = None if percent else resolve_target(config, 0, rounding)
    run_seed = None
    if method in ("RANDOM", "SYSTEMATIC"):
        run_seed = int(seed) if seed not in (None, "") else random.SystemRandom().getrandbits(63)
    template = str(run_key or "{as_of_date}")

    early = None
    try:
        params = resolve_params(conn, s, sample, run_params, as_of)
        run_key = substitute_params(template, params, escape=False)
    except ValueError as e:
        early, params = ("config", "CONFIG_ERROR", e), dict(run_params or {})
    except Exception as e:  # noqa: BLE001
        early, params = ("technical", "PARAM_FAILURE", e), dict(run_params or {})
    if early:
        run_key = template if "{" not in template and "$" not in template else as_of.isoformat()
    if len(run_key) > RUN_KEY_MAX:
        early = early or ("config", "CONFIG_ERROR", f"run_key is {len(run_key)} characters, over the {RUN_KEY_MAX} "
                                                    f"Run_Key holds")
        run_key = run_key[:RUN_KEY_MAX]
    params["run_key"] = run_key
    if str(extra_filters or "").strip():
        params[EXTRA_FILTERS] = str(extra_filters).strip()
    params_text = json.dumps(params, default=str)
    if len(params_text) > RUN_PARAMS_MAX:
        early = early or ("config", "CONFIG_ERROR", f"the run parameters are {len(params_text)} characters as JSON, "
                                                    f"over the {RUN_PARAMS_MAX} Run_Parm_Txt holds")
        params_text = None
    triggered_by = str(triggered_by or "SYSTEM")[:USER_MAX]

    label = sample.label
    attempt = 0 if dry_run else count_prior_attempts(conn, s, sample.key, run_key)
    run_id = generate_run_id(label, run_key, f"attempt-{attempt + 1}", triggered_by, timestamp=started)
    summary = {"project_cd": project, "table_nm": table, "run_ty": run_type,
               "smpl_run_id": None if dry_run else run_id, "run_key": run_key, "as_of_date": as_of.isoformat(),
               "method": method, "seed": run_seed, "target_cnt": target, "params": params, "in_scope": None,
               "excluded": None, "candidates": 0, "selected": 0, "shortfall": None, "exclusions": {},
               "by_stratum": {}, "buckets": {}, "retired_runs": 0, "dry_run": dry_run,
               "config_hash": sample.config_hash}
    log.info("%s | %s | starting | run_key %s | method %s | target %s | meta_db %s | host %s%s", STEP, label, run_key,
             method, target if target is not None else f"{config.get('target_val')} %", s.meta_db, conn.host,
             " | dry run" if dry_run else "")

    def fail(kind: str, error_type: str, error, exc_info: bool = False) -> dict:
        problems = error if isinstance(error, list) else [error if isinstance(error, str) else error_text(error)]
        message = "; ".join(problems)
        if len(problems) > 1:
            log.error("%s | %s | %s: %d problems\n    %s", STEP, label, error_type, len(problems),
                      "\n    ".join(problems))
        else:
            log.error("%s | %s | %s: %s", STEP, label, error_type, message, exc_info=exc_info)
        summary.update(status="ERROR", error_kind=kind, error_type=error_type, error=message)
        if not dry_run:
            stored = problems[0] + (f" (+{len(problems) - 1} more: see the task log)" if len(problems) > 1 else "")
            try:
                _finish_run(conn, s, run_id, summary, error_type, stored)
            except Exception as e:  # noqa: BLE001
                log.error("%s | %s | could not close the run row %s: %s", STEP, label, run_id, error_text(e))
        return summary

    force_reason = str(force_reason or "").strip()[:1000] or None
    try:
        final = None if early else current_run(conn, s, sample.key, run_key)
        final = final if final and _up(final.get("run_stat")) == "FINAL" else None
        if not dry_run:
            _start_run(conn, s, run_id, run_key, sample, run_seed, as_of, params_text, triggered_by, started,
                       force_reason if final else None)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "START_FAILURE", e, exc_info=True)
    if early:
        return fail(early[0], early[1], early[2])
    if final:
        summary["finalized_run"] = final["smpl_run_id"]
        if not force_reason and not dry_run:
            return fail("config", "FINALIZED", f"the sample of run_key {run_key} is final (run "
                                               f"{final['smpl_run_id']}, finalised by {final.get('finalized_by')} "
                                               f"at {final.get('finalized_dtts')}): rerun with a force reason to "
                                               f"replace it")
        if force_reason:
            log.warning("%s | %s | replacing the FINAL sample %s of run_key %s: %s", STEP, label,
                        final["smpl_run_id"], run_key, force_reason)
    problems = config_problems(sample)
    if problems:
        return fail("config", "CONFIG_ERROR", problems)
    try:
        queries = build_queries(sample, s, params)
        summary["tgt_table_nm"] = queries.get("tgt_table_nm")
        gate_queries(sample, s, params)
    except (ValueError, KeyError) as e:
        return fail("config", "CONFIG_ERROR", e)
    try:
        waiting = unmet_gates(conn, s, sample, params)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "GATE_FAILURE", e, exc_info=True)
    if waiting:
        return fail("technical", "GATE_NOT_MET", "not ready yet: " + "; ".join(waiting))
    if sample.gates:
        log.info("%s | %s | %d gate(s) met", STEP, label, len(sample.gates))

    try:
        log_sql(f"{STEP} | {label} | pull", queries["pull_template"], queries["pull"], params)
        rows = conn.stream(queries["pull"], s.chunk_rows)
        in_scope = len(rows)
        log.info("%s | %s | %d row(s) in scope", STEP, label, in_scope)
        history, cache = {}, {}
        for x in sample.exclusions:
            if _up(x.get("rule_ty"), QUERY) in HISTORY_TYPES:
                history[x["rule_id"]] = _history(conn, s, sample, x, run_key, as_of, cache)
        for p in sample.priorities:
            if _up(p.get("rule_ty"), QUERY) in HISTORY_TYPES:
                history[p["rule_id"]] = _history(conn, s, sample, p, run_key, as_of, cache)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "PULL_FAILURE", e, exc_info=True)

    try:
        for r in rows:
            r["_key"] = case_key(r, sample.keys)
        if len({r["_key"] for r in rows}) != len(rows):
            raise ConfigError("key_columns_txt do not identify a case uniquely (the pull returned duplicate keys)")
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    try:
        summary["rule_queries"] = mark_query_rules(conn, s, sample, rows, queries["key_queries"], params)
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "QUERY_RULE_FAILURE", e, exc_info=True)

    try:
        candidates, excluded, counts = apply_exclusions(rows, sample, history)
        candidates = order_candidates(candidates, sample, history)
        target = resolve_target(config, len(candidates), rounding)
        summary["target_cnt"] = target
        strat = Stratifier(sample, run_seed, rounding)
        caps = Caps(sample, target, rounding)
        chosen = apply_caps(caps, strat, candidates, strat.run(candidates, target))
        extra = []
        if len(chosen) < target and _up(config.get("shortfall_cd"), "TOPUP") == "TOPUP":
            taken = {r["_key"] for r in chosen}
            rest = [r for r in candidates if r["_key"] not in taken]
            for r in strat.top_up(rest, len(rest), chosen):
                if len(extra) >= target - len(chosen):
                    break
                if caps.blocking(r) is None:
                    caps.take(r)
                    extra.append(r)
            chosen += extra
        for r in chosen[target:]:
            strat.unselect(r.get("_path"))
        chosen = sorted(chosen[:target], key=lambda r: r["_order"])
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    except Exception as e:  # noqa: BLE001
        return fail("technical", "SELECT_FAILURE", e, exc_info=True)
    summary.update(in_scope=in_scope, excluded=in_scope - len(candidates), candidates=len(candidates),
                   selected=len(chosen), shortfall=max(0, target - len(chosen)), exclusions=counts,
                   by_stratum=strat.leaves, buckets=strat.nodes, notes=strat.notes, caps=caps.report)
    _log_selection(label, summary, len(extra))

    output = None
    if queries["output"]:
        try:
            queries["output_columns"] = target_table_columns(conn, queries, sample)
            output = fetch_output(conn, queries, sample, chosen)
            log.info("%s | %s | %d source row(s) read for the target table %s", STEP, label, len(output),
                     queries["tgt_table_nm"])
        except ConfigError as e:
            return fail("config", "CONFIG_ERROR", e)
        except Exception as e:  # noqa: BLE001
            return fail("technical", "OUTPUT_FAILURE", e, exc_info=True)

    if dry_run:
        summary["status"] = "COMPLETED"
        log.info("%s | %s | dry run complete: nothing written", STEP, label)
        return summary

    try:
        with conn.transaction():
            summary["retired_runs"] = _persist(conn, s, run_id, run_key, sample, queries, candidates, excluded,
                                               chosen, output, as_of, forced=bool(force_reason))
            _finish_run(conn, s, run_id, summary)
    except FinalizedMeanwhile as e:
        return fail("config", "FINALIZED", str(e))
    except Exception as e:  # noqa: BLE001
        return fail("technical", "PERSIST_FAILURE", e, exc_info=True)

    summary["status"] = "COMPLETED"
    log.info("%s | %s | complete | run %s | in scope %s | excluded %s | candidates %d | selected %d of %d | "
             "%d earlier run(s) retired", STEP, label, run_id, in_scope, summary["excluded"], len(candidates),
             len(chosen), target, summary["retired_runs"])
    return summary


def finalize_run(conn: Connection, s: Settings, key, run_key: str, by: str) -> dict:
    key = sample_key(key)
    run = current_run(conn, s, key, run_key)
    if not run:
        raise ConfigError(f"{'.'.join(key)} has no current sample for run_key {run_key!r}")
    if _up(run["run_stat"]) == "FINAL":
        raise ConfigError(f"run {run['smpl_run_id']} is already final (by {run.get('finalized_by')})")
    by = str(by or "")[:USER_MAX]
    done = conn.execute(f"UPDATE {s.meta_db}.GOVT_RGLTRY_SamplingRun SET Run_Stat = 'FINAL', Finalized_By = ?, "
                        f"Finalized_Dtts = ?, LastUpdt_Dtts = ?, LastUpdt_ID = USER "
                        f"WHERE Smpl_Run_ID = ? AND Run_Stat = 'COMPLETED' AND Curr_Ind = 1",
                        [by, s.now(), s.now(), run["smpl_run_id"]])
    if done == 0:
        raise ConfigError(f"run {run['smpl_run_id']} is no longer the current COMPLETED sample of run_key "
                          f"{run_key!r} (a rerun replaced it meanwhile): check and finalise again")
    log.info("FINALIZE | %s | run_key %s | run %s finalised by %s", ".".join(key), run_key, run["smpl_run_id"], by)
    return {"smpl_run_id": run["smpl_run_id"], "project_cd": key[0], "table_nm": key[1], "run_ty": key[2],
            "run_key": run_key, "status": "FINAL", "finalized_by": by}


def parse_case_key(text: str, keys: list) -> dict:
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


_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?)?$")


def _typed_keys(conn: Connection, queries: dict, sample: Sample, rows: list) -> list:
    looks = [k for k in sample.keys if any(isinstance(r.get(k.lower()), str) and _ISO_DATE.match(r[k.lower()])
                                           for r in rows)]
    if not looks:
        return rows
    probe = conn.query(f"SELECT TOP 1 {', '.join(looks)} {queries['scope']}")
    if not probe:
        return rows
    for k in looks:
        kind = probe[0].get(k.lower())
        convert = datetime.fromisoformat if isinstance(kind, datetime) else \
            date.fromisoformat if isinstance(kind, date) else None
        if convert:
            for r in rows:
                if isinstance(r.get(k.lower()), str) and _ISO_DATE.match(r[k.lower()]):
                    r[k.lower()] = convert(r[k.lower()])
    return rows


def run_sample(conn: Connection, s: Settings, run: dict) -> Sample:
    config = {**{k: run.get(k) for k in SAMPLE_KEY}, **{k: run.get(k) for k in _RUN_CONFIG_COLUMNS}}
    rules = conn.query(f"SELECT * FROM {s.meta_db}.GOVT_RGLTRY_SamplingRunRule WHERE Smpl_Run_ID = ?",
                       [run["smpl_run_id"]])
    return build_sample(config, [{k: v for k, v in r.items() if k != "smpl_run_id"} for r in rules])


def replace_case(conn: Connection, s: Settings, key, run_key: str, case_key_text: str, reason: str, by: str,
                 any_bucket: bool = False) -> dict:
    if not str(reason or "").strip():
        raise ConfigError("a reason is required to replace a case")
    key = sample_key(key)
    run = current_run(conn, s, key, run_key)
    if not run or _up(run["run_stat"]) not in ("COMPLETED", "FINAL"):
        raise ConfigError(f"{'.'.join(key)} has no completed current sample for run_key {run_key!r}")
    db, run_id = s.meta_db, run["smpl_run_id"]
    rows = conn.query(f"SELECT Case_Key, Selected_Ind, Selection_Order_Nbr, Priority_Rank_Nbr, Excluded_Ind, "
                      f"Stratum_Path_Txt, Selection_Stat FROM {db}.GOVT_RGLTRY_SampleSelection "
                      f"WHERE Smpl_Run_ID = ?", [run_id])
    old = next((r for r in rows if r["case_key"] == case_key_text), None)
    if not old or int(old["selected_ind"] or 0) != 1:
        raise ConfigError(f"{case_key_text!r} is not in the current sample of run_key {run_key!r}")
    pool = [r for r in rows if int(r["selected_ind"] or 0) == 0 and int(r["excluded_ind"] or 0) == 0
            and r["selection_stat"] is None]
    if not any_bucket:
        pool = [r for r in pool if r["stratum_path_txt"] == old["stratum_path_txt"]]
    if not pool:
        raise ConfigError(f"no candidate left to replace {case_key_text!r}"
                          + ("" if any_bucket else f" in bucket {old['stratum_path_txt']!r} (allow any bucket)"))
    pool.sort(key=lambda r: (r["priority_rank_nbr"] is None, r["priority_rank_nbr"] or 0, r["case_key"]))
    if _up(run.get("smpl_method_cd")) == "RANDOM":
        new = random.Random(f"{run.get('random_seed_nbr')}|{run_id}|{case_key_text}").choice(pool)
    else:
        new = pool[0]

    output = queries = sample = gone = None
    if _text(run, "tgt_table_nm"):
        sample = run_sample(conn, s, run)
        params = json.loads(run.get("run_parm_txt") or "{}")
        queries = build_queries(sample, s, params)
        queries["output_columns"] = target_table_columns(conn, queries, sample)
        row = {**parse_case_key(new["case_key"], sample.keys), "_key": new["case_key"]}
        gone = parse_case_key(old["case_key"], sample.keys)
        _typed_keys(conn, queries, sample, [row, gone])
        output = fetch_output(conn, queries, sample, [row])

    now = s.now()
    with conn.transaction():
        for case, flag, order, status, linked in ((old["case_key"], 0, None, "REPLACED", new["case_key"]),
                                                  (new["case_key"], 1, old["selection_order_nbr"], "REPLACEMENT",
                                                   old["case_key"])):
            conn.execute(f"""
                UPDATE {db}.GOVT_RGLTRY_SampleSelection
                SET Selected_Ind = ?, Selection_Order_Nbr = ?, Selection_Stat = ?, Linked_Case_Key = ?,
                    Stat_Rsn_Txt = ?, Stat_By = ?, Stat_Dtts = ?, LastUpdt_Dtts = ?, LastUpdt_ID = USER
                WHERE Smpl_Run_ID = ? AND Case_Key = ?""",
                [flag, order, status, linked, reason[:1000], by, now, now, run_id, case])
        if output is not None:
            where = []
            _retire_output(conn, queries, run_key, now,
                           _key_condition(sample.keys, gone, where), where)
            _write_output(conn, s, queries, run_id, run_key, key[2], run.get("as_of_dt_key"), output,
                          [(new["case_key"], old["selection_order_nbr"], new["stratum_path_txt"])], now)
        conn.execute(f"UPDATE {db}.GOVT_RGLTRY_SamplingRun SET LastUpdt_Dtts = ?, LastUpdt_ID = USER "
                     f"WHERE Smpl_Run_ID = ?", [now, run_id])
    log.info("REPLACE | %s | run_key %s | run %s | a case of bucket %s replaced by the next candidate of %s, by %s",
             ".".join(key), run_key, run_id, old["stratum_path_txt"], new["stratum_path_txt"], by)
    return {"smpl_run_id": run_id, "replaced": old["case_key"], "replacement": new["case_key"],
            "stratum_path": new["stratum_path_txt"], "selection_order": old["selection_order_nbr"],
            "reason": reason, "by": by}


def discover_configs(conn: Connection, s: Settings, project: Optional[str] = None, table: Optional[str] = None,
                     run_type: Optional[str] = None) -> list:
    if not str(project or "").strip():
        raise ConfigError("give a project (and optionally a table and a run type): a run never samples every "
                          "project at once")
    where, params = ["Active_Ind = 1", "UPPER(Project_Cd) = ?"], [_up(project)]
    for column, value in (("Table_Nm", table), ("Run_Ty", run_type)):
        if str(value or "").strip():
            where.append(f"UPPER({column}) = ?")
            params.append(_up(value))
    rows = conn.query(f"SELECT Project_Cd, Table_Nm, Run_Ty FROM {s.meta_db}.GOVT_RGLTRY_SamplingConfig "
                      f"WHERE {' AND '.join(where)} ORDER BY Table_Nm, Run_Ty", params)
    return [tuple(_up(r[k]) for k in SAMPLE_KEY) for r in rows]


def _scope(project, table, run_type) -> dict:
    return {k: v for k, v in (("project", project), ("table", table), ("run_type", run_type))
            if str(v or "").strip()}


def run_scope(conn: Connection, s: Settings, project: Optional[str] = None, table: Optional[str] = None,
              run_type: Optional[str] = None, **kwargs) -> dict:
    scope = _scope(project, table, run_type)
    keys = discover_configs(conn, s, project, table, run_type)
    if not keys:
        raise ConfigError(f"no active GOVT_RGLTRY_SamplingConfig matches {scope}")
    runs = []
    for key in keys:
        failed = {"project_cd": key[0], "table_nm": key[1], "run_ty": key[2], "status": "ERROR"}
        try:
            runs.append(run_sampling(conn, s, key, **kwargs))
        except ConfigError as e:
            runs.append({**failed, "error_kind": "config", "error_type": "CONFIG_ERROR", "error": str(e)})
        except Exception as e:  # noqa: BLE001
            log.error("%s | %s | START_FAILURE: %s", STEP, ".".join(key), error_text(e), exc_info=True)
            runs.append({**failed, "error_kind": "technical", "error_type": "START_FAILURE", "error": error_text(e)})
    failed = [r for r in runs if r.get("status") != "COMPLETED"]
    result = "ALL SAMPLES COMPLETED" if not failed else f"{len(failed)} OF {len(runs)} SAMPLE(S) FAILED"
    return {"result": result, "scope": scope, "runs": runs, "failed": len(failed)}


def validate(conn: Connection, s: Settings, project: Optional[str] = None, table: Optional[str] = None,
             run_type: Optional[str] = None, run_params: Optional[dict] = None,
             check_sql: bool = False, as_of: Optional[date] = None, extra_filters: Optional[str] = None) -> dict:
    out = []
    for key in discover_configs(conn, s, project, table, run_type):
        entry = {"project_cd": key[0], "table_nm": key[1], "run_ty": key[2]}
        try:
            sample = load_config(conn, s, key)
        except ConfigError as e:
            out.append({**entry, "problems": [str(e)]})
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
                for name in ("pull", "output"):
                    if queries.get(name):
                        conn.query("EXPLAIN " + queries[name] + (" AND 1=0" if name == "output" else ""))
                if queries.get("output"):
                    target_table_columns(conn, queries, sample)
                for _, text in [*gate_queries(sample, s, params), *queries["key_queries"]]:
                    conn.query("EXPLAIN " + text)
            except Exception as e:  # noqa: BLE001
                problems.append(f"SQL check failed: {error_text(e)}")
        out.append({**entry, "smpl_desc": sample.config.get("smpl_desc"), "problems": problems})
    bad = sum(1 for c in out if c["problems"])
    return {"result": "VALID" if not bad else f"{bad} SAMPLE(S) WITH PROBLEMS", "configs": out}
