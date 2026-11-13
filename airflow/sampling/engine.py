from __future__ import annotations

import hashlib
import json
import logging
import math
import random
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from fnmatch import fnmatchcase
from typing import Optional, Sequence

from sampling import __version__
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
RULE_CLASSES = ("PARAM", "EXCLUSION", "PRIORITY", "LEVEL", "BUCKET", "CAP")
_AUDIT_COLUMNS = ("load_dtts", "loaduser_id", "lastupdt_dtts", "lastupdt_id")
_EFFECT_COLUMNS = ("active_ind", "eff_start_dt", "eff_end_dt")
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
CURRENT_ROWS = "current_rows"
AS_SEEN = "_as_seen"
RUN_KEY_MAX, RUN_PARAMS_MAX, USER_MAX = 250, 4000, 100
BUILT_IN_PARAMS = {"as_of_date", "project_cd", "table_nm", "run_ty", "run_key", EXTRA_FILTERS, CURRENT_ROWS}
_CURRENT_ROWS_RE = re.compile(r"\{current_rows(?::([A-Za-z_][A-Za-z0-9_]*))?\}", re.IGNORECASE)


def current_rows(as_seen: Optional[datetime] = None, alias: Optional[str] = None) -> str:
    a = f"{alias}." if alias else ""
    if as_seen is None:
        return f"{a}ETL_IS_CURR_IND = 'Y' AND {a}ETL_IS_DEL_IND = 'N'"
    at = f"TIMESTAMP '{as_seen:%Y-%m-%d %H:%M:%S.%f}'"
    return (f"{a}ETL_LOAD_DT_TM <= {at} AND ({a}ETL_IS_CURR_IND = 'Y' OR {a}ETL_LAST_UPDT_DT_TM > {at}) "
            f"AND ({a}ETL_IS_DEL_IND = 'N' OR {a}ETL_LAST_UPDT_DT_TM > {at})")


def _expand(text: str, params: dict) -> str:
    seen = (params or {}).get(AS_SEEN)
    return _CURRENT_ROWS_RE.sub(lambda m: f"({current_rows(seen, m.group(1))})", text) if text else text


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


IN_EFFECT = "Active_Ind = 1 AND Eff_Start_Dt <= ? AND Eff_End_Dt >= ?"


def load_config(conn: Connection, s: Settings, key, rules_as_of: Optional[date] = None) -> Sample:
    db, (project, table, run_type) = s.meta_db, sample_key(key)
    day = rules_as_of or s.today()
    rows = conn.query(f"SELECT * FROM {db}.GOVT_RGLTRY_SamplingConfig WHERE UPPER(Project_Cd) = ? "
                      f"AND UPPER(Table_Nm) = ? AND UPPER(Run_Ty) = ? AND {IN_EFFECT}",
                      [project, table, run_type, day, day])
    if not rows:
        raise ConfigError(f"no active GOVT_RGLTRY_SamplingConfig row for {project}.{table}.{run_type} in effect on "
                          f"{day.isoformat()}")
    if len(rows) > 1:
        raise ConfigError(f"{len(rows)} active GOVT_RGLTRY_SamplingConfig rows for {project}.{table}.{run_type} in "
                          f"effect on {day.isoformat()}: their Eff_Start_Dt / Eff_End_Dt overlap")
    rules = conn.query(f"SELECT * FROM {db}.GOVT_RGLTRY_SamplingRule WHERE UPPER(Project_Cd) = ? "
                       f"AND UPPER(Table_Nm) IN (?, '*') AND UPPER(Run_Ty) IN (?, '*') AND {IN_EFFECT}",
                       [project, table, run_type, day, day])
    seen, twice = set(), set()
    for r in rules:
        (twice if r["rule_id"] in seen else seen).add(r["rule_id"])
    if twice:
        raise ConfigError(f"Rule_ID {sorted(twice)} has more than one active row in effect on {day.isoformat()}: "
                          f"their Eff_Start_Dt / Eff_End_Dt overlap")
    return build_sample(rows[0], resolve_rules(rules, table, run_type))


def snapshot_hash(snapshot: dict) -> str:
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def build_sample(config: dict, rules: list) -> Sample:
    def clean(row):
        return {k: v for k, v in row.items() if k not in _AUDIT_COLUMNS and k not in _EFFECT_COLUMNS}
    snapshot = {"config": clean(config), "rules": sorted((clean(r) for r in rules), key=lambda r: r["rule_id"])}
    ordered = sorted((dict(r) for r in snapshot["rules"]),
                     key=lambda r: (r.get("seq_nbr") is not None, r.get("seq_nbr") or 0, r["rule_id"]))
    by_class = {c: [] for c in RULE_CLASSES}
    sample = Sample(config=dict(config), snapshot=snapshot, config_hash=snapshot_hash(snapshot))
    for r in ordered:
        r["rule_class_cd"] = _up(r.get("rule_class_cd"))
        by_class.get(r["rule_class_cd"], sample.stray).append(r)
    sample.params, sample.exclusions = by_class["PARAM"], by_class["EXCLUSION"]
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
    bad_keys = [k for k in sample.keys if not _IDENT.match(k)]
    if bad_keys:
        problems.append(f"key_columns_txt {bad_keys} must be plain column names, comma separated")
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
        return max(0, math.floor(value + 0.5 + 1e-9))
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
                                select=cls == "PARAM" or _is_query(r))
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
    return {"PARAM": sample.params, "EXCLUSION": sample.exclusions, "PRIORITY": sample.priorities,
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
        return substitute_params(_expand(s.resolve_env(str(text or "").strip(), identifier=identifier), params), params)

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
                     params: Optional[dict] = None, matched: Optional[list] = None) -> dict:
    keys_lower = [k.lower() for k in sample.keys]
    counts, done = {}, {}
    for rule, text in key_queries:
        name, cls, value_rule = rule["rule_nm"], rule["rule_class_cd"], _is_value(rule)
        what = f"{cls} rule {name}"
        same = done.get((text.strip(), value_rule))
        if same:
            found = same[1]
            log.info("%s | %s | %s has the same SQL as %s: its %d case(s) are reused", STEP, sample.label, what,
                     same[0], len(found))
            _set_rule_column(rows, rule, found, value_rule)
            counts[str(name)] = len(found)
            _matched(matched, rule, rows, found)
            continue
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
        _set_rule_column(rows, rule, found, value_rule)
        _matched(matched, rule, rows, found)
        done[(text.strip(), value_rule)] = (what, found)
        counts[str(name)] = len(found)
        log.info("%s | %s | %s returned %d case(s)", STEP, sample.label, what, len(found))
    return counts


def _matched(matched: Optional[list], rule: dict, rows: list, found: dict) -> None:
    if matched is not None:
        matched.append((rule, sum(1 for row in rows if row["_key"] in found)))


def _set_rule_column(rows: list, rule: dict, found: dict, value_rule: bool) -> None:
    col = _col(rule)
    for row in rows:
        row[col] = found.get(row["_key"]) if value_rule else (1 if row["_key"] in found else 0)


def resolve_params(conn: Connection, s: Settings, sample: Sample, given: dict, as_of: date) -> dict:
    project, table, run_type = sample.key
    given_names = {str(k).lower() for k in (given or {})}
    passed = {str(k).lower() for k, v in (given or {}).items() if v is not None and str(v).strip() != ""}
    reserved = sorted(given_names & BUILT_IN_PARAMS)
    if reserved:
        raise ValueError(f"run_params {reserved} are set by the engine and cannot be passed with the run")
    params = {k: v for k, v in (given or {}).items() if str(k).lower() in passed}
    params.update(as_of_date=as_of.isoformat(), project_cd=project, table_nm=table, run_ty=run_type)
    for p in sample.params:
        name = str(p["rule_nm"])
        if name.lower() in BUILT_IN_PARAMS:
            raise ValueError(f"PARAM rule {name}: {name.lower()} is set by the engine, give the rule another name")
        if name.lower() in passed:
            continue
        text = substitute_params(_expand(s.resolve_env(_text(p), identifier=False), params), params)
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


def case_key(row: dict, keys: list) -> str:
    parts = []
    for c in keys:
        k = c.lower()
        if k not in row:
            raise ConfigError(f"key_columns entry {c!r} is not among the pulled columns {sorted(row)}")
        parts.append(f"{c}={_label(row[k])}")
    return "|".join(parts)


def _history(conn: Connection, s: Settings, sample: Sample, rule: dict, run_key: str, as_of: date,
             cache: dict, ahead: bool = False, as_seen: Optional[datetime] = None) -> dict:
    scope = _up(rule.get("history_scope_cd"), "SAMPLE")
    days = int(rule.get("lookback_days") or 0)
    if (scope, days, ahead) in cache:
        return cache[(scope, days, ahead)]
    project, table, run_type = sample.key
    selected, left_out, seen = "Selected_Ind", "CASE WHEN Selection_Stat IN ('REPLACED', 'SKIPPED') THEN 1 ELSE 0 END", []
    where = ["Curr_Ind = 1"]
    if as_seen is not None:
        selected = ("CASE WHEN Stat_Dtts > ? THEN CASE WHEN Selection_Stat = 'REPLACED' THEN 1 ELSE 0 END "
                    "ELSE Selected_Ind END")
        left_out = ("CASE WHEN Selection_Stat = 'SKIPPED' THEN 1 "
                    "WHEN Selection_Stat = 'REPLACED' AND Stat_Dtts <= ? THEN 1 ELSE 0 END")
        where = ["Load_Dtts <= ?", "(Curr_Ind = 1 OR LastUpdt_Dtts > ?)"]
        seen = [as_seen, as_seen]
    where += ["Excluded_Ind = 0", "Smpl_Cycle_Dt_Key >= ?", "Smpl_Cycle_Dt_Key <= ?",
              f"NOT ({_same_sample()} AND Run_Key = ?)", "UPPER(Project_Cd) = ?"]
    params = [*seen, as_of - timedelta(days=days), as_of + timedelta(days=days if ahead else 0), project, table,
              run_type, run_key, project]
    if scope in ("SAMPLE", "TABLE"):
        where.append("UPPER(Table_Nm) = ?")
        params.append(table)
    if scope == "SAMPLE":
        where.append("UPPER(Run_Ty) = ?")
        params.append(run_type)
    rows = conn.query(f"SELECT Case_Key, MAX({selected}) AS sel, MAX({left_out}) AS left_out "
                      f"FROM {s.meta_db}.GOVT_RGLTRY_SampleSelection WHERE {' AND '.join(where)} GROUP BY Case_Key",
                      seen + params)
    out = {"selected": {r["case_key"] for r in rows if int(r["sel"] or 0) == 1},
           "unselected": {r["case_key"] for r in rows if int(r["sel"] or 0) == 0 and int(r["left_out"] or 0) == 0}}
    cache[(scope, days, ahead)] = out
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


def _sort_value(v) -> tuple:
    if isinstance(v, bool) or isinstance(v, (int, float, Decimal)):
        return (0, v, "")
    if isinstance(v, datetime):
        return (1, v.replace(tzinfo=None), "")
    if isinstance(v, date):
        return (1, datetime.combine(v, time()), "")
    return (2, 0, str(v))


def _sortable(v) -> tuple:
    return (v is None, (0, 0, "") if v is None else _sort_value(v))


def order_candidates(rows: list, sample: Sample, history: dict) -> list:
    keys = [k.lower() for k in sample.keys]
    rows = sorted(rows, key=lambda r: tuple(_sortable(r.get(k)) for k in keys))
    for p in reversed(sample.priorities):
        kind, col = _up(p.get("rule_ty"), QUERY), _col(p)
        if kind == QUERY:
            rows.sort(key=lambda r: 0 if r.get(col) == 1 else 1)
        elif kind == "EXPRESSION":
            if _up(p.get("sort_dir_cd"), "ASC") == "DESC":
                rows.sort(key=lambda r: (r.get(col) is not None,
                                         (0, 0, "") if r.get(col) is None else _sort_value(r.get(col))),
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
        step = len(rows) / n
        start = rng.random() * step
        return [rows[min(len(rows) - 1, int(start + i * step))] for i in range(n)]
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
        self.ceilings = {}
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

    def top_up(self, rest: list, chosen: list) -> tuple:
        pool = select(rest, len(rest), self.method, self.rng("TOPUP"), None, self.use_diversity)
        used = {}
        for r in chosen:
            used[_norm(r.get(DIVERSITY))] = used.get(_norm(r.get(DIVERSITY)), 0) + 1
        return pool, self.cap_for(self.target), used

    def diversity_why(self, row: dict, cap: Optional[int], used: dict, where: str = "the sample") -> Optional[str]:
        if cap is None or used.get(_norm(row.get(DIVERSITY)), 0) < cap:
            return None
        return f"Diversity cap: {_label(row.get(DIVERSITY))} already has {cap} in {where}"

    def unselect(self, path: str) -> None:
        self._count(path, -1)

    def add(self, path: str) -> None:
        self._count(path, 1)

    def _count(self, path: str, step: int) -> None:
        parts = path.split("/")
        for i in range(1, len(parts) + 1):
            node = self.nodes.get("/".join(parts[:i]))
            if node:
                node["selected"] += step
        if path in self.leaves and self.leaves[path] is not self.nodes.get(path):
            self.leaves[path]["selected"] += step

    def ceiling_hit(self, path: str) -> Optional[str]:
        parts = path.split("/")
        for i in range(1, len(parts) + 1):
            prefix = "/".join(parts[:i])
            ceiling = self.ceilings.get(prefix)
            if ceiling is not None and (self.nodes.get(prefix) or {}).get("selected", 0) >= ceiling:
                return f"Bucket {prefix} at its most of {ceiling}"
        return None

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
            fixed, active, done = {}, list(keys), {}
            while active:
                base, extra = divmod(max(0, budget - sum(fixed.values())), len(active))
                full = []
                for i, k in enumerate(active):
                    want = base + (1 if i < extra else 0)
                    if cap(k) is not None:
                        want = min(want, cap(k))
                    targets[k] = want
                    if done.get(k) != want:
                        self._forget(sub(k))
                        chosen[k] = self._allocate(buckets[k], want, index + 1, sub(k))
                        done[k] = want
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
            if mode(key) == "SKIP" or (mode(key) == "CNT" and not _number(group[key].get("quota_val"))):
                for r in buckets[key]:
                    r["_skip"] = True

        for key in buckets:
            if cap(key) is not None:
                self.ceilings[sub(key)] = cap(key)
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
            if limit == 0 and _up(c.get("max_ty"), "CNT") == "PCT" and (_number(c.get("max_val")) or 0) > 0:
                limit = 1
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
            if per_value and value is not None and str(value).strip():
                out.append((i, _norm(value)))
            elif not per_value and value == 1:
                out.append((i, ""))
        return out

    def blocking(self, row: dict) -> Optional[str]:
        hit = self._hit(row)
        return None if hit is None else str(self.rules[hit[0]][0]["rule_nm"])

    def why(self, row: dict) -> Optional[str]:
        hit = self._hit(row)
        if hit is None:
            return None
        i, group = hit
        cap, limit, _, per_value = self.rules[i]
        where = _label(cap.get("parent_path_txt")) if _up(cap.get("parent_path_txt"), "*") != "*" else "the sample"
        if per_value:
            return f"Over cap {cap['rule_nm']}: {group} already has {limit} in {where}"
        return f"Over cap {cap['rule_nm']}: {where} already has its {limit}"

    def _hit(self, row: dict):
        for i, group in self.groups(row):
            if self.counts[i].get(group, 0) >= self.rules[i][1]:
                return i, group
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
            row["_why"] = caps.why(row)
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
    spread = {}
    for r in kept:
        spread.setdefault(r.get("_path"), {}).setdefault(_norm(r.get(DIVERSITY)), 0)
        spread[r.get("_path")][_norm(r.get(DIVERSITY))] += 1

    def diverse(path, c) -> bool:
        limit = strat.cap_for((strat.leaves.get(path) or {}).get("target") or 0)
        return limit is None or spread.get(path, {}).get(_norm(c.get(DIVERSITY)), 0) < limit

    for row, name in over:
        path = row.get("_path")
        pool = pools.get(path, [])
        new = None
        for c in pool:
            if caps.blocking(c) is not None:
                c["_why"] = caps.why(c)
            elif diverse(path, c):
                new = c
                break
        if new:
            spread.setdefault(path, {}).setdefault(_norm(new.get(DIVERSITY)), 0)
            spread[path][_norm(new.get(DIVERSITY))] += 1
            pool.remove(new)
            caps.take(new)
            kept.append(new)
            caps.report[name]["replaced"] += 1
        else:
            strat.unselect(row.get("_path"))
            strat.notes.append(f"{row.get('_path')}: a case over the cap {name} left out: its bucket has no "
                               f"other candidate within the caps")
    return sorted(kept, key=lambda r: r["_order"])


def explain_left_out(candidates: list, chosen: list, strat: "Stratifier") -> dict:
    taken = {r["_key"] for r in chosen}
    counts = {}
    for r in candidates:
        if r["_key"] in taken:
            r.pop("_why", None)
            continue
        path = r.get("_path") or "ALL"
        leaf = strat.leaves.get(path) or {}
        full = (f"Bucket {path} gets no slots this run" if not leaf.get("target")
                else f"Bucket {path} full: its {leaf['target']} slot(s) went to higher-ranked cases")
        why = r.get("_why")
        if r.get("_skip"):
            why, kind = f"Bucket {path} is not sampled", "bucket not sampled"
        elif why == "top up":
            why, kind = f"{full}; the top-up filled the sample before reaching it", "bucket full"
        elif why and why.startswith("Over cap "):
            kind = why.split(":")[0][len("Over cap "):].strip()
            kind = f"cap {kind}"
        elif why and why.startswith("Bucket ") and " at its most of " in why:
            kind = "bucket at its most"
        elif why and why.startswith("Diversity cap: "):
            kind = "diversity cap"
        elif why:
            kind = "sample full"
        elif strat.use_diversity and leaf.get("target") and leaf.get("selected", 0) < leaf["target"]:
            limit = strat.cap_for(leaf["target"])
            why = (f"Diversity cap: {_label(r.get(DIVERSITY))} already has {limit} in bucket {path}" if limit
                   else full)
            kind = "diversity cap" if limit else "bucket full"
        else:
            why, kind = full, "bucket full"
        r["_why"] = why[:1000]
        counts[kind] = counts.get(kind, 0) + 1
    return counts


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
               override_reason=None, rerun_reason=None, rules_day=None):
    config, now = sample.config, s.now()
    columns = ["Smpl_Run_ID", "Project_Cd", "Table_Nm", "Run_Ty", "Run_Key", "As_Of_Dt_Key", "Run_Stat", "Curr_Ind",
               "Random_Seed_Nbr", "Run_Parm_Txt", "Config_Hash", "Override_Rsn_Txt", "Rerun_Rsn_Txt", "Rules_As_Of_Dt", "Triggered_By",
               "Start_Dtts",
               *_RUN_CONFIG_COLUMNS, "Load_Dtts", "LastUpdt_Dtts"]
    values = [run_id, *sample.key, run_key, as_of, "RUNNING", 0, seed, params_text,
              sample.config_hash, override_reason, rerun_reason, rules_day, triggered_by, started,
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


def _retire_output(conn, s, queries, key, run_key, now, where: str = "", params: Sequence = ()):
    have = {c.lower(): c for c in queries["output_columns"]}
    sets, values = [f"{have['etl_is_curr_ind']} = 'N'"], []
    if "etl_is_del_ind" in have:
        sets.append(f"{have['etl_is_del_ind']} = 'Y'")
    if "etl_last_updt_dt_tm" in have:
        sets.append(f"{have['etl_last_updt_dt_tm']} = ?")
        values.append(now)
    conds, args = [f"{have['smpl_run_key']} = ?", f"{have['etl_is_curr_ind']} = 'Y'"], [run_key]
    if "smpl_run_id" in have:
        conds.append(f"{have['smpl_run_id']} IN (SELECT Smpl_Run_ID FROM {s.meta_db}.GOVT_RGLTRY_SamplingRun "
                     f"WHERE {_same_sample()} AND Run_Key = ?)")
        args += [*key, run_key]
    elif "smpl_run_ty" in have:
        conds.append(f"UPPER({have['smpl_run_ty']}) = ?")
        args.append(key[2])
    if where:
        conds.append(where)
        args += list(params)
    conn.execute(f"UPDATE {queries['tgt_table_nm']} SET {', '.join(sets)} WHERE {' AND '.join(conds)}",
                 [*values, *args])


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
             as_of: date, forced: bool = False, stats: Optional[list] = None) -> int:
    now = s.now()
    c, key = sample.config, sample.key
    live = None if forced else current_run(conn, s, key, run_key)
    if live and _up(live.get("run_stat")) == "FINAL":
        raise FinalizedMeanwhile(f"the sample of run_key {run_key} was finalised while this run was selecting (run "
                                 f"{live['smpl_run_id']}, by {live.get('finalized_by')}): rerun with a force reason "
                                 f"to replace it")
    order = {r["_key"]: i + 1 for i, r in enumerate(chosen)}
    ranks, seen = {}, {}
    for r in sorted(candidates, key=lambda r: (order.get(r["_key"]) is None, order.get(r["_key"]) or 0, r["_order"])):
        seen[r.get("_path")] = seen.get(r.get("_path"), 0) + 1
        ranks[r["_key"]] = seen[r.get("_path")]
    rows = [[run_id, *key, run_key, as_of, r["_key"], r["_order"] + 1,
             1 if r["_key"] in order else 0, order.get(r["_key"]), ranks[r["_key"]], 0, None, r.get("_path"),
             "SELECTED" if r["_key"] in order else "SKIPPED" if r.get("_skip") else None,
             None if r["_key"] in order else r.get("_why"), now, now]
            for r in candidates]
    if _up(c.get("keep_excluded_ind"), "1") not in ("0", "N", "FALSE"):
        rows += [[run_id, *key, run_key, as_of, r["_key"], None, 0, None, None, 1, r["_reason"], None,
                  None, None, now, now] for r in excluded]
    conn.executemany(f"""
        INSERT INTO {s.meta_db}.GOVT_RGLTRY_SampleSelection (
            Smpl_Run_ID, Project_Cd, Table_Nm, Run_Ty, Run_Key, Smpl_Cycle_Dt_Key, Case_Key, Priority_Rank_Nbr,
            Selected_Ind, Selection_Order_Nbr, Bucket_Rank_Nbr, Excluded_Ind, Exclusion_Rsn_Txt, Stratum_Path_Txt,
            Selection_Stat, Not_Selected_Rsn_Txt, Curr_Ind, Load_Dtts, LastUpdt_Dtts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)""", rows, s.chunk_rows)
    _write_stats(conn, s, run_id, run_key, key, as_of, stats or [], now)
    if output is not None:
        _retire_output(conn, s, queries, key, run_key, now)
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


STAT_COLUMNS = ("Stat_Ty", "Stat_Nm", "Rule_ID", "Leaf_Ind", "Candidate_Cnt", "Target_Cnt", "Selected_Cnt",
                "Limit_Cnt", "Over_Cnt", "Replaced_Cnt", "Case_Cnt", "Stat_Txt")


def run_stats(sample: Sample, strat: "Stratifier", caps: "Caps", counts: dict, matched: list, not_picked: dict,
              topped: int) -> list:
    def rid(rule):
        value = rule.get("rule_id")
        return value if isinstance(value, int) else None

    def stat(kind, name, **cols):
        return {"Stat_Ty": kind, "Stat_Nm": str(name)[:1000], **cols}

    out = [stat("BUCKET", path, Leaf_Ind=1 if path in strat.leaves else 0, Candidate_Cnt=n["candidates"],
                Target_Cnt=n["target"], Selected_Cnt=n["selected"]) for path, n in strat.nodes.items()]
    for cap, limit, _, _ in caps.rules:
        report = caps.report[str(cap["rule_nm"])]
        out.append(stat("CAP", cap["rule_nm"], Rule_ID=rid(cap), Limit_Cnt=limit, Over_Cnt=report["over"],
                        Replaced_Cnt=report["replaced"]))
    out += [stat("EXCLUSION", x["rule_nm"], Rule_ID=rid(x), Case_Cnt=counts.get(str(x["rule_nm"]), 0))
            for x in sample.exclusions]
    out += [stat("RULE", f"{rule['rule_class_cd']} {rule['rule_nm']}", Rule_ID=rid(rule), Case_Cnt=n)
            for rule, n in matched]
    out += [stat("NOT_PICKED", kind, Case_Cnt=n) for kind, n in not_picked.items()]
    out.append(stat("TOPUP", "top-up", Case_Cnt=topped))
    out += [stat("NOTE", note.split(":")[0], Stat_Txt=note[:1000]) for note in strat.notes]
    return out


def _write_stats(conn, s, run_id, run_key, key, as_of, stats: list, now) -> None:
    columns = ["Smpl_Run_ID", "Project_Cd", "Table_Nm", "Run_Ty", "Run_Key", "As_Of_Dt_Key", "Stat_Seq_Nbr",
               *STAT_COLUMNS, "Load_Dtts", "LastUpdt_Dtts"]
    conn.executemany(f"INSERT INTO {s.meta_db}.GOVT_RGLTRY_SamplingRunStat ({', '.join(columns)}) "
                     f"VALUES ({', '.join(['?'] * len(columns))})",
                     [[run_id, *key, run_key, as_of, i + 1, *(r.get(c) for c in STAT_COLUMNS), now, now]
                      for i, r in enumerate(stats)], s.chunk_rows)


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
    for kind, n in (summary.get("not_picked") or {}).items():
        log.info("%s | %s | not picked | %s | %d case(s)", STEP, label, kind, n)
    for note in summary.get("notes") or []:
        log.warning("%s | %s | %s", STEP, label, note)
    if summary["shortfall"]:
        log.warning("%s | %s | short of the target by %d", STEP, label, summary["shortfall"])


def _day(value) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _made_since(live: Optional[dict], since: Optional[datetime]) -> bool:
    started = (live or {}).get("start_dtts")
    return bool(since and isinstance(started, datetime) and started >= since)


def _resumed(s: Settings, summary: dict, live: dict, label: str) -> dict:
    target = live.get("tgt_table_nm")
    summary.update(status="COMPLETED", resumed=True, smpl_run_id=live["smpl_run_id"],
                   in_scope=live.get("in_scope_cnt"), excluded=live.get("excluded_cnt"),
                   candidates=live.get("candidate_cnt") or 0, selected=live.get("selected_cnt") or 0,
                   shortfall=live.get("shortfall_cnt"), target_cnt=live.get("target_cnt"),
                   config_hash=live.get("config_hash"))
    if str(target or "").strip():
        summary["tgt_table_nm"] = f"{s.resolve_env(str(live.get('tgt_schema_nm') or '').strip())}." \
                                  f"{s.resolve_env(str(target).strip())}"
    log.info("%s | %s | run_key %s already sampled by run %s earlier in this Airflow run (started %s): kept, not "
             "run again", STEP, label, summary["run_key"], live["smpl_run_id"], live.get("start_dtts"))
    return summary


def run_sampling(conn: Connection, s: Settings, key, run_key: Optional[str] = None,
                 run_params: Optional[dict] = None, seed: Optional[int] = None, triggered_by: str = "SYSTEM",
                 as_of: Optional[date] = None, dry_run: bool = False, force_reason: Optional[str] = None,
                 extra_filters: Optional[str] = None, rerun_reason: Optional[str] = None,
                 rules_as_of: Optional[date] = None, resume_since: Optional[datetime] = None,
                 replay: Optional[dict] = None) -> dict:
    if replay:
        as_of, dry_run, seed = _day(replay["as_of_dt_key"]), True, replay.get("random_seed_nbr")
        rules_as_of = _day(replay.get("rules_as_of_dt")) or as_of
    as_of = as_of or s.today()
    started = s.now()
    rules_day = rules_as_of or s.today()
    sample = run_sample(conn, s, replay) if replay else load_config(conn, s, key, rules_day)
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
        if replay:
            if not replay.get("run_parm_txt"):
                raise ValueError(f"run {replay['smpl_run_id']} stored no run parameters, so it cannot be replayed")
            params, run_key = json.loads(replay["run_parm_txt"]), replay["run_key"]
            params[AS_SEEN] = replay["start_dtts"]
        else:
            params = resolve_params(conn, s, sample, run_params, as_of)
            run_key = substitute_params(template, params, escape=False)
    except ValueError as e:
        early, params = ("config", "CONFIG_ERROR", e), dict(run_params or {})
    except Exception as e:
        early, params = ("technical", "PARAM_FAILURE", e), dict(run_params or {})
    if early:
        known = {**(run_params or {}), "as_of_date": as_of.isoformat(), "project_cd": project, "table_nm": table,
                 "run_ty": run_type}
        try:
            run_key = substitute_params(template, known, escape=False)
        except ValueError:
            run_key = template
    if len(run_key) > RUN_KEY_MAX:
        early = early or ("config", "CONFIG_ERROR", f"run_key is {len(run_key)} characters, over the {RUN_KEY_MAX} "
                                                    f"Run_Key holds")
        run_key = run_key[:RUN_KEY_MAX]
    params["run_key"] = run_key
    if str(extra_filters or "").strip() and not replay:
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
               "config_hash": sample.config_hash, "rules_as_of": rules_day.isoformat()}
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
            except Exception as e:
                log.error("%s | %s | could not close the run row %s: %s", STEP, label, run_id, error_text(e))
        return summary

    force_reason = str(force_reason or "").strip()[:1000] or None
    given = rerun_reason is not None
    rerun_reason = str(rerun_reason or "").strip()[:1000] or None
    try:
        live = None if early or replay else current_run(conn, s, sample.key, run_key)
        final = live if live and _up(live.get("run_stat")) == "FINAL" else None
        if _made_since(live, resume_since) and rerun_reason is None and not dry_run:
            return _resumed(s, summary, live, label)
        if not dry_run:
            _start_run(conn, s, run_id, run_key, sample, run_seed, as_of, params_text, triggered_by, started,
                       force_reason if final else None, rerun_reason, rules_day)
    except Exception as e:
        return fail("technical", "START_FAILURE", e, exc_info=True)
    if early:
        return fail(early[0], early[1], early[2])
    if given and not rerun_reason:
        return fail("config", "RERUN_REASON", "rerun_reason is blank: give the reason for the rerun, or remove it "
                                              "for a regular run")
    if live and not final and not rerun_reason:
        return fail("config", "RERUN_REASON", f"run_key {run_key} already has a completed sample (run "
                                              f"{live['smpl_run_id']}): to rerun it, set rerun_reason")
    if not live and rerun_reason:
        return fail("config", "RERUN_REASON", f"rerun_reason is set but run_key {run_key} has no sample yet: remove "
                                              f"it for a regular run (was it left from an earlier rerun?)")
    if live and rerun_reason:
        summary["rerun_reason"] = rerun_reason
        log.warning("%s | %s | rerun of run_key %s (current run %s): %s", STEP, label, run_key,
                    live["smpl_run_id"], rerun_reason)
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
    except (ValueError, KeyError) as e:
        return fail("config", "CONFIG_ERROR", e)

    try:
        log_sql(f"{STEP} | {label} | pull", queries["pull_template"], queries["pull"], params)
        rows = conn.stream(queries["pull"], s.chunk_rows)
        in_scope = len(rows)
        log.info("%s | %s | %d row(s) in scope", STEP, label, in_scope)
        history, cache = {}, {}
        for x in sample.exclusions:
            if _up(x.get("rule_ty"), QUERY) in HISTORY_TYPES:
                history[x["rule_id"]] = _history(conn, s, sample, x, run_key, as_of, cache, as_seen=params.get(AS_SEEN))
        for p in sample.priorities:
            if _up(p.get("rule_ty"), QUERY) in HISTORY_TYPES:
                history[p["rule_id"]] = _history(conn, s, sample, p, run_key, as_of, cache, as_seen=params.get(AS_SEEN))
    except Exception as e:
        return fail("technical", "PULL_FAILURE", e, exc_info=True)

    try:
        for r in rows:
            r["_key"] = case_key(r, sample.keys)
        if len({r["_key"] for r in rows}) != len(rows):
            raise ConfigError("key_columns_txt do not identify a case uniquely (the pull returned duplicate keys)")
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    try:
        matched = []
        summary["rule_queries"] = mark_query_rules(conn, s, sample, rows, queries["key_queries"], params, matched)
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    except Exception as e:
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
            rest = [r for r in candidates if r["_key"] not in taken and not r.get("_skip")]
            pool, spread_cap, used = strat.top_up(rest, chosen)
            for r in pool:
                if len(extra) >= target - len(chosen):
                    r["_why"] = r.get("_why") or "top up"
                    continue
                blocked = caps.why(r) or strat.ceiling_hit(r.get("_path")) or strat.diversity_why(r, spread_cap, used)
                if blocked:
                    r["_why"] = blocked
                    continue
                used[_norm(r.get(DIVERSITY))] = used.get(_norm(r.get(DIVERSITY)), 0) + 1
                r.pop("_why", None)
                caps.take(r)
                strat.add(r.get("_path"))
                extra.append(r)
            chosen += extra
        for r in chosen[target:]:
            strat.unselect(r.get("_path"))
            r["_why"] = f"Sample full: the target of {target} was reached first"
        chosen = sorted(chosen[:target], key=lambda r: r["_order"])
        summary["not_picked"] = explain_left_out(candidates, chosen, strat)
        stats = run_stats(sample, strat, caps, counts, matched, summary["not_picked"], len(extra))
    except ConfigError as e:
        return fail("config", "CONFIG_ERROR", e)
    except Exception as e:
        return fail("technical", "SELECT_FAILURE", e, exc_info=True)
    summary.update(in_scope=in_scope, excluded=in_scope - len(candidates), candidates=len(candidates),
                   selected=len(chosen), shortfall=max(0, target - len(chosen)), exclusions=counts,
                   by_stratum=strat.leaves, buckets=strat.nodes, notes=strat.notes, caps=caps.report)
    _log_selection(label, summary, len(extra))

    output = None
    if queries["output"] and not replay:
        try:
            queries["output_columns"] = target_table_columns(conn, queries, sample)
            output = fetch_output(conn, queries, sample, chosen)
            log.info("%s | %s | %d source row(s) read for the target table %s", STEP, label, len(output),
                     queries["tgt_table_nm"])
        except ConfigError as e:
            return fail("config", "CONFIG_ERROR", e)
        except Exception as e:
            return fail("technical", "OUTPUT_FAILURE", e, exc_info=True)

    if dry_run:
        summary["picked"] = [{"case_key": r["_key"], "order": i + 1, "path": r.get("_path")}
                             for i, r in enumerate(chosen)]
        if replay:
            summary["scope_keys"] = sorted(r["_key"] for r in rows)
        summary["status"] = "COMPLETED"
        log.info("%s | %s | dry run complete: nothing written", STEP, label)
        return summary

    try:
        with conn.transaction():
            summary["retired_runs"] = _persist(conn, s, run_id, run_key, sample, queries, candidates, excluded,
                                               chosen, output, as_of, forced=bool(force_reason), stats=stats)
            _finish_run(conn, s, run_id, summary)
    except FinalizedMeanwhile as e:
        return fail("config", "FINALIZED", str(e))
    except Exception as e:
        return fail("technical", "PERSIST_FAILURE", e, exc_info=True)

    summary["status"] = "COMPLETED"
    log.info("%s | %s | complete | run %s | run_key %s | in scope %s | excluded %s | candidates %d | selected %d "
             "of %d | shortfall %d | %d earlier run(s) retired | %.1fs", STEP, label, run_id, run_key, in_scope,
             summary["excluded"], len(candidates), len(chosen), target, summary["shortfall"],
             summary["retired_runs"], (s.now() - started).total_seconds())
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


REVAL_STATUSES = ("MATCH", "DIFFERENT", "SOURCE_CHANGED", "FAILED")
DIFF_TXT_MAX = 4000


def _key_list(title: str, keys: list) -> str:
    return f"{title} ({len(keys)}): {', '.join(keys)}" if keys else ""


def _diff_text(missing: list, only_orig: list, only_replay: list) -> Optional[str]:
    parts = [p for p in (_key_list("not in the source as it was", missing),
                         _key_list("picked then, not now", only_orig),
                         _key_list("picked now, not then", only_replay)) if p]
    text = "; ".join(parts)
    if len(text) <= DIFF_TXT_MAX:
        return text or None
    more = " ... (cut short: the revalidate command lists them all)"
    return text[:DIFF_TXT_MAX - len(more)] + more


def revalidate_run(conn: Connection, s: Settings, run_id: str, triggered_by: str = "SYSTEM") -> dict:
    found = conn.query(f"SELECT * FROM {s.meta_db}.GOVT_RGLTRY_SamplingRun WHERE Smpl_Run_ID = ?", [run_id])
    if not found:
        raise ConfigError(f"no run {run_id!r} in GOVT_RGLTRY_SamplingRun")
    run = found[0]
    if _up(run.get("run_stat")) not in ("COMPLETED", "FINAL"):
        raise ConfigError(f"run {run_id} is {run.get('run_stat')}: only a COMPLETED or FINAL run can be replayed")
    key = tuple(_up(run.get(k)) for k in SAMPLE_KEY)
    label = ".".join(key)
    original = [r["case_key"] for r in conn.query(
        f"SELECT Case_Key, Selection_Order_Nbr FROM {s.meta_db}.GOVT_RGLTRY_SampleSelection WHERE Smpl_Run_ID = ? "
        f"AND Selection_Stat IN ('SELECTED', 'REPLACED') ORDER BY Selection_Order_Nbr", [run_id])]
    triggered_by = str(triggered_by or "SYSTEM")[:USER_MAX]
    now = s.now()
    out = {"reval_id": generate_run_id("REVAL", label, run.get("run_key"), timestamp=now), "smpl_run_id": run_id,
           "project_cd": key[0], "table_nm": key[1], "run_ty": key[2], "run_key": run.get("run_key"),
           "as_of_date": _day(run.get("as_of_dt_key")), "as_seen": run.get("start_dtts"),
           "orig_selected": len(original), "replay_selected": None, "same": None, "only_orig": [], "only_replay": [],
           "missing": [], "error": None}
    log.info("REVALIDATE | %s | run %s | run_key %s | replaying %d pick(s) on the source as it was at %s", label,
             run_id, run.get("run_key"), len(original), run.get("start_dtts"))
    try:
        summary = run_sampling(conn, s, key, triggered_by=triggered_by, replay=run)
    except Exception as e:
        summary = {"status": "ERROR", "error": error_text(e)}
    if summary.get("status") != "COMPLETED":
        out.update(status="FAILED", error=str(summary.get("error") or "the replay did not complete"))
    else:
        replayed = [p["case_key"] for p in summary["picked"]]
        scope, now_set, then_set = set(summary["scope_keys"]), set(replayed), set(original)
        out.update(replay_selected=len(replayed), same=len(now_set & then_set),
                   only_orig=[k for k in original if k not in now_set],
                   only_replay=[k for k in replayed if k not in then_set],
                   missing=[k for k in original if k not in scope])
        out["status"] = ("SOURCE_CHANGED" if out["missing"] else
                         "DIFFERENT" if out["only_orig"] or out["only_replay"] else "MATCH")
    out["diff_txt"] = _diff_text(out["missing"], out["only_orig"], out["only_replay"])
    conn.execute(f"""
        INSERT INTO {s.meta_db}.GOVT_RGLTRY_SamplingRevalidation (
            Reval_ID, Smpl_Run_ID, Project_Cd, Table_Nm, Run_Ty, Run_Key, As_Of_Dt_Key, Source_As_Of_Dtts, Reval_Dtts,
            Triggered_By, Reval_Stat, Orig_Selected_Cnt, Replay_Selected_Cnt, Same_Cnt, Only_Orig_Cnt,
            Only_Replay_Cnt, Missing_Cnt, Diff_Txt, Config_Hash, Engine_Version_Txt, Error_Txt, Load_Dtts,
            LastUpdt_Dtts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [out["reval_id"], run_id, *key, run.get("run_key"), out["as_of_date"], run.get("start_dtts"), now,
         triggered_by, out["status"], len(original), out["replay_selected"], out["same"],
         None if out["status"] == "FAILED" else len(out["only_orig"]),
         None if out["status"] == "FAILED" else len(out["only_replay"]),
         None if out["status"] == "FAILED" else len(out["missing"]), out["diff_txt"], run.get("config_hash"),
         __version__, _stored_error(out["error"]) if out["error"] else None, now, now])
    log_at = log.info if out["status"] == "MATCH" else log.warning
    log_at("REVALIDATE | %s | run %s | %s | then %d, now %s, same %s%s", label, run_id, out["status"],
           len(original), out["replay_selected"], out["same"],
           f" | {out['diff_txt'] or out['error']}" if out["status"] != "MATCH" else "")
    return out


def revalidate_since(conn: Connection, s: Settings, since: date, until: Optional[date] = None,
                     project: Optional[str] = None, table: Optional[str] = None, run_type: Optional[str] = None,
                     triggered_by: str = "SYSTEM") -> dict:
    where, params = ["Curr_Ind = 1", "Run_Stat IN ('COMPLETED', 'FINAL')", "As_Of_Dt_Key >= ?"], [since]
    if until:
        where.append("As_Of_Dt_Key <= ?")
        params.append(until)
    scope, scope_params = _scope_where(project, table, run_type)
    where, params = where + scope, params + scope_params
    runs = conn.query(f"SELECT Smpl_Run_ID FROM {s.meta_db}.GOVT_RGLTRY_SamplingRun WHERE {' AND '.join(where)} "
                      f"ORDER BY Project_Cd, Table_Nm, Run_Ty, As_Of_Dt_Key, Run_Key", params)
    results = []
    for r in runs:
        try:
            results.append(revalidate_run(conn, s, r["smpl_run_id"], triggered_by))
        except Exception as e:
            log.error("REVALIDATE | run %s | could not be revalidated: %s", r["smpl_run_id"], error_text(e))
            results.append({"smpl_run_id": r["smpl_run_id"], "status": "FAILED", "error": error_text(e)})
    counts = {k: sum(1 for r in results if r["status"] == k) for k in REVAL_STATUSES}
    log.info("REVALIDATE | %d run(s) since %s | %s", len(results), since,
             ", ".join(f"{k} {v}" for k, v in counts.items()))
    return {"since": since, "until": until, "runs": len(results), "counts": counts, "results": results}


def signoff_revalidation(conn: Connection, s: Settings, reval_id: str, by: str, note: str) -> dict:
    note, by = str(note or "").strip()[:1000], str(by or "").strip()[:USER_MAX]
    if not note:
        raise ConfigError("give a sign-off note: what was reviewed and the decision")
    if not by:
        raise ConfigError("give who signs off")
    now = s.now()
    done = conn.execute(f"UPDATE {s.meta_db}.GOVT_RGLTRY_SamplingRevalidation SET Signoff_By = ?, Signoff_Dtts = ?, "
                        f"Signoff_Txt = ?, LastUpdt_Dtts = ?, LastUpdt_ID = USER "
                        f"WHERE Reval_ID = ? AND Signoff_By IS NULL", [by, now, note, now, reval_id])
    if done == 0:
        rows = conn.query(f"SELECT Signoff_By, Signoff_Dtts FROM {s.meta_db}.GOVT_RGLTRY_SamplingRevalidation "
                          f"WHERE Reval_ID = ?", [reval_id])
        if not rows:
            raise ConfigError(f"no revalidation {reval_id!r}")
        raise ConfigError(f"revalidation {reval_id} is already signed off by {rows[0]['signoff_by']} at "
                          f"{rows[0]['signoff_dtts']}")
    log.info("REVALIDATE | %s signed off by %s: %s", reval_id, by, note)
    return {"reval_id": reval_id, "signoff_by": by, "signoff_dtts": now, "signoff_txt": note}


REFINE_SHORT_RUNS = 3
REFINE_SWING = 0.5
REFINE_SWING_RUNS = 4
REFINE_SWING_MIN = 5
REFINE_KINDS = ("SHORT_BUCKET", "CAP_BLOCKS", "DEAD_RULE", "EXCLUSION_SWING")


def _scope_where(project, table, run_type, prefix: str = "") -> tuple:
    where, params = [], []
    for col, value in (("Project_Cd", project), ("Table_Nm", table), ("Run_Ty", run_type)):
        if value not in (None, ""):
            where.append(f"UPPER({prefix}{col}) = ?")
            params.append(_up(value))
    return where, params


def _refine_findings(label: str, rows: list, short_runs: int, swing: float, swing_runs: int, swing_min: int,
                     after: Optional[date] = None) -> list:
    runs = list(dict.fromkeys((r["as_of_dt_key"], r["run_key"], r["smpl_run_id"]) for r in rows))
    by = {}
    for r in rows:
        by.setdefault((r["stat_ty"], r["stat_nm"]), []).append(r)
    out = []

    def found(kind, name, rule_id, text, **figures):
        out.append({"sample": label, "kind": kind, "name": name, "rule_id": rule_id, "runs": len(runs),
                    **figures, "text": text})

    for (ty, name), stats in by.items():
        rule_id = next((s["rule_id"] for s in stats if s["rule_id"] is not None), None)
        if ty == "BUCKET":
            short = [s for s in stats if s["leaf_ind"] == 1 and (s["selected_cnt"] or 0) < (s["target_cnt"] or 0)]
            if len(short) >= short_runs:
                gap = sum(s["target_cnt"] - (s["selected_cnt"] or 0) for s in short)
                found("SHORT_BUCKET", name, rule_id, f"short of its target in {len(short)} of {len(runs)} run(s), "
                      f"{gap} case(s) in all", short_runs=len(short), missing_cnt=gap,
                      last=_day(short[-1]["as_of_dt_key"]))
        elif ty == "CAP":
            blocked = sum(s["over_cnt"] or 0 for s in stats)
            if blocked:
                lost = blocked - sum(s["replaced_cnt"] or 0 for s in stats)
                found("CAP_BLOCKS", name, rule_id, f"blocked {blocked} pick(s) in "
                      f"{sum(1 for s in stats if s['over_cnt'])} run(s); {lost} not made up by another case",
                      blocked_cnt=blocked, lost_cnt=lost)
        elif ty == "RULE" and len(stats) >= short_runs and not any(s["case_cnt"] for s in stats):
            found("DEAD_RULE", name, rule_id, f"matched no case in any of its {len(stats)} run(s)",
                  last=_day(stats[-1]["as_of_dt_key"]))
        elif ty == "EXCLUSION":
            for i in range(swing_runs, len(stats)):
                if after and _day(stats[i]["as_of_dt_key"]) <= after:
                    continue
                prior = [s["case_cnt"] or 0 for s in stats[i - swing_runs:i]]
                avg, now = sum(prior) / len(prior), stats[i]["case_cnt"] or 0
                if max(now, avg) >= swing_min and (now > (1 + swing) * avg or now < (1 - swing) * avg):
                    found("EXCLUSION_SWING", name, rule_id, f"excluded {now} case(s) on "
                          f"{_day(stats[i]['as_of_dt_key'])} against {avg:g} on average in the {len(prior)} run(s) "
                          f"before", case_cnt=now, prior_avg=round(avg, 2), last=_day(stats[i]["as_of_dt_key"]))
    return out


REFINE_COLUMNS = ("Finding_Ty", "Finding_Nm", "Rule_ID", "Short_Run_Cnt", "Missing_Cnt", "Blocked_Cnt", "Lost_Cnt",
                  "Case_Cnt", "Prior_Avg_Val", "Last_As_Of_Dt", "Finding_Txt")
_REFINE_FIELDS = {"Finding_Ty": "kind", "Finding_Nm": "name", "Rule_ID": "rule_id", "Short_Run_Cnt": "short_runs",
                  "Missing_Cnt": "missing_cnt", "Blocked_Cnt": "blocked_cnt", "Lost_Cnt": "lost_cnt",
                  "Case_Cnt": "case_cnt", "Prior_Avg_Val": "prior_avg", "Last_As_Of_Dt": "last", "Finding_Txt": "text"}


def _refine_totals(label: str, stats: list, replaced: list) -> list:
    sums = {}
    for st in stats:
        if st["stat_ty"] in ("NOT_PICKED", "TOPUP"):
            name = (st["stat_ty"], str(st["stat_nm"]))
            sums[name] = sums.get(name, 0) + (st["case_cnt"] or 0)
    for x in replaced:
        if ".".join(_up(x[k]) for k in SAMPLE_KEY) == label:
            sums[("REPLACED", str(x["stat_rsn_txt"] or "no reason"))] = x["case_cnt"]
    words = {"NOT_PICKED": "case(s) not picked for this reason", "TOPUP": "case(s) taken by the top-up",
             "REPLACED": "case(s) replaced by reviewers for this reason"}
    return [{"sample": label, "kind": kind, "name": name, "rule_id": None, "case_cnt": n,
             "text": f"{n} {words[kind]}"} for (kind, name), n in sums.items()]


def _record_refinement(conn: Connection, s: Settings, sample: dict, since: date, until: date, thresholds: str,
                       triggered_by: str, now: datetime) -> None:
    key = sample["sample"].split(".")
    columns = ["Refine_ID", "Finding_Seq_Nbr", "Project_Cd", "Table_Nm", "Run_Ty", "Window_Start_Dt",
               "Window_End_Dt", "Run_Cnt", *REFINE_COLUMNS, "Threshold_Txt", "Report_Dtts", "Triggered_By",
               "Engine_Version_Txt", "Load_Dtts", "LastUpdt_Dtts"]

    def value(row, col):
        v = row.get(_REFINE_FIELDS[col])
        return str(v)[:1000] if col in ("Finding_Nm", "Finding_Txt") and v is not None else v

    rows = [[sample["refine_id"], i + 1, *key, since, until, sample["runs"], *(value(f, c) for c in REFINE_COLUMNS),
             thresholds, now, triggered_by, __version__, now, now]
            for i, f in enumerate(sample["findings"] + sample["totals"])]
    if rows:
        conn.executemany(f"INSERT INTO {s.meta_db}.GOVT_RGLTRY_SamplingRefinement ({', '.join(columns)}) "
                         f"VALUES ({', '.join(['?'] * len(columns))})", rows, s.chunk_rows)


def refinement_report(conn: Connection, s: Settings, since: date, until: Optional[date] = None,
                      project: Optional[str] = None, table: Optional[str] = None, run_type: Optional[str] = None,
                      short_runs: int = REFINE_SHORT_RUNS, swing: float = REFINE_SWING,
                      swing_runs: int = REFINE_SWING_RUNS, swing_min: int = REFINE_SWING_MIN,
                      triggered_by: str = "SYSTEM", record: bool = True) -> dict:
    if short_runs < 1 or swing_runs < 1 or swing <= 0 or swing_min < 1:
        raise ConfigError(f"refinement thresholds must be above 0: short runs {short_runs}, swing {swing}, "
                          f"swing runs {swing_runs}, swing min {swing_min}")
    end = until or s.today()
    where, params = _scope_where(project, table, run_type, "t.")
    where += ["t.As_Of_Dt_Key >= ?", "t.As_Of_Dt_Key <= ?"]
    params += [since, end]
    rows = conn.query(f"""
        SELECT t.Smpl_Run_ID, t.Project_Cd, t.Table_Nm, t.Run_Ty, t.Run_Key, t.As_Of_Dt_Key, t.Stat_Ty, t.Stat_Nm,
               t.Rule_ID, t.Leaf_Ind, t.Target_Cnt, t.Selected_Cnt, t.Over_Cnt, t.Replaced_Cnt, t.Case_Cnt
        FROM {s.meta_db}.GOVT_RGLTRY_SamplingRunStat t
        JOIN {s.meta_db}.GOVT_RGLTRY_SamplingRun r ON r.Smpl_Run_ID = t.Smpl_Run_ID AND r.Curr_Ind = 1
        WHERE {' AND '.join(where)}
        ORDER BY t.Project_Cd, t.Table_Nm, t.Run_Ty, t.As_Of_Dt_Key, t.Run_Key, t.Stat_Seq_Nbr""", params)
    where, params = _scope_where(project, table, run_type, "x.")
    where += ["x.Selection_Stat = 'REPLACED'", "x.Smpl_Cycle_Dt_Key >= ?", "x.Smpl_Cycle_Dt_Key <= ?"]
    params += [since, end]
    replaced = conn.query(f"""
        SELECT x.Project_Cd, x.Table_Nm, x.Run_Ty, x.Stat_Rsn_Txt, COUNT(*) AS Case_Cnt
        FROM {s.meta_db}.GOVT_RGLTRY_SampleSelection x
        JOIN {s.meta_db}.GOVT_RGLTRY_SamplingRun r ON r.Smpl_Run_ID = x.Smpl_Run_ID AND r.Curr_Ind = 1
        WHERE {' AND '.join(where)}
        GROUP BY x.Project_Cd, x.Table_Nm, x.Run_Ty, x.Stat_Rsn_Txt""", params)
    where, params = _scope_where(project, table, run_type)
    earlier = {".".join(_up(r[k]) for k in SAMPLE_KEY): _day(r["last_end"]) for r in conn.query(
        f"SELECT Project_Cd, Table_Nm, Run_Ty, MAX(Window_End_Dt) AS Last_End "
        f"FROM {s.meta_db}.GOVT_RGLTRY_SamplingRefinement {'WHERE ' + ' AND '.join(where) if where else ''} "
        f"GROUP BY Project_Cd, Table_Nm, Run_Ty", params)}
    by_sample = {}
    for r in rows:
        by_sample.setdefault(".".join(_up(r[k]) for k in SAMPLE_KEY), []).append(r)
    triggered_by = str(triggered_by or "SYSTEM")[:USER_MAX]
    thresholds = f"short runs {short_runs}, swing {swing:g}, swing runs {swing_runs}, swing min {swing_min}"
    now = s.now()
    samples = []
    for label, stats in by_sample.items():
        sample = {"sample": label, "refine_id": generate_run_id("REFINE", label, timestamp=now) if record else None,
                  "runs": len({st["smpl_run_id"] for st in stats}),
                  "swings_after": earlier.get(label),
                  "findings": _refine_findings(label, stats, short_runs, swing, swing_runs, swing_min,
                                               earlier.get(label)),
                  "totals": _refine_totals(label, stats, replaced)}
        for f in sample["findings"]:
            log.warning("REFINE | %s | %s | %s | %s", label, f["kind"], f["name"], f["text"])
        if record:
            _record_refinement(conn, s, sample, since, end, thresholds, triggered_by, now)
        samples.append(sample)
    counts = {k: sum(1 for o in samples for f in o["findings"] if f["kind"] == k) for k in REFINE_KINDS}
    log.info("REFINE | %d sample(s), %d run(s) from %s to %s | %s%s", len(samples), sum(o["runs"] for o in samples),
             since, end, ", ".join(f"{k} {v}" for k, v in counts.items()),
             " | recorded in GOVT_RGLTRY_SamplingRefinement" if record and samples else "")
    return {"since": since, "until": end, "thresholds": thresholds, "samples": samples, "counts": counts}


def review_refinement(conn: Connection, s: Settings, refine_id: str, by: str, note: str,
                      seq: Optional[int] = None) -> dict:
    note, by = str(note or "").strip()[:1000], str(by or "").strip()[:USER_MAX]
    if not note:
        raise ConfigError("give a review note: what was decided")
    if not by:
        raise ConfigError("give who reviews")
    table = f"{s.meta_db}.GOVT_RGLTRY_SamplingRefinement"
    where, params = "Refine_ID = ?", [refine_id]
    if seq is not None:
        where, params = where + " AND Finding_Seq_Nbr = ?", params + [seq]
    rows = conn.query(f"SELECT Finding_Seq_Nbr, Review_By, Review_Dtts FROM {table} WHERE {where}", params)
    if not rows:
        raise ConfigError(f"no refinement {refine_id!r}" + (f" finding {seq}" if seq is not None else ""))
    open_rows = [r["finding_seq_nbr"] for r in rows if r["review_by"] is None]
    if not open_rows:
        done = rows[0]
        raise ConfigError(f"refinement {refine_id}" + (f" finding {seq}" if seq is not None else "") +
                          f" is already reviewed (by {done['review_by']} at {done['review_dtts']})")
    now = s.now()
    conn.execute(f"UPDATE {table} SET Review_By = ?, Review_Dtts = ?, Review_Txt = ?, LastUpdt_Dtts = ?, "
                 f"LastUpdt_ID = USER WHERE {where} AND Review_By IS NULL", [by, now, note, now, *params])
    log.info("REFINE | %s | %d finding(s) reviewed by %s: %s", refine_id, len(open_rows), by, note)
    return {"refine_id": refine_id, "reviewed": sorted(open_rows), "review_by": by, "review_dtts": now,
            "review_txt": note}


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


def _within_caps(conn: Connection, s: Settings, sample: Sample, params: dict, run: dict, rows: list, old: dict,
                 pool: list) -> list:
    kept = [r for r in rows if int(r["selected_ind"] or 0) == 1 and r["case_key"] != old["case_key"]]
    cases = {r["case_key"]: {"_key": r["case_key"], "_path": r["stratum_path_txt"]} for r in (*kept, *pool)}
    key_queries = [(r, text) for r, text in build_queries(sample, s, params)["key_queries"]
                   if r["rule_class_cd"] == "CAP"]
    mark_query_rules(conn, s, sample, list(cases.values()), key_queries, params)
    caps = Caps(sample, int(run.get("target_cnt") or len(kept) + 1), _up(run.get("rounding_cd"), "FLOOR"))
    for r in kept:
        caps.take(cases[r["case_key"]])
    out = [r for r in pool if caps.blocking(cases[r["case_key"]]) is None]
    if len(out) < len(pool):
        log.info("REPLACE | %s | %d candidate(s) left out: over a CAP rule", sample.label, len(pool) - len(out))
    return out


def replace_case(conn: Connection, s: Settings, key, run_key: str, case_key_text: str, reason: str, by: str,
                 any_bucket: bool = False) -> dict:
    if not str(reason or "").strip():
        raise ConfigError("a reason is required to replace a case")
    by = str(by or "")[:USER_MAX]
    key = sample_key(key)
    run = current_run(conn, s, key, run_key)
    if not run or _up(run["run_stat"]) not in ("COMPLETED", "FINAL"):
        raise ConfigError(f"{'.'.join(key)} has no completed current sample for run_key {run_key!r}")
    db, run_id = s.meta_db, run["smpl_run_id"]
    rows = conn.query(f"SELECT Case_Key, Selected_Ind, Selection_Order_Nbr, Bucket_Rank_Nbr, Priority_Rank_Nbr, "
                      f"Excluded_Ind, "
                      f"Stratum_Path_Txt, Selection_Stat FROM {db}.GOVT_RGLTRY_SampleSelection "
                      f"WHERE Smpl_Run_ID = ?", [run_id])
    old = next((r for r in rows if r["case_key"] == case_key_text), None)
    if not old or int(old["selected_ind"] or 0) != 1:
        raise ConfigError(f"{case_key_text!r} is not in the current sample of run_key {run_key!r}")
    pool = [r for r in rows if int(r["selected_ind"] or 0) == 0 and int(r["excluded_ind"] or 0) == 0
            and r["selection_stat"] is None]
    if not any_bucket:
        pool = [r for r in pool if r["stratum_path_txt"] == old["stratum_path_txt"]]
    if not pool and int(run.get("candidate_cnt") or 0) > sum(1 for r in rows if int(r["excluded_ind"] or 0) == 0):
        raise ConfigError(f"the unselected candidates of run {run_id} were purged (retention), so it has none left "
                          f"to replace {case_key_text!r} with: rerun the cycle with a force reason if a case must go")
    if not pool:
        raise ConfigError(f"no candidate left to replace {case_key_text!r}"
                          + ("" if any_bucket else f" in bucket {old['stratum_path_txt']!r} (allow any bucket)"))
    pool.sort(key=lambda r: (r["priority_rank_nbr"] is None, r["priority_rank_nbr"] or 0, r["case_key"]))
    sample = run_sample(conn, s, run)
    params = json.loads(run.get("run_parm_txt") or "{}")
    seen, cache = set(), {}
    for x in sample.exclusions:
        if _up(x.get("rule_ty")) == "PRIOR_SELECTION":
            seen |= _history(conn, s, sample, x, run_key, run["as_of_dt_key"], cache, ahead=True)["selected"]
    if seen:
        kept = [r for r in pool if r["case_key"] not in seen]
        if len(kept) < len(pool):
            log.info("REPLACE | %s | %d candidate(s) left out: selected by another cycle", sample.label,
                     len(pool) - len(kept))
        pool = kept
        if not pool:
            raise ConfigError(f"no candidate left to replace {case_key_text!r}: the rest were selected by another "
                              f"cycle (PRIOR_SELECTION)")
    if sample.caps:
        pool = _within_caps(conn, s, sample, params, run, rows, old, pool)
        if not pool:
            raise ConfigError(f"no candidate left to replace {case_key_text!r} within the CAP rules"
                              + ("" if any_bucket else f" in bucket {old['stratum_path_txt']!r} (allow any bucket)"))
    if _up(run.get("smpl_method_cd")) == "RANDOM":
        new = random.Random(f"{run.get('random_seed_nbr')}|{run_id}|{case_key_text}").choice(pool)
    else:
        new = pool[0]

    output = queries = gone = None
    if _text(run, "tgt_table_nm"):
        queries = build_queries(sample, s, params)
        queries["output_columns"] = target_table_columns(conn, queries, sample)
        row = {**parse_case_key(new["case_key"], sample.keys), "_key": new["case_key"]}
        gone = parse_case_key(old["case_key"], sample.keys)
        _typed_keys(conn, queries, sample, [row, gone])
        output = fetch_output(conn, queries, sample, [row])

    now = s.now()
    with conn.transaction():
        same = old["stratum_path_txt"] == new["stratum_path_txt"]
        for case, flag, order, rank, status, linked in (
                (old["case_key"], 0, None, new["bucket_rank_nbr"] if same else old["bucket_rank_nbr"], "REPLACED",
                 new["case_key"]),
                (new["case_key"], 1, old["selection_order_nbr"], old["bucket_rank_nbr"] if same else
                 new["bucket_rank_nbr"], "REPLACEMENT", old["case_key"])):
            conn.execute(f"""
                UPDATE {db}.GOVT_RGLTRY_SampleSelection
                SET Selected_Ind = ?, Selection_Order_Nbr = ?, Bucket_Rank_Nbr = ?, Selection_Stat = ?,
                    Linked_Case_Key = ?, Stat_Rsn_Txt = ?, Stat_By = ?, Stat_Dtts = ?, LastUpdt_Dtts = ?,
                    LastUpdt_ID = USER
                WHERE Smpl_Run_ID = ? AND Case_Key = ?""",
                [flag, order, rank, status, linked, reason[:1000], by, now, now, run_id, case])
        if output is not None:
            where = []
            _retire_output(conn, s, queries, key, run_key, now,
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
                     run_type: Optional[str] = None, rules_as_of: Optional[date] = None) -> list:
    if not str(project or "").strip():
        raise ConfigError("give a project (and optionally a table and a run type): a run never samples every "
                          "project at once")
    day = rules_as_of or s.today()
    where, params = [IN_EFFECT, "UPPER(Project_Cd) = ?"], [day, day, _up(project)]
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
    keys = discover_configs(conn, s, project, table, run_type, kwargs.get("rules_as_of"))
    if not keys:
        raise ConfigError(f"no active GOVT_RGLTRY_SamplingConfig matches {scope}")
    runs = []
    for key in keys:
        failed = {"project_cd": key[0], "table_nm": key[1], "run_ty": key[2], "status": "ERROR"}
        try:
            runs.append(run_sampling(conn, s, key, **kwargs))
        except ConfigError as e:
            log.error("%s | %s | CONFIG_ERROR: %s", STEP, ".".join(key), e)
            runs.append({**failed, "error_kind": "config", "error_type": "CONFIG_ERROR", "error": str(e)})
        except Exception as e:
            log.error("%s | %s | START_FAILURE: %s", STEP, ".".join(key), error_text(e), exc_info=True)
            runs.append({**failed, "error_kind": "technical", "error_type": "START_FAILURE", "error": error_text(e)})
    failed = [r for r in runs if r.get("status") != "COMPLETED"]
    result = "ALL SAMPLES COMPLETED" if not failed else f"{len(failed)} OF {len(runs)} SAMPLE(S) FAILED"
    (log.error if failed else log.info)("%s | %s | %s", STEP, scope or "all", result)
    return {"result": result, "scope": scope, "runs": runs, "failed": len(failed)}


def validate(conn: Connection, s: Settings, project: Optional[str] = None, table: Optional[str] = None,
             run_type: Optional[str] = None, run_params: Optional[dict] = None,
             check_sql: bool = False, as_of: Optional[date] = None, extra_filters: Optional[str] = None,
             rules_as_of: Optional[date] = None) -> dict:
    out = []
    for key in discover_configs(conn, s, project, table, run_type, rules_as_of):
        entry = {"project_cd": key[0], "table_nm": key[1], "run_ty": key[2]}
        try:
            sample = load_config(conn, s, key, rules_as_of)
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
                for _, text in queries["key_queries"]:
                    conn.query("EXPLAIN " + text)
            except Exception as e:
                problems.append(f"SQL check failed: {error_text(e)}")
        out.append({**entry, "smpl_desc": sample.config.get("smpl_desc"), "problems": problems})
    bad = sum(1 for c in out if c["problems"])
    return {"result": "VALID" if not bad else f"{bad} SAMPLE(S) WITH PROBLEMS", "configs": out}
