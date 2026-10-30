"""Looking after the sampling tables: configuration from a file, and retention of the candidate rows.

Configuration file (JSON, one sample)::

    {"config": {gre_sampling_config columns},
     "rules":  [{gre_sampling_rule columns}, ...]}

`export_config` writes it from the tables, `load_config_file` reads it back: the file is checked like a run
checks its rows (config_problems), compared with the rows in the tables, and applied in one transaction. A rule
missing from the file is switched off (act_ind = 0), never deleted.

Retention: `purge_selections` deletes the gre_sample_selections rows that no history rule, replacement or
audit needs any more. See OPERATIONAL_RUNBOOK.md ("Retention").
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Optional

from sampling.db import Connection
from sampling.engine import (HISTORY_TYPES, _AUDIT_COLUMNS, _up, build_sample, config_problems, discover_configs)
from sampling.settings import ConfigError, Settings

log = logging.getLogger(__name__)

# the columns of sql/schema.sql a file may set (the audit columns are set here); a test keeps them in step
CONFIG_COLUMNS = ("config_id", "act_ind", "project_name", "process_name", "sample_name", "universe_table",
                  "key_columns", "scope_sql", "exclusion_sql", "target_volume", "sampling_method",
                  "priority_rank_sql", "rounding_mode", "diversity_expr", "diversity_cap", "shortfall_policy",
                  "keep_excluded", "output_table", "schedule_cron")
RULE_COLUMNS = ("rule_id", "config_id", "act_ind", "rule_class", "seq_no", "rule_name", "rule_type", "rule_sql",
                "level_order", "parent_path", "unlisted_policy", "quota_mode", "target_fraction", "max_count",
                "sort_dir", "lookback_days", "history_scope", "description")


# ---------------------------------------------------------------------------
# Configuration from a file
# ---------------------------------------------------------------------------

def _value(v):
    """A value as the file and the tables can both hold it: '' is NULL, whole floats are ints."""
    if isinstance(v, str):
        v = v.rstrip()
        return v or None
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if hasattr(v, "as_integer_ratio") and not isinstance(v, (int, float)):   # Decimal
        return _value(float(v))
    return v


def _row(row: dict, columns: tuple) -> dict:
    return {c: _value(row.get(c)) for c in columns}


def export_config(conn: Connection, s: Settings, config_id) -> dict:
    """The config row and every rule row of config_id (active or not), audit columns left out."""
    rows = conn.query(f"SELECT * FROM {s.meta_db}.gre_sampling_config WHERE config_id = ?", [config_id])
    if not rows:
        raise ConfigError(f"no gre_sampling_config row for config_id={config_id}")
    rules = conn.query(f"SELECT * FROM {s.meta_db}.gre_sampling_rule WHERE config_id = ? ORDER BY rule_id",
                       [config_id])
    return {"config": _row(rows[0], CONFIG_COLUMNS), "rules": [_row(r, RULE_COLUMNS) for r in rules]}


def read_config_file(text: str) -> dict:
    """The {config, rules} of a configuration file, its shape checked (not yet its content)."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ConfigError(f"the configuration file is not JSON: {e}") from e
    if not isinstance(data, dict) or not isinstance(data.get("config"), dict) or \
            not isinstance(data.get("rules", []), list) or not all(isinstance(r, dict) for r in data.get("rules", [])):
        raise ConfigError('the configuration file must be {"config": {...}, "rules": [{...}, ...]}')
    problems = []
    config, rules = data["config"], data.get("rules", [])
    unknown = sorted(set(config) - set(CONFIG_COLUMNS) - set(_AUDIT_COLUMNS))
    if unknown:
        problems.append(f"config: unknown column(s) {', '.join(unknown)}")
    if config.get("config_id") in (None, ""):
        problems.append("config: config_id is required")
    ids = []
    for i, r in enumerate(rules):
        unknown = sorted(set(r) - set(RULE_COLUMNS) - set(_AUDIT_COLUMNS))
        if unknown:
            problems.append(f"rule #{i + 1}: unknown column(s) {', '.join(unknown)}")
        if r.get("rule_id") in (None, ""):
            problems.append(f"rule #{i + 1} ({r.get('rule_name')}): rule_id is required")
        if r.get("config_id") not in (None, "", config.get("config_id")):
            problems.append(f"rule {r.get('rule_id')}: config_id {r['config_id']} is not the file's "
                            f"{config.get('config_id')}")
        ids.append(r.get("rule_id"))
    repeated = sorted({str(i) for i in ids if ids.count(i) > 1})
    if repeated:
        problems.append(f"rule_id repeated: {', '.join(repeated)}")
    if problems:
        raise ConfigError("; ".join(problems))
    cid = config["config_id"]
    return {"config": {**_row(config, CONFIG_COLUMNS), "act_ind": _value(config.get("act_ind", 1))},
            "rules": [{**_row(r, RULE_COLUMNS), "config_id": cid, "act_ind": _value(r.get("act_ind", 1)),
                       "parent_path": _value(r.get("parent_path")) or "*", "seq_no": _value(r.get("seq_no", 100))}
                      for r in rules]}


def _changes(old: dict, new: dict, columns: tuple) -> dict:
    return {c: {"from": _value(old.get(c)), "to": new.get(c)} for c in columns if _value(old.get(c)) != new.get(c)}


def load_config_file(conn: Connection, s: Settings, text: str, by: str, apply: bool = False) -> dict:
    """Check a configuration file and show how it differs from the tables; with apply, write it (one transaction).
    Refused when the rules it would make active have problems, or a rule_id belongs to another config."""
    data = read_config_file(text)
    config, rules = data["config"], data["rules"]
    cid = config["config_id"]
    db = s.meta_db
    if int(config.get("act_ind") or 0) == 1:
        problems = config_problems(build_sample(config, [r for r in rules if int(r.get("act_ind") or 0) == 1]))
    else:
        problems = []
    ids = [r["rule_id"] for r in rules]
    taken = []
    if ids:
        marks = ", ".join("?" * len(ids))
        taken = conn.query(f"SELECT rule_id, config_id FROM {db}.gre_sampling_rule WHERE rule_id IN ({marks}) "
                           f"AND config_id <> ?", [*ids, cid])
    problems += [f"rule_id {t['rule_id']} belongs to config_id {t['config_id']}" for t in taken]

    old_config = conn.query(f"SELECT * FROM {db}.gre_sampling_config WHERE config_id = ?", [cid])
    old_rules = {r["rule_id"]: r for r in conn.query(f"SELECT * FROM {db}.gre_sampling_rule WHERE config_id = ?",
                                                     [cid])}
    config_change = _changes(old_config[0], config, CONFIG_COLUMNS) if old_config else "new"
    added = [r for r in rules if r["rule_id"] not in old_rules]
    changed = {r["rule_id"]: _changes(old_rules[r["rule_id"]], r, RULE_COLUMNS) for r in rules
               if r["rule_id"] in old_rules}
    changed = {k: v for k, v in changed.items() if v}
    in_file = set(ids)
    switched_off = [i for i, r in old_rules.items() if i not in in_file and int(r.get("act_ind") or 0) == 1]
    out = {"config_id": cid, "config": config_change or "unchanged",
           "rules_added": [f"{r['rule_id']} {r['rule_class']} {r['rule_name']}" for r in added],
           "rules_changed": changed, "rules_switched_off": sorted(switched_off), "problems": problems,
           "applied": False}
    if problems or not apply:
        return out
    now = s.now()
    with conn.transaction():
        if not old_config:
            cols = [*CONFIG_COLUMNS, "created_by", "last_updated_by", "load_datetime", "last_updated_datetime"]
            conn.execute(f"INSERT INTO {db}.gre_sampling_config ({', '.join(cols)}) VALUES "
                         f"({', '.join('?' * len(cols))})", [*(config[c] for c in CONFIG_COLUMNS), by, by, now, now])
        elif config_change:
            sets = list(config_change)
            conn.execute(f"UPDATE {db}.gre_sampling_config SET {', '.join(f'{c} = ?' for c in sets)}, "
                         f"last_updated_by = ?, last_updated_datetime = ? WHERE config_id = ?",
                         [*(config[c] for c in sets), by, now, cid])
        cols = [*RULE_COLUMNS, "created_by", "load_datetime", "last_updated_datetime"]
        if added:
            conn.executemany(f"INSERT INTO {db}.gre_sampling_rule ({', '.join(cols)}) VALUES "
                             f"({', '.join('?' * len(cols))})",
                             [[*(r[c] for c in RULE_COLUMNS), by, now, now] for r in added], s.chunk_rows)
        new = {r["rule_id"]: r for r in rules}
        for rule_id, change in changed.items():
            sets = list(change)
            conn.execute(f"UPDATE {db}.gre_sampling_rule SET {', '.join(f'{c} = ?' for c in sets)}, "
                         f"last_updated_datetime = ? WHERE rule_id = ?", [*(new[rule_id][c] for c in sets), now, rule_id])
        for rule_id in switched_off:
            conn.execute(f"UPDATE {db}.gre_sampling_rule SET act_ind = 0, last_updated_datetime = ? WHERE rule_id = ?",
                         [now, rule_id])
    log.info("config_id=%s loaded by %s: config %s, %d rule(s) added, %d changed, %d switched off", cid, by,
             "new" if not old_config else ("changed" if config_change else "unchanged"), len(added), len(changed),
             len(switched_off))
    out["applied"] = True
    return out


# ---------------------------------------------------------------------------
# Retention of gre_sample_selections
# ---------------------------------------------------------------------------

def history_days(conn: Connection, s: Settings, config_id) -> int:
    """The longest lookback_days of the active history rules that read config_id's rows: its own, and the
    PROJECT-scope ones of every config of its project."""
    db = s.meta_db
    rows = conn.query(f"""
        SELECT r.config_id, r.rule_type, r.lookback_days, r.history_scope
        FROM {db}.gre_sampling_rule r JOIN {db}.gre_sampling_config c ON c.config_id = r.config_id
        WHERE r.act_ind = 1 AND c.act_ind = 1 AND r.rule_class IN ('EXCLUSION', 'PRIORITY')
          AND c.project_name = (SELECT project_name FROM {db}.gre_sampling_config WHERE config_id = ?)""",
        [config_id])
    days = [int(r["lookback_days"] or 0) for r in rows if _up(r["rule_type"]) in HISTORY_TYPES
            and (r["config_id"] == config_id or _up(r["history_scope"], "CONFIG") == "PROJECT")]
    return max(days, default=0)


def purge_selections(conn: Connection, s: Settings, older_than_days: int, config_id=None,
                     project: Optional[str] = None, process_name: Optional[str] = None, apply: bool = False,
                     as_of: Optional[date] = None) -> dict:
    """Delete the gre_sample_selections rows of runs older than older_than_days (by sample_cycle) that nothing
    needs any more, per config of the scope:

      - a run no longer current (a rerun replaced it): all its rows;
      - a current COMPLETED run: its excluded rows and the candidates never selected (the selected, REPLACED and
        REPLACEMENT rows stay).

    FINAL runs keep every row, and gre_sampling_run rows (with their report and rules) are never deleted.
    older_than_days must cover the longest lookback of the history rules reading the config's rows. Without
    apply, only counts what would go."""
    try:
        days = int(older_than_days)
    except (TypeError, ValueError):
        raise ConfigError(f"older_than_days must be a whole number, got {older_than_days!r}") from None
    if days < 1:
        raise ConfigError("older_than_days must be >= 1")
    cutoff = (as_of or s.today()) - timedelta(days=days)
    db = s.meta_db
    retired = (f"SELECT run_id FROM {db}.gre_sampling_run WHERE config_id = ? AND etl_is_curr_ind = 'N' "
               f"AND status <> 'FINAL' AND as_of_date < ?")
    current = (f"SELECT run_id FROM {db}.gre_sampling_run WHERE config_id = ? AND etl_is_curr_ind = 'Y' "
               f"AND status = 'COMPLETED' AND as_of_date < ?")
    unused = "(excluded_flag = 1 OR (selected_flag = 0 AND selection_status IS NULL))"
    out = []
    for cid in discover_configs(conn, s, config_id, project, process_name):
        needed = history_days(conn, s, cid)
        if days < needed:
            out.append({"config_id": cid, "cutoff": cutoff.isoformat(), "skipped": f"history rules look back "
                        f"{needed} days: keep at least that many"})
            continue
        parts = ((f"sample_run_id IN ({retired})", "retired_runs"),
                 (f"sample_run_id IN ({current}) AND {unused}", "unselected_of_current_runs"))
        counts = {}
        for where, name in parts:
            counts[name] = int(conn.query(f"SELECT COUNT(*) AS cnt FROM {db}.gre_sample_selections WHERE {where}",
                                          [cid, cutoff])[0]["cnt"])
        if apply and any(counts.values()):
            with conn.transaction():
                for where, _ in parts:
                    conn.execute(f"DELETE FROM {db}.gre_sample_selections WHERE {where}", [cid, cutoff])
            log.info("config_id=%s: deleted %s gre_sample_selections rows before %s", cid, counts, cutoff)
        out.append({"config_id": cid, "cutoff": cutoff.isoformat(), "rows": counts,
                    "deleted": apply and any(counts.values())})
    return {"apply": apply, "older_than_days": days, "configs": out}
