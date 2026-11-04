"""Looking after the sampling tables: retention of the candidate rows.

Retention: `purge_selections` deletes the GOVT_RGLTRY_SampleSelection rows that no history rule, replacement or
audit needs any more. See OPERATIONAL_RUNBOOK.md ("Retention").
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from sampling.db import Connection
from sampling.engine import ALL_TABLES, HISTORY_TYPES, _up, discover_configs, sample_key
from sampling.settings import ConfigError, Settings

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retention of GOVT_RGLTRY_SampleSelection
# ---------------------------------------------------------------------------

def history_days(conn: Connection, s: Settings, key) -> int:
    """The longest Lookback_Days of the active history rules that may read a sample's rows: every PROJECT-scope
    rule of its project, the TABLE-scope rules of its table (or '*'), the SAMPLE-scope rules of its table and run
    type (or '*')."""
    project, table, run_type = sample_key(key)
    rows = conn.query(f"""
        SELECT Table_Nm, Run_Ty, Rule_Ty, Lookback_Days, History_Scope_Cd
        FROM {s.meta_db}.GOVT_RGLTRY_SamplingRule
        WHERE Active_Ind = 1 AND UPPER(Rule_Class_Cd) IN ('EXCLUSION', 'PRIORITY') AND UPPER(Project_Cd) = ?""",
        [project])

    def reads(r) -> bool:
        scope = _up(r["history_scope_cd"], "SAMPLE")
        same_table = _up(r["table_nm"], ALL_TABLES) in (table, ALL_TABLES)
        same_type = _up(r["run_ty"], ALL_TABLES) in (run_type, ALL_TABLES)
        return scope == "PROJECT" or (scope == "TABLE" and same_table) or (same_table and same_type)
    days = [int(r["lookback_days"] or 0) for r in rows if _up(r["rule_ty"]) in HISTORY_TYPES and reads(r)]
    return max(days, default=0)


def purge_selections(conn: Connection, s: Settings, older_than_days: int, project: Optional[str] = None,
                     table: Optional[str] = None, run_type: Optional[str] = None, apply: bool = False,
                     as_of: Optional[date] = None) -> dict:
    """Delete the GOVT_RGLTRY_SampleSelection rows of runs older than older_than_days (by As_Of_Dt_Key) that
    nothing needs any more, per sample of the scope (a project, optionally a table and / or a run type):

      - a run no longer current (a rerun replaced it): all its rows;
      - a current COMPLETED run: its excluded rows and the candidates never selected (the selected, REPLACED and
        REPLACEMENT rows stay).

    FINAL runs keep every row, and GOVT_RGLTRY_SamplingRun / SamplingRunRule rows are never deleted.
    older_than_days must cover the longest lookback of the history rules reading the sample's rows. Without
    apply, only counts what would go."""
    try:
        days = int(older_than_days)
    except (TypeError, ValueError):
        raise ConfigError(f"older_than_days must be a whole number, got {older_than_days!r}") from None
    if days < 1:
        raise ConfigError("older_than_days must be >= 1")
    cutoff = (as_of or s.today()) - timedelta(days=days)
    db = s.meta_db
    same = "UPPER(Project_Cd) = ? AND UPPER(Table_Nm) = ? AND UPPER(Run_Ty) = ?"
    retired = (f"SELECT Smpl_Run_ID FROM {db}.GOVT_RGLTRY_SamplingRun WHERE {same} AND Curr_Ind = 0 "
               f"AND Run_Stat <> 'FINAL' AND As_Of_Dt_Key < ?")
    current = (f"SELECT Smpl_Run_ID FROM {db}.GOVT_RGLTRY_SamplingRun WHERE {same} AND Curr_Ind = 1 "
               f"AND Run_Stat = 'COMPLETED' AND As_Of_Dt_Key < ?")
    unused = "(Excluded_Ind = 1 OR (Selected_Ind = 0 AND Selection_Stat IS NULL))"
    out = []
    for key in discover_configs(conn, s, project, table, run_type):
        entry = {"project_cd": key[0], "table_nm": key[1], "run_ty": key[2], "cutoff": cutoff.isoformat()}
        needed = history_days(conn, s, key)
        if days < needed:
            out.append({**entry, "skipped": f"history rules look back {needed} days: keep at least that many"})
            continue
        parts = ((f"Smpl_Run_ID IN ({retired})", "retired_runs"),
                 (f"Smpl_Run_ID IN ({current}) AND {unused}", "unselected_of_current_runs"))
        counts = {}
        for where, name in parts:
            counts[name] = int(conn.query(f"SELECT COUNT(*) AS cnt FROM {db}.GOVT_RGLTRY_SampleSelection "
                                          f"WHERE {where}", [*key, cutoff])[0]["cnt"])
        if apply and any(counts.values()):
            with conn.transaction():
                for where, _ in parts:
                    conn.execute(f"DELETE FROM {db}.GOVT_RGLTRY_SampleSelection WHERE {where}", [*key, cutoff])
            log.info("%s: deleted %s GOVT_RGLTRY_SampleSelection rows before %s", ".".join(key), counts, cutoff)
        out.append({**entry, "rows": counts, "deleted": apply and any(counts.values())})
    return {"apply": apply, "older_than_days": days, "configs": out}
