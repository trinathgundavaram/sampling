"""Looking after the sampling tables: retention of the candidate rows.

Retention: `purge_selections` deletes the GOVT_RGLTRY_SAMPLE_SELECTIONS rows that no history rule, replacement or
audit needs any more. See OPERATIONAL_RUNBOOK.md ("Retention").
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from sampling.db import Connection
from sampling.engine import HISTORY_TYPES, _up, discover_configs
from sampling.settings import ConfigError, Settings

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retention of GOVT_RGLTRY_SAMPLE_SELECTIONS
# ---------------------------------------------------------------------------

def history_days(conn: Connection, s: Settings, config_id) -> int:
    """The longest lookback_days of the active history rules that read config_id's rows: its own, and the
    PROJECT-scope ones of every config of its project."""
    db = s.meta_db
    rows = conn.query(f"""
        SELECT r.config_id, r.rule_type, r.lookback_days, r.history_scope
        FROM {db}.GOVT_RGLTRY_SAMPLING_RULE r JOIN {db}.GOVT_RGLTRY_SAMPLING_CONFIG c ON c.config_id = r.config_id
        WHERE r.act_ind = 1 AND c.act_ind = 1 AND r.rule_class IN ('EXCLUSION', 'PRIORITY')
          AND c.project_name = (SELECT project_name FROM {db}.GOVT_RGLTRY_SAMPLING_CONFIG WHERE config_id = ?)""",
        [config_id])
    days = [int(r["lookback_days"] or 0) for r in rows if _up(r["rule_type"]) in HISTORY_TYPES
            and (r["config_id"] == config_id or _up(r["history_scope"], "CONFIG") == "PROJECT")]
    return max(days, default=0)


def purge_selections(conn: Connection, s: Settings, older_than_days: int, config_id=None,
                     project: Optional[str] = None, process_name: Optional[str] = None, apply: bool = False,
                     as_of: Optional[date] = None) -> dict:
    """Delete the GOVT_RGLTRY_SAMPLE_SELECTIONS rows of runs older than older_than_days (by sample_cycle) that nothing
    needs any more, per config of the scope:

      - a run no longer current (a rerun replaced it): all its rows;
      - a current COMPLETED run: its excluded rows and the candidates never selected (the selected, REPLACED and
        REPLACEMENT rows stay).

    FINAL runs keep every row, and GOVT_RGLTRY_SAMPLING_RUN rows (with their report and rules) are never deleted.
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
    retired = (f"SELECT run_id FROM {db}.GOVT_RGLTRY_SAMPLING_RUN WHERE config_id = ? AND etl_is_curr_ind = 'N' "
               f"AND status <> 'FINAL' AND as_of_date < ?")
    current = (f"SELECT run_id FROM {db}.GOVT_RGLTRY_SAMPLING_RUN WHERE config_id = ? AND etl_is_curr_ind = 'Y' "
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
            counts[name] = int(conn.query(f"SELECT COUNT(*) AS cnt FROM {db}.GOVT_RGLTRY_SAMPLE_SELECTIONS WHERE {where}",
                                          [cid, cutoff])[0]["cnt"])
        if apply and any(counts.values()):
            with conn.transaction():
                for where, _ in parts:
                    conn.execute(f"DELETE FROM {db}.GOVT_RGLTRY_SAMPLE_SELECTIONS WHERE {where}", [cid, cutoff])
            log.info("config_id=%s: deleted %s GOVT_RGLTRY_SAMPLE_SELECTIONS rows before %s", cid, counts, cutoff)
        out.append({"config_id": cid, "cutoff": cutoff.isoformat(), "rows": counts,
                    "deleted": apply and any(counts.values())})
    return {"apply": apply, "older_than_days": days, "configs": out}
