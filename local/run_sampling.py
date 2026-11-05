"""Local command line for the sampling engine: the same engine as the Airflow DAG, credentials from dev.env.

    python run_sampling.py run --project UMCM --run-type WKLY --run-key 2026-10-30
    python run_sampling.py run --project UMCM --table ODAG1 --run-type WKLY --run-param pull_date=2026-10-30 --dry-run
    python run_sampling.py run --project UMCM --table ODAG1 --run-type WKLY --filter "BTCH_ID = 'B42'" --filter "ETL_IS_CURR_IND = 'Y'"
    python run_sampling.py validate --project UMCM --check-sql
    python run_sampling.py show-config --project UMCM --table ODAG1 --run-type WKLY
    python run_sampling.py runs --project UMCM --table ODAG1 --run-type WKLY
    python run_sampling.py errors
    python run_sampling.py finalize --project UMCM --table ODAG1 --run-type WKLY --run-key B42
    python run_sampling.py replace --project UMCM --table ODAG1 --run-type WKLY --run-key B42 --case-key "AUTHZN_ID=123" --reason "no record"
    python run_sampling.py run --project UMCM --table ODAG1 --run-type WKLY --run-key B42 --force-reason "rules corrected, approved by ..."
    python run_sampling.py purge --project UMCM --older-than-days 400 [--apply]
    python run_sampling.py mark-failed --run-id <run_id>
    python run_sampling.py test-connection
    python run_sampling.py deploy-schema --dry-run [--drop-first]

Settings come from the environment, filled from dev.env (next to this file; SAMPLING_ENV_FILE names another).
A variable already set in the shell wins over the file. See dev.env.example.

Exit codes: 0 = done; 1 = done with problems (a sample failed or fell short, validation found problems);
2 = could not run (configuration, connection).
"""
from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

_HERE = Path(__file__).resolve().parent
# one engine for both versions: the package deployed to Airflow (airflow/sampling); its DAG module (dags.py) is
# the only part that imports Airflow, and it is not imported here
_ENGINE_DIR = _HERE.parent / "airflow"
if str(_ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(_ENGINE_DIR))

from sampling import __version__  # noqa: E402
from sampling.admin import purge_selections  # noqa: E402
from sampling.db import connect_teradata, error_text  # noqa: E402
from sampling.engine import (config_problems, finalize_run, load_config, replace_case, run_scope,  # noqa: E402
                             validate)
from sampling.settings import ConfigError, Settings  # noqa: E402

log = logging.getLogger("sampling.cli")
SQL_DIR = _HERE.parent / "sql"
PREFIX = "SAMPLING_"


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

def read_env_file(path: Path) -> dict:
    """KEY=value lines; # comments, blank lines and 'export ' are ignored, surrounding quotes removed."""
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.removeprefix("export ").split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        out[key.strip()] = value
    return out


def load_env_file() -> Path | None:
    """Fill os.environ from dev.env without overwriting what the shell already set."""
    name = os.environ.get(PREFIX + "ENV_FILE") or "dev.env"
    path = Path(name) if Path(name).is_absolute() else _HERE / name
    if not path.is_file():
        return None
    for key, value in read_env_file(path).items():
        if value != "" and not os.environ.get(key):
            os.environ[key] = value
    return path


def settings_from_env() -> Settings:
    return Settings.from_mapping({
        "meta_db": os.environ.get(PREFIX + "META_DB"),
        "environment": os.environ.get(PREFIX + "ENVIRONMENT"),
        "env_value": os.environ.get(PREFIX + "ENV_VALUE"),
        "timezone": os.environ.get(PREFIX + "TIMEZONE"),
        "chunk_rows": os.environ.get(PREFIX + "CHUNK_ROWS"),
        "access_lock": os.environ.get(PREFIX + "ACCESS_LOCK"),
    })


def configure_logging(level: str) -> Path:
    """Console (stderr) plus one log file per run in SAMPLING_LOG_DIR (default local/logs)."""
    log_dir = Path(os.environ.get(PREFIX + "LOG_DIR") or _HERE / "logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"sampling_{datetime.now():%Y%m%d_%H%M%S_%f}_{os.getpid()}.log"
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")   # the framework's format
    handlers = [logging.StreamHandler(sys.stderr), logging.FileHandler(path, encoding="utf-8")]
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=level.upper(), handlers=handlers, force=True)
    return path


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def _run_params(args) -> dict:
    params = {}
    if args.run_params:
        try:
            params.update(json.loads(args.run_params))
        except json.JSONDecodeError as e:
            raise ConfigError(f"--run-params must be a JSON object: {e}") from e
    for item in args.run_param or []:
        if "=" not in item:
            raise ConfigError(f"--run-param must be KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        params[key.strip()] = value
    return params


def _filters(args) -> str:
    """--filter "SQL condition" (repeat): ANDed to the scope for this run."""
    items = [f.strip() for f in args.filter or [] if f.strip()]
    return " AND ".join(f"({f})" for f in items) if len(items) > 1 else (items[0] if items else None)


def _scope(args) -> dict:
    return {k: v for k, v in (("project", args.project), ("table", args.table), ("run_type", args.run_type))
            if v not in (None, "")}


def _key(args) -> tuple:
    """One sample: (project, table, run_type)."""
    return (args.project, args.table, args.run_type)


def _filter_sample(args, where: list, params: list) -> None:
    for col, value in (("Project_Cd", args.project), ("Table_Nm", args.table), ("Run_Ty", args.run_type)):
        if value not in (None, ""):
            where.append(f"UPPER({col}) = ?")
            params.append(str(value).strip().upper())


def cmd_run(conn, s, args) -> tuple:
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    out = run_scope(conn, s, **_scope(args), run_key=args.run_key, run_params=_run_params(args),
                    extra_filters=_filters(args), seed=args.seed,
                    as_of=as_of, triggered_by=args.triggered_by or f"local-{getpass.getuser()}",
                    dry_run=args.dry_run, force_reason=args.force_reason)
    problems = out["failed"] or any(r.get("shortfall") for r in out["runs"])
    return out, 1 if problems else 0


def _user(args) -> str:
    return args.by or getpass.getuser()


def cmd_finalize(conn, s, args) -> tuple:
    return finalize_run(conn, s, _key(args), args.run_key, _user(args)), 0


def cmd_replace(conn, s, args) -> tuple:
    return replace_case(conn, s, _key(args), args.run_key, args.case_key, args.reason, _user(args),
                        any_bucket=args.any_bucket), 0


def cmd_validate(conn, s, args) -> tuple:
    out = validate(conn, s, **_scope(args), run_params=_run_params(args), check_sql=args.check_sql,
                   extra_filters=_filters(args))
    return out, 0 if out["result"] == "VALID" else 1


def cmd_show_config(conn, s, args) -> tuple:
    sample = load_config(conn, s, _key(args))
    return {"config": sample.config, "params": sample.params, "gates": sample.gates,
            "exclusions": sample.exclusions, "priorities": sample.priorities, "levels": sample.levels,
            "caps": sample.caps, "unknown_rules": sample.stray, "problems": config_problems(sample)}, 0


def cmd_runs(conn, s, args) -> tuple:
    where, params = ["1=1"], []
    _filter_sample(args, where, params)
    for col, value in (("Run_Key", args.run_key), ("Run_Stat", args.status)):
        if value not in (None, ""):
            where.append(f"{col} = ?")
            params.append(value)
    rows = conn.query(f"SELECT TOP {int(args.limit)} Smpl_Run_ID, Project_Cd, Table_Nm, Run_Ty, Run_Key, Run_Stat, "
                      f"Curr_Ind, Config_Hash, Finalized_By, Override_Rsn_Txt, Smpl_Method_Cd, Random_Seed_Nbr, "
                      f"Target_Ty, Target_Val, Target_Cnt, In_Scope_Cnt, Excluded_Cnt, Candidate_Cnt, Selected_Cnt, Shortfall_Cnt, "
                      f"Start_Dtts, End_Dtts, Triggered_By, Error_Ty, Error_Txt "
                      f"FROM {s.meta_db}.GOVT_RGLTRY_SamplingRun WHERE {' AND '.join(where)} ORDER BY Start_Dtts DESC",
                      params)
    return {"runs": rows}, 0


def cmd_errors(conn, s, args) -> tuple:
    """Open errors: runs whose latest attempt of their (sample, run_key) failed."""
    where, params = ["1=1"], []
    _filter_sample(args, where, params)
    if args.run_key not in (None, ""):
        where.append("Run_Key = ?")
        params.append(args.run_key)
    rows = conn.query(f"SELECT Smpl_Run_ID, Project_Cd, Table_Nm, Run_Ty, Run_Key, Error_Ty, Error_Txt, Start_Dtts "
                      f"FROM (SELECT Smpl_Run_ID, Project_Cd, Table_Nm, Run_Ty, Run_Key, Run_Stat, Error_Ty, "
                      f"Error_Txt, Start_Dtts FROM {s.meta_db}.GOVT_RGLTRY_SamplingRun WHERE {' AND '.join(where)} "
                      f"QUALIFY ROW_NUMBER() OVER (PARTITION BY Project_Cd, Table_Nm, Run_Ty, Run_Key "
                      f"ORDER BY Start_Dtts DESC) = 1) latest WHERE Run_Stat = 'ERROR' ORDER BY Start_Dtts DESC",
                      params)
    return {"errors": rows[:int(args.limit)]}, 0


def cmd_mark_failed(conn, s, args) -> tuple:
    """Close a run left RUNNING by a crashed process (it wrote no selections: persistence is one transaction)."""
    n = conn.execute(f"UPDATE {s.meta_db}.GOVT_RGLTRY_SamplingRun SET Run_Stat = 'ERROR', End_Dtts = ?, "
                     f"Error_Ty = 'ABANDONED', Error_Txt = ?, LastUpdt_Dtts = ?, LastUpdt_ID = USER "
                     f"WHERE Smpl_Run_ID = ? AND Run_Stat = 'RUNNING'",
                     [s.now(), f"marked failed by {getpass.getuser()}: {args.reason}"[:4000], s.now(), args.run_id])
    return {"smpl_run_id": args.run_id, "updated": n}, 0 if n else 1


def cmd_purge(conn, s, args) -> tuple:
    out = purge_selections(conn, s, args.older_than_days, **_scope(args), apply=args.apply)
    return out, 1 if any("skipped" in c for c in out["configs"]) else 0


def cmd_test_connection(conn, s, args) -> tuple:
    rows = conn.query("SELECT CURRENT_TIMESTAMP AS db_time, USER AS db_user, DATABASE AS default_database")
    tables = conn.query("SELECT TableName FROM DBC.TablesV WHERE DatabaseName = ? AND UPPER(TableName) LIKE 'GOVT_RGLTRY_SAMPL%'",
                        [s.meta_db])
    return {"connected": True, "host": conn.host, **(rows[0] if rows else {}),
            "sampling_tables": sorted(str(t["tablename"]).strip() for t in tables)}, 0


def render_sql(path: Path, meta_db: str) -> str:
    text = path.read_text(encoding="utf-8")
    if "{{META_DB}}" not in text:
        raise ConfigError(f"{path} has no {{{{META_DB}}}} placeholder")
    return text.replace("{{META_DB}}", meta_db)


def split_statements(sql: str) -> list:
    """The statements of a script: split on ';' at line ends, -- comment lines dropped."""
    out, current = [], []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        current.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(current).strip().rstrip(";").strip()
            if stmt:
                out.append(stmt)
            current = []
    tail = "\n".join(current).strip()
    if tail:
        out.append(tail)
    return out


def cmd_deploy_schema(conn, s, args) -> tuple:
    sql_dir = Path(args.sql_dir) if args.sql_dir else SQL_DIR
    files = [*(["schema_drop.sql"] if args.drop_first else []), "schema.sql"]
    scripts = [(f, render_sql(sql_dir / f, s.meta_db)) for f in files]
    if args.dry_run:
        for f, text in scripts:
            print(f"-- ===== {f} ({s.meta_db}) =====\n{text}")
        return {"dry_run": True, "files": files, "meta_db": s.meta_db}, 0
    executed = 0
    for f, text in scripts:
        for stmt in split_statements(text):
            log.info("%s: %s", f, stmt.splitlines()[0][:120])
            try:
                conn.execute(stmt)
            except Exception as e:  # noqa: BLE001
                if f == "schema_drop.sql" and "3807" in str(e):  # table does not exist
                    log.info("  (not there, skipped)")
                    continue
                raise
            executed += 1
    return {"meta_db": s.meta_db, "files": files, "statements": executed}, 0


COMMANDS = {"run": cmd_run, "validate": cmd_validate, "show-config": cmd_show_config, "runs": cmd_runs,
            "errors": cmd_errors, "mark-failed": cmd_mark_failed, "test-connection": cmd_test_connection,
            "deploy-schema": cmd_deploy_schema, "finalize": cmd_finalize, "replace": cmd_replace,
            "purge": cmd_purge}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="run_sampling.py", description=f"sampling {__version__} - local runner")
    p.add_argument("--log-level", default=None, help="DEBUG / INFO / WARNING (default SAMPLING_LOG_LEVEL or INFO)")
    sub = p.add_subparsers(dest="command", required=True)

    def scope(sp, required: bool = False):
        """a project, optionally one table and / or one run type (required: exactly one sample)"""
        sp.add_argument("--project", required=required, help="Project_Cd")
        sp.add_argument("--table", required=required, help="Table_Nm")
        sp.add_argument("--run-type", required=required, help="Run_Ty (DLY, WKLY, MNT ...)")

    def params(sp):
        sp.add_argument("--run-param", action="append", metavar="KEY=VALUE", help="a {key} / $key value (repeat)")
        sp.add_argument("--run-params", metavar="JSON", help='all values at once, e.g. {"pull_date": "2026-10-30"}')
        sp.add_argument("--filter", action="append", metavar="SQL",
                        help="a condition on the rows Scope_Sql_Txt returns (repeat), "
                             "e.g. \"BTCH_ID = 'B42'\"")

    r = sub.add_parser("run", help="run the samples of a scope (a project, a table, a run type)")
    scope(r)
    params(r)
    r.add_argument("--run-key", help="which logical run (default: the as-of date)")
    r.add_argument("--as-of", help="business date YYYY-MM-DD (default: today in SAMPLING_TIMEZONE)")
    r.add_argument("--seed", type=int, help="RANDOM / SYSTEMATIC seed (default: a new one)")
    r.add_argument("--triggered-by", help="recorded on the run row (default local-<user>)")
    r.add_argument("--dry-run", action="store_true", help="count and select, write nothing")
    r.add_argument("--force-reason", help="replace a FINAL sample of the run key (recorded on the run row)")

    fz = sub.add_parser("finalize", help="make the current sample of a run key final")
    scope(fz, required=True)
    fz.add_argument("--run-key", required=True)
    fz.add_argument("--by", help="recorded as finalized_by (default: your user name)")

    rp = sub.add_parser("replace", help="replace one selected case by the next candidate of its bucket")
    scope(rp, required=True)
    rp.add_argument("--run-key", required=True)
    rp.add_argument("--case-key", required=True, help="as in GOVT_RGLTRY_SampleSelection.Case_Key")
    rp.add_argument("--reason", required=True)
    rp.add_argument("--any-bucket", action="store_true", help="take the next candidate of any bucket when its "
                                                              "own has none left")
    rp.add_argument("--by", help="recorded as status_by (default: your user name)")

    v = sub.add_parser("validate", help="check the samples of a scope")
    scope(v)
    params(v)
    v.add_argument("--check-sql", action="store_true", help="also run the parameter lookups and EXPLAIN every query")

    sc = sub.add_parser("show-config", help="one sample with the rule rows it uses and its problems")
    scope(sc, required=True)

    for name, helptext in (("runs", "recent runs (GOVT_RGLTRY_SamplingRun)"), ("errors", "open errors: latest attempt failed")):
        sp = sub.add_parser(name, help=helptext)
        scope(sp)
        sp.add_argument("--run-key")
        sp.add_argument("--limit", type=int, default=20)
        if name == "runs":
            sp.add_argument("--status", choices=("RUNNING", "COMPLETED", "FINAL", "ERROR"))

    pg = sub.add_parser("purge", help="delete candidate rows old runs no longer need (counts only without --apply)")
    scope(pg)
    pg.add_argument("--older-than-days", type=int, required=True, help="runs whose as-of date is older than this")
    pg.add_argument("--apply", action="store_true", help="delete (default: count what would go)")

    mf = sub.add_parser("mark-failed", help="close a run left RUNNING by a crashed process")
    mf.add_argument("--run-id", required=True)
    mf.add_argument("--reason", default="process ended without closing the run")

    sub.add_parser("test-connection", help="connect, show the session and the sampling tables")

    d = sub.add_parser("deploy-schema", help="create the tables in SAMPLING_META_DB from ../sql")
    d.add_argument("--dry-run", action="store_true", help="print the rendered SQL, run nothing")
    d.add_argument("--drop-first", action="store_true", help="drop the tables first (DESTROYS their rows)")
    d.add_argument("--sql-dir", help=f"folder holding schema.sql (default {SQL_DIR})")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    env_file = load_env_file()
    level = str(args.log_level or os.environ.get(PREFIX + "LOG_LEVEL") or "INFO").strip().upper()
    if not isinstance(logging.getLevelName(level), int):   # every name logging accepts (WARN too)
        print(json.dumps({"command": args.command, "result": "COULD NOT RUN",
                          "problem": f"ConfigError: log level must be DEBUG, INFO, WARNING, ERROR or CRITICAL, "
                                     f"got {level!r}"}, indent=2))
        return 2
    log_path = configure_logging(level)
    log.info("sampling %s | env file %s | log %s", __version__, env_file or "none", log_path)
    try:
        s = settings_from_env()
        if args.command == "deploy-schema" and args.dry_run:
            out, code = cmd_deploy_schema(None, s, args)
        else:
            with connect_teradata(s.meta_db, s.access_lock) as conn:
                out, code = COMMANDS[args.command](conn, s, args)
    except (ConfigError, ValueError, EnvironmentError, ImportError) as e:
        out, code = {"command": args.command, "result": "COULD NOT RUN", "problem": error_text(e)}, 2
    except Exception as e:  # noqa: BLE001
        log.exception("%s failed", args.command)
        out, code = {"command": args.command, "result": "COULD NOT RUN", "problem": error_text(e)}, 2
    print(json.dumps(out, indent=2, default=str))
    return code


if __name__ == "__main__":
    sys.exit(main())
