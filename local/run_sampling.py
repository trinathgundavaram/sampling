from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import sys
from datetime import date, datetime
from typing import Optional
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ENGINE_DIR = _HERE.parent / "airflow"
if str(_ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(_ENGINE_DIR))

from sampling import __version__
from sampling.admin import purge_selections
from sampling.db import connect_teradata, error_text
from sampling.engine import (config_problems, finalize_run, load_config, replace_case, run_scope,
                             validate)
from sampling.settings import ConfigError, Settings

log = logging.getLogger("sampling.cli")
PREFIX = "SAMPLING_"


def read_env_file(path: Path) -> dict:
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.removeprefix("export ").split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        out[key.strip()] = value
    return out


_SECRETS = {}


def load_env_file() -> Path | None:
    name = os.environ.get(PREFIX + "ENV_FILE") or "dev.env"
    path = Path(name) if Path(name).is_absolute() else _HERE / name
    if not path.is_file():
        return None
    for key, value in read_env_file(path).items():
        if key == "TERADATA_PASSWORD":
            if value != "" and not os.environ.get(key):
                _SECRETS["password"] = value
        elif value != "" and not os.environ.get(key):
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
    log_dir = Path(os.environ.get(PREFIX + "LOG_DIR") or _HERE / "logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"sampling_{datetime.now():%Y%m%d_%H%M%S_%f}_{os.getpid()}.log"
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    handlers = [logging.StreamHandler(sys.stderr), logging.FileHandler(path, encoding="utf-8")]
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=level.upper(), handlers=handlers, force=True)
    return path


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
    items = [f.strip() for f in args.filter or [] if f.strip()]
    return " AND ".join(f"({f})" for f in items) if len(items) > 1 else (items[0] if items else None)


def _scope(args) -> dict:
    return {k: v for k, v in (("project", args.project), ("table", args.table), ("run_type", args.run_type))
            if v not in (None, "")}


def _key(args) -> tuple:
    return (args.project, args.table, args.run_type)


def _filter_sample(args, where: list, params: list) -> None:
    for col, value in (("Project_Cd", args.project), ("Table_Nm", args.table), ("Run_Ty", args.run_type)):
        if value not in (None, ""):
            where.append(f"UPPER({col}) = ?")
            params.append(str(value).strip().upper())


def _day(value, name: str) -> Optional[date]:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        raise ConfigError(f"{name} must be YYYY-MM-DD, got {value!r}") from None


def _rules_as_of(args) -> Optional[date]:
    given = getattr(args, "rules_as_of", None)
    return _day(given if given is not None else os.environ.get(PREFIX + "RULES_AS_OF"), "rules_as_of")


def cmd_run(conn, s, args) -> tuple:
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    run_key = args.run_key or os.environ.get(PREFIX + "RUN_KEY") or None
    out = run_scope(conn, s, **_scope(args), run_key=run_key, run_params=_run_params(args),
                    extra_filters=_filters(args), seed=args.seed,
                    as_of=as_of, triggered_by=args.triggered_by or f"local-{getpass.getuser()}",
                    dry_run=args.dry_run, force_reason=args.force_reason,
                    rerun_reason=args.rerun_reason if args.rerun_reason is not None
                    else os.environ.get(PREFIX + "RERUN_REASON"), rules_as_of=_rules_as_of(args))
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
                   extra_filters=_filters(args), rules_as_of=_rules_as_of(args))
    return out, 0 if out["result"] == "VALID" else 1


def cmd_show_config(conn, s, args) -> tuple:
    sample = load_config(conn, s, _key(args), _rules_as_of(args))
    return {"config": sample.config, "params": sample.params,
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


COMMANDS = {"run": cmd_run, "validate": cmd_validate, "show-config": cmd_show_config, "runs": cmd_runs,
            "errors": cmd_errors, "mark-failed": cmd_mark_failed, "test-connection": cmd_test_connection,
            "finalize": cmd_finalize, "replace": cmd_replace,
            "purge": cmd_purge}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="run_sampling.py", description=f"sampling {__version__} - local runner")
    p.add_argument("--log-level", default=None, help="DEBUG / INFO / WARNING (default SAMPLING_LOG_LEVEL or INFO)")
    sub = p.add_subparsers(dest="command", required=True)

    def scope(sp, required: bool = False):
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
    r.add_argument("--run-key", help="which logical run (default: SAMPLING_RUN_KEY, else the as-of date)")
    r.add_argument("--as-of", help="business date YYYY-MM-DD (default: today in SAMPLING_TIMEZONE)")
    r.add_argument("--seed", type=int, help="RANDOM / SYSTEMATIC seed (default: a new one)")
    r.add_argument("--triggered-by", help="recorded on the run row (default local-<user>)")
    r.add_argument("--dry-run", action="store_true", help="count and select, write nothing")
    r.add_argument("--force-reason", help="replace a FINAL sample of the run key (recorded on the run row)")
    r.add_argument("--rules-as-of", help="use the rules and config in effect on this date YYYY-MM-DD (default SAMPLING_RULES_AS_OF, else today)")
    r.add_argument("--rerun-reason", help="why the run key is run again (default SAMPLING_RERUN_REASON): required "
                                          "when the run key already has a sample, refused when it has none")

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
    v.add_argument("--rules-as-of", help="use the rules and config in effect on this date YYYY-MM-DD (default SAMPLING_RULES_AS_OF, else today)")

    sc = sub.add_parser("show-config", help="one sample with the rule rows it uses and its problems")
    scope(sc, required=True)
    sc.add_argument("--rules-as-of", help="use the rules and config in effect on this date YYYY-MM-DD (default SAMPLING_RULES_AS_OF, else today)")

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

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    env_file = load_env_file()
    level = str(args.log_level or os.environ.get(PREFIX + "LOG_LEVEL") or "INFO").strip().upper()
    if not isinstance(logging.getLevelName(level), int):
        print(json.dumps({"command": args.command, "result": "COULD NOT RUN",
                          "problem": f"ConfigError: log level must be DEBUG, INFO, WARNING, ERROR or CRITICAL, "
                                     f"got {level!r}"}, indent=2))
        return 2
    log_path = configure_logging(level)
    log.info("sampling %s | env file %s | log %s", __version__, env_file or "none", log_path)
    try:
        s = settings_from_env()
        with connect_teradata(s.meta_db, s.access_lock, credentials=dict(_SECRETS) or None) as conn:
            out, code = COMMANDS[args.command](conn, s, args)
    except (ConfigError, ValueError, EnvironmentError, ImportError) as e:
        out, code = {"command": args.command, "result": "COULD NOT RUN", "problem": error_text(e)}, 2
    except Exception as e:
        log.exception("%s failed", args.command)
        out, code = {"command": args.command, "result": "COULD NOT RUN", "problem": error_text(e)}, 2
    print(json.dumps(out, indent=2, default=str))
    return code


if __name__ == "__main__":
    sys.exit(main())
