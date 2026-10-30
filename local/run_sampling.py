"""Local command line for the sampling engine: the same engine as the Airflow DAG, credentials from dev.env.

    python run_sampling.py run --process-name UMCM_WEEKLY_SAMPLE --run-key 2026-10-30
    python run_sampling.py run --config-id 1 --run-param pull_date=2026-10-30 --dry-run
    python run_sampling.py validate --project UMCM --check-sql
    python run_sampling.py show-config --config-id 1
    python run_sampling.py runs --config-id 1
    python run_sampling.py errors
    python run_sampling.py mark-failed --run-id <run_id>
    python run_sampling.py test-connection
    python run_sampling.py deploy-schema --dry-run [--drop-first] [--seed]

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
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from sampling import __version__  # noqa: E402
from sampling.db import connect_teradata, error_text  # noqa: E402
from sampling.engine import config_problems, load_config, run_scope, validate  # noqa: E402
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
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
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


def _scope(args) -> dict:
    return {k: v for k, v in (("config_id", args.config_id), ("project", args.project),
                              ("process_name", args.process_name)) if v not in (None, "")}


def cmd_run(conn, s, args) -> tuple:
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    out = run_scope(conn, s, **_scope(args), run_key=args.run_key, run_params=_run_params(args), seed=args.seed,
                    as_of=as_of, triggered_by=args.triggered_by or f"local-{getpass.getuser()}",
                    dry_run=args.dry_run)
    problems = out["failed"] or any(r.get("shortfall") for r in out["runs"])
    return out, 1 if problems else 0


def cmd_validate(conn, s, args) -> tuple:
    out = validate(conn, s, **_scope(args), run_params=_run_params(args), check_sql=args.check_sql)
    return out, 0 if out["result"] == "VALID" else 1


def cmd_show_config(conn, s, args) -> tuple:
    sample = load_config(conn, s, args.config_id)
    return {"config": sample.config, "params": sample.params, "exclusions": sample.exclusions,
            "priorities": sample.priorities, "levels": sample.levels, "output_columns": sample.output_cols,
            "problems": config_problems(sample)}, 0


def cmd_runs(conn, s, args) -> tuple:
    where, params = ["1=1"], []
    for col, value in (("sample_config_id", args.config_id), ("run_key", args.run_key), ("status", args.status)):
        if value not in (None, ""):
            where.append(f"{col} = ?")
            params.append(value)
    rows = conn.query(f"SELECT TOP {int(args.limit)} run_id, run_key, sample_config_id, sample_name, status, "
                      f"sampling_method, random_seed, target_volume, total_in_scope, total_excluded, "
                      f"total_candidates, total_selected, shortfall, started_at, ended_at, triggered_by, "
                      f"error_message FROM {s.meta_db}.gre_sampling_audit WHERE {' AND '.join(where)} "
                      f"ORDER BY started_at DESC", params)
    return {"runs": rows}, 0


def cmd_errors(conn, s, args) -> tuple:
    where, params = ["active_ind = 'Y'"], []
    for col, value in (("config_id", args.config_id), ("run_key", args.run_key)):
        if value not in (None, ""):
            where.append(f"{col} = ?")
            params.append(value)
    rows = conn.query(f"SELECT TOP {int(args.limit)} run_id, config_id, process_name, run_key, error_type, "
                      f"error_message, occurred_at FROM {s.meta_db}.gre_sampling_errors "
                      f"WHERE {' AND '.join(where)} ORDER BY occurred_at DESC", params)
    return {"errors": rows}, 0


def cmd_mark_failed(conn, s, args) -> tuple:
    """Close a run left RUNNING by a crashed process (it wrote no selections: persistence is one transaction)."""
    n = conn.execute(f"UPDATE {s.meta_db}.gre_sampling_audit SET status = 'ERROR', ended_at = ?, "
                     f"error_message = ? WHERE run_id = ? AND status = 'RUNNING'",
                     [s.now(), f"marked failed by {getpass.getuser()}: {args.reason}"[:2000], args.run_id])
    return {"run_id": args.run_id, "updated": n}, 0 if n else 1


def cmd_test_connection(conn, s, args) -> tuple:
    rows = conn.query("SELECT CURRENT_TIMESTAMP AS db_time, USER AS db_user, DATABASE AS default_database")
    tables = conn.query("SELECT TableName FROM DBC.TablesV WHERE DatabaseName = ? AND TableName LIKE 'gre_sampl%'",
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
    files = [*(["schema_drop.sql"] if args.drop_first else []), "schema.sql",
             *(["seed/umcm_sample_config.sql"] if args.seed else [])]
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
            "deploy-schema": cmd_deploy_schema}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="run_sampling.py", description=f"sampling {__version__} - local runner")
    p.add_argument("--log-level", default=None, help="DEBUG / INFO / WARNING (default SAMPLING_LOG_LEVEL or INFO)")
    sub = p.add_subparsers(dest="command", required=True)

    def scope(sp):
        sp.add_argument("--config-id", type=int)
        sp.add_argument("--project")
        sp.add_argument("--process-name")

    def params(sp):
        sp.add_argument("--run-param", action="append", metavar="KEY=VALUE", help="a {key} / $key value (repeat)")
        sp.add_argument("--run-params", metavar="JSON", help='all values at once, e.g. {"pull_date": "2026-10-30"}')

    r = sub.add_parser("run", help="run the sampling configs of a scope")
    scope(r)
    params(r)
    r.add_argument("--run-key", help="which logical run (default: the as-of date)")
    r.add_argument("--as-of", help="business date YYYY-MM-DD (default: today in SAMPLING_TIMEZONE)")
    r.add_argument("--seed", type=int, help="RANDOM / SYSTEMATIC seed (default: a new one)")
    r.add_argument("--triggered-by", help="recorded on the audit row (default local-<user>)")
    r.add_argument("--dry-run", action="store_true", help="count and select, write nothing")

    v = sub.add_parser("validate", help="check the configs of a scope")
    scope(v)
    params(v)
    v.add_argument("--check-sql", action="store_true", help="also run the parameter lookups and EXPLAIN every query")

    sc = sub.add_parser("show-config", help="one config with every rule row and its problems")
    sc.add_argument("--config-id", type=int, required=True)

    for name, helptext in (("runs", "recent runs (gre_sampling_audit)"), ("errors", "active errors")):
        sp = sub.add_parser(name, help=helptext)
        sp.add_argument("--config-id", type=int)
        sp.add_argument("--run-key")
        sp.add_argument("--limit", type=int, default=20)
        if name == "runs":
            sp.add_argument("--status", choices=("RUNNING", "COMPLETED", "ERROR"))

    mf = sub.add_parser("mark-failed", help="close a run left RUNNING by a crashed process")
    mf.add_argument("--run-id", required=True)
    mf.add_argument("--reason", default="process ended without closing the run")

    sub.add_parser("test-connection", help="connect, show the session and the sampling tables")

    d = sub.add_parser("deploy-schema", help="create the tables in SAMPLING_META_DB from ../sql")
    d.add_argument("--dry-run", action="store_true", help="print the rendered SQL, run nothing")
    d.add_argument("--drop-first", action="store_true", help="drop the tables first (DESTROYS their rows)")
    d.add_argument("--seed", action="store_true", help="also insert sql/seed/umcm_sample_config.sql")
    d.add_argument("--sql-dir", help=f"folder holding schema.sql (default {SQL_DIR})")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    env_file = load_env_file()
    log_path = configure_logging(args.log_level or os.environ.get(PREFIX + "LOG_LEVEL") or "INFO")
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
