"""The local version: dev.env loading, the SQL scripts, and the run / validate commands."""
import json

import pytest

from conftest import ROOT, build_universe, load_cli, um_config

cli = load_cli()


def test_env_file_fills_but_never_overrides_the_shell(tmp_path, monkeypatch):
    env = tmp_path / "x.env"
    env.write_text("# comment\nexport SAMPLING_META_DB=FROM_FILE\nTERADATA_HOST='td.file'\nEMPTY=\n")
    monkeypatch.setenv("SAMPLING_ENV_FILE", str(env))
    monkeypatch.setenv("TERADATA_HOST", "td.shell")
    monkeypatch.delenv("SAMPLING_META_DB", raising=False)
    assert cli.load_env_file() == env
    import os
    assert os.environ["SAMPLING_META_DB"] == "FROM_FILE" and os.environ["TERADATA_HOST"] == "td.shell"
    assert "EMPTY" not in os.environ


def test_sql_scripts_render_and_split():
    schema = cli.render_sql(ROOT / "sql" / "schema.sql", "HSABC_DEV")
    statements = cli.split_statements(schema)
    tables = [s for s in statements if s.startswith("CREATE MULTISET TABLE")]
    assert len(tables) == 4 and all("HSABC_DEV." in s for s in statements) and "{{META_DB}}" not in schema
    drops = cli.split_statements(cli.render_sql(ROOT / "sql" / "schema_drop.sql", "X"))
    assert len(drops) == 4
    assert not (ROOT / "sql" / "seed").exists()     # no configuration in files: the rows are entered in the tables


def test_deploy_schema_dry_run_needs_no_connection(monkeypatch, capsys):
    monkeypatch.setenv("SAMPLING_META_DB", "HSABC_QA")
    monkeypatch.setenv("SAMPLING_ENV_FILE", "does-not-exist.env")
    monkeypatch.setenv("SAMPLING_LOG_DIR", str(ROOT / "local" / "logs"))
    assert cli.main(["deploy-schema", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "CREATE MULTISET TABLE HSABC_QA.GOVT_RGLTRY_SAMPLING_CONFIG" in out and "INSERT INTO" not in out


@pytest.fixture
def local_db(monkeypatch, raw, tmp_path):
    from sampling.db import Connection
    monkeypatch.setenv("SAMPLING_META_DB", "main")
    monkeypatch.setenv("SAMPLING_ACCESS_LOCK", "false")
    monkeypatch.setenv("SAMPLING_ENV_FILE", "does-not-exist.env")
    monkeypatch.setenv("SAMPLING_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "connect_teradata", lambda db, lock: Connection(raw, host="duckdb", access_lock=False))
    monkeypatch.setattr(raw, "close", lambda: None)   # each command closes its connection: keep the test DB
    build_universe(raw.db)
    um_config(raw.db)
    return raw


def test_run_command(local_db, capsys):
    code = cli.main(["run", "--process-name", "WEEKLY", "--run-param", "batch_id=2026-08-01", "--as-of", "2026-08-01",
                     "--triggered-by", "jdoe"])
    report = json.loads(capsys.readouterr().out)
    assert code == 0 and report["runs"][0]["selected"] == 150
    assert local_db.db.execute("SELECT triggered_by FROM GOVT_RGLTRY_SAMPLING_RUN").fetchall() == [("jdoe",)]


def test_run_command_exit_codes(local_db, capsys):
    assert cli.main(["run", "--config-id", "1"]) == 1            # {batch_id} has no value: the sample fails
    assert cli.main(["run", "--process-name", "NOPE"]) == 2       # nothing to run
    assert cli.main(["run", "--config-id", "1", "--run-params", "not json"]) == 2
    capsys.readouterr()


def test_validate_and_show_config(local_db, capsys):
    assert cli.main(["validate", "--project", "UMCM", "--check-sql", "--run-param", "batch_id=x"]) == 0
    capsys.readouterr()
    assert cli.main(["show-config", "--config-id", "1"]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert {b["rule_name"]: b["target_fraction"] for b in shown["levels"][0]["buckets"]} == \
        {"Denied": 0.8, "Withdrawn": 0.1, "Dismissed": 0.02, "Approved": 0.08}
    assert shown["problems"] == []


def test_runs_and_errors_commands(local_db, capsys):
    cli.main(["run", "--config-id", "1", "--run-key", "K1"])                       # fails: {batch_id} has no value
    cli.main(["run", "--config-id", "1", "--run-key", "K2", "--run-param", "batch_id=2026-08-01"])
    capsys.readouterr()
    assert cli.main(["runs", "--config-id", "1"]) == 0
    assert {r["run_key"]: r["status"] for r in json.loads(capsys.readouterr().out)["runs"]} == \
        {"K1": "ERROR", "K2": "COMPLETED"}
    assert cli.main(["errors"]) == 0
    assert [e["run_key"] for e in json.loads(capsys.readouterr().out)["errors"]] == ["K1"]
    cli.main(["run", "--config-id", "1", "--run-key", "K1", "--run-param", "batch_id=2026-08-01"])
    capsys.readouterr()
    cli.main(["errors"])
    assert json.loads(capsys.readouterr().out)["errors"] == []                    # a later attempt succeeded


def test_mark_failed(local_db, capsys):
    local_db.db.execute("INSERT INTO GOVT_RGLTRY_SAMPLING_RUN (run_id, status) VALUES ('stuck', 'RUNNING')")
    assert cli.main(["mark-failed", "--run-id", "stuck"]) == 0
    assert cli.main(["mark-failed", "--run-id", "stuck"]) == 1   # already closed
    capsys.readouterr()
    assert local_db.db.execute("SELECT status, error_type FROM GOVT_RGLTRY_SAMPLING_RUN").fetchall() == [("ERROR", "ABANDONED")]
