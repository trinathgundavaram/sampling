"""Report and parameter fixes: notes of a water-filled bucket, bucket counts after a rounded-up share, timestamp
parameters, the cap groups of a case."""
from datetime import date, datetime

from conftest import insert_bucket, insert_config, insert_rule, key, set_config
from sampling.engine import Caps, _param_text, run_sampling

AS_OF = date(2026, 8, 7)


def _table(db, rows):
    db.execute("CREATE TABLE u (case_id VARCHAR, a VARCHAR, b VARCHAR)")
    db.executemany("INSERT INTO u VALUES (?, ?, ?)", rows)


def _config(db, target, **kw):
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=target,
                  order_by="case_id", **kw)


def _run(conn, settings):
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    return out


def test_a_bucket_shared_again_keeps_only_its_last_notes(raw, conn, settings):
    """A1 has 2 cases, so A2's share of the ALL budget grows from 5 to 8: only the note of 8 holds."""
    _table(raw.db, [(f"x{i:03d}", "A1", "B") for i in range(2)] + [(f"y{i:03d}", "A2", "B") for i in range(20)])
    _config(raw.db, 10)
    insert_rule(raw.db, "LEVEL", 1000, "a", seq_nbr=1, level_nbr=0, rule_sql_txt="a")
    insert_bucket(raw.db, 1001, 0, "A1", seq_nbr=1, quota_cd="ALL")
    insert_bucket(raw.db, 1002, 0, "A2", seq_nbr=2, quota_cd="ALL")
    insert_rule(raw.db, "LEVEL", 2000, "b", seq_nbr=2, level_nbr=1, rule_sql_txt="b")
    insert_bucket(raw.db, 2001, 1, "B", seq_nbr=1, quota_cd="CNT", quota_val=50)
    notes = _run(conn, settings)["notes"]
    assert [n.split(":")[0] for n in notes] == ["A1/B", "A2/B"] and "cut to 8" in notes[1]


def test_bucket_counts_follow_the_cut_to_the_target(raw, conn, settings):
    """CEIL shares of 10 between 3 values are 4 each (12): the 2 cut leave the counts at 10."""
    _table(raw.db, [(f"{v}{i:03d}", v, "B") for v in "PQR" for i in range(10)])
    _config(raw.db, 10)
    set_config(raw.db, rounding_cd="CEIL")
    insert_rule(raw.db, "LEVEL", 1000, "a", seq_nbr=1, level_nbr=0, rule_sql_txt="a")
    out = _run(conn, settings)
    by = {k: v["selected"] for k, v in out["by_stratum"].items()}
    assert out["selected"] == 10 and sum(by.values()) == 10
    picked = raw.db.execute("SELECT Stratum_Path_Txt, COUNT(*) FROM GOVT_RGLTRY_SampleSelection "
                            "WHERE Selected_Ind = 1 GROUP BY 1").fetchall()
    assert dict(picked) == by


def test_a_timestamp_parameter_reads_as_teradata_does(raw, conn, settings):
    assert _param_text(datetime(2026, 1, 1, 10, 0, 0)) == "2026-01-01 10:00:00"
    assert _param_text(date(2026, 1, 1)) == "2026-01-01" and _param_text(7) == 7
    _table(raw.db, [("x", "A", "B")])
    _config(raw.db, 1)
    insert_rule(raw.db, "PARAM", 201, "ts", seq_nbr=1, rule_sql_txt="SELECT TIMESTAMP '2026-01-01 10:00:00' AS v")
    assert _run(conn, settings)["params"]["ts"] == "2026-01-01 10:00:00"


def test_cap_groups_are_worked_out_once_per_case(monkeypatch):
    from sampling.engine import Sample
    cap = {"rule_id": 50, "rule_nm": "OON", "rule_class_cd": "CAP", "max_ty": "CNT", "max_val": 1,
           "parent_path_txt": "*"}
    caps = Caps(Sample(config={}, caps=[cap]), 10, "FLOOR")
    calls = []
    real = caps._find_groups
    monkeypatch.setattr(caps, "_find_groups", lambda row: calls.append(row["_key"]) or real(row))
    row = {"_key": "k=1", "_path": "Denied", "smp_r_50": 1}
    assert caps.blocking(row) is None
    caps.take(row)
    assert caps.blocking(row) == "OON" and calls == ["k=1"]
