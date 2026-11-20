from datetime import date

import pytest

from conftest import insert_bucket, insert_config, insert_rule, key
from sampling.engine import run_sampling

AS_OF = date(2026, 8, 7)


def _setup(db, target, dismissed=100, **minimum):
    db.execute("CREATE TABLE u (case_id VARCHAR, disp VARCHAR)")
    rows = [(f"x{i:04d}", "Dismissed") for i in range(dismissed)]
    rows += [(f"a{i:04d}", "Approved") for i in range(400)] + [(f"d{i:04d}", "Denied") for i in range(1500)]
    db.executemany("INSERT INTO u VALUES (?, ?)", rows)
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=target,
                  order_by="case_id", rounding_cd="ROUND")
    insert_rule(db, "LEVEL", 1000, "disp", seq_nbr=1, level_nbr=0, rule_sql_txt="disp", unlisted_cd="SKIP")
    insert_bucket(db, 1001, 0, "Dismissed", seq_nbr=1, **{"quota_val": 2, "min_quota_cnt": 1, **minimum})
    insert_bucket(db, 1002, 0, "Approved", seq_nbr=2, quota_val=13)
    insert_bucket(db, 1003, 0, "Denied", seq_nbr=3, quota_cd="REMAINDER")


def _counts(db):
    rows = db.execute("SELECT Stat_Nm, Selected_Cnt FROM GOVT_RGLTRY_SamplingRunStat WHERE Stat_Ty = 'BUCKET'")
    return dict(rows.fetchall())


@pytest.mark.parametrize("target, expected", [(20, (1, 3, 16)), (150, (3, 20, 127)), (1000, (20, 130, 850))])
def test_a_minimum_keeps_a_small_share_at_one_and_a_large_one_at_its_percentage(raw, conn, settings, target,
                                                                                 expected):
    _setup(raw.db, target)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    counts = _counts(raw.db)
    assert (counts["Dismissed"], counts["Approved"], counts["Denied"]) == expected


def test_without_a_minimum_a_small_share_rounds_to_zero(raw, conn, settings):
    _setup(raw.db, 20, min_quota_cnt=None)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    assert _counts(raw.db)["Dismissed"] == 0


def test_a_bucket_with_no_cases_gives_its_minimum_to_the_remainder(raw, conn, settings):
    _setup(raw.db, 20, dismissed=0)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    counts = _counts(raw.db)
    assert counts.get("Dismissed", 0) == 0 and counts["Approved"] == 3 and counts["Denied"] == 17


@pytest.mark.parametrize("cols, message", [
    ({"quota_cd": "CNT", "quota_val": 1, "min_quota_cnt": 1}, "min_quota_cnt is only used with quota_cd PCT"),
    ({"min_quota_cnt": -1}, "min_quota_cnt must be a whole number >= 0"),
])
def test_a_wrong_minimum_fails_the_config_check(raw, conn, settings, cols, message):
    _setup(raw.db, 20, **cols)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "ERROR" and message in out["error"]


def test_the_largest_bucket_beside_it_gives_up_the_case_the_minimum_adds(raw, conn, settings):
    _setup(raw.db, 20)
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingRule SET Seq_Nbr = 3, Quota_Val = 30 WHERE Rule_ID = 1002")
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingRule SET Seq_Nbr = 2, Quota_Cd = 'PCT', Quota_Val = 68 "
                   "WHERE Rule_ID = 1003")
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    counts = _counts(raw.db)
    assert (counts["Dismissed"], counts["Denied"], counts["Approved"]) == (1, 13, 6)
    assert any("Dismissed: raised to its minimum of 1" in n and "Denied" in n for n in out["notes"])
