from datetime import date

from conftest import insert_bucket, insert_config, insert_rule, key
from sampling.engine import run_sampling

AS_OF = date(2026, 8, 7)


def _setup(db):
    db.execute("CREATE TABLE u (case_id VARCHAR, area VARCHAR, disp VARCHAR)")
    rows = [(f"a{i:02d}", "A", "D" if i % 2 else "P") for i in range(10)] + [(f"b{i:02d}", "B", "D") for i in range(10)]
    db.executemany("INSERT INTO u VALUES (?, ?, ?)", rows)
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=10, order_by="case_id")
    insert_rule(db, "LEVEL", 1000, "area", seq_nbr=1, level_nbr=0, rule_sql_txt="area", unlisted_cd="SKIP")
    insert_bucket(db, 1001, 0, "A", seq_nbr=1, quota_val=50)
    insert_bucket(db, 1002, 0, "B", seq_nbr=2, quota_val=30)
    insert_bucket(db, 1003, 0, "C", seq_nbr=3, quota_cd="REMAINDER")
    insert_rule(db, "LEVEL", 1100, "disp", seq_nbr=2, level_nbr=1, rule_sql_txt="disp", unlisted_cd="SKIP")
    insert_bucket(db, 1101, 1, "P", seq_nbr=1, quota_val=40)
    insert_bucket(db, 1102, 1, "D", seq_nbr=2, quota_cd="REMAINDER")


def test_a_configured_bucket_without_cases_is_written_with_zeros(raw, conn, settings):
    _setup(raw.db)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    rows = raw.db.execute("SELECT Stat_Nm, Leaf_Ind, Candidate_Cnt, Target_Cnt, Selected_Cnt "
                          "FROM GOVT_RGLTRY_SamplingRunStat WHERE Stat_Ty = 'BUCKET'").fetchall()
    stats = {r[0]: r[1:] for r in rows}
    assert stats["B/P"] == (1, 0, 0, 0)
    assert stats["C"] == (0, 0, 0, 0) and stats["C/P"] == (1, 0, 0, 0) and stats["C/D"] == (1, 0, 0, 0)
    assert stats["A/P"][0] == 1 and stats["A/P"][1] == 5 and stats["A"][0] == 0
    assert set(out["by_stratum"]) == {"A/P", "A/D", "B/D"}
