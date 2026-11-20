from datetime import date

from conftest import insert_bucket, insert_config, insert_rule, key
from sampling.engine import run_sampling

AS_OF = date(2026, 8, 7)
SELECTED = "SELECT COUNT(*) FROM GOVT_RGLTRY_SampleSelection s JOIN u ON s.Case_Key = 'case_id=' || u.case_id " \
           "WHERE s.Selected_Ind = 1 AND "


def _rows(prefix, area, n, rsn=None, oon=0):
    return [(f"{prefix}{i:02d}", area, "D" if i % 4 else "P", rsn, "NON PAR" if i < oon else "PAR")
            for i in range(n)]


def _setup(db, oon_max=10):
    db.execute("CREATE TABLE u (case_id VARCHAR, area VARCHAR, disp VARCHAR, rsn VARCHAR, prov VARCHAR)")
    rows = _rows("a", "A", 40, oon=12) + _rows("b", "B", 40, oon=12) + _rows("c", "A", 6, "Concurrent Review")
    rows += [("x00", "A", "W", "Concurrent Review", "PAR")]
    db.executemany("INSERT INTO u VALUES (?, ?, ?, ?, ?)", rows)
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=20, order_by="case_id")
    insert_rule(db, "LEVEL", 1, "area", seq_nbr=1, level_nbr=0, unlisted_cd="SKIP")
    insert_bucket(db, 2, 0, "Concurrent Review", seq_nbr=0, quota_cd="CNT", quota_val=3,
                  rule_sql_txt="rsn = 'Concurrent Review'")
    insert_bucket(db, 3, 0, "A", seq_nbr=1, quota_val=50, rule_sql_txt="area = 'A'")
    insert_bucket(db, 4, 0, "B", seq_nbr=2, quota_cd="REMAINDER", rule_sql_txt="area = 'B'")
    insert_rule(db, "LEVEL", 10, "disp", seq_nbr=2, level_nbr=1, unlisted_cd="SKIP")
    for n, area in enumerate(("A", "B")):
        insert_bucket(db, 11 + 2 * n, 1, "P", parent_path_txt=area, seq_nbr=1, quota_val=25,
                      rule_sql_txt="disp = 'P'")
        insert_bucket(db, 12 + 2 * n, 1, "D", parent_path_txt=area, seq_nbr=2, quota_cd="REMAINDER",
                      rule_sql_txt="disp = 'D'")
    insert_rule(db, "CAP", 20, "OON", seq_nbr=20, rule_ty="QUERY", parent_path_txt="*", max_ty="CNT",
                max_val=oon_max, rule_sql_txt="SELECT case_id FROM main.u WHERE prov = 'NON PAR'", raw_sql=True)


def test_three_concurrent_reviews_of_any_determination_come_first(raw, conn, settings):
    _setup(raw.db)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    assert out["selected"] == 20
    assert out["by_stratum"]["Concurrent Review"]["selected"] == 3
    assert raw.db.execute(SELECTED + "u.rsn = 'Concurrent Review'").fetchone()[0] == 3
    assert {k: v["selected"] for k, v in out["by_stratum"].items() if k != "Concurrent Review"} == {
        "A/P": 2, "A/D": 8, "B/P": 1, "B/D": 6}


def test_out_of_network_cases_are_capped_for_the_whole_sample(raw, conn, settings):
    _setup(raw.db, oon_max=2)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    assert out["selected"] == 20
    assert raw.db.execute(SELECTED + "u.prov = 'NON PAR'").fetchone()[0] == 2
    assert out["caps"]["OON"]["limit"] == 2


def test_concurrent_reviews_are_spread_across_areas(raw, conn, settings):
    _setup(raw.db)
    raw.db.executemany("INSERT INTO u VALUES (?, ?, ?, ?, ?)",
                       [("d00", "B", "D", "Concurrent Review", "PAR"), ("d01", "B", "P", "Concurrent Review", "PAR")])
    picked = "SELECT u.case_id FROM GOVT_RGLTRY_SampleSelection s JOIN u ON s.Case_Key = 'case_id=' || u.case_id " \
             "WHERE s.Selected_Ind = 1 AND u.rsn = 'Concurrent Review' ORDER BY u.case_id"
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert [r[0] for r in raw.db.execute(picked).fetchall()] == ["c00", "c01", "c02"]
    raw.db.execute("DELETE FROM GOVT_RGLTRY_SampleSelection")
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingConfig SET Diversity_Sql_Txt = "
                   "'SELECT case_id, area FROM main.u WHERE rsn = ''Concurrent Review'''")
    out = run_sampling(conn, settings, key(1), run_key="K2", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    assert [r[0] for r in raw.db.execute(picked).fetchall()] == ["c00", "c01", "d00"]
    assert {k: v["selected"] for k, v in out["by_stratum"].items() if k != "Concurrent Review"} == {
        "A/P": 2, "A/D": 8, "B/P": 1, "B/D": 6}


def test_a_bucket_whose_parent_matches_no_area_fails_the_config_check(raw, conn, settings):
    _setup(raw.db)
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingRule SET Parent_Path_Txt = 'AA' WHERE Rule_ID = 11")
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["error_type"] == "CONFIG_ERROR"
    assert "buckets P under parent_path_txt 'AA' never apply" in out["error"]
