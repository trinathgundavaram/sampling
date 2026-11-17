from datetime import date

from conftest import insert_bucket, insert_config, insert_rule, key
from sampling.engine import config_problems, load_config, replace_case, run_sampling

AS_OF = date(2026, 8, 7)
EXP = "SELECT case_id FROM main.u WHERE exp = 'E'"
SELECTED_E = ("SELECT COUNT(*) FROM GOVT_RGLTRY_SampleSelection s JOIN u ON s.Case_Key = 'case_id=' || u.case_id "
              "WHERE s.Selected_Ind = 1 AND u.exp = 'E' AND ")


def _rows(prefix, area, sub, n, expedited):
    return [(f"{prefix}{i:02d}", area, sub, "D" if i % 2 == 0 else "P", "E" if i < expedited else "S")
            for i in range(n)]


def _setup(db, level_nbr, max_val, rule_ty="PER_BUCKET"):
    db.execute("CREATE TABLE u (case_id VARCHAR, area VARCHAR, sub VARCHAR, disp VARCHAR, exp VARCHAR)")
    db.executemany("INSERT INTO u VALUES (?, ?, ?, ?, ?)",
                   _rows("a", "A", None, 20, 15) + _rows("b1_", "B", "B1", 10, 7) + _rows("b2_", "B", "B2", 10, 7))
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=20, order_by="case_id")
    insert_rule(db, "LEVEL", 1000, "area", seq_nbr=1, level_nbr=0, rule_sql_txt="area", unlisted_cd="SKIP")
    insert_bucket(db, 1001, 0, "A", seq_nbr=1, quota_val=60)
    insert_bucket(db, 1002, 0, "B", seq_nbr=2, quota_val=40)
    insert_rule(db, "LEVEL", 1100, "sub", seq_nbr=2, level_nbr=1, rule_sql_txt="sub", parent_path_txt="B",
                unlisted_cd="SKIP")
    insert_bucket(db, 1101, 1, "B1", parent_path_txt="B", seq_nbr=1, quota_val=50)
    insert_bucket(db, 1102, 1, "B2", parent_path_txt="B", seq_nbr=2, quota_val=50)
    insert_rule(db, "LEVEL", 1200, "disp", seq_nbr=3, level_nbr=2, rule_sql_txt="disp", unlisted_cd="SHARE")
    insert_rule(db, "CAP", 50, "EXP", seq_nbr=50, rule_ty=rule_ty, rule_sql_txt=EXP, parent_path_txt="*",
                max_ty="PCT", max_val=max_val, level_nbr=level_nbr, raw_sql=True)


def _run(conn, settings):
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    return out


def _expedited(raw, where):
    return raw.db.execute(SELECTED_E + where).fetchone()[0]


def test_each_bucket_keeps_its_own_share_rounded_down(raw, conn, settings):
    _setup(raw.db, 1, 70)
    out = _run(conn, settings)
    assert out["selected"] == 20
    assert {k: v["selected"] for k, v in out["by_stratum"].items()} == {
        "A/D": 6, "A/P": 6, "B/B1/D": 2, "B/B1/P": 2, "B/B2/D": 2, "B/B2/P": 2}
    assert out["caps"] == {"EXP: A": {"limit": 8, "over": 4, "replaced": 4},
                           "EXP: B/B1": {"limit": 2, "over": 2, "replaced": 2},
                           "EXP: B/B2": {"limit": 2, "over": 2, "replaced": 2}}
    assert _expedited(raw, "u.area = 'A'") == 8
    assert _expedited(raw, "u.sub = 'B1'") == 2 and _expedited(raw, "u.sub = 'B2'") == 2


def test_a_lower_level_nbr_makes_the_bucket_wider(raw, conn, settings):
    _setup(raw.db, 0, 75)
    out = _run(conn, settings)
    assert set(out["caps"]) == {"EXP: A", "EXP: B"}
    assert out["caps"]["EXP: A"]["limit"] == 9 and out["caps"]["EXP: B"]["limit"] == 6
    assert _expedited(raw, "u.area = 'A'") == 9 and _expedited(raw, "u.area = 'B'") == 6


def test_the_bucket_caps_are_saved_as_run_stats(raw, conn, settings):
    _setup(raw.db, 1, 70)
    out = _run(conn, settings)
    rows = raw.db.execute("SELECT Stat_Nm, Rule_ID, Limit_Cnt, Over_Cnt, Replaced_Cnt FROM GOVT_RGLTRY_SamplingRunStat "
                          "WHERE Smpl_Run_ID = ? AND Stat_Ty = 'CAP' ORDER BY 1", [out["smpl_run_id"]]).fetchall()
    assert rows == [("EXP: A", 50, 8, 4, 4), ("EXP: B/B1", 50, 2, 2, 2), ("EXP: B/B2", 50, 2, 2, 2)]


def test_a_case_over_its_bucket_cap_says_which_bucket(raw, conn, settings):
    _setup(raw.db, 1, 70)
    out = _run(conn, settings)
    why = raw.db.execute("SELECT Not_Selected_Rsn_Txt FROM GOVT_RGLTRY_SampleSelection WHERE Smpl_Run_ID = ? "
                         "AND Case_Key = 'case_id=b1_02'", [out["smpl_run_id"]]).fetchone()[0]
    assert why == "Over cap EXP: bucket B/B1 already has its 2"


def test_a_replacement_keeps_the_bucket_cap(raw, conn, settings):
    _setup(raw.db, 1, 70)
    _run(conn, settings)
    out = replace_case(conn, settings, key(1), "K", "case_id=b1_07", "no record", "auditor")
    assert out["replacement"] == "case_id=b1_09"
    assert _expedited(raw, "u.sub = 'B1'") == 2


def test_per_bucket_caps_are_checked(raw, conn, settings):
    _setup(raw.db, None, 75)
    insert_rule(raw.db, "CAP", 51, "unknown_level", seq_nbr=51, rule_ty="PER_BUCKET", rule_sql_txt=EXP,
                max_ty="PCT", max_val=75, level_nbr=7, raw_sql=True)
    insert_rule(raw.db, "CAP", 52, "level_on_query", seq_nbr=52, rule_ty="QUERY", rule_sql_txt=EXP,
                max_ty="CNT", max_val=5, level_nbr=1, raw_sql=True)
    problems = " | ".join(config_problems(load_config(conn, settings, key(1))))
    for name in ("EXP", "unknown_level"):
        assert f"CAP rule {name}: PER_BUCKET needs level_nbr" in problems, problems
    assert "CAP rule level_on_query: level_nbr is only used by rule_ty PER_BUCKET" in problems
