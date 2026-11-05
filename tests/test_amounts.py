from datetime import date

from conftest import insert_bucket, insert_config, insert_rule, key, set_config
from sampling.engine import config_problems, load_config, run_sampling

AS_OF = date(2026, 8, 7)


def _cases_table(db, rows):
    db.execute("CREATE TABLE u (case_id VARCHAR, disp VARCHAR, code VARCHAR, flag VARCHAR)")
    db.executemany("INSERT INTO u VALUES (?, ?, ?, ?)", rows)


def _cases(prefix, n, disp="D", code="X", flag="N"):
    return [(f"{prefix}{i:03d}", disp, code, flag) for i in range(n)]


def _config(db, target=10, **kw):
    kw.setdefault("order_by", "case_id")
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=target, **kw)


def _run(conn, s, run_key, **kw):
    return run_sampling(conn, s, key(1), run_key=run_key, as_of=AS_OF, **kw)


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


def _level(db, buckets: dict):
    insert_rule(db, "LEVEL", 1000, "disp", seq_nbr=1, level_nbr=0, rule_sql_txt="disp", unlisted_cd="SKIP")
    for i, (name, cols) in enumerate(buckets.items()):
        insert_bucket(db, 1001 + i, 0, name, seq_nbr=i + 1, **cols)


def _selected(out):
    return {k: v["selected"] for k, v in out["by_stratum"].items()}


def test_a_percentage_target_is_of_the_candidates(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 20) + _cases("b", 20, flag="Y"))
    _config(raw.db, target_ty="PCT", target_val=50)
    insert_rule(raw.db, "EXCLUSION", 1, "flagged", rule_sql_txt="flag = 'Y'")
    out = _run(conn, settings, "P50")
    assert (out["in_scope"], out["candidates"], out["target_cnt"], out["selected"]) == (40, 20, 10, 10)
    assert _rows(raw, "SELECT Target_Ty, Target_Val, Target_Cnt FROM GOVT_RGLTRY_SamplingRun") == [("PCT", 50, 10)]


def test_a_percentage_is_rounded_and_kept_within_its_limits(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 40))
    _config(raw.db, target_ty="PCT", target_val=13)
    assert _run(conn, settings, "FLOOR")["target_cnt"] == 5
    set_config(raw.db, rounding_cd="CEIL")
    assert _run(conn, settings, "CEIL")["target_cnt"] == 6
    set_config(raw.db, rounding_cd="FLOOR", target_val=10, min_target_cnt=6)
    assert _run(conn, settings, "MIN")["target_cnt"] == 6
    set_config(raw.db, target_val=50, min_target_cnt=None, max_target_cnt=15)
    assert _run(conn, settings, "MAX")["target_cnt"] == 15
    set_config(raw.db, target_val=10, min_target_cnt=60, max_target_cnt=None)
    out = _run(conn, settings, "SHORT")
    assert (out["target_cnt"], out["selected"], out["shortfall"]) == (60, 40, 20)


def test_a_number_target_stays_a_number(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 40))
    _config(raw.db, 7)
    out = _run(conn, settings, "CNT")
    assert (out["target_cnt"], out["selected"]) == (7, 7)


def test_buckets_of_a_number_a_percentage_and_the_rest(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 10, "A") + _cases("b", 10, "B") + _cases("c", 10, "C"))
    _config(raw.db, 10)
    _level(raw.db, {"A": {"quota_cd": "CNT", "quota_val": 3}, "B": {"quota_cd": "PCT", "quota_val": 50},
                    "C": {"quota_cd": "REMAINDER"}})
    out = _run(conn, settings, "MIX")
    assert _selected(out) == {"A": 3, "B": 5, "C": 2} and not out["notes"]


def test_number_quotas_never_take_more_than_the_parents_target(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 10, "A") + _cases("b", 10, "B") + _cases("c", 10, "C"))
    _config(raw.db, 4)
    _level(raw.db, {"A": {"quota_cd": "CNT", "quota_val": 3}, "B": {"quota_cd": "CNT", "quota_val": 3},
                    "C": {"quota_cd": "REMAINDER"}})
    out = _run(conn, settings, "OVER")
    assert _selected(out) == {"A": 3, "B": 1, "C": 0}
    assert out["notes"] == ["B: quota CNT 3 (3) cut to 1: the quotas under the sample exceed its target of 4"]


def test_a_bucket_cap_as_a_number_or_a_percentage_of_its_candidates(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 10, "A") + _cases("b", 20, "B"))
    _config(raw.db, 10)
    _level(raw.db, {"A": {"quota_cd": "ALL", "max_ty": "PCT", "max_val": 50}, "B": {"quota_cd": "REMAINDER"}})
    assert _selected(_run(conn, settings, "PCT")) == {"A": 5, "B": 5}
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingRule SET Max_Ty = 'CNT', Max_Val = 2 WHERE Rule_Nm = 'A'")
    assert _selected(_run(conn, settings, "CNT")) == {"A": 2, "B": 8}


def test_a_diversity_cap_as_a_percentage_of_the_target(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 20, code="X") + _cases("b", 20, code="Y") + _cases("c", 3, code="Z"))
    _config(raw.db, 10, diversity_sql_txt="code", diversity_cap_ty="PCT", diversity_cap_val=20)
    out = _run(conn, settings, "PCT")
    assert (out["selected"], out["shortfall"]) == (6, 4)
    set_config(raw.db, diversity_cap_ty="CNT", diversity_cap_val=3)
    out = _run(conn, settings, "CNT")
    assert (out["selected"], out["shortfall"]) == (9, 1)


def test_the_diversity_value_comes_from_its_own_select(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 20, code="X") + _cases("b", 20, code="Y") + _cases("c", 3, code="Z"))
    raw.db.execute("CREATE TABLE fam (code VARCHAR, family VARCHAR)")
    raw.db.execute("INSERT INTO fam VALUES ('X', 'F1'), ('Y', 'F1'), ('Z', 'F2')")
    _config(raw.db, 6, diversity_sql_txt="SELECT u.case_id, f.family FROM main.u u JOIN main.fam f ON f.code = u.code")
    out = _run(conn, settings, "FAM")
    assert out["selected"] == 6 and out["rule_queries"]["Diversity_Sql_Txt"] == 43
    assert _rows(raw, "SELECT SUBSTR(Case_Key, 9, 1), COUNT(*) FROM GOVT_RGLTRY_SampleSelection WHERE Selected_Ind = 1 "
                      "GROUP BY 1 ORDER BY 1") == [("a", 3), ("c", 3)]


SAMPLE_COLS = ("etl_is_curr_ind VARCHAR, etl_is_del_ind VARCHAR, etl_load_dt_tm TIMESTAMP, "
               "etl_last_updt_dt_tm TIMESTAMP, smpl_run_id VARCHAR, smpl_run_key VARCHAR, smpl_cycle_dt DATE, "
               "smpl_order_nbr INTEGER, smpl_stratum_path VARCHAR, smpl_run_ty VARCHAR")


def test_the_scope_select_joins_and_picks_the_rows_the_target_table_gets(raw, conn, settings):
    db = raw.db
    db.execute("CREATE TABLE src (case_id VARCHAR, code VARCHAR, etl_is_curr_ind VARCHAR)")
    db.executemany("INSERT INTO src VALUES (?, ?, ?)", [(f"d{i:03d}", f"K{i}-{v}", ind) for i in range(10)
                                                         for v, ind in (("new", "Y"), ("old", "N"))])
    db.execute("CREATE TABLE bt (case_id VARCHAR, btch VARCHAR)")
    db.executemany("INSERT INTO bt VALUES (?, ?)", [(f"d{i:03d}", "B1" if i < 5 else "B2") for i in range(10)])
    db.execute(f"CREATE TABLE smpl_t (case_id VARCHAR, code VARCHAR, {SAMPLE_COLS})")
    insert_config(db, src_table_nm="src", exclusion_sql=None, target_cnt=4, order_by=None,
                  scope_sql_txt="SELECT s.* FROM main.src s JOIN main.bt b ON b.case_id = s.case_id "
                                "WHERE b.btch = '{batch}' AND s.etl_is_curr_ind = 'Y'",
                  tgt_schema_nm="main", tgt_table_nm="smpl_t")
    insert_rule(db, "PRIORITY", 8001, "ORDER", seq_nbr=10000, rule_ty="EXPRESSION", raw_sql=True,
                rule_sql_txt="SELECT DISTINCT case_id, case_id AS v FROM main.src")
    out = _run(conn, settings, "B1", run_params={"batch": "B1"})
    assert (out["status"], out["in_scope"], out["selected"]) == ("COMPLETED", 5, 4), out.get("error")
    assert _rows(raw, "SELECT case_id, code FROM smpl_t ORDER BY 1") == [(f"d{i:03d}", f"K{i}-new") for i in range(4)]


def test_without_a_scope_every_source_row_is_in_scope(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 12))
    _config(raw.db, 5)
    assert _run(conn, settings, "ALL")["in_scope"] == 12


def _problems(conn, settings) -> str:
    return " | ".join(config_problems(load_config(conn, settings, key(1))))


def test_amounts_and_selects_are_checked(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 5))
    _config(raw.db, 5)
    checks = [({"target_ty": "PCT", "target_val": 0}, "target_val must be above 0 and at most 100"),
              ({"target_ty": "PCT", "target_val": 150}, "target_val must be above 0 and at most 100"),
              ({"target_ty": "CNT", "target_val": 2.5}, "target_val must be a whole number >= 1"),
              ({"target_ty": "XX", "target_val": 5}, "target_ty must be one of CNT, PCT"),
              ({"target_ty": "CNT", "target_val": 5, "min_target_cnt": 3}, "min_target_cnt is only used with target_ty PCT"),
              ({"target_ty": "PCT", "target_val": 5, "min_target_cnt": 10, "max_target_cnt": 5},
               "min_target_cnt is above max_target_cnt"),
              ({"target_ty": "CNT", "min_target_cnt": None, "max_target_cnt": None, "diversity_cap_ty": "CNT",
                "diversity_cap_val": 2}, "diversity_cap_val needs diversity_sql_txt"),
              ({"diversity_cap_ty": "PCT", "diversity_cap_val": 0}, "diversity_cap_val must be above 0")]
    for cols, text in checks:
        set_config(raw.db, **cols)
        assert text in _problems(conn, settings), (cols, text)
    set_config(raw.db, diversity_cap_ty=None, diversity_cap_val=None)
    assert _problems(conn, settings) == ""
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingConfig SET Scope_Sql_Txt = 'flag = ''N''', Diversity_Sql_Txt = 'code'")
    problems = _problems(conn, settings)
    assert "scope_sql_txt: must be a SELECT" in problems and "diversity_sql_txt: must be a SELECT" in problems


def test_bucket_amounts_are_checked(raw, conn, settings):
    _cases_table(raw.db, _cases("a", 5))
    _config(raw.db, 5)
    _level(raw.db, {"A": {"quota_cd": "SHARE", "quota_val": 50}, "B": {"quota_cd": "CNT", "quota_val": -1},
                    "C": {"quota_cd": "PCT", "quota_val": 120}, "D": {"quota_cd": "ALL", "max_ty": "PCT", "max_val": 0},
                    "E": {"quota_cd": "REMAINDER", "quota_val": 3}})
    problems = _problems(conn, settings)
    for text in ("bucket 'A': quota_cd must be one of PCT, CNT, ALL, REMAINDER", "bucket 'B': CNT needs quota_val",
                 "bucket 'C': PCT needs quota_val between 0 and 100", "bucket 'D': max_val must be above 0",
                 "bucket 'E': quota_val is not used with quota_cd REMAINDER"):
        assert text in problems, (text, problems)
