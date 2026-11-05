"""CAP rules: a limit across the whole sample (or the buckets under a parent), not one bucket at a time - at most 3
cases of one CPT code, at most 10 out-of-network denials."""
from datetime import date

from conftest import insert_bucket, insert_config, insert_rule, key, set_config
from sampling.engine import config_problems, load_config, run_sampling

AS_OF = date(2026, 8, 7)


def _table(db, rows):
    """rows: (case_id, disp, cpt, oon)."""
    db.execute("CREATE TABLE u (case_id VARCHAR, disp VARCHAR, cpt VARCHAR, oon VARCHAR)")
    db.executemany("INSERT INTO u VALUES (?, ?, ?, ?)", rows)


def _config(db, target, mix: dict, **kw):
    kw.setdefault("order_by", "case_id")
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=target, **kw)
    insert_rule(db, "LEVEL", 1000, "disp", seq_nbr=1, level_nbr=0, rule_sql_txt="disp", unlisted_cd="SKIP")
    for i, (name, pct) in enumerate(mix.items()):
        insert_bucket(db, 1001 + i, 0, name, seq_nbr=i + 1, quota_val=pct)


def _cap(db, rule_id, name, sql, max_val, rule_ty=None, parent="*", max_ty="CNT"):
    insert_rule(db, "CAP", rule_id, name, seq_nbr=rule_id, rule_ty=rule_ty, rule_sql_txt=sql, parent_path_txt=parent,
                max_ty=max_ty, max_val=max_val, raw_sql=True)


def _run(conn, s, run_key="K", **kw):
    return run_sampling(conn, s, key(1), run_key=run_key, as_of=AS_OF, **kw)


def _selected(raw, where="1=1"):
    return raw.db.execute("SELECT s.Case_Key FROM GOVT_RGLTRY_SampleSelection s WHERE s.Selected_Ind = 1 AND "
                          f"{where} ORDER BY 1").fetchall()


def _count(raw, sql):
    return raw.db.execute(sql).fetchone()[0]


CPT_CAP = "SELECT case_id, cpt FROM main.u"
OON_CAP = "SELECT case_id FROM main.u WHERE oon = 'Y'"
SELECTED_U = ("SELECT COUNT(*) FROM GOVT_RGLTRY_SampleSelection s JOIN u ON s.Case_Key = 'case_id=' || u.case_id "
              "WHERE s.Selected_Ind = 1 AND ")


def test_at_most_3_of_one_code_across_every_bucket(raw, conn, settings):
    """Both buckets would take 5 cases of C1; across the sample only 3 may be, each bucket keeps its quota."""
    rows = [(f"a{i:03d}", "A", "C1" if i < 5 else f"A{i}", "N") for i in range(10)]
    rows += [(f"b{i:03d}", "B", "C1" if i < 5 else f"B{i}", "N") for i in range(10)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"A": 50, "B": 50})
    _cap(raw.db, 50, "CPT_VARIETY", CPT_CAP, 3, rule_ty="PER_VALUE")
    out = _run(conn, settings)
    assert out["status"] == "COMPLETED", out.get("error")
    assert _count(raw, SELECTED_U + "u.cpt = 'C1'") == 3
    assert {k: v["selected"] for k, v in out["by_stratum"].items()} == {"A": 5, "B": 5}     # the mix holds
    assert out["caps"] == {"CPT_VARIETY": {"limit": 3, "over": 7, "replaced": 7}}
    # the highest in priority order stay: a000-a002, then the next of each bucket
    assert [k for (k,) in _selected(raw)] == [f"case_id={c}" for c in
                                             ("a000", "a001", "a002", "a005", "a006", "b005", "b006", "b007", "b008",
                                              "b009")]


def test_a_cap_under_a_parent_only(raw, conn, settings):
    """At most 2 out-of-network denials; approvals are not limited."""
    rows = [(f"d{i:03d}", "Denied", f"D{i}", "Y" if i < 10 else "N") for i in range(20)]
    rows += [(f"p{i:03d}", "Approved", f"P{i}", "Y") for i in range(10)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"Denied": 80, "Approved": 20})
    _cap(raw.db, 51, "OON_DENIALS", OON_CAP, 2, parent="Denied")
    out = _run(conn, settings)
    assert _count(raw, SELECTED_U + "u.oon = 'Y' AND u.disp = 'Denied'") == 2
    assert _count(raw, SELECTED_U + "u.oon = 'Y' AND u.disp = 'Approved'") == 2
    assert out["selected"] == 10 and out["caps"]["OON_DENIALS"] == {"limit": 2, "over": 6, "replaced": 6}


def test_no_candidate_left_tops_up_within_the_caps_or_stays_short(raw, conn, settings):
    rows = [(f"d{i:03d}", "Denied", f"D{i}", "Y") for i in range(10)]               # every denial is OON
    rows += [(f"p{i:03d}", "Approved", f"P{i}", "Y" if i < 2 else "N") for i in range(10)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"Denied": 80, "Approved": 20})
    _cap(raw.db, 51, "OON", OON_CAP, 4)                                             # the whole sample
    out = _run(conn, settings, "TOPUP")
    assert out["selected"] == 10 and _count(raw, SELECTED_U + "u.oon = 'Y'") == 4    # topped up with in-network cases
    # in priority order the denials d000-d003 keep the 4 places; d004-d007 have no in-network denial to give way to,
    # the 2 OON approvals give way to in-network ones
    assert out["by_stratum"]["Denied"]["selected"] == 4 and len(out["notes"]) == 4
    set_config(raw.db, shortfall_cd="NONE")
    out = _run(conn, settings, "SHORT")
    assert (out["selected"], out["shortfall"]) == (6, 4)


def test_a_cap_as_a_percentage_of_the_target(raw, conn, settings):
    rows = [(f"a{i:03d}", "A", "C1", "N") for i in range(20)] + [(f"b{i:03d}", "A", f"B{i}", "N") for i in range(20)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"A": 100})
    _cap(raw.db, 50, "CPT", CPT_CAP, 20, rule_ty="PER_VALUE", max_ty="PCT")       # 20 % of 10: 2 per code
    out = _run(conn, settings)
    assert out["caps"]["CPT"]["limit"] == 2 and _count(raw, SELECTED_U + "u.cpt = 'C1'") == 2 and out["selected"] == 10


def test_random_replacements_follow_the_seed(raw, conn, settings):
    rows = [(f"a{i:03d}", "A", "C1" if i % 2 else f"A{i}", "N") for i in range(30)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"A": 100}, smpl_method_cd="RANDOM")
    _cap(raw.db, 50, "CPT", CPT_CAP, 1, rule_ty="PER_VALUE")
    first = _run(conn, settings, "R1", seed=7)
    picked = _selected(raw, "s.Smpl_Run_ID = '" + first["smpl_run_id"] + "'")
    second = _run(conn, settings, "R2", seed=7)
    assert picked == _selected(raw, "s.Smpl_Run_ID = '" + second["smpl_run_id"] + "'")
    assert _count(raw, SELECTED_U + "u.cpt = 'C1' AND s.Smpl_Run_ID = '" + first["smpl_run_id"] + "'") == 1


def test_cap_rules_are_checked(raw, conn, settings):
    _table(raw.db, [("a000", "A", "C", "N")])
    _config(raw.db, 1, {"A": 100})
    _cap(raw.db, 50, "no_limit", CPT_CAP, None, rule_ty="PER_VALUE", max_ty=None)
    _cap(raw.db, 51, "bad_type", OON_CAP, 2, rule_ty="BOGUS")
    _cap(raw.db, 52, "no_sql", None, 2)
    problems = " | ".join(config_problems(load_config(conn, settings, key(1))))
    for text in ("CAP rule no_limit: max_val is empty", "CAP rule bad_type: rule_ty must be one of QUERY, PER_VALUE",
                 "CAP rule no_sql: rule_sql_txt is empty"):
        assert text in problems, (text, problems)
