from datetime import date

from conftest import insert_bucket, insert_config, insert_rule, key, set_config
from sampling.engine import config_problems, load_config, run_sampling

AS_OF = date(2026, 8, 7)


def _table(db, rows):
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
    rows = [(f"a{i:03d}", "A", "C1" if i < 5 else f"A{i}", "N") for i in range(10)]
    rows += [(f"b{i:03d}", "B", "C1" if i < 5 else f"B{i}", "N") for i in range(10)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"A": 50, "B": 50})
    _cap(raw.db, 50, "CPT_VARIETY", CPT_CAP, 3, rule_ty="PER_VALUE")
    out = _run(conn, settings)
    assert out["status"] == "COMPLETED", out.get("error")
    assert _count(raw, SELECTED_U + "u.cpt = 'C1'") == 3
    assert {k: v["selected"] for k, v in out["by_stratum"].items()} == {"A": 5, "B": 5}
    assert out["caps"] == {"CPT_VARIETY": {"limit": 3, "over": 7, "replaced": 7}}
    assert [k for (k,) in _selected(raw)] == [f"case_id={c}" for c in
                                             ("a000", "a001", "a002", "a005", "a006", "b005", "b006", "b007", "b008",
                                              "b009")]


def test_a_cap_under_a_parent_only(raw, conn, settings):
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
    rows = [(f"d{i:03d}", "Denied", f"D{i}", "Y") for i in range(10)]
    rows += [(f"p{i:03d}", "Approved", f"P{i}", "Y" if i < 2 else "N") for i in range(10)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"Denied": 80, "Approved": 20})
    _cap(raw.db, 51, "OON", OON_CAP, 4)
    out = _run(conn, settings, "TOPUP")
    assert out["selected"] == 10 and _count(raw, SELECTED_U + "u.oon = 'Y'") == 4
    assert out["by_stratum"]["Denied"]["selected"] == 4 and len(out["notes"]) == 4
    set_config(raw.db, shortfall_cd="NONE")
    out = _run(conn, settings, "SHORT")
    assert (out["selected"], out["shortfall"]) == (6, 4)


def test_a_cap_as_a_percentage_of_the_target(raw, conn, settings):
    rows = [(f"a{i:03d}", "A", "C1", "N") for i in range(20)] + [(f"b{i:03d}", "A", f"B{i}", "N") for i in range(20)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"A": 100})
    _cap(raw.db, 50, "CPT", CPT_CAP, 20, rule_ty="PER_VALUE", max_ty="PCT")
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


def test_caps_with_the_same_sql_run_it_once(raw, conn, settings, monkeypatch, caplog):
    rows = [(f"d{i:03d}", "Denied", f"D{i}", "Y" if i < 10 else "N") for i in range(20)]
    rows += [(f"p{i:03d}", "Approved", f"P{i}", "Y" if i < 5 else "N") for i in range(10)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"Denied": 80, "Approved": 20})
    _cap(raw.db, 51, "OON_DENIALS", OON_CAP, 2, parent="Denied")
    _cap(raw.db, 52, "OON_APPROVED", OON_CAP, 1, parent="Approved")
    caplog.set_level("INFO", "sampling")
    calls = []
    stream = type(conn).stream
    monkeypatch.setattr(type(conn), "stream", lambda self, sql, n: calls.append(sql) or stream(self, sql, n))
    out = _run(conn, settings)
    assert out["status"] == "COMPLETED", out.get("error")
    assert sum("oon = 'Y'" in sql for sql in calls) == 1
    assert "has the same SQL as CAP rule OON_DENIALS" in caplog.text
    assert _count(raw, SELECTED_U + "u.oon = 'Y' AND u.disp = 'Denied'") == 2
    assert _count(raw, SELECTED_U + "u.oon = 'Y' AND u.disp = 'Approved'") == 1


def test_a_replacement_keeps_the_caps(raw, conn, settings):
    from sampling.engine import replace_case
    rows = [(f"a{i:03d}", "A", "C1" if i < 4 else f"A{i}", "N") for i in range(8)]
    _table(raw.db, rows)
    _config(raw.db, 4, {"A": 100})
    _cap(raw.db, 50, "CPT", CPT_CAP, 3, rule_ty="PER_VALUE")
    assert _run(conn, settings)["status"] == "COMPLETED"
    assert [k for (k,) in _selected(raw)] == ["case_id=a000", "case_id=a001", "case_id=a002", "case_id=a004"]
    out = replace_case(conn, settings, key(1), "K", "case_id=a004", "no record", "auditor")
    assert out["replacement"] == "case_id=a005"
    assert _count(raw, SELECTED_U + "u.cpt = 'C1'") == 3


def _reasons(raw, out):
    rows = raw.db.execute("SELECT Case_Key, Selected_Ind, Not_Selected_Rsn_Txt FROM GOVT_RGLTRY_SampleSelection "
                          "WHERE Smpl_Run_ID = ? ORDER BY 1", [out["smpl_run_id"]]).fetchall()
    assert all((sel == 1) == (why is None) for _, sel, why in rows), rows
    return {k[len("case_id="):]: why for k, sel, why in rows if not sel}


def test_every_case_left_out_gets_a_reason(raw, conn, settings):
    rows = [(f"a{i:03d}", "A", "C1" if i < 5 else f"A{i}", "N") for i in range(10)]
    rows += [(f"b{i:03d}", "B", "C1" if i < 5 else f"B{i}", "N") for i in range(10)]
    rows += [("z000", "Z", "Z", "N")]
    _table(raw.db, rows)
    _config(raw.db, 10, {"A": 50, "B": 50})
    _cap(raw.db, 50, "CPT_VARIETY", CPT_CAP, 3, rule_ty="PER_VALUE")
    out = _run(conn, settings)
    over = "Over cap CPT_VARIETY: C1 already has 3 in the sample"
    full = "Bucket A full: its 5 slot(s) went to higher-ranked cases"
    assert _reasons(raw, out) == {"a003": over, "a004": over, "a007": full, "a008": full, "a009": full,
                                  **{f"b00{i}": over for i in range(5)}, "z000": "Bucket Z is not sampled"}
    assert out["not_picked"] == {"cap CPT_VARIETY": 7, "bucket full": 3, "bucket not sampled": 1}


def test_reasons_after_a_top_up(raw, conn, settings):
    rows = [(f"d{i:03d}", "Denied", f"D{i}", "Y") for i in range(10)]
    rows += [(f"p{i:03d}", "Approved", f"P{i}", "Y" if i < 2 else "N") for i in range(10)]
    _table(raw.db, rows)
    _config(raw.db, 10, {"Denied": 80, "Approved": 20})
    _cap(raw.db, 51, "OON", OON_CAP, 4)
    why = _reasons(raw, _run(conn, settings, "TOPUP"))
    over = "Over cap OON: the sample already has its 4"
    late = ("Bucket Approved full: its 2 slot(s) went to higher-ranked cases; the top-up filled the sample before "
            "reaching it")
    assert why == {**{f"d00{i}": over for i in range(4, 10)}, "p000": over, "p001": over, "p008": late, "p009": late}


def test_a_dry_run_counts_the_reasons(raw, conn, settings):
    _table(raw.db, [(f"a{i:03d}", "A", f"A{i}", "N") for i in range(6)])
    _config(raw.db, 4, {"A": 100})
    out = _run(conn, settings, dry_run=True)
    assert out["not_picked"] == {"bucket full": 2}
