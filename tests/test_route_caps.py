from datetime import date

from conftest import insert_bucket, insert_config, insert_rule, key
from sampling.engine import run_sampling

AS_OF = date(2026, 8, 7)
ROUTES = ("NOPA", "GENETIC", "IPO_IF_RECEIVED", "IPO", "FAST_CERT")
SELECTED = "SELECT u.route, COUNT(*) FROM GOVT_RGLTRY_SampleSelection s JOIN u ON s.Case_Key = 'case_id=' || u.case_id " \
           "WHERE s.Selected_Ind = 1 GROUP BY u.route"


def _setup(db, target):
    db.execute("CREATE TABLE u (case_id VARCHAR, route VARCHAR)")
    rows = [(f"a{n}{i:02d}", route) for n, route in enumerate(ROUTES) for i in range(8)]
    rows += [(f"z{i:03d}", "OTHER") for i in range(200)]
    db.executemany("INSERT INTO u VALUES (?, ?)", rows)
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=target, order_by="case_id")
    insert_rule(db, "LEVEL", 1, "all", seq_nbr=1, level_nbr=0, unlisted_cd="SKIP")
    insert_bucket(db, 2, 0, "All", seq_nbr=1, quota_cd="REMAINDER", rule_sql_txt="1 = 1")
    for n, route in enumerate(ROUTES):
        insert_rule(db, "CAP", 20 + n, f"{route}_MAX_5", seq_nbr=20 + n, rule_ty="QUERY", parent_path_txt="*",
                    max_ty="CNT", max_val=5, rule_sql_txt=f"SELECT case_id FROM main.u WHERE route = '{route}'",
                    raw_sql=True)


def test_each_route_has_its_own_cap_of_five(raw, conn, settings):
    _setup(raw.db, 60)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    assert out["selected"] == 60
    counts = dict(raw.db.execute(SELECTED).fetchall())
    assert {r: counts.get(r, 0) for r in ROUTES} == {r: 5 for r in ROUTES}
    assert counts["OTHER"] == 35
    for route in ROUTES:
        assert out["caps"][f"{route}_MAX_5"]["limit"] == 5
        assert out["caps"][f"{route}_MAX_5"]["over"] == 3
