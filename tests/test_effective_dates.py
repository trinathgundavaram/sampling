from datetime import date

import pytest

from conftest import insert_rule, set_config
from sampling.engine import load_config, run_scope, run_sampling
from sampling.settings import ConfigError
from test_caps import CPT_CAP, _config, _table

AS_OF = date(2026, 8, 7)
JULY, AUGUST = date(2026, 7, 15), date(2026, 8, 15)


def _cpt_cap(db, limit, start, end):
    insert_rule(db, "CAP", 50, "CPT", seq_nbr=50, rule_ty="PER_VALUE", rule_sql_txt=CPT_CAP, parent_path_txt="*",
                max_ty="CNT", max_val=limit, raw_sql=True, eff_start_dt=start, eff_end_dt=end)


@pytest.fixture
def two_versions(raw):
    _table(raw.db, [(f"a{i:03d}", "A", "C1", "N") for i in range(10)])
    _config(raw.db, 10, {"A": 100}, shortfall_cd="NONE")
    _cpt_cap(raw.db, 2, date(1900, 1, 1), date(2026, 7, 31))
    _cpt_cap(raw.db, 5, date(2026, 8, 1), date(9999, 12, 31))
    return raw


def _run(conn, s, run_key, **kw):
    return run_sampling(conn, s, ("UMCM", "T1", "WKLY"), run_key=run_key, as_of=AS_OF, **kw)


def test_a_run_uses_the_rule_version_in_effect_on_its_rules_date(two_versions, conn, settings):
    july = _run(conn, settings, "J", rules_as_of=JULY)
    august = _run(conn, settings, "A", rules_as_of=AUGUST)
    assert july["caps"]["CPT"]["limit"] == 2 and july["selected"] == 2 and july["rules_as_of"] == "2026-07-15"
    assert august["caps"]["CPT"]["limit"] == 5 and august["selected"] == 5
    stored = two_versions.db.execute("SELECT Run_Key, Rules_As_Of_Dt FROM GOVT_RGLTRY_SamplingRun "
                                     "ORDER BY Run_Key").fetchall()
    assert stored == [("A", AUGUST), ("J", JULY)]


def test_without_a_rules_date_the_rules_of_today_apply(two_versions, conn, settings, monkeypatch):
    monkeypatch.setattr(type(settings), "today", lambda self: AUGUST)
    out = _run(conn, settings, "T")
    assert out["rules_as_of"] == "2026-08-15" and out["caps"]["CPT"]["limit"] == 5


def test_both_end_dates_are_inclusive(two_versions, conn, settings):
    assert load_config(conn, settings, ("UMCM", "T1", "WKLY"), date(2026, 7, 31)).caps[0]["max_val"] == 2
    assert load_config(conn, settings, ("UMCM", "T1", "WKLY"), date(2026, 8, 1)).caps[0]["max_val"] == 5


def test_overlapping_versions_of_a_rule_are_refused(two_versions, conn, settings):
    two_versions.db.execute("UPDATE GOVT_RGLTRY_SamplingRule SET Eff_End_Dt = DATE '2026-08-10' "
                            "WHERE Rule_ID = 50 AND Eff_Start_Dt = DATE '1900-01-01'")
    with pytest.raises(ConfigError, match=r"Rule_ID \[50\] has more than one active row in effect on 2026-08-05"):
        load_config(conn, settings, ("UMCM", "T1", "WKLY"), date(2026, 8, 5))
    assert load_config(conn, settings, ("UMCM", "T1", "WKLY"), date(2026, 8, 11)).caps[0]["max_val"] == 5


def test_a_config_not_in_effect_is_not_run(two_versions, conn, settings):
    set_config(two_versions.db, eff_end_dt=date(2026, 7, 31))
    with pytest.raises(ConfigError, match="no active GOVT_RGLTRY_SamplingConfig"):
        run_scope(conn, settings, project="UMCM", run_key="X", as_of=AS_OF, rules_as_of=AUGUST)
    assert run_scope(conn, settings, project="UMCM", run_key="X", as_of=AS_OF,
                     rules_as_of=JULY)["runs"][0]["status"] == "COMPLETED"


def test_end_dating_a_row_alone_keeps_the_config_hash(two_versions, conn, settings):
    before = load_config(conn, settings, ("UMCM", "T1", "WKLY"), AUGUST).config_hash
    two_versions.db.execute("UPDATE GOVT_RGLTRY_SamplingRule SET Eff_End_Dt = DATE '2027-12-31' "
                            "WHERE Rule_ID = 50 AND Eff_Start_Dt = DATE '2026-08-01'")
    assert load_config(conn, settings, ("UMCM", "T1", "WKLY"), AUGUST).config_hash == before
