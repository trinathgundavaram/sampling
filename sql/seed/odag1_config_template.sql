-- ============================================================================
-- ODAG1 (COMO weekly sample) - configuration TEMPLATE, config_id 2, inactive
--
-- Universe: EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 (loaded by
-- the framework). Only what is known is filled in. Every TODO(Qn) is an open
-- question in airflow_resources/docs/OPEN_ITEMS.md - replace it, then set
-- act_ind = 1 on the rows and run
--   python local/run_sampling.py validate --config-id 2 --check-sql
--
-- Not loaded by deploy-schema --seed. Load it by hand once the TODOs are gone.
-- ============================================================================

INSERT INTO {{META_DB}}.gre_sampling_config (
    config_id, act_ind, project_name, process_name, sample_name, universe_table, key_columns,
    scope_sql, target_volume, sampling_method, rounding_mode, diversity_expr, diversity_cap,
    shortfall_policy, keep_excluded, output_table, created_by
) VALUES (
    2, 0, 'UMCM', 'COMO_WEEKLY_SAMPLE', 'COMO_WEEKLY_SAMPLE',
    'EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1',
    'TODO(Q5) the columns identifying one case, e.g. AUTHZN_ID',
    'ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N'' AND TODO(Q6) batch or decision-date scope',
    150, 'RANKED', 'FLOOR', 'TODO(Q9) code-variety expression or NULL', NULL,
    'TODO(Q8) TOPUP or NONE', 1, 'TODO(Q10) database.table of the output snapshot', 'TEMPLATE'
);

-- TODO(Q6): a run parameter, e.g. the latest framework batch
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (201, 2, 0, 'PARAM', 1, 'batch_id', 'TODO(Q6) SELECT ... FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1');

-- TODO(Q4): exclusions (auto-approvals, SHRPA / no PA required, diabetic supplies)
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (211, 2, 0, 'EXCLUSION', 1, 'AUTO_APPROVED', 'SQL', 'TODO(Q4)');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (212, 2, 0, 'EXCLUSION', 2, 'NO_PA_REQUIRED', 'SQL', 'TODO(Q4)');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (213, 2, 0, 'EXCLUSION', 3, 'DIABETIC_SUPPLIES', 'SQL', 'TODO(Q4)');

-- TODO(Q3): priorities (Revision has no ODAG1 column yet), then the others in order
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (221, 2, 0, 'PRIORITY', 1, 'REVISION', 'CONDITION', 'TODO(Q3)');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (222, 2, 0, 'PRIORITY', 2, 'EXPEDITED', 'CONDITION', 'TODO(Q1) a condition on AUTHZN_REQ_STD_OR_EXPDT_IND');

-- LEVEL 0: by determination. TODO(Q1) the AUTHZN_REQ_DETRMTN_TY values and their quotas
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, level_order, parent_path, unlisted_policy)
VALUES (231, 2, 0, 'LEVEL', 1, 'determination', 'AUTHZN_REQ_DETRMTN_TY', 0, '*', 'SKIP');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode)
VALUES (232, 2, 0, 'BUCKET', 1, 'TODO(Q1) denial value', 0, '*', 'REMAINDER');

-- LEVEL 1: functional area within denials. TODO(Q2) one BUCKET per area: rule_sql on ISS_DESC / SVC_CTGRY_CD
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, level_order, parent_path, unlisted_policy)
VALUES (241, 2, 0, 'LEVEL', 1, 'functional_area', NULL, 1, 'TODO(Q8) denial bucket name', 'SHARE');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, level_order, parent_path, quota_mode, target_fraction)
VALUES (242, 2, 0, 'BUCKET', 1, 'TODO(Q2) area name', 'TODO(Q2) condition', 1, '*', 'FRACTION', NULL);

-- OUTPUT: snapshot columns. TODO(Q10) which ODAG1 columns the auditors need
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (251, 2, 0, 'OUTPUT', 1, 'AUTHZN_ID', 'AUTHZN_ID');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (252, 2, 0, 'OUTPUT', 2, 'MBR_ID', 'MBR_ID');
