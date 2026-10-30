-- ============================================================================
-- ODAG1 (COMO weekly sample) - configuration TEMPLATE, config_id 2, inactive
--
-- Universe: EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 (loaded by
-- the framework). Only what is known is filled in. Every TODO(Qn) is an open
-- question in airflow_resources/docs/OPEN_ITEMS.md - replace it, then set
-- act_ind = 1 on the config row and run
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
INSERT INTO {{META_DB}}.gre_sampling_param (param_id, config_id, act_ind, seq_no, param_name, param_sql, description)
VALUES (21, 2, 0, 1, 'batch_id', 'TODO(Q6) SELECT ... FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1', 'TODO(Q6)');

-- TODO(Q4): exclusions (auto-approvals, SHRPA / no PA required, diabetic supplies)
INSERT INTO {{META_DB}}.gre_sampling_exclusion (exclusion_id, config_id, act_ind, seq_no, exclusion_name, rule_type, exclusion_sql)
VALUES (21, 2, 0, 1, 'AUTO_APPROVED', 'SQL', 'TODO(Q4)');
INSERT INTO {{META_DB}}.gre_sampling_exclusion (exclusion_id, config_id, act_ind, seq_no, exclusion_name, rule_type, exclusion_sql)
VALUES (22, 2, 0, 2, 'NO_PA_REQUIRED', 'SQL', 'TODO(Q4)');
INSERT INTO {{META_DB}}.gre_sampling_exclusion (exclusion_id, config_id, act_ind, seq_no, exclusion_name, rule_type, exclusion_sql)
VALUES (23, 2, 0, 3, 'DIABETIC_SUPPLIES', 'SQL', 'TODO(Q4)');

-- TODO(Q3): priorities (Revision has no ODAG1 column yet), then the others in order
INSERT INTO {{META_DB}}.gre_sampling_priority (priority_id, config_id, act_ind, seq_no, criterion_name, rule_type, criterion_sql)
VALUES (21, 2, 0, 1, 'REVISION', 'CONDITION', 'TODO(Q3)');
INSERT INTO {{META_DB}}.gre_sampling_priority (priority_id, config_id, act_ind, seq_no, criterion_name, rule_type, criterion_sql)
VALUES (22, 2, 0, 2, 'EXPEDITED', 'CONDITION', 'TODO(Q1) a condition on AUTHZN_REQ_STD_OR_EXPDT_IND');

-- Level 0: by determination. TODO(Q1) the AUTHZN_REQ_DETRMTN_TY values and their quotas
INSERT INTO {{META_DB}}.gre_sampling_strata (strata_id, config_id, level_order, level_name, stratify_expr, applies_to, unlisted_policy, act_ind)
VALUES (21, 2, 0, 'determination', 'AUTHZN_REQ_DETRMTN_TY', '*', 'SKIP', 0);
INSERT INTO {{META_DB}}.gre_sampling_bucket (bucket_id, strata_id, act_ind, parent_path, seq_no, bucket_name, quota_mode)
VALUES (21, 21, 0, '*', 1, 'TODO(Q1) denial value', 'REMAINDER');

-- Level 1: functional area within denials. TODO(Q2) one bucket per area: bucket_sql on ISS_DESC / SVC_CTGRY_CD
INSERT INTO {{META_DB}}.gre_sampling_strata (strata_id, config_id, level_order, level_name, stratify_expr, applies_to, unlisted_policy, act_ind)
VALUES (22, 2, 1, 'functional_area', NULL, 'TODO(Q8) denial bucket name', 'SHARE', 0);
INSERT INTO {{META_DB}}.gre_sampling_bucket (bucket_id, strata_id, act_ind, parent_path, seq_no, bucket_name, bucket_sql, quota_mode, target_fraction)
VALUES (22, 22, 0, '*', 1, 'TODO(Q2) area name', 'TODO(Q2) condition', 'FRACTION', NULL);

-- Output snapshot columns: TODO(Q10) which ODAG1 columns the auditors need
INSERT INTO {{META_DB}}.gre_sampling_output_col (output_col_id, config_id, act_ind, seq_no, column_name, expr_sql)
VALUES (21, 2, 0, 1, 'AUTHZN_ID', 'AUTHZN_ID');
INSERT INTO {{META_DB}}.gre_sampling_output_col (output_col_id, config_id, act_ind, seq_no, column_name, expr_sql)
VALUES (22, 2, 0, 2, 'MBR_ID', 'MBR_ID');
