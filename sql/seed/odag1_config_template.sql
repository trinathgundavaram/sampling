-- ============================================================================
-- ODAG1 (COMO weekly sample) - configuration TEMPLATE, config_id 2, inactive
--
-- Universe: EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 (loaded by
-- the framework). Only what is known is filled in. Every TODO(Qn) is an open
-- question in airflow_resources/docs/OPEN_ITEMS.md - replace it, then set
-- act_ind = 1 on the rows and run
--   python local/run_sampling.py validate --config-id 2 --check-sql
--
-- Decided (OPEN_ITEMS.md): one weekly sample per framework WKLY batch of
-- ODAG1 (Q6), started by hand (Q11), cases identified by AUTHZN_ID +
-- SRC_DATA_KEY (Q5), short buckets topped up without failing the run (Q8),
-- no repeat within 90 days and the last 14 days' unselected cases first (Q12),
-- no code-variety rule yet (Q9), every row kept 10 years: no purge (Q14).
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
    'AUTHZN_ID, SRC_DATA_KEY',
    'ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N'' AND BTCH_ID = ''{batch_id}''',
    150, 'RANKED', 'FLOOR', NULL, NULL,
    'TOPUP', 1, 'TODO(Q10) database.table of the output snapshot', 'TEMPLATE'
);

-- batch_id: the latest ODAG1 WKLY batch whose every source row is closed (a run
-- given batch_id=... samples that batch instead). A framework batch is one
-- ComplianceRequestControl row per source, in the framework's HSABC_$ENV
-- (HSABC_DEV in DEV, HSABC in UAT / PROD). TODO(Q15) the Project_Cd of ODAG1
-- there (CMSUNIV in its example setup).
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (201, 2, 0, 'PARAM', 1, 'batch_id', 'SELECT MAX(Btch_ID) FROM (SELECT Btch_ID FROM HSABC_$ENV.ComplianceRequestControl WHERE Project_Cd = ''CMSUNIV'' AND Table_Nm = ''ODAG1'' AND Run_Ty = ''WKLY'' GROUP BY Btch_ID HAVING MIN(Batch_Close_Ind) = 1) closed_batches');

-- never sample a batch with a source still open (also when batch_id is given by hand)
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, description)
VALUES (205, 2, 0, 'GATE', 1, 'BATCH_CLOSED', 'SELECT 1 FROM HSABC_$ENV.ComplianceRequestControl WHERE Btch_ID = ''{batch_id}'' HAVING COUNT(*) > 0 AND MIN(Batch_Close_Ind) = 1', 'every source of the WKLY batch is closed');

-- Q12: no case twice within 90 days
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, lookback_days, history_scope)
VALUES (210, 2, 0, 'EXCLUSION', 0, 'SELECTED_IN_LAST_90_DAYS', 'PRIOR_SELECTION', 90, 'CONFIG');

-- TODO(Q4): exclusions (auto-approvals, SHRPA / no PA required, diabetic supplies). Each is a SELECT of the
-- cases' AUTHZN_ID, SRC_DATA_KEY (any join), as a GRE rule
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (211, 2, 0, 'EXCLUSION', 1, 'AUTO_APPROVED', 'QUERY', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N'' AND TODO(Q4)');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (212, 2, 0, 'EXCLUSION', 2, 'NO_PA_REQUIRED', 'QUERY', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N'' AND TODO(Q4)');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (213, 2, 0, 'EXCLUSION', 3, 'DIABETIC_SUPPLIES', 'QUERY', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N'' AND TODO(Q4)');

-- Q12: the unselected candidates of the last 14 days first
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, lookback_days, history_scope)
VALUES (220, 2, 0, 'PRIORITY', 0, 'CARRY_OVER_14_DAYS', 'PRIOR_CANDIDATE', 14, 'CONFIG');

-- TODO(Q3): priorities (Revision has no ODAG1 column yet), then the others in order
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (221, 2, 0, 'PRIORITY', 1, 'REVISION', 'QUERY', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N'' AND TODO(Q3)');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (222, 2, 0, 'PRIORITY', 2, 'EXPEDITED', 'QUERY', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N'' AND TODO(Q1) a condition on AUTHZN_REQ_STD_OR_EXPDT_IND');

-- LEVEL 0: by determination (a SELECT of each case's value). TODO(Q1) the AUTHZN_REQ_DETRMTN_TY values and their quotas
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, level_order, parent_path, unlisted_policy)
VALUES (231, 2, 0, 'LEVEL', 1, 'determination', 'SELECT AUTHZN_ID, SRC_DATA_KEY, AUTHZN_REQ_DETRMTN_TY FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N''', 0, '*', 'SKIP');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode)
VALUES (232, 2, 0, 'BUCKET', 1, 'TODO(Q1) denial value', 0, '*', 'REMAINDER');

-- LEVEL 1: functional area within denials. TODO(Q2) one BUCKET per area: rule_sql a SELECT of its cases
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, level_order, parent_path, unlisted_policy)
VALUES (241, 2, 0, 'LEVEL', 1, 'functional_area', NULL, 1, 'TODO(Q8) denial bucket name', 'SHARE');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, level_order, parent_path, quota_mode, target_fraction)
VALUES (242, 2, 0, 'BUCKET', 1, 'TODO(Q2) area name', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND ETL_IS_CURR_IND = ''Y'' AND ETL_IS_DEL_IND = ''N'' AND TODO(Q2) condition', 1, '*', 'FRACTION', NULL);

-- OUTPUT: snapshot columns. TODO(Q10) which ODAG1 columns the auditors need
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (251, 2, 0, 'OUTPUT', 1, 'AUTHZN_ID', 'AUTHZN_ID');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (252, 2, 0, 'OUTPUT', 2, 'MBR_ID', 'MBR_ID');
