-- ============================================================================
-- Example configuration rows (config_id 1) - every kind of rule once
--
-- A worked example on a placeholder universe ({{META_DB}}.um_universe and
-- its column names are illustrative, not the ODAG1 table). It shows how a
-- requirement becomes rows, with no field name or value in the code:
--
--   PARAM      {batch_id} = the latest batch loaded
--   EXCLUSION  three SQL rules + "selected in the last 90 days"
--   PRIORITY   last cycle's unselected cases first, then four SQL conditions
--   LEVEL 0    by disposition: Denied takes the REMAINDER the others leave
--   LEVEL 1    by functional area, only within Denied (parent_path)
--   OUTPUT     the snapshot columns of each selected case
--   config     diversity_expr rotates picks across code families
--
-- The real ODAG1 configuration is sql/seed/odag1_config_template.sql.
-- {{META_DB}} is replaced by deploy-schema --seed, or by hand.
-- ============================================================================

INSERT INTO {{META_DB}}.gre_sampling_config (
    config_id, act_ind, project_name, process_name, sample_name, universe_table, key_columns,
    scope_sql, target_volume, sampling_method, rounding_mode, diversity_expr, shortfall_policy,
    keep_excluded, output_table, schedule_cron, created_by
) VALUES (
    1, 1, 'UMCM', 'UMCM_WEEKLY_SAMPLE', 'UMCM_WEEKLY_SAMPLE', '{{META_DB}}.um_universe',
    'enrollee_id, auth_or_claim_number',
    'batch_id = ''{batch_id}''',
    150, 'RANKED', 'FLOOR', 'procedure_code_family', 'TOPUP',
    1, '{{META_DB}}.um_sample_output', '0 8 * * FRI', 'SEED'
);

-- PARAM: the latest batch (a value passed with the run wins)
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (1, 1, 1, 'PARAM', 1, 'batch_id', 'SELECT MAX(batch_id) FROM {{META_DB}}.um_universe');

-- EXCLUSION: the first matching rule is the reason stored
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (11, 1, 1, 'EXCLUSION', 1, 'AUTO_APPROVED', 'SQL', 'auto_approved = ''Y''');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (12, 1, 1, 'EXCLUSION', 2, 'NO_PA_REQUIRED', 'SQL', 'shrpa_no_pa_flag = ''Y''');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (13, 1, 1, 'EXCLUSION', 3, 'DIABETIC_SUPPLIES', 'SQL', 'issue_type = ''DIABETIC_SUPPLIES''');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, lookback_days, history_scope)
VALUES (14, 1, 1, 'EXCLUSION', 4, 'SELECTED_LAST_90_DAYS', 'PRIOR_SELECTION', 90, 'CONFIG');

-- PRIORITY: criterion 1 first, then 2, ...
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, lookback_days, history_scope)
VALUES (21, 1, 1, 'PRIORITY', 1, 'CARRY_OVER', 'PRIOR_CANDIDATE', 14, 'CONFIG');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (22, 1, 1, 'PRIORITY', 2, 'REVISION', 'CONDITION', 'revision = 1');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (23, 1, 1, 'PRIORITY', 3, 'INPATIENT', 'CONDITION', 'place_of_service = ''Inpatient''');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (24, 1, 1, 'PRIORITY', 4, 'EXPEDITED', 'CONDITION', 'service_type IN (''EXPEDITED'', ''EXPEDITED_PART_B'')');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_type, rule_sql)
VALUES (25, 1, 1, 'PRIORITY', 5, 'CLINICAL_REVIEW', 'CONDITION', 'clinical_review_required_flag = ''Y''');

-- LEVEL 0: by disposition. Denied takes what Withdrawn / Dismissed / Approved leave
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, level_order, parent_path, unlisted_policy)
VALUES (31, 1, 1, 'LEVEL', 1, 'request_disposition', 'request_disposition', 0, '*', 'SKIP');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode, target_fraction)
VALUES (32, 1, 1, 'BUCKET', 1, 'Withdrawn', 0, '*', 'FRACTION', 0.10);
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode, target_fraction)
VALUES (33, 1, 1, 'BUCKET', 2, 'Dismissed', 0, '*', 'FRACTION', 0.02);
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode, target_fraction)
VALUES (34, 1, 1, 'BUCKET', 3, 'Approved', 0, '*', 'FRACTION', 0.08);
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode)
VALUES (35, 1, 1, 'BUCKET', 4, 'Denied', 0, '*', 'REMAINDER');

-- LEVEL 1: functional area, only within Denied (other areas share what is left)
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql, level_order, parent_path, unlisted_policy)
VALUES (41, 1, 1, 'LEVEL', 1, 'functional_area', 'functional_area', 1, 'Denied', 'SHARE');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode, target_fraction)
VALUES (42, 1, 1, 'BUCKET', 1, 'Part B', 1, 'Denied', 'FRACTION', 0.13);
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode, target_fraction)
VALUES (43, 1, 1, 'BUCKET', 2, 'Behavioral Health', 1, 'Denied', 'FRACTION', 0.08);

-- OUTPUT: snapshot columns (the table: sql/output_table_template.sql)
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (51, 1, 1, 'OUTPUT', 1, 'request_disposition', 'request_disposition');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (52, 1, 1, 'OUTPUT', 2, 'functional_area', 'functional_area');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, rule_sql)
VALUES (53, 1, 1, 'OUTPUT', 3, 'procedure_code_family', 'procedure_code_family');
