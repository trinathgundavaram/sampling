-- ============================================================================
-- Example configuration rows (config_id 1) - every kind of rule once
--
-- A worked example on a placeholder universe ({{META_DB}}.um_universe and
-- its column names are illustrative, not the ODAG1 table). It shows how a
-- requirement becomes rows, with no field name or value in the code:
--
--   gre_sampling_param      {batch_id} = the latest batch loaded
--   gre_sampling_exclusion  three SQL rules + "selected in the last 90 days"
--   gre_sampling_priority   carry-over of last cycle's unselected cases first,
--                           then four SQL conditions
--   gre_sampling_strata     level 0 by disposition, level 1 by functional
--   gre_sampling_bucket     area under Denied only (applies_to/parent_path)
--                           Denied takes the REMAINDER the others leave
--   gre_sampling_config     diversity_expr rotates picks across code families
--   gre_sampling_output_col the snapshot columns of each selected case
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

-- Run parameter: the latest batch (a value passed with the run wins)
INSERT INTO {{META_DB}}.gre_sampling_param (param_id, config_id, act_ind, seq_no, param_name, param_sql, description)
VALUES (1, 1, 1, 1, 'batch_id', 'SELECT MAX(batch_id) FROM {{META_DB}}.um_universe', 'Latest batch loaded');

-- Exclusions: the first matching rule is the reason stored
INSERT INTO {{META_DB}}.gre_sampling_exclusion (exclusion_id, config_id, act_ind, seq_no, exclusion_name, rule_type, exclusion_sql)
VALUES (1, 1, 1, 1, 'AUTO_APPROVED', 'SQL', 'auto_approved = ''Y''');
INSERT INTO {{META_DB}}.gre_sampling_exclusion (exclusion_id, config_id, act_ind, seq_no, exclusion_name, rule_type, exclusion_sql)
VALUES (2, 1, 1, 2, 'NO_PA_REQUIRED', 'SQL', 'shrpa_no_pa_flag = ''Y''');
INSERT INTO {{META_DB}}.gre_sampling_exclusion (exclusion_id, config_id, act_ind, seq_no, exclusion_name, rule_type, exclusion_sql)
VALUES (3, 1, 1, 3, 'DIABETIC_SUPPLIES', 'SQL', 'issue_type = ''DIABETIC_SUPPLIES''');
INSERT INTO {{META_DB}}.gre_sampling_exclusion (exclusion_id, config_id, act_ind, seq_no, exclusion_name, rule_type, lookback_days, history_scope)
VALUES (4, 1, 1, 4, 'SELECTED_LAST_90_DAYS', 'PRIOR_SELECTION', 90, 'CONFIG');

-- Priority: criterion 1 first, then 2, ...
INSERT INTO {{META_DB}}.gre_sampling_priority (priority_id, config_id, act_ind, seq_no, criterion_name, rule_type, lookback_days, history_scope)
VALUES (1, 1, 1, 1, 'CARRY_OVER', 'PRIOR_CANDIDATE', 14, 'CONFIG');
INSERT INTO {{META_DB}}.gre_sampling_priority (priority_id, config_id, act_ind, seq_no, criterion_name, rule_type, criterion_sql)
VALUES (2, 1, 1, 2, 'REVISION', 'CONDITION', 'revision = 1');
INSERT INTO {{META_DB}}.gre_sampling_priority (priority_id, config_id, act_ind, seq_no, criterion_name, rule_type, criterion_sql)
VALUES (3, 1, 1, 3, 'INPATIENT', 'CONDITION', 'place_of_service = ''Inpatient''');
INSERT INTO {{META_DB}}.gre_sampling_priority (priority_id, config_id, act_ind, seq_no, criterion_name, rule_type, criterion_sql)
VALUES (4, 1, 1, 4, 'EXPEDITED', 'CONDITION', 'service_type IN (''EXPEDITED'', ''EXPEDITED_PART_B'')');
INSERT INTO {{META_DB}}.gre_sampling_priority (priority_id, config_id, act_ind, seq_no, criterion_name, rule_type, criterion_sql)
VALUES (5, 1, 1, 5, 'CLINICAL_REVIEW', 'CONDITION', 'clinical_review_required_flag = ''Y''');

-- Level 0: by disposition. Denied takes what Withdrawn / Dismissed / Approved leave
INSERT INTO {{META_DB}}.gre_sampling_strata (strata_id, config_id, level_order, level_name, stratify_expr, applies_to, unlisted_policy, act_ind)
VALUES (1, 1, 0, 'request_disposition', 'request_disposition', '*', 'SKIP', 1);
INSERT INTO {{META_DB}}.gre_sampling_bucket (bucket_id, strata_id, act_ind, parent_path, seq_no, bucket_name, quota_mode, target_fraction)
VALUES (1, 1, 1, '*', 1, 'Withdrawn', 'FRACTION', 0.10);
INSERT INTO {{META_DB}}.gre_sampling_bucket (bucket_id, strata_id, act_ind, parent_path, seq_no, bucket_name, quota_mode, target_fraction)
VALUES (2, 1, 1, '*', 2, 'Dismissed', 'FRACTION', 0.02);
INSERT INTO {{META_DB}}.gre_sampling_bucket (bucket_id, strata_id, act_ind, parent_path, seq_no, bucket_name, quota_mode, target_fraction)
VALUES (3, 1, 1, '*', 3, 'Approved', 'FRACTION', 0.08);
INSERT INTO {{META_DB}}.gre_sampling_bucket (bucket_id, strata_id, act_ind, parent_path, seq_no, bucket_name, quota_mode)
VALUES (4, 1, 1, '*', 4, 'Denied', 'REMAINDER');

-- Level 1: functional area, only within Denied (other areas share what is left)
INSERT INTO {{META_DB}}.gre_sampling_strata (strata_id, config_id, level_order, level_name, stratify_expr, applies_to, unlisted_policy, act_ind)
VALUES (2, 1, 1, 'functional_area', 'functional_area', 'Denied', 'SHARE', 1);
INSERT INTO {{META_DB}}.gre_sampling_bucket (bucket_id, strata_id, act_ind, parent_path, seq_no, bucket_name, quota_mode, target_fraction)
VALUES (5, 2, 1, 'Denied', 1, 'Part B', 'FRACTION', 0.13);
INSERT INTO {{META_DB}}.gre_sampling_bucket (bucket_id, strata_id, act_ind, parent_path, seq_no, bucket_name, quota_mode, target_fraction)
VALUES (6, 2, 1, 'Denied', 2, 'Behavioral Health', 'FRACTION', 0.08);

-- Output snapshot columns (the table: sql/output_table_template.sql)
INSERT INTO {{META_DB}}.gre_sampling_output_col (output_col_id, config_id, act_ind, seq_no, column_name, expr_sql)
VALUES (1, 1, 1, 1, 'request_disposition', 'request_disposition');
INSERT INTO {{META_DB}}.gre_sampling_output_col (output_col_id, config_id, act_ind, seq_no, column_name, expr_sql)
VALUES (2, 1, 1, 2, 'functional_area', 'functional_area');
INSERT INTO {{META_DB}}.gre_sampling_output_col (output_col_id, config_id, act_ind, seq_no, column_name, expr_sql)
VALUES (3, 1, 1, 3, 'procedure_code_family', 'procedure_code_family');
