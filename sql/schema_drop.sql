-- ============================================================================
-- Drops the 12 sampling tables in {{META_DB}} - DESTROYS every row in them.
-- Only for a fresh redeploy: python local/run_sampling.py deploy-schema --drop-first
-- Output snapshot tables (gre_sampling_config.output_table) are not dropped.
-- ============================================================================
DROP TABLE {{META_DB}}.gre_sampling_errors;
DROP TABLE {{META_DB}}.gre_sampling_audit;
DROP TABLE {{META_DB}}.gre_sampling_run_stats;
DROP TABLE {{META_DB}}.gre_sample_selection_attrs;
DROP TABLE {{META_DB}}.gre_sample_selections;
DROP TABLE {{META_DB}}.gre_sampling_output_col;
DROP TABLE {{META_DB}}.gre_sampling_bucket;
DROP TABLE {{META_DB}}.gre_sampling_strata;
DROP TABLE {{META_DB}}.gre_sampling_priority;
DROP TABLE {{META_DB}}.gre_sampling_exclusion;
DROP TABLE {{META_DB}}.gre_sampling_param;
DROP TABLE {{META_DB}}.gre_sampling_config;
