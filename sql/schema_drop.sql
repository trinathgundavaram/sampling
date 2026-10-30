-- ============================================================================
-- Drops the 4 sampling tables in {{META_DB}} - DESTROYS every row in them.
-- Only for a fresh redeploy: python local/run_sampling.py deploy-schema --drop-first
-- Output snapshot tables (gre_sampling_config.output_table) are not dropped.
-- ============================================================================
DROP TABLE {{META_DB}}.gre_sample_selections;
DROP TABLE {{META_DB}}.gre_sampling_run;
DROP TABLE {{META_DB}}.gre_sampling_rule;
DROP TABLE {{META_DB}}.gre_sampling_config;
