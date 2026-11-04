-- ============================================================================
-- Drops the 4 sampling tables in {{META_DB}} - DESTROYS every row in them.
-- Only for a fresh redeploy: python local/run_sampling.py deploy-schema --drop-first
-- The sample tables (GOVT_RGLTRY_SAMPLING_CONFIG.output_db / output_table) are not dropped.
-- ============================================================================
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SAMPLE_SELECTIONS;
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SAMPLING_RUN;
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SAMPLING_RULE;
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SAMPLING_CONFIG;
