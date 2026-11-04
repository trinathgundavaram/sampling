-- ============================================================================
-- Drops the 5 sampling tables in {{META_DB}} - DESTROYS every row in them.
-- Only for a fresh redeploy: python local/run_sampling.py deploy-schema --drop-first
-- The sample tables (GOVT_RGLTRY_SamplingConfig.Smpl_Schema_Nm / Smpl_Table_Nm) are not dropped.
-- ============================================================================
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SampleSelection;
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SamplingRunRule;
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SamplingRun;
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SamplingRule;
DROP TABLE {{META_DB}}.GOVT_RGLTRY_SamplingConfig;
