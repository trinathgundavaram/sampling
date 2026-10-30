-- ============================================================================
-- Stratified sampling - Teradata DDL (12 tables)
--
-- {{META_DB}} is a placeholder: replace it with the metadata database
-- (the Variable's meta_db / SAMPLING_META_DB) before running, or let
--   python local/run_sampling.py deploy-schema
-- do it. The engine never creates or alters tables.
--
-- Like the GRE rules engine, everything a sample selects is configuration
-- rows holding SQL - no field name or value is known to the code:
--
--   gre_sampling_config     one row per sample: universe, scope, size, method, output
--   gre_sampling_param      run parameters looked up by SQL (e.g. the latest batch id)
--   gre_sampling_exclusion  exclusion rules (SQL, or "already selected before")
--   gre_sampling_priority   priority criteria, in order (SQL, or history based)
--   gre_sampling_strata     stratification levels, in order
--   gre_sampling_bucket     the buckets of a level: SQL condition or value, quota
--   gre_sampling_output_col columns of the output snapshot table
--
-- Written by the engine (read only):
--   gre_sample_selections, gre_sample_selection_attrs, gre_sampling_run_stats,
--   gre_sampling_audit, gre_sampling_errors
--
-- act_ind = 1 means in use, 0 means ignored: never delete a configuration row.
-- Rows the engine writes are never deleted: a rerun of the same
-- (config_id, run_key) marks the earlier run's rows etl_is_curr_ind = 'N'.
-- No statement or comment in this file may contain a semicolon except at the
-- end of a statement (deploy-schema splits on them).
-- ============================================================================


-- 1. gre_sampling_config - one row per sample --------------------------------
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_config (
    config_id          INTEGER NOT NULL,
    act_ind            BYTEINT DEFAULT 1,
    project_name       VARCHAR(100) NOT NULL,          -- scope key (the Variable's project)
    process_name       VARCHAR(100) NOT NULL,          -- scope key (the Variable's process_name)
    sample_name        VARCHAR(100) NOT NULL,          -- shown in run ids and reports
    universe_table     VARCHAR(200) NOT NULL,          -- database.table or view to sample from, $env allowed
    key_columns        VARCHAR(500) NOT NULL,          -- comma separated columns identifying one case
    scope_sql          CLOB,                           -- WHERE condition: this run's rows (default 1=1),
                                                       -- {name} / $name parameters and $env allowed
    exclusion_sql      CLOB,                           -- optional quick exclusion (not itemised per rule:
                                                       -- prefer gre_sampling_exclusion rows)
    target_volume      INTEGER NOT NULL DEFAULT 150,   -- sample size
    sampling_method    VARCHAR(20) DEFAULT 'RANKED',   -- RANKED | RANDOM | SYSTEMATIC
    priority_rank_sql  CLOB,                           -- optional ORDER BY list applied after the
                                                       -- gre_sampling_priority criteria
    rounding_mode      VARCHAR(10) DEFAULT 'FLOOR',    -- FLOOR | ROUND | CEIL for each bucket's share
    diversity_expr     VARCHAR(1000),                  -- optional SQL expression: picks rotate across its
                                                       -- values (e.g. a code family) within each bucket
    diversity_cap      INTEGER,                        -- optional: at most this many picks per value
    shortfall_policy   VARCHAR(10) DEFAULT 'TOPUP',    -- TOPUP: fill a short sample from the rest of the
                                                       -- candidates by priority | NONE: leave it short
    keep_excluded      BYTEINT DEFAULT 1,              -- 1 = store excluded rows with their reason too
    output_table       VARCHAR(200),                   -- optional database.table of the output snapshot
                                                       -- (gre_sampling_output_col), $env allowed
    schedule_cron      VARCHAR(50),                    -- documentation only: the schedule is in the Variable
    created_by         VARCHAR(100),
    last_updated_by    VARCHAR(100),
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)
)
UNIQUE PRIMARY INDEX (config_id);

CREATE INDEX gre_sampling_config_scope_ix (project_name, process_name) ON {{META_DB}}.gre_sampling_config;


-- 2. gre_sampling_param - run parameters looked up by SQL --------------------
-- Run before the pull: the first column of the first row becomes {param_name}.
-- A value passed with the run (run_params) wins. param_sql may use {as_of_date}
-- and the run parameters given.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_param (
    param_id           INTEGER NOT NULL,
    config_id          INTEGER NOT NULL,
    act_ind            BYTEINT DEFAULT 1,
    seq_no             INTEGER DEFAULT 100,            -- order: a later lookup may use an earlier one
    param_name         VARCHAR(100) NOT NULL,
    param_sql          CLOB NOT NULL,                  -- a SELECT returning one value
    description        VARCHAR(1000)
)
UNIQUE PRIMARY INDEX (param_id);


-- 3. gre_sampling_exclusion - exclusion rules, in order -----------------------
-- A row matching any active rule is not a candidate. The first matching rule
-- (by seq_no) is its exclusion_reason and every rule's count is stored.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_exclusion (
    exclusion_id       INTEGER NOT NULL,
    config_id          INTEGER NOT NULL,
    act_ind            BYTEINT DEFAULT 1,
    seq_no             INTEGER DEFAULT 100,
    exclusion_name     VARCHAR(200) NOT NULL,
    rule_type          VARCHAR(30) DEFAULT 'SQL',      -- SQL: exclusion_sql is true |
                                                       -- PRIOR_SELECTION: selected by an earlier run
    exclusion_sql      CLOB,                           -- SQL rule: a condition on the universe row
    lookback_days      INTEGER,                        -- PRIOR_SELECTION: how far back (sample_cycle)
    history_scope      VARCHAR(10) DEFAULT 'CONFIG',   -- PRIOR_SELECTION: CONFIG (this sample) | PROJECT
    description        VARCHAR(1000)
)
UNIQUE PRIMARY INDEX (exclusion_id);

CREATE INDEX gre_sampling_exclusion_config_ix (config_id) ON {{META_DB}}.gre_sampling_exclusion;


-- 4. gre_sampling_priority - priority criteria, in order ---------------------
-- Candidates are ordered by criterion 1, then 2, ... then priority_rank_sql,
-- then key_columns. RANKED takes the first ones, SYSTEMATIC steps through
-- the order, RANDOM ignores it.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_priority (
    priority_id        INTEGER NOT NULL,
    config_id          INTEGER NOT NULL,
    act_ind            BYTEINT DEFAULT 1,
    seq_no             INTEGER DEFAULT 100,
    criterion_name     VARCHAR(200) NOT NULL,
    rule_type          VARCHAR(30) DEFAULT 'CONDITION',-- CONDITION: rows where criterion_sql is true first |
                                                       -- EXPRESSION: by the value of criterion_sql (sort_dir) |
                                                       -- PRIOR_CANDIDATE: considered earlier, never selected,
                                                       --   first (carry-over) |
                                                       -- PRIOR_SELECTION: selected earlier, last
    criterion_sql      CLOB,                           -- CONDITION / EXPRESSION
    sort_dir           VARCHAR(4) DEFAULT 'ASC',       -- EXPRESSION: ASC | DESC (NULLs always last)
    lookback_days      INTEGER,                        -- PRIOR_*: how far back (sample_cycle)
    history_scope      VARCHAR(10) DEFAULT 'CONFIG',   -- PRIOR_*: CONFIG | PROJECT
    description        VARCHAR(1000)
)
UNIQUE PRIMARY INDEX (priority_id);

CREATE INDEX gre_sampling_priority_config_ix (config_id) ON {{META_DB}}.gre_sampling_priority;


-- 5. gre_sampling_strata - stratification levels, in order -------------------
-- No rows for a config = no stratification (the whole pool is one bucket).
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_strata (
    strata_id          INTEGER NOT NULL,
    config_id          INTEGER NOT NULL,
    level_order        INTEGER NOT NULL,               -- 0 = first split, 1 = within it, ...
    level_name         VARCHAR(100),
    stratify_expr      VARCHAR(1000),                  -- VALUE level: a column or SQL expression, each value
                                                       -- is a bucket. NULL = CONDITION level: the buckets are
                                                       -- the gre_sampling_bucket rows' bucket_sql
    applies_to         VARCHAR(1000) DEFAULT '*',      -- path pattern of the parent buckets this level splits,
                                                       -- e.g. 'Denied' or 'Denied/IP PAC' (* = any)
    unlisted_policy    VARCHAR(10) DEFAULT 'SHARE',    -- buckets with no gre_sampling_bucket row (VALUE level),
                                                       -- or rows matching no condition (CONDITION level, bucket
                                                       -- 'OTHER'): SHARE = share 1 - SUM(fractions) |
                                                       -- REMAINDER = as quota_mode REMAINDER | SKIP = 0
    act_ind            BYTEINT DEFAULT 1
)
UNIQUE PRIMARY INDEX (strata_id);

CREATE INDEX gre_sampling_strata_config_ix (config_id, level_order) ON {{META_DB}}.gre_sampling_strata;


-- 6. gre_sampling_bucket - the buckets of a level and their quota -------------
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_bucket (
    bucket_id          INTEGER NOT NULL,
    strata_id          INTEGER NOT NULL,
    act_ind            BYTEINT DEFAULT 1,
    parent_path        VARCHAR(1000) DEFAULT '*',      -- path pattern this row applies under (* = any parent):
                                                       -- the same level can have a different mix per parent
    seq_no             INTEGER DEFAULT 100,            -- CONDITION level: conditions are tried in this order,
                                                       -- the first true one wins
    bucket_name        VARCHAR(200) NOT NULL,          -- VALUE level: the value (no case, no trailing blanks).
                                                       -- No '/' (it separates path levels)
    bucket_sql         CLOB,                           -- CONDITION level: the condition on the universe row
    quota_mode         VARCHAR(10) DEFAULT 'FRACTION', -- FRACTION: target_fraction of the parent's target |
                                                       -- ALL: every case there is (up to max_count) |
                                                       -- REMAINDER: what the other buckets leave unused,
                                                       --   shared equally between REMAINDER buckets
    target_fraction    FLOAT,                          -- FRACTION: 0 to 1
    max_count          INTEGER,                        -- optional cap for any mode
    description        VARCHAR(1000)
)
UNIQUE PRIMARY INDEX (bucket_id);

CREATE INDEX gre_sampling_bucket_strata_ix (strata_id) ON {{META_DB}}.gre_sampling_bucket;


-- 7. gre_sampling_output_col - columns of the output snapshot -----------------
-- With gre_sampling_config.output_table set, every selected case is written
-- there with these columns, evaluated on its universe row at run time.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_output_col (
    output_col_id      INTEGER NOT NULL,
    config_id          INTEGER NOT NULL,
    act_ind            BYTEINT DEFAULT 1,
    seq_no             INTEGER DEFAULT 100,
    column_name        VARCHAR(128) NOT NULL,          -- the output table's column
    expr_sql           CLOB NOT NULL,                  -- a column or SQL expression of the universe row
    description        VARCHAR(1000)
)
UNIQUE PRIMARY INDEX (output_col_id);


-- 8. gre_sample_selections - every row considered: selected, not, excluded ----
CREATE MULTISET TABLE {{META_DB}}.gre_sample_selections (
    sample_run_id      VARCHAR(200) NOT NULL,          -- = gre_sampling_audit.run_id
    config_id          INTEGER NOT NULL,
    project_name       VARCHAR(100),
    process_name       VARCHAR(100),
    run_key            VARCHAR(100) NOT NULL,
    sample_cycle       DATE,                           -- the run's as-of (business) date
    case_key           VARCHAR(500) NOT NULL,          -- col1=val1|col2=val2 of key_columns
    priority_rank      INTEGER,                        -- 1 = highest priority, NULL for RANDOM / excluded
    selected_flag      BYTEINT DEFAULT 0,              -- 1 = in the sample
    selection_order    INTEGER,                        -- selected: 1..n in priority order
    excluded_flag      BYTEINT DEFAULT 0,              -- 1 = left out by an exclusion rule
    exclusion_reason   VARCHAR(200),                   -- the first exclusion rule that matched
    stratum_path       VARCHAR(1000),                  -- leaf bucket, e.g. Denied/Part B
    etl_is_curr_ind    CHAR(1) DEFAULT 'Y',            -- Y = current run of its (config_id, run_key)
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)                 -- set when etl_is_curr_ind turns N
)
PRIMARY INDEX (sample_run_id);

CREATE INDEX gre_sample_selections_curr_ix (config_id, run_key, etl_is_curr_ind) ON {{META_DB}}.gre_sample_selections;
CREATE INDEX gre_sample_selections_hist_ix (project_name, sample_cycle, selected_flag) ON {{META_DB}}.gre_sample_selections;


-- 9. gre_sample_selection_attrs - each candidate's bucket per level ----------
CREATE MULTISET TABLE {{META_DB}}.gre_sample_selection_attrs (
    sample_run_id      VARCHAR(200) NOT NULL,
    config_id          INTEGER NOT NULL,
    run_key            VARCHAR(100) NOT NULL,
    case_key           VARCHAR(500) NOT NULL,
    strata_id          INTEGER NOT NULL,
    level_order        INTEGER,
    bucket_value       VARCHAR(200),
    etl_is_curr_ind    CHAR(1) DEFAULT 'Y',
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)
)
PRIMARY INDEX (sample_run_id, case_key);

CREATE INDEX gre_sample_selection_attrs_curr_ix (config_id, run_key, etl_is_curr_ind) ON {{META_DB}}.gre_sample_selection_attrs;


-- 10. gre_sampling_run_stats - counts per run: each exclusion rule, each bucket
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_run_stats (
    sample_run_id      VARCHAR(200) NOT NULL,
    config_id          INTEGER NOT NULL,
    run_key            VARCHAR(100) NOT NULL,
    sample_cycle       DATE,
    stat_type          VARCHAR(20) NOT NULL,           -- EXCLUSION | BUCKET
    stat_name          VARCHAR(1000) NOT NULL,         -- exclusion_name, or the bucket path
    candidate_cnt      INTEGER,                        -- EXCLUSION: rows it matched (first match) |
                                                       -- BUCKET: candidates in the bucket
    target_cnt         INTEGER,
    selected_cnt       INTEGER,
    etl_is_curr_ind    CHAR(1) DEFAULT 'Y',
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)
)
PRIMARY INDEX (sample_run_id);

CREATE INDEX gre_sampling_run_stats_curr_ix (config_id, run_key, etl_is_curr_ind) ON {{META_DB}}.gre_sampling_run_stats;


-- 11. gre_sampling_audit - one row per run (attempt) ------------------------
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_audit (
    run_id             VARCHAR(200) NOT NULL,
    run_key            VARCHAR(100),
    sample_config_id   INTEGER,
    project_name       VARCHAR(100),
    process_name       VARCHAR(100),
    sample_name        VARCHAR(100),
    sampling_method    VARCHAR(20),
    random_seed        BIGINT,                         -- RANDOM / SYSTEMATIC: rerun with it to reproduce
    target_volume      INTEGER,
    as_of_date         DATE,
    run_params_txt     VARCHAR(4000),                  -- the run parameters used, looked-up ones included (JSON)
    total_in_scope     INTEGER,                        -- rows matching scope_sql
    total_excluded     INTEGER,                        -- of those, left out by exclusions
    total_candidates   INTEGER,
    total_selected     INTEGER,
    shortfall          INTEGER,                        -- target_volume - total_selected
    started_at         TIMESTAMP(6),
    ended_at           TIMESTAMP(6),
    status             VARCHAR(20),                    -- RUNNING | COMPLETED | ERROR
    error_message      VARCHAR(2000),
    triggered_by       VARCHAR(100),                   -- airflow-scheduled, airflow-manual-<user>, local-<user>
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
)
UNIQUE PRIMARY INDEX (run_id);

CREATE INDEX gre_sampling_audit_config_run_key_ix (sample_config_id, run_key) ON {{META_DB}}.gre_sampling_audit;
CREATE INDEX gre_sampling_audit_status_ix (status) ON {{META_DB}}.gre_sampling_audit;


-- 12. gre_sampling_errors - what went wrong, per run -------------------------
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_errors (
    error_id           BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 NO CYCLE),
    run_id             VARCHAR(200),
    config_id          INTEGER,
    process_name       VARCHAR(100),
    run_key            VARCHAR(100),
    error_type         VARCHAR(50),                    -- CONFIG_ERROR | PARAM_FAILURE | PULL_FAILURE |
                                                       -- SELECT_FAILURE | OUTPUT_FAILURE | PERSIST_FAILURE
    error_message      VARCHAR(2000),
    error_detail       CLOB,                           -- traceback (technical errors)
    active_ind         CHAR(1) DEFAULT 'Y',            -- N = a later run of the same (config_id, run_key) replaced it
    occurred_at        TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)
)
PRIMARY INDEX (run_id);

CREATE INDEX gre_sampling_errors_active_ix (config_id, run_key, active_ind) ON {{META_DB}}.gre_sampling_errors;
