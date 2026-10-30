-- ============================================================================
-- Stratified sampling - Teradata DDL (4 tables)
--
-- {{META_DB}} is a placeholder: replace it with the metadata database
-- (the Variable's meta_db / SAMPLING_META_DB) before running, or let
--   python local/run_sampling.py deploy-schema
-- do it. The engine never creates or alters tables.
--
--   gre_sampling_config     you    one row per sample: universe, scope, size, method, output
--   gre_sampling_rule       you    every rule of a sample, one row each (like gre_rules):
--                                  PARAM, GATE, EXCLUSION, PRIORITY, LEVEL, BUCKET, OUTPUT
--   gre_sampling_run        engine one row per run attempt: status, totals, errors, report
--   gre_sample_selections   engine one row per case considered by a run
--
-- act_ind = 1 means in use, 0 means ignored: never delete a configuration row.
-- A rerun of the same (config_id, run_key) marks the earlier run's rows
-- etl_is_curr_ind = 'N'. Run rows are never deleted. gre_sample_selections
-- rows no longer needed are deleted only by purge (runbook section 12).
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
                                                       -- prefer EXCLUSION rules)
    target_volume      INTEGER NOT NULL DEFAULT 150,   -- sample size
    sampling_method    VARCHAR(20) DEFAULT 'RANKED',   -- RANKED | RANDOM | SYSTEMATIC
    priority_rank_sql  CLOB,                           -- optional ORDER BY list applied after the
                                                       -- PRIORITY rules
    rounding_mode      VARCHAR(10) DEFAULT 'FLOOR',    -- FLOOR | ROUND | CEIL for each bucket's share
    diversity_expr     VARCHAR(1000),                  -- optional SQL expression: picks rotate across its
                                                       -- values (e.g. a code family) within each bucket
    diversity_cap      INTEGER,                        -- optional: at most this many picks per value
    shortfall_policy   VARCHAR(10) DEFAULT 'TOPUP',    -- TOPUP: fill a short sample from the rest of the
                                                       -- candidates by priority | NONE: leave it short
    keep_excluded      BYTEINT DEFAULT 1,              -- 1 = store excluded rows with their reason too
    output_table       VARCHAR(200),                   -- optional database.table of the output snapshot
                                                       -- (OUTPUT rules), $env allowed
    schedule_cron      VARCHAR(50),                    -- documentation only: the schedule is in the Variable
    created_by         VARCHAR(100),
    last_updated_by    VARCHAR(100),
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)
)
UNIQUE PRIMARY INDEX (config_id);

CREATE INDEX gre_sampling_config_scope_ix (project_name, process_name) ON {{META_DB}}.gre_sampling_config;


-- 2. gre_sampling_rule - every rule of a sample, one row each ----------------
-- rule_class says what the row is. Columns not used by a class stay NULL
-- (validate reports any that are set).
--
--   PARAM      a run parameter: rule_name = the {token}, rule_sql = a SELECT
--              returning one value (first column, first row). Run first, in
--              seq_no order. A value passed with the run wins.
--   GATE       a readiness check: rule_sql is a SELECT that must return at
--              least one row (e.g. "the batch is closed"). Run after the
--              PARAM rules: no row = GATE_NOT_MET, retried like a technical
--              error. rule_name and description go into the message.
--   EXCLUSION  rule_type SQL: rule_sql is a condition, true = excluded |
--              PRIOR_SELECTION: selected by an earlier run (lookback_days,
--              history_scope). The first match (seq_no) is the reason stored.
--   PRIORITY   rule_type CONDITION: rows where rule_sql is true first |
--              EXPRESSION: by the value of rule_sql (sort_dir) |
--              PRIOR_CANDIDATE: considered earlier, never selected, first |
--              PRIOR_SELECTION: selected earlier, last. Applied in seq_no order.
--   LEVEL      a stratification level: level_order, rule_name = its name,
--              rule_sql = the expression whose values are the buckets, or NULL
--              for a level whose buckets are conditions (BUCKET rule_sql).
--              parent_path = which parent buckets it splits (* = all).
--              unlisted_policy = values / rows no BUCKET row covers.
--   BUCKET     a bucket of the level with the same level_order: rule_name =
--              the value or the bucket's name (no '/'), rule_sql = its
--              condition (condition levels only, first true in seq_no order),
--              parent_path = under which parents this row applies, quota_mode,
--              target_fraction, max_count.
--   OUTPUT     a column of the output snapshot: rule_name = the column,
--              rule_sql = its expression on the universe row.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_rule (
    rule_id            INTEGER NOT NULL,               -- unique in the table
    config_id          INTEGER NOT NULL,
    act_ind            BYTEINT DEFAULT 1,
    rule_class         VARCHAR(20) NOT NULL,           -- PARAM | GATE | EXCLUSION | PRIORITY | LEVEL | BUCKET | OUTPUT
    seq_no             INTEGER DEFAULT 100,            -- order within the class
    rule_name          VARCHAR(200) NOT NULL,
    rule_type          VARCHAR(30),                    -- EXCLUSION: SQL (default) | PRIOR_SELECTION
                                                       -- PRIORITY: CONDITION (default) | EXPRESSION |
                                                       --   PRIOR_CANDIDATE | PRIOR_SELECTION
    rule_sql           CLOB,                           -- condition, expression, or SELECT (see above)
    level_order        INTEGER,                        -- LEVEL / BUCKET: 0 = first split, 1 = within it, ...
    parent_path        VARCHAR(1000) DEFAULT '*',      -- LEVEL / BUCKET: path pattern of parent buckets,
                                                       -- e.g. 'Denied' or 'Denied/*' (* and ? wildcards)
    unlisted_policy    VARCHAR(10),                    -- LEVEL: SHARE (default) = share 1 - SUM(fractions) |
                                                       -- REMAINDER | SKIP = never selected
    quota_mode         VARCHAR(10),                    -- BUCKET: FRACTION (default) | ALL | REMAINDER
    target_fraction    FLOAT,                          -- BUCKET FRACTION: 0 to 1
    max_count          INTEGER,                        -- BUCKET: optional cap
    sort_dir           VARCHAR(4),                     -- PRIORITY EXPRESSION: ASC (default) | DESC
    lookback_days      INTEGER,                        -- PRIOR_*: how far back (sample_cycle)
    history_scope      VARCHAR(10),                    -- PRIOR_*: CONFIG (default, this sample) | PROJECT
    description        VARCHAR(1000),
    created_by         VARCHAR(100),
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)
)
UNIQUE PRIMARY INDEX (rule_id);

CREATE INDEX gre_sampling_rule_config_ix (config_id, rule_class) ON {{META_DB}}.gre_sampling_rule;


-- 3. gre_sampling_run - one row per run attempt ------------------------------
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_run (
    run_id             VARCHAR(200) NOT NULL,
    config_id          INTEGER,
    project_name       VARCHAR(100),
    process_name       VARCHAR(100),
    sample_name        VARCHAR(100),
    run_key            VARCHAR(100),
    as_of_date         DATE,
    sampling_method    VARCHAR(20),
    random_seed        BIGINT,                         -- RANDOM / SYSTEMATIC: rerun with it to reproduce
    target_volume      INTEGER,
    run_params_txt     VARCHAR(4000),                  -- the run parameters used, looked-up ones included (JSON)
    total_in_scope     INTEGER,                        -- rows matching scope_sql
    total_excluded     INTEGER,                        -- of those, left out by exclusions
    total_candidates   INTEGER,
    total_selected     INTEGER,
    shortfall          INTEGER,                        -- target_volume - total_selected
    status             VARCHAR(20),                    -- RUNNING | COMPLETED | ERROR | FINAL (finalised:
                                                       -- a rerun of its run_key needs a force reason)
    error_type         VARCHAR(50),                    -- CONFIG_ERROR | PARAM_FAILURE | PULL_FAILURE |
                                                       -- SELECT_FAILURE | OUTPUT_FAILURE | PERSIST_FAILURE |
                                                       -- ABANDONED (closed by mark-failed) |
                                                       -- FINALIZED (its run_key's sample is final) |
                                                       -- GATE_NOT_MET (a GATE rule returned no row: retried) |
                                                       -- GATE_FAILURE (a GATE rule's SQL failed)
    error_message      VARCHAR(2000),
    error_detail       CLOB,                           -- traceback (technical errors)
    run_report         CLOB,                           -- JSON: count per exclusion rule, candidates / target /
                                                       -- selected per bucket, the config and rule rows used
    config_hash        VARCHAR(64),                    -- SHA-256 of the config and rule rows used: equal
                                                       -- hashes = same rules
    override_reason    VARCHAR(1000),                  -- why this run replaced a FINAL run (force)
    finalized_by       VARCHAR(100),
    finalized_at       TIMESTAMP(6),
    etl_is_curr_ind    CHAR(1) DEFAULT 'N',            -- Y = the run whose sample is current for its
                                                       -- (config_id, run_key)
    triggered_by       VARCHAR(100),                   -- airflow-scheduled, airflow-manual-<user>, local-<user>
    started_at         TIMESTAMP(6),
    ended_at           TIMESTAMP(6),
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)
)
UNIQUE PRIMARY INDEX (run_id);

CREATE INDEX gre_sampling_run_key_ix (config_id, run_key) ON {{META_DB}}.gre_sampling_run;
CREATE INDEX gre_sampling_run_status_ix (status) ON {{META_DB}}.gre_sampling_run;


-- 4. gre_sample_selections - every case considered: selected, not, excluded --
CREATE MULTISET TABLE {{META_DB}}.gre_sample_selections (
    sample_run_id      VARCHAR(200) NOT NULL,          -- = gre_sampling_run.run_id
    config_id          INTEGER NOT NULL,
    project_name       VARCHAR(100),
    process_name       VARCHAR(100),
    run_key            VARCHAR(100) NOT NULL,
    sample_cycle       DATE,                           -- the run's as-of (business) date
    case_key           VARCHAR(500) NOT NULL,          -- col1=val1|col2=val2 of key_columns
    priority_rank      INTEGER,                        -- 1 = highest priority, NULL when excluded
    selected_flag      BYTEINT DEFAULT 0,              -- 1 = in the sample
    selection_order    INTEGER,                        -- selected: 1..n in priority order
    excluded_flag      BYTEINT DEFAULT 0,              -- 1 = left out by an exclusion rule
    exclusion_reason   VARCHAR(200),                   -- the first exclusion rule that matched
    stratum_path       VARCHAR(1000),                  -- the case's bucket at every level, e.g. Denied/Part B
    selection_status   VARCHAR(20),                    -- SELECTED | REPLACED (taken out of the sample) |
                                                       -- REPLACEMENT (put in for a REPLACED case) | NULL
    linked_case_key    VARCHAR(500),                   -- REPLACED: its replacement, REPLACEMENT: the case replaced
    status_reason      VARCHAR(1000),                  -- why it was replaced
    status_by          VARCHAR(100),
    status_at          TIMESTAMP(6),
    etl_is_curr_ind    CHAR(1) DEFAULT 'Y',            -- Y = current run of its (config_id, run_key)
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6)                 -- set when etl_is_curr_ind turns N or the case is replaced
)
PRIMARY INDEX (sample_run_id);

CREATE INDEX gre_sample_selections_curr_ix (config_id, run_key, etl_is_curr_ind) ON {{META_DB}}.gre_sample_selections;
CREATE INDEX gre_sample_selections_hist_ix (project_name, sample_cycle, selected_flag) ON {{META_DB}}.gre_sample_selections;
