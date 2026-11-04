-- ============================================================================
-- Stratified sampling - Teradata DDL (5 tables), named as the framework's tables
--
-- {{META_DB}} is a placeholder: replace it with the metadata database
-- (the Variable's meta_db / SAMPLING_META_DB) before running, or let
--   python local/run_sampling.py deploy-schema
-- do it. The engine never creates or alters tables. No secondary indexes:
-- each table has only its primary index.
--
-- A sample is one Project_Cd + Table_Nm + Run_Ty (DLY, WKLY, MNT ...), as the
-- framework scopes its batches. Rules apply to it when their Table_Nm and
-- Run_Ty are its own or '*' (every table / every run type); for the same rule
-- the most specific row wins.
--
--   GOVT_RGLTRY_SamplingConfig   you    one row per sample: source, scope, size, method, target table
--   GOVT_RGLTRY_SamplingRule     you    every rule, one row each:
--                                       PARAM, GATE, EXCLUSION, PRIORITY, LEVEL, BUCKET
--   GOVT_RGLTRY_SamplingRun      engine one row per run attempt: status, counts, errors, the config it used
--   GOVT_RGLTRY_SamplingRunRule  engine the rule rows each run used (its snapshot)
--   GOVT_RGLTRY_SampleSelection  engine one row per case considered by a run
--
-- Active_Ind = 1 means in use, 0 means ignored: never delete a configuration row.
-- A rerun of the same sample and Run_Key marks the earlier run's rows
-- Curr_Ind = 0. Run rows are never deleted. GOVT_RGLTRY_SampleSelection rows no
-- longer needed are deleted only by purge (runbook section 12).
-- No statement or comment in this file may contain a semicolon except at the
-- end of a statement (deploy-schema splits on them).
-- ============================================================================


-- 1. GOVT_RGLTRY_SamplingConfig - one row per sample ---------------------------
CREATE MULTISET TABLE {{META_DB}}.GOVT_RGLTRY_SamplingConfig (
  Project_Cd             VARCHAR(30)    NOT NULL,          -- the framework's project
  Table_Nm               VARCHAR(63)    NOT NULL,          -- the framework's table (e.g. ODAG1)
  Run_Ty                 VARCHAR(20)    NOT NULL,          -- DLY, WKLY, MNT ... (ComplianceRunType)
  Smpl_Desc              VARCHAR(200),
  Src_Schema_Nm          VARCHAR(128)   NOT NULL,          -- database of the source table or view, $env allowed
  Src_Table_Nm           VARCHAR(128)   NOT NULL,          -- the source table or view, $env allowed
  Key_Columns_Txt        VARCHAR(1000)  NOT NULL,          -- comma separated columns identifying one case
  Scope_Sql_Txt          VARCHAR(8000),                    -- WHERE condition: this run's rows (default 1=1),
                                                           -- {name} / $name parameters and $env allowed
  Target_Cnt             INTEGER        DEFAULT 150 NOT NULL,   -- sample size
  Smpl_Method_Cd         VARCHAR(20)    DEFAULT 'RANKED' NOT NULL,  -- RANKED | RANDOM | SYSTEMATIC
  Priority_Rank_Sql_Txt  VARCHAR(8000),                    -- optional ORDER BY list after the PRIORITY rules
  Rounding_Cd            VARCHAR(10)    DEFAULT 'FLOOR' NOT NULL,   -- FLOOR | ROUND | CEIL of each bucket's share
  Diversity_Sql_Txt      VARCHAR(1000),                    -- optional SQL expression: picks rotate across its
                                                           -- values (e.g. a code family) within each bucket
  Diversity_Cap_Cnt      INTEGER,                          -- optional: at most this many picks per value
  Shortfall_Cd           VARCHAR(10)    DEFAULT 'TOPUP' NOT NULL,   -- TOPUP: fill a short sample from the other
                                                           -- candidates by priority | NONE: leave it short
  Keep_Excluded_Ind      SMALLINT       DEFAULT 1 NOT NULL,       -- 1 = store excluded rows with their reason too
  Tgt_Schema_Nm          VARCHAR(128),                     -- optional, with Tgt_Table_Nm (both or neither):
                                                           -- database of the target table, $env allowed
  Tgt_Table_Nm           VARCHAR(128),                     -- the target table (created by another
                                                           -- process): one row per selected case, every column
                                                           -- of its source row, the ETL_* audit and SMPL_* run
                                                           -- columns set by sampling (runbook 4.3), $env allowed
  Active_Ind             SMALLINT       DEFAULT 1 NOT NULL,
  Load_Dtts              TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LoadUser_ID            VARCHAR(100)   DEFAULT USER NOT NULL,
  LastUpdt_Dtts          TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LastUpdt_ID            VARCHAR(100)   DEFAULT USER NOT NULL
)
UNIQUE PRIMARY INDEX (Project_Cd, Table_Nm, Run_Ty);


-- 2. GOVT_RGLTRY_SamplingRule - every rule, one row each -------------------------
-- Rule_Class_Cd says what the row is. Columns not used by a class stay NULL
-- (validate reports any that are set). Table_Nm / Run_Ty '*' = every table /
-- every run type of the project. When rows of the same class and Rule_Nm
-- (and Level_Nbr, Parent_Path_Txt) apply to a sample, the most specific wins:
-- its table and run type, then its table, then its run type, then '*'.
--   PARAM      a run parameter: Rule_Nm = the {token}, Rule_Sql_Txt = a SELECT
--              whose first column of the first row is the value (a value
--              passed with the run wins). No value = PARAM_FAILURE (retried).
--   GATE       a readiness check: Rule_Sql_Txt = a SELECT that must return a
--              row before the run may sample, e.g. "the batch is closed".
--              Runs after the PARAM rules: no row = GATE_NOT_MET, retried like a
--              technical error. Rule_Nm and Rule_Desc go into the message.
--   Every EXCLUSION / PRIORITY / LEVEL / BUCKET Rule_Sql_Txt is a whole SELECT
--   (any joins) returning the Key_Columns_Txt columns of the cases it matches
--   (+ one value column for a PRIORITY EXPRESSION or a LEVEL).
--   EXCLUSION  Rule_Ty QUERY (default): the cases it returns are excluded |
--              PRIOR_SELECTION: selected by an earlier run (Lookback_Days,
--              History_Scope_Cd SAMPLE | TABLE | PROJECT).
--              The first matching rule (Seq_Nbr) is the reason stored.
--   PRIORITY   Rule_Ty QUERY (default): the cases it returns first |
--              EXPRESSION: ordered by the value it returns (Sort_Dir_Cd) |
--              PRIOR_CANDIDATE: unselected in an earlier run first |
--              PRIOR_SELECTION: selected in an earlier run last.
--              Criterion 1 (Seq_Nbr) first, then 2 ...
--   LEVEL      a stratification level (Level_Nbr 0, 1 ...): Rule_Sql_Txt =
--              a SELECT of the key columns and each case's value (NULL: the
--              level's buckets have their own SELECTs). Parent_Path_Txt = under
--              which parent buckets it splits ('*' everywhere), Unlisted_Cd =
--              values / rows no BUCKET row covers.
--   BUCKET     a bucket of the level with the same Level_Nbr: Rule_Nm = the
--              value or the bucket's name (no '/'), Rule_Sql_Txt = its SELECT
--              (bucket levels only, first in Seq_Nbr order, NULL = the rest),
--              Parent_Path_Txt = under which parents this row applies, Quota_Cd,
--              Target_Fraction_Val, Max_Cnt.
CREATE MULTISET TABLE {{META_DB}}.GOVT_RGLTRY_SamplingRule (
  Rule_ID                INTEGER        NOT NULL,          -- unique in the table
  Project_Cd             VARCHAR(30)    NOT NULL,
  Table_Nm               VARCHAR(63)    DEFAULT '*' NOT NULL,     -- a table, or '*' = every table of the project
  Run_Ty                 VARCHAR(20)    DEFAULT '*' NOT NULL,     -- a run type, or '*' = every run type
  Rule_Class_Cd          VARCHAR(20)    NOT NULL,          -- PARAM | GATE | EXCLUSION | PRIORITY | LEVEL | BUCKET
  Seq_Nbr                INTEGER        DEFAULT 100 NOT NULL,     -- order within the class (and level)
  Rule_Nm                VARCHAR(200)   NOT NULL,
  Rule_Ty                VARCHAR(30),                      -- see above, per class
  Rule_Sql_Txt           VARCHAR(32000),                   -- a SELECT (see above)
  Level_Nbr              INTEGER,                          -- LEVEL / BUCKET
  Parent_Path_Txt        VARCHAR(1000)  DEFAULT '*',       -- LEVEL / BUCKET: parent bucket path pattern
  Unlisted_Cd            VARCHAR(10),                      -- LEVEL: SHARE (default) | REMAINDER | SKIP
  Quota_Cd               VARCHAR(10),                      -- BUCKET: FRACTION (default) | ALL | REMAINDER
  Target_Fraction_Val    FLOAT,                            -- BUCKET FRACTION: 0 to 1 of the parent's target
  Max_Cnt                INTEGER,                          -- BUCKET: optional cap
  Sort_Dir_Cd            VARCHAR(4),                       -- PRIORITY EXPRESSION: ASC (default) | DESC
  Lookback_Days          INTEGER,                          -- PRIOR_SELECTION / PRIOR_CANDIDATE
  History_Scope_Cd       VARCHAR(10),                      -- SAMPLE (default) | TABLE | PROJECT
  Rule_Desc              VARCHAR(1000),
  Active_Ind             SMALLINT       DEFAULT 1 NOT NULL,
  Load_Dtts              TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LoadUser_ID            VARCHAR(100)   DEFAULT USER NOT NULL,
  LastUpdt_Dtts          TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LastUpdt_ID            VARCHAR(100)   DEFAULT USER NOT NULL
)
UNIQUE PRIMARY INDEX (Rule_ID);


-- 3. GOVT_RGLTRY_SamplingRun - one row per run attempt (engine) ------------------
-- Carries the configuration the run used (its config columns); its rules are in
-- GOVT_RGLTRY_SamplingRunRule.
CREATE MULTISET TABLE {{META_DB}}.GOVT_RGLTRY_SamplingRun (
  Smpl_Run_ID            VARCHAR(250)   NOT NULL,          -- generated by the engine
  Project_Cd             VARCHAR(30)    NOT NULL,
  Table_Nm               VARCHAR(63)    NOT NULL,
  Run_Ty                 VARCHAR(20)    NOT NULL,
  Run_Key                VARCHAR(250)   NOT NULL,          -- the cycle (e.g. the batch id)
  As_Of_Dt_Key           DATE           NOT NULL,          -- the run's business date
  Run_Stat               VARCHAR(20)    NOT NULL,          -- RUNNING | COMPLETED | FINAL | ERROR
  Curr_Ind               SMALLINT       DEFAULT 0 NOT NULL,       -- 1 = the current sample of its Run_Key
  Random_Seed_Nbr        BIGINT,
  Run_Parm_Txt           VARCHAR(4000),                    -- the run's parameters (JSON)
  In_Scope_Cnt           INTEGER,
  Excluded_Cnt           INTEGER,
  Candidate_Cnt          INTEGER,
  Selected_Cnt           INTEGER,
  Shortfall_Cnt          INTEGER,
  Error_Ty               VARCHAR(50),                      -- CONFIG_ERROR | FINALIZED | GATE_NOT_MET | GATE_FAILURE |
                                                           -- PARAM_FAILURE | PULL_FAILURE | QUERY_RULE_FAILURE |
                                                           -- SELECT_FAILURE | OUTPUT_FAILURE | PERSIST_FAILURE |
                                                           -- ABANDONED
  Error_Txt              VARCHAR(4000),
  Error_Detail_Txt       VARCHAR(8000),                    -- the end of the traceback
  Run_Rpt_Txt            VARCHAR(16000),                   -- counts per rule and bucket (JSON)
  Config_Hash            CHAR(64),                         -- SHA-256 of the config and rule rows used
  Override_Rsn_Txt       VARCHAR(1000),                    -- why a FINAL sample was replaced
  Finalized_By           VARCHAR(100),
  Finalized_Dtts         TIMESTAMP(6),
  Triggered_By           VARCHAR(100),
  Start_Dtts             TIMESTAMP(6),
  End_Dtts               TIMESTAMP(6),
  -- the configuration the run used
  Src_Schema_Nm          VARCHAR(128),
  Src_Table_Nm           VARCHAR(128),
  Key_Columns_Txt        VARCHAR(1000),
  Scope_Sql_Txt          VARCHAR(8000),
  Target_Cnt             INTEGER,
  Smpl_Method_Cd         VARCHAR(20),
  Priority_Rank_Sql_Txt  VARCHAR(8000),
  Rounding_Cd            VARCHAR(10),
  Diversity_Sql_Txt      VARCHAR(1000),
  Diversity_Cap_Cnt      INTEGER,
  Shortfall_Cd           VARCHAR(10),
  Keep_Excluded_Ind      SMALLINT,
  Tgt_Schema_Nm          VARCHAR(128),
  Tgt_Table_Nm           VARCHAR(128),
  Load_Dtts              TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LoadUser_ID            VARCHAR(100)   DEFAULT USER NOT NULL,
  LastUpdt_Dtts          TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LastUpdt_ID            VARCHAR(100)   DEFAULT USER NOT NULL
)
UNIQUE PRIMARY INDEX (Smpl_Run_ID);


-- 4. GOVT_RGLTRY_SamplingRunRule - the rule rows each run used (engine) ----------
CREATE MULTISET TABLE {{META_DB}}.GOVT_RGLTRY_SamplingRunRule (
  Smpl_Run_ID            VARCHAR(250)   NOT NULL,
  Rule_ID                INTEGER        NOT NULL,
  Project_Cd             VARCHAR(30)    NOT NULL,
  Table_Nm               VARCHAR(63)    NOT NULL,
  Run_Ty                 VARCHAR(20)    NOT NULL,
  Rule_Class_Cd          VARCHAR(20)    NOT NULL,
  Seq_Nbr                INTEGER,
  Rule_Nm                VARCHAR(200)   NOT NULL,
  Rule_Ty                VARCHAR(30),
  Rule_Sql_Txt           VARCHAR(32000),
  Level_Nbr              INTEGER,
  Parent_Path_Txt        VARCHAR(1000),
  Unlisted_Cd            VARCHAR(10),
  Quota_Cd               VARCHAR(10),
  Target_Fraction_Val    FLOAT,
  Max_Cnt                INTEGER,
  Sort_Dir_Cd            VARCHAR(4),
  Lookback_Days          INTEGER,
  History_Scope_Cd       VARCHAR(10),
  Rule_Desc              VARCHAR(1000),
  Load_Dtts              TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LoadUser_ID            VARCHAR(100)   DEFAULT USER NOT NULL,
  LastUpdt_Dtts          TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LastUpdt_ID            VARCHAR(100)   DEFAULT USER NOT NULL
)
PRIMARY INDEX (Smpl_Run_ID);


-- 5. GOVT_RGLTRY_SampleSelection - one row per case considered (engine) ----------
CREATE MULTISET TABLE {{META_DB}}.GOVT_RGLTRY_SampleSelection (
  Smpl_Run_ID            VARCHAR(250)   NOT NULL,
  Project_Cd             VARCHAR(30)    NOT NULL,
  Table_Nm               VARCHAR(63)    NOT NULL,
  Run_Ty                 VARCHAR(20)    NOT NULL,
  Run_Key                VARCHAR(250)   NOT NULL,
  Smpl_Cycle_Dt_Key      DATE           NOT NULL,          -- the run's As_Of_Dt_Key
  Case_Key               VARCHAR(1000)  NOT NULL,          -- 'col1=val1|col2=val2' of the key columns
  Priority_Rank_Nbr      INTEGER,                          -- the case's place in priority order
  Selected_Ind           SMALLINT       DEFAULT 0 NOT NULL,
  Selection_Order_Nbr    INTEGER,
  Excluded_Ind           SMALLINT       DEFAULT 0 NOT NULL,
  Exclusion_Rsn_Txt      VARCHAR(200),                     -- the EXCLUSION rule's Rule_Nm
  Stratum_Path_Txt       VARCHAR(1000),                    -- the bucket path, e.g. Denied/Part B
  Selection_Stat         VARCHAR(20),                      -- SELECTED | REPLACED | REPLACEMENT
  Linked_Case_Key        VARCHAR(1000),                    -- the case it replaced / was replaced by
  Stat_Rsn_Txt           VARCHAR(1000),
  Stat_By                VARCHAR(100),
  Stat_Dtts              TIMESTAMP(6),
  Curr_Ind               SMALLINT       DEFAULT 1 NOT NULL,
  Load_Dtts              TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LoadUser_ID            VARCHAR(100)   DEFAULT USER NOT NULL,
  LastUpdt_Dtts          TIMESTAMP(6)   DEFAULT CURRENT_TIMESTAMP(6) NOT NULL,
  LastUpdt_ID            VARCHAR(100)   DEFAULT USER NOT NULL
)
PRIMARY INDEX (Smpl_Run_ID);
