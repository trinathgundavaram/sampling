-- ============================================================================
-- Output snapshot table - TEMPLATE (one per config with output_table set)
--
-- Replace {{OUTPUT_TABLE}} with gre_sampling_config.output_table and add one
-- column per active gre_sampling_output_col row (column_name, a type fitting
-- its expr_sql). The engine writes the first 11 columns itself and never
-- creates or alters this table. A rerun of the same (config_id, run_key)
-- marks the earlier rows etl_is_curr_ind = 'N'.
-- ============================================================================
CREATE MULTISET TABLE {{OUTPUT_TABLE}} (
    sample_run_id      VARCHAR(200) NOT NULL,
    config_id          INTEGER NOT NULL,
    run_key            VARCHAR(100) NOT NULL,
    sample_cycle       DATE,
    case_key           VARCHAR(500) NOT NULL,
    selection_order    INTEGER,
    priority_rank      INTEGER,
    stratum_path       VARCHAR(1000),
    etl_is_curr_ind    CHAR(1) DEFAULT 'Y',
    load_datetime      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    last_updated_datetime TIMESTAMP(6),
    -- the configured columns, e.g. for the example seed (config_id 1):
    request_disposition   VARCHAR(100),
    functional_area       VARCHAR(100),
    procedure_code_family VARCHAR(100)
)
PRIMARY INDEX (sample_run_id);
