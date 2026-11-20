SELECT e.error_id AS GE_Error_ID, e.run_id AS GE_Run_ID, CAST(e.rule_id AS INTEGER) AS GE_Rule_ID, e.rule_group AS GE_Rule_Group, e.rule_variant AS GE_Rule_Variant, e.run_key AS GE_Run_Key, e.error_type AS GE_Error_Type, e.error_message AS GE_Error_Message, e.occurred_at AS GE_Occurred_Dtts, CAST(1 AS INTEGER) AS GE_Error_Cnt
FROM HSABC_V.gre_rule_errors e
WHERE e.active_ind = 'Y' AND UPPER(TRIM(e.rule_group)) LIKE 'ODAG1%'
