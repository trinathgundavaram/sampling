DELETE FROM HSLABBOSS.GOVT_RGLTRY_SamplingRule WHERE Rule_ID BETWEEN 200 AND 299;

DELETE FROM HSLABBOSS.GOVT_RGLTRY_SamplingConfig WHERE Project_Cd = 'UMCM' AND Table_Nm = 'ODAG1' AND Run_Ty = 'WKLY';

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingConfig (Project_Cd, Table_Nm, Run_Ty, Smpl_Desc, Src_Schema_Nm, Src_Table_Nm, Key_Columns_Txt, Scope_Sql_Txt, Target_Ty, Target_Val, Smpl_Method_Cd, Rounding_Cd, Shortfall_Cd, Keep_Excluded_Ind, Tgt_Schema_Nm, Tgt_Table_Nm, Active_Ind)
VALUES ('UMCM', 'ODAG1', 'WKLY', 'ODAG1 weekly UM audit sample: 150 cases by functional area and determination', 'HSLABBOSS', 'SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1', 'AUTHZN_ID, SRC_DATA_KEY', 'SELECT * FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows}', 'CNT', 150, 'RANKED', 'ROUND', 'TOPUP', 1, 'HSLABBOSS', 'SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1_Sample', 1);

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Sql_Txt, Rule_Desc)
VALUES (201, 'UMCM', '*', '*', 'PARAM', 1, 'batch_id', 'SELECT TOP 1 Btch_ID FROM HSLABBOSS.GOVT_RGLTRY_RequestControl WHERE Project_Cd = ''{project_cd}'' AND Table_Nm = ''{table_nm}'' AND Run_Ty = ''{run_ty}'' GROUP BY Btch_ID HAVING MIN(Batch_Close_Ind) = 1 ORDER BY SUBSTR(Btch_ID, 1, 8) DESC, CHARACTER_LENGTH(Btch_ID) DESC, Btch_ID DESC', 'The latest batch of the sample''s project, table and run type with every source closed');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Lookback_Days, History_Scope_Cd, Rule_Desc)
VALUES (210, 'UMCM', 'ODAG1', 'WKLY', 'EXCLUSION', 0, 'SELECTED_IN_LAST_90_DAYS', 'PRIOR_SELECTION', 90, 'SAMPLE', 'Not a case this sample selected in the last 90 days');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Rule_Sql_Txt, Rule_Desc)
VALUES (211, 'UMCM', 'ODAG1', 'WKLY', 'EXCLUSION', 1, 'AUTO_APPROVED', 'QUERY', 'SELECT o.AUTHZN_ID, o.SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 o WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND EXISTS (SELECT 1 FROM HSLABBOSS.UMCM_SHRPA_PROCEDURE_CODES s WHERE POSITION('','' || TRIM(s.PROCEDURE_CODE) || '','' IN '','' || OREPLACE(o.CPT_PROC_CD, '' '', '''') || '','') > 0 AND {current_rows:s} AND s.IS_Auth_Required LIKE ''%approve%'')', 'SHRPA marks any of the procedure codes auto-approved');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Rule_Sql_Txt, Rule_Desc)
VALUES (212, 'UMCM', 'ODAG1', 'WKLY', 'EXCLUSION', 2, 'NO_PA_REQUIRED', 'QUERY', 'SELECT o.AUTHZN_ID, o.SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 o WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND EXISTS (SELECT 1 FROM HSLABBOSS.UMCM_SHRPA_PROCEDURE_CODES s WHERE POSITION('','' || TRIM(s.PROCEDURE_CODE) || '','' IN '','' || OREPLACE(o.CPT_PROC_CD, '' '', '''') || '','') > 0 AND {current_rows:s} AND s.IS_Auth_Required = ''No Auth Required - Check to see if a global rule applies'')', 'SHRPA says any of the procedure codes needs no prior authorisation');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Rule_Sql_Txt, Rule_Desc)
VALUES (213, 'UMCM', 'ODAG1', 'WKLY', 'EXCLUSION', 3, 'DIABETIC_SUPPLIES', 'QUERY', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND '','' || OREPLACE(CPT_PROC_CD, '' '', '''') || '','' LIKE ANY (''%,A4233,%'', ''%,A4234,%'', ''%,A4235,%'', ''%,A4236,%'', ''%,A4238,%'', ''%,A4239,%'', ''%,A4244,%'', ''%,A4245,%'', ''%,A4246,%'', ''%,A4247,%'', ''%,A4250,%'', ''%,A4253,%'', ''%,A4255,%'', ''%,A4256,%'', ''%,A4257,%'', ''%,A4258,%'', ''%,A4259,%'', ''%,A4271,%'', ''%,E0607,%'', ''%,E0784,%'', ''%,E2100,%'', ''%,E2101,%'', ''%,E2102,%'', ''%,E2103,%'', ''%,K0552,%'')', 'Any of the procedure codes is a diabetic supply code');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Rule_Sql_Txt, Rule_Desc)
VALUES (214, 'UMCM', 'ODAG1', 'WKLY', 'EXCLUSION', 4, 'REVISION', 'QUERY', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND RVSN_NBR <> ''1''', 'Only initial authorisations');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Lookback_Days, History_Scope_Cd, Rule_Desc)
VALUES (220, 'UMCM', 'ODAG1', 'WKLY', 'PRIORITY', 0, 'CARRY_OVER_14_DAYS', 'PRIOR_CANDIDATE', 14, 'SAMPLE', 'Cases an earlier run considered in the last 14 days and did not select go first');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Rule_Sql_Txt, Rule_Desc)
VALUES (221, 'UMCM', 'ODAG1', 'WKLY', 'PRIORITY', 1, 'EXPEDITED', 'QUERY', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND AUTHZN_REQ_STD_OR_EXPDT_IND = ''E''', 'Expedited cases next');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Unlisted_Cd, Rule_Desc)
VALUES (231, 'UMCM', 'ODAG1', 'WKLY', 'LEVEL', 1, 'functional_area', 0, '*', 'SKIP', 'Functional area from ISS_DESC; a case in no area is OTHER and not sampled');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val, Rule_Sql_Txt)
VALUES (232, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 1, 'Part B', 0, '*', 'PCT', 13, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Drugs - Biologics Part B''');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val, Rule_Sql_Txt)
VALUES (233, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 2, 'BH', 0, '*', 'PCT', 8, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND (TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) LIKE ''Behavioral Health%'' OR (TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Outpatient'' AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) = ''Behavioral Health''))');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val, Rule_Sql_Txt)
VALUES (234, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 3, 'DME', 0, '*', 'PCT', 20, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Outpatient'' AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) LIKE ''Durable Medical Equipment%''');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val, Rule_Sql_Txt)
VALUES (235, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 4, 'HHC', 0, '*', 'PCT', 8, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Outpatient'' AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) = ''Home Health Services''');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val, Rule_Sql_Txt)
VALUES (236, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 5, 'IP PAC', 0, '*', 'PCT', 22, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Inpatient'' AND (TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) IN (''Rehab'', ''LTAC'') OR TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) LIKE ''SNF%'')');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Rule_Sql_Txt)
VALUES (238, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 6, 'OP Medical', 0, '*', 'REMAINDER', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Outpatient''');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Rule_Sql_Txt)
VALUES (239, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 7, 'IP Medical', 0, '*', 'REMAINDER', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Inpatient''');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Unlisted_Cd, Rule_Desc)
VALUES (241, 'UMCM', 'ODAG1', 'WKLY', 'LEVEL', 2, 'ip_pac_type', 1, 'IP PAC', 'SKIP', 'IP PAC split equally into IPRF, LTAC, SNF');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Rule_Sql_Txt)
VALUES (242, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 1, 'IPRF', 1, 'IP PAC', 'REMAINDER', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Inpatient'' AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) = ''Rehab''');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Rule_Sql_Txt)
VALUES (243, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 2, 'LTAC', 1, 'IP PAC', 'REMAINDER', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Inpatient'' AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) = ''LTAC''');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Rule_Sql_Txt)
VALUES (244, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 3, 'SNF', 1, 'IP PAC', 'REMAINDER', 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Inpatient'' AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) LIKE ''SNF%''');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Unlisted_Cd, Rule_Sql_Txt, Rule_Desc)
VALUES (251, 'UMCM', 'ODAG1', 'WKLY', 'LEVEL', 3, 'determination', 2, '*', 'SKIP', 'SELECT AUTHZN_ID, SRC_DATA_KEY, CASE WHEN AUTHZN_REQ_DETRMTN_TY = ''Partially Approved'' THEN ''Approved'' ELSE AUTHZN_REQ_DETRMTN_TY END AS DETRMTN_TY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows}', 'Determination within every area; Partially Approved counts as Approved');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val)
VALUES (252, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 1, 'Dismissed', 2, '*', 'PCT', 12);

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val)
VALUES (253, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 2, 'Approved', 2, '*', 'PCT', 8);

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd)
VALUES (254, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 3, 'Denied', 2, '*', 'REMAINDER');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (260, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 1, 'BH_MAX_8_PCT', 'QUERY', '*', 'PCT', 8, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND (TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) LIKE ''Behavioral Health%'' OR (TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 1)) = ''Outpatient'' AND TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''''), '','', 2)) = ''Behavioral Health''))', 'BH never above 8 % of the sample, top-up included');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (261, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 2, 'PER_CPT_CODE', 'PER_VALUE', '*', 'CNT', 3, 'SELECT AUTHZN_ID, SRC_DATA_KEY, NULLIF(OREPLACE(CPT_PROC_CD, '' '', ''''), '''') AS CPT_PROC_CD FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows}', 'At most 3 cases of one CPT code list (spaces ignored; a blank list is not limited)');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (262, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 3, 'OON_PART_B', 'QUERY', 'Part B/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the Part B denials');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (263, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 4, 'OON_BH', 'QUERY', 'BH/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the BH denials');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (264, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 5, 'OON_DME', 'QUERY', 'DME/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the DME denials');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (265, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 6, 'OON_HHC', 'QUERY', 'HHC/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the HHC denials');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (266, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 7, 'OON_IPRF', 'QUERY', 'IP PAC/IPRF/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the IP PAC/IPRF denials');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (267, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 8, 'OON_LTAC', 'QUERY', 'IP PAC/LTAC/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the IP PAC/LTAC denials');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (268, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 9, 'OON_SNF', 'QUERY', 'IP PAC/SNF/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the IP PAC/SNF denials');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (269, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 10, 'OON_OP_MEDICAL', 'QUERY', 'OP Medical/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the OP Medical denials');

INSERT INTO HSLABBOSS.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Rule_Ty, Parent_Path_Txt, Max_Ty, Max_Val, Rule_Sql_Txt, Rule_Desc)
VALUES (270, 'UMCM', 'ODAG1', 'WKLY', 'CAP', 11, 'OON_IP_MEDICAL', 'QUERY', 'IP Medical/Denied', 'CNT', 10, 'SELECT AUTHZN_ID, SRC_DATA_KEY FROM HSLABBOSS.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 WHERE BTCH_ID = ''{batch_id}'' AND {current_rows} AND UPPER(TRIM(SVC_PROV_TY)) = ''NON PAR''', 'At most 10 out-of-network cases in the IP Medical denials');
