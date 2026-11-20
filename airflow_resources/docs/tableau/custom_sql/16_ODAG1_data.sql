SELECT o.*,
REGEXP_REPLACE(TRIM(CAST(o.SRC_DATA_KEY AS VARCHAR(40))), '[.]0*$', '', 1, 0, 'c') AS Src_Data_Key_Txt,
UPPER(TRIM(o.RUN_TY)) AS Odag1_Run_Ty,
UPPER(TRIM(o.ETL_IS_CURR_IND)) AS Odag1_Curr_Ind,
UPPER(TRIM(o.ETL_IS_DEL_IND)) AS Odag1_Del_Ind,
TRYCAST(SUBSTR(o.BTCH_ID, 1, 4) || '-' || SUBSTR(o.BTCH_ID, 5, 2) || '-' || SUBSTR(o.BTCH_ID, 7, 2) AS DATE) AS Odag1_Batch_Dt,
TRYCAST(OREPLACE(TRIM(o.AUTHZN_REQ_DT), '/', '-') AS DATE) AS Odag1_Request_Dt,
TRYCAST(OREPLACE(TRIM(o.DETRMTN_NOTFTN_DT), '/', '-') AS DATE) AS Odag1_Determination_Dt,
c.Rc_Start_Dt AS Odag1_Rpt_Start_Dt,
c.Rc_End_Dt AS Odag1_Rpt_End_Dt,
c.Rc_Version AS Odag1_Rpt_Version,
c.Rc_Req_Dt AS Odag1_Req_Dt
FROM HSLABBOSS.SDO_GOVT_RGLTRY_CLNCL_AUD_UM_AUTHZN_ODAG1 o
LEFT JOIN (
SELECT TRIM(Btch_ID) AS Rc_Btch_ID, UPPER(TRIM(Run_Ty)) AS Rc_Run_Ty, MIN(Rpt_Start_Dt_Key) AS Rc_Start_Dt, MAX(Rpt_End_Dt_Key) AS Rc_End_Dt, MAX(Rpt_Version) AS Rc_Version, MIN(Req_Dt_Key) AS Rc_Req_Dt
FROM HSLABBOSS.GOVT_RGLTRY_RequestControl
WHERE UPPER(TRIM(Project_Cd)) = 'UMCM' AND UPPER(TRIM(Table_Nm)) = 'ODAG1'
GROUP BY 1, 2
) c ON c.Rc_Btch_ID = TRIM(o.BTCH_ID) AND c.Rc_Run_Ty = UPPER(TRIM(o.RUN_TY))
