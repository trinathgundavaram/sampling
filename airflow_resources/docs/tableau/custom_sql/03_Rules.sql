SELECT CAST(t.Rule_ID AS INTEGER) AS Rule_ID, t.Rules_Project_Cd, t.Rules_Table_Nm, t.Rules_Run_Ty, t.Rule_Class_Cd, t.Seq_Nbr, t.Rule_Nm, t.Rule_Ty, t.Level_Nbr, t.Parent_Path_Txt, t.Quota_Cd, t.Quota_Val, t.Max_Ty, t.Max_Val, t.Lookback_Days, t.History_Scope_Cd, t.Rule_Desc, t.Active_Ind, t.Eff_Start_Dt, t.Eff_End_Dt, t.Rule_Sql_Txt, t.In_Effect, t.Rule_Cnt
FROM (
SELECT v.Rule_ID, v.Project_Cd AS Rules_Project_Cd, v.Table_Nm AS Rules_Table_Nm, v.Run_Ty AS Rules_Run_Ty, v.Rule_Class_Cd, v.Seq_Nbr, v.Rule_Nm, v.Rule_Ty, v.Level_Nbr, v.Parent_Path_Txt, v.Quota_Cd, v.Quota_Val, v.Max_Ty, v.Max_Val, v.Lookback_Days, v.History_Scope_Cd, v.Rule_Desc, v.Active_Ind, v.Eff_Start_Dt, v.Eff_End_Dt, v.Rule_Sql_Txt, v.In_Effect, v.Rule_Cnt
FROM (
SELECT g.Rule_ID, g.Project_Cd, g.Table_Nm, g.Run_Ty, g.Rule_Class_Cd, g.Seq_Nbr, g.Rule_Nm, g.Rule_Ty, g.Level_Nbr, g.Parent_Path_Txt, g.Quota_Cd, g.Quota_Val, g.Max_Ty, g.Max_Val, g.Lookback_Days, g.History_Scope_Cd, g.Rule_Desc, g.Active_Ind, g.Eff_Start_Dt, g.Eff_End_Dt, g.Rule_Sql_Txt, CASE WHEN g.Active_Ind = 1 AND CURRENT_DATE BETWEEN g.Eff_Start_Dt AND g.Eff_End_Dt THEN 'In effect' ELSE 'Not in effect' END AS In_Effect, 1 AS Rule_Cnt FROM HSLABBOSS.GOVT_RGLTRY_SamplingRule g WHERE UPPER(g.Project_Cd) IN ('UMCM', '*')
) v
) t
