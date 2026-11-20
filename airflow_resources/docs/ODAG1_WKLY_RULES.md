# ODAG1 weekly rules (UMCM / ODAG1 / WKLY)

The 44 rules of the ODAG1 weekly sample, as loaded by `local_hslabboss/ODAG1_WKLY_HSLABBOSS_dml.sql` and `ODAG1_WKLY_inserts.sql`. Full SQL and settings are in those files. Rules 19–21 and 27–44 split each area by determination, one set of three per area.

| Rule_ID | Rule_Nm | Class | Description |
|---|---|---|---|
| 1 | `batch_id` | PARAM | Use the latest batch whose sources are all closed, with its run type |
| 2 | `SELECTED_IN_LAST_90_DAYS` | EXCLUSION | Skip cases this sample already picked in the last 90 days |
| 3 | `REVISION` | EXCLUSION | Sample only revision 1: skip later revisions and rows with no revision number |
| 4 | `EXPEDITED` | PRIORITY | Pick expedited cases before standard ones |
| 5 | `functional_area` | LEVEL | Split the sample by functional area, read from the issue description; cases in no area are not sampled |
| 6 | `Part B` | BUCKET | Part B drug requests: 13% of the sample |
| 7 | `BH` | BUCKET | Behavioral health requests: 8% of the sample |
| 8 | `DME` | BUCKET | Outpatient durable medical equipment: 20% of the sample |
| 9 | `HHC` | BUCKET | Outpatient home health: 8% of the sample |
| 10 | `IP PAC` | BUCKET | Inpatient rehab, LTAC and SNF: 22% of the sample |
| 11 | `Concurrent Review` | BUCKET | Concurrent reviews: 3 cases of any determination, taken before the areas |
| 12 | `OP Medical` | BUCKET | Other outpatient requests: share what is left with IP Medical |
| 13 | `IP Medical` | BUCKET | Other inpatient requests: share what is left with OP Medical |
| 14 | `ip_pac_type` | LEVEL | Split IP PAC into IPRF, LTAC and SNF |
| 15 | `IPRF` | BUCKET | Inpatient rehab: an equal third of IP PAC |
| 16 | `LTAC` | BUCKET | Long-term acute care: an equal third of IP PAC |
| 17 | `SNF` | BUCKET | Skilled nursing: an equal third of IP PAC |
| 18 | `determination` | LEVEL | Split each area by determination: the buckets name the areas, so Concurrent Review is not split; cases with any other determination are not sampled |
| 19 | `Dismissed` | BUCKET | Part B dismissed cases: 2% of Part B, never fewer than 1 case |
| 20 | `Approved` | BUCKET | Part B approved cases: 13% of Part B |
| 21 | `Denied` | BUCKET | Part B denied cases: the rest of Part B |
| 27 | `Dismissed` | BUCKET | BH dismissed cases: 2% of BH, never fewer than 1 case |
| 28 | `Approved` | BUCKET | BH approved cases: 13% of BH |
| 29 | `Denied` | BUCKET | BH denied cases: the rest of BH |
| 30 | `Dismissed` | BUCKET | DME dismissed cases: 2% of DME, never fewer than 1 case |
| 31 | `Approved` | BUCKET | DME approved cases: 13% of DME |
| 32 | `Denied` | BUCKET | DME denied cases: the rest of DME |
| 33 | `Dismissed` | BUCKET | HHC dismissed cases: 2% of HHC, never fewer than 1 case |
| 34 | `Approved` | BUCKET | HHC approved cases: 13% of HHC |
| 35 | `Denied` | BUCKET | HHC denied cases: the rest of HHC |
| 36 | `Dismissed` | BUCKET | IP PAC dismissed cases: 2% of each of IPRF, LTAC and SNF, never fewer than 1 case |
| 37 | `Approved` | BUCKET | IP PAC approved cases: 13% of each of IPRF, LTAC and SNF |
| 38 | `Denied` | BUCKET | IP PAC denied cases: the rest of each of IPRF, LTAC and SNF |
| 39 | `Dismissed` | BUCKET | OP Medical dismissed cases: 2% of OP Medical, never fewer than 1 case |
| 40 | `Approved` | BUCKET | OP Medical approved cases: 13% of OP Medical |
| 41 | `Denied` | BUCKET | OP Medical denied cases: the rest of OP Medical |
| 42 | `Dismissed` | BUCKET | IP Medical dismissed cases: 2% of IP Medical, never fewer than 1 case |
| 43 | `Approved` | BUCKET | IP Medical approved cases: 13% of IP Medical |
| 44 | `Denied` | BUCKET | IP Medical denied cases: the rest of IP Medical |
| 22 | `BH_MAX_8_PCT` | CAP | No more than 8% of the sample from behavioral health, top-up included; concurrent reviews do not count |
| 23 | `PER_CPT_CODE` | CAP | No more than 3 cases with the same list of procedure codes |
| 24 | `OON_MAX_10` | CAP | No more than 10 out-of-network cases in the whole sample |
| 25 | `CAPPED_ROUTES_MAX_5` | CAP | No more than 5 cases in the whole sample with a procedure code routed as NOPA, Genetic, IPO List or Fast Cert (Clinical_Mgt_Route in SHRPA) |
| 26 | `EXPEDITED_MAX_75_PCT` | CAP | No more than 75% expedited cases in each area, with IPRF, LTAC and SNF counted separately |
