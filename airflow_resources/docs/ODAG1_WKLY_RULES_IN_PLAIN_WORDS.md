# ODAG1 weekly sample: what each rule does

Each week the job picks **150 authorization cases** from the latest ODAG1 batch for the UM audit. This page says what each rule does, in plain words. The technical version, with names and settings, is [ODAG1_WKLY_RULES.md](ODAG1_WKLY_RULES.md). The diagram is [09_odag1_wkly.png](diagrams/png/09_odag1_wkly.png).

The rules work in this order:

1. Choose the batch.
2. Leave out cases that must not be sampled.
3. Put the urgent cases first.
4. Share the 150 places across the business areas and decisions.
5. Apply the limits (caps).

## 1. Which batch

| Rule | What it does |
|---|---|
| 1 | Uses the most recent batch whose data has fully arrived. |

## 2. Cases left out

| Rule | What it does |
|---|---|
| 2 | Leaves out any case this sample already picked in the last 90 days, so the same case is not audited twice. |
| 3 | Uses only the first version (revision 1) of each case. Later revisions, and rows with no revision number, are left out. |

## 3. Order of picking

| Rule | What it does |
|---|---|
| 4 | Picks expedited (urgent) cases before standard ones. |

## 4. How the 150 places are shared

**By business area (rule 5).** The area comes from the case's issue description. A case that fits no area is never picked.

| Rule | Area | Share of the 150 |
|---|---|---|
| 11 | Concurrent Review | 3 cases, taken first. These can have any decision and any area. |
| 6 | Part B drugs | 13% |
| 7 | Behavioral health (BH) | 8% |
| 8 | Durable medical equipment (DME) | 20% |
| 9 | Home health (HHC) | 8% |
| 10 | Inpatient rehab, long-term acute care and skilled nursing (IP PAC) | 22% |
| 12 | Other outpatient (OP Medical) | Shares what is left with IP Medical |
| 13 | Other inpatient (IP Medical) | Shares what is left with OP Medical |

**IP PAC split into three (rule 14).** An IP PAC case that is none of these three is never picked.

| Rule | Type | Share of IP PAC |
|---|---|---|
| 15 | Inpatient rehab (IPRF) | One third |
| 16 | Long-term acute care (LTAC) | One third |
| 17 | Skilled nursing (SNF) | One third |

**Each area split by decision (rule 18).** Concurrent Review is not split. A case with any other decision, such as pended or blank, is never picked.

| Rules | Area | Dismissed | Approved | Denied |
|---|---|---|---|---|
| 19, 20, 21 | Part B | 2% of the area, at least 1 case | 13% of the area | The rest |
| 27, 28, 29 | BH | Same | Same | Same |
| 30, 31, 32 | DME | Same | Same | Same |
| 33, 34, 35 | HHC | Same | Same | Same |
| 36, 37, 38 | IPRF, LTAC and SNF, each on its own | Same | Same | Same |
| 39, 40, 41 | OP Medical | Same | Same | Same |
| 42, 43, 44 | IP Medical | Same | Same | Same |

## 5. Limits (caps)

The caps are checked after the cases have been picked. They apply to the whole sample. If a picked case would go over any cap, it is skipped and the next case in the same group is taken instead. If the sample still ends up short, a top-up fills the gap, and the top-up obeys the same caps.

| Rule | Limit |
|---|---|
| 22 | Behavioral health is at most 8% of the sample (12 cases). Concurrent reviews do not count toward this. |
| 23 | At most 3 cases with the same list of procedure (CPT) codes. |
| 24 | At most 10 out-of-network (non-participating provider) cases. |
| 26 | At most 75% expedited cases in each area. IPRF, LTAC and SNF are counted separately. |

The next five caps come from the Source of Truth. Each one limits the cases with a given Clinical Management Route in SHRPA, at **5 cases a week per route**. A case counts toward a route's cap when **any** of its procedure codes has that route. If a case's codes have two different routes, it counts toward both caps.

| Rule | Route | Limit |
|---|---|---|
| 25 | NOPA – Approve PAR. Pend OON. | At most 5 cases |
| 45 | Genetic: Administratively Approve – No Network Review Required | At most 5 cases |
| 46 | IPO List. Approve and place in concurrent review if request received. | At most 5 cases |
| 47 | IPO List. Approve and place in Concurrent Review (without "if request received") | At most 5 cases |
| 48 | Fast Cert – Approve PAR. Pend OON. | At most 5 cases |

Each cap is a separate rule, so one route's cap can be switched off without changing the others.
