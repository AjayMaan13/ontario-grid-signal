# IESO data notes (Weekend 0)

Measured against reports-public.ieso.ca on 28 Sep 2026. Fixtures in `tests/fixtures/` are real files, byte-for-byte.
Some checks below used extra files that were downloaded temporarily and are not in the repo; the evidence column says how to redo them.

## Check items from the build guide (Part 7)

| # | Question | Answer | Evidence |
|---|---|---|---|
| 1 | Timezone and hour convention | Fixed **EST (UTC-5) all year**, no DST. Hours are **hour-ending**: HE17 = 16:00-17:00 EST. | ICIDemand has exactly 24 rows on 2022-11-06 and 2023-03-12. File `Last-Modified` (GMT) minus 5h equals the `CREATED AT` header. Hourly mean of the 5-min `ONTARIO DEMAND` matches ICIDemand at shift 0 (corr 1.000, max diff 0.5 MW) but not at +/-1 hour (corr 0.92). |
| 2 | ICI demand field | Column `Ontario Demand`, MW, one row per (Date, Hour-ending), integer. It is the **rounded mean of the twelve 5-minute `ONTARIO DEMAND` values** in the RealtimeTotals XML. The Peak Tracker holds the unrounded value (22607.37 vs 22607). | Compared 168 hours (21-27 Sep): mean diff 0.0 MW. |
| 3 | Small 2024 tracker / tiny 2021 file | `ICIPeakTracker_2024.xml` is a snapshot from 4 May 2025 containing **1-4 May 2025** (status `Initial`), i.e. the wrong base period. **Unusable.** `ICIDemand_2021.csv` is header-only. **Unusable.** Valid IESO ground truth: **2022, 2023, 2025**. 2024 can only be derived from ICIDemand_2024, which is itself incomplete (ends 2025-04-29 HE14; missing 2024-10-18 HE14-15). | Open the two files. |
| 4 | Update cadence of ICI files | **Continuous, hourly** (at about :12 past, roughly 10 min after the hour ends). Versions v3519 (26 Sep) to v3571 (28 Sep) were one per hour. Last week's "burst" was a stale listing. Between those versions no earlier row changed; rows were only appended. | Directory listing timestamps; diff of v3519 vs v3571. |
| 5 | ETag / Last-Modified | **Both supported**, and `If-Modified-Since` returns **304**. Polling can be a cheap conditional GET. | `curl -I`, then `curl -I -H "If-Modified-Since: ..."`. |
| 6 | Retention | RealtimeTotals: about **31 days** of all versions. PredispTotals: all versions for only about **11 days** (from 17 Sep); older days keep the base file and last version only. The guide's "about a month" is wrong for PredispTotals. | Directory listings. |
| 7 | Is PredispTotals still publishing? | Yes. `_v14` for 28 Sep was published 09:11 EST. | Listing. |
| 8 | Airflow chart version, BigQuery quotas | **Chart 1.22.0, which ships Airflow 3.2.2** (needs Kubernetes 1.30+ and Helm 3.19+). **BigQuery limits how fast a single table may be changed**: two writes per run to one small table, with runs overlapping, returned `429 Exceeded rate limits: too many table update operations for this table`. Batch writes, or use one `MERGE`. | Checked 6 Oct 2026 against the chart and a real failure; see docs/results.md. |

## Things the guide got wrong or missed

- **ICI peaks are the highest hour of five different days**, not five hours. The tracker lists 10 days (highest first); ICI uses the top 5. Deriving "highest hour per day, top 5 days" from ICIDemand reproduces the IESO tracker exactly for 2022, 2023 and 2025.
- **The tracker ends with a placeholder row** (value 0, blank status). The parser drops it.
- **RealtimeTotals CSV has no Ontario demand.** Only the XML has `ONTARIO DEMAND`. The CSV `TOTAL LOAD` includes exports and averaged about 2.3 GW above Ontario demand (corr 0.86). Use the XML.
- **RealtimeTotals `_vN` holds N five-minute intervals** (v1 = interval 1 ... v12 = full hour). Each version is a strict prefix of the next, and v1 is published about 3 minutes *before* its interval starts. The base file equals v12.
- **PredispTotals has no Ontario demand**, only `TOTAL LOAD` (forecast, includes exports; about 2.2 GW above actual demand on 26 Sep).
- **`Adequacy3/` contains an hourly `ForecastOntDemand`** (Ontario demand forecast, published weeks ahead). On the delivery day it was within about 130-190 MW MAE of actual. Caveat: the "final" file for a day is written around 19:50 EST on that day, so it is not a clean forecast. Only archived intermediate versions would be. Worth a decision before Weekend 3.

## Restated files

- Three July 2026 hours in RealtimeTotals were re-issued on 25 Sep. For the one pair I could compare (2026-07-17 HE9, v11 vs v12) **the values are identical; only the `CREATED AT` header line differs.** n = 1, so do not generalise. Consequence: hash the **parsed values**, not the raw bytes, or a header-only change looks like a restatement.
- Base file equals the highest `_vN` in every case checked (RealtimeTotals 0928 HE8 and July hours, ICIDemand 2025).

## Format quirks the parsers handle

- ICIDemand: 3 comment lines, header on line 4, LF endings. RealtimeTotals/PredispTotals CSV: CRLF, header on line 5.
- Truncation limit: if a file is cut in the middle of a number (`15937` -> `159`) or exactly at a line boundary, no parser can tell. Only a row-count check against the expected number of hours can.
