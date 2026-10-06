# Measured results

Numbers from real runs on the real cluster, with the date they were taken. Nothing here is an estimate.

## Producer (Weekend 3)

| Measurement | Value | Date |
|---|---|---|
| Backfill from the archive | 6,698 of 6,699 files parsed in about 8 minutes; the 1 failure is the empty `ICIDemand_2021` file, counted and logged | 6 Oct 2026 |
| Events sent | 57,728, from the rows of those files (the checkpoint skipped the rest) | 6 Oct 2026 |
| Earlier backfill | 57,207 events from 524,977 parsed rows: about 89% of rows skipped as already sent | 5 Oct 2026 |
| Kafka after backfill | 38,827 ICIDemand + 7,568 predispatch + 11,333 realtime = 57,728, matching the events sent exactly | 6 Oct 2026 |
| Kill test | Producer pod deleted mid-run: 0 events re-sent after restart (all 678 files skipped) | 5 Oct 2026 |

## Consumer and BigQuery (Weekend 4)

| Measurement | Value | Date |
|---|---|---|
| Replay from zero | 58,762 events in 152 seconds (about 390 events a second, 12 batches of up to 5,000) | 5 Oct 2026 |
| Rebuilt tables | Identical row counts and checksums to before: 57,400 raw rows and 50,892 current rows | 5 Oct 2026 |
| Quality checks | 0 violations on all four (duplicate keys, current matches raw, values in range, no five-minute gaps) | 5 and 6 Oct 2026 |
| Version history | One forecast hour (2026-09-19 04:00 UTC) was published 27 times: raw holds all 27, current holds version 27 | 5 Oct 2026 |

## Reconciliation (Weekend 5)

| Measurement | Value | Date |
|---|---|---|
| Normal runs | 4 intervals (3 to 6 Oct), all `ok`; 3,018 / 3,332 / 3,670 / 3,984 files listed; 0 missed and 0 re-issued | 6 Oct 2026 |
| Seeded test | One finished hour (`RealtimeTotals_2026100311`: 12 files, 12 intervals) was removed from the producer's checkpoint and from BigQuery. The DAG found 12 `new_group` files, sent 12 events, confirmed 12 landed, status `ok`. | 6 Oct 2026 |
| Seeded test result | Both tables identical before and after: 57,782 raw rows and 51,149 current rows, same checksums | 6 Oct 2026 |
| Time to detect and fix | About 80 seconds from run start to the summary row (most of it the consumer's 60-second batch wait) | 6 Oct 2026 |
| Fastest healthy run | 5 tasks in about 27 seconds when there was nothing to fix | 6 Oct 2026 |

## Problems met and what they taught

- **Helm hooks deadlock with `--wait`**: the chart's migration job is a hook that Helm runs only after the wait finishes, but the pods wait for the migration. Fixed by running the setup jobs as plain jobs.
- **The chart's default scheduler log disk is 100 GB**: reduced to 2 GB. Logs also need the scheduler to be a StatefulSet, or the screen cannot fetch them.
- **BigQuery rate limit**: two writes per run to one small table (`429 too many table update operations`) when runs overlapped. Fixed with a single atomic `MERGE`.
- **A sensor does not pass its True/False to the next task** unless it returns `PokeReturnValue(xcom_value=...)`. A clean run was first recorded as `needs_attention`.
- **Marking a failed task as success hides the failure**: it left a run with no row. A failed run now records its own `needs_attention` row.
