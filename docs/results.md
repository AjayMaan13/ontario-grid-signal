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
| Full run with data-quality task | 6 tasks, all green, in about 24 seconds; `check_data_quality` found 0 violations after the repair | 6 Oct 2026 |

## Problems met and what they taught

- **Helm hooks deadlock with `--wait`**: the chart's migration job is a hook that Helm runs only after the wait finishes, but the pods wait for the migration. Fixed by running the setup jobs as plain jobs.
- **The chart's default scheduler log disk is 100 GB**: reduced to 2 GB. Logs also need the scheduler to be a StatefulSet, or the screen cannot fetch them.
- **BigQuery rate limit**: two writes per run to one small table (`429 too many table update operations`) when runs overlapped. Fixed with a single atomic `MERGE`.
- **A sensor does not pass its True/False to the next task** unless it returns `PokeReturnValue(xcom_value=...)`. A clean run was first recorded as `needs_attention`.
- **Marking a failed task as success hides the failure**: it left a run with no row. A failed run now records its own `needs_attention` row.

## The peak-risk signal (Weekend 6)

| Measurement | Value | Date |
|---|---|---|
| Held-out result, 45 minutes of notice | 10 of 10 peaks caught with 245 alert-hours over two years; flagging the same number of hours at random catches about 1 | 6 Oct 2026 |
| Held-out result, about 16 hours of notice | 8 of 10 peaks with 441 alert-hours | 6 Oct 2026 |
| Held-out result, about 2 h 45 min of notice | 7 of 10 peaks with 122 alert-hours | 6 Oct 2026 |
| Perfect-knowledge ceiling | 10 of 10 with 241 alert-hours: even a perfect forecast costs about 120 alert-hours a year, mostly early-season uncertainty | 6 Oct 2026 |
| Publication lag of an hour of demand | median 11.7 min; 135 of 137 normal hours within 20 min (107 hours excluded: archive-pause artifacts) | 6 Oct 2026 |
| Live evaluator | live decisions equal the backtest function's for every hour of 23 to 24 June 2025; the 2025 peak hour is flagged with 17.75 hours of notice | 6 Oct 2026 |

## Observability and autoscaling (Weekend 7)

| Measurement | Value | Date |
|---|---|---|
| Partition spread after the key fix | all 6 partitions of every topic populated (before: 5 of 6 empty) | 6 Oct 2026 |
| Drain of 57,897 events, 1 replica | 1,100 s (52.6 events/s) | 6 Oct 2026 |
| Drain of 57,910 events, KEDA 1 to 6 replicas | 192 s (301.6 events/s), 5.7 times faster; 4 replicas at 30 s, 6 at 45 s, back to 1 at 423 s | 6 Oct 2026 |
| Monitors | stale data and consumer lag fired (email and screenshot in `docs/images/`); the freshness test used a 10-minute threshold, then restored to 90 | 6 Oct 2026 |
| Producer-errors monitor | exercised with 5 deliberately malformed files: the backfill reported 6 failed files (the 5 plus the known empty 2021 file) against a threshold of 3 | 6 Oct 2026 |
| Tests | 114 with no cloud (about 35 s), 6 Airflow DAG tests, 17 on real BigQuery in a throwaway dataset, 1 on real Kafka in Docker | 6 Oct 2026 |

### More problems met and what they taught

- **A false lag alert, found by testing the alert.** After KEDA scaled the consumer in, the lag gauge counted partitions the consumer had not yet read from as entirely unread and reported about 57,000 events behind while Kafka's own number was 0. The gauge now falls back to the group's committed offset, with a regression test.
- **A load-test script that trusted an exit code.** `kafka-consumer-groups` prints an error and still exits 0 when it refuses to rewind an active group. The script now verifies the result instead of trusting the code.
- **A full laptop disk** (472 MB free) silently broke a CSV write. About 14 GB of regenerable caches fixed it. Worth checking before long runs.
- **The Datadog chart's Service Discovery** needs a `system-probe` container that cannot start on this node image; it is disabled in `k8s/datadog/values.yaml`.
- **A topic on one partition** (one zone, so one key) left nothing for extra consumers to share. The key now includes the delivery date.
