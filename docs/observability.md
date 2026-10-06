# Observability

The question that matters for a peak signal: **is the data fresh?** A silent producer means a missed peak, not a missing chart, so freshness is alerted first, then lag, then errors. If lag is zero while data is stale, the producer stopped; if lag is rising while the producer is healthy, the consumers cannot keep up. Two metrics together localise the fault.

## Metrics (sent by DogStatsD to the Datadog agent on each node)

| Metric | From | Meaning |
|---|---|---|
| `grid.consumer.data_age_s` | consumer | Seconds since IESO published the newest data the consumer has written to BigQuery. Keeps growing if data stops. |
| `grid.consumer.lag`, `grid.consumer.lag_total` | consumer | Events waiting, per partition and in total (per replica; sum across replicas) |
| `grid.consumer.up` | consumer | 1 per running replica: summing it counts the replicas |
| `grid.consumer.events_written`, `.batches`, `.events_invalid` | consumer | Throughput and bad messages |
| `grid.consumer.batch_seconds`, `.latency_s` | consumer | Time to write a batch; IESO publish to BigQuery for live events |
| `grid.producer.files_failed` | producer | Files that failed to parse |
| `grid.producer.data_age_s`, `grid.producer.cycles` | producer | Age of the newest IESO file seen; a heartbeat per poll |
| `grid.signals.rows_written`, `grid.signals.at_risk` | signals evaluator | Decisions written, and how many flagged an hour |
| `kafka.consumer_lag` | Datadog's Kafka consumer check | The same lag as seen from the broker |

## Monitors (infra/datadog, as code)

| Monitor | Fires when | Thresholds are variables |
|---|---|---|
| Data is stale | `data_age_s` above 90 minutes for 10 minutes, or **no data for 20 minutes** (the consumer is down) | `freshness_minutes` |
| Consumer is falling behind | total lag above 2,000 events for 5 minutes | `lag_events` |
| Producer failing to parse files | more than 3 failed files in 15 minutes | `failed_files` |

A replay from zero makes data age look stale until the replay reaches recent data: the gauge reflects the replay position, so expect the freshness monitor to fire during an intentional replay.

## Triggering each one on purpose (the guide's test)

| Monitor | How | Then |
|---|---|---|
| Lag | rewind the consumer group (the load test does this) | it fires after about 5 minutes of lag |
| Freshness | `kubectl -n grid scale deployment producer --replicas=0`, with `freshness_minutes=10` | fires after about 10 to 15 minutes; scale back up and restore 90 |
| Producer errors | put 5 malformed files in the raw bucket and run the backfill job | fires; delete the files |

## Lessons from testing the alerts

- The lag gauge originally reported a whole partition as lag when the consumer had not fetched from it yet, which happens right after a scale-in. That raised a false "consumer is falling behind" alert while Kafka's own lag was 0. The gauge now uses the group's committed offset when the position is unknown (see `Metrics.heartbeat` and its tests). Compare `grid.consumer.lag_total` with `kafka.consumer_lag` (Datadog's own check) when in doubt: they should agree.
