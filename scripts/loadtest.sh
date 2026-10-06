#!/usr/bin/env bash
# Rewind the BigQuery consumer group to the start of Kafka and time how long it takes to catch up.
#   ./scripts/loadtest.sh baseline   pinned at 1 replica
#   ./scripts/loadtest.sh scaled     KEDA decides (1 to 6 replicas)
#   ./scripts/loadtest.sh lag        just print how far behind the consumer is, and change nothing
# Samples replicas and lag every 10 seconds into loadtest/<mode>-<time>.csv, then prints the summary.
set -euo pipefail
MODE=${1:?usage: loadtest.sh baseline|scaled|lag}
NS=grid
BOOT=grid-kafka-kafka-bootstrap.kafka:9092
GROUP=bq-sink
IMAGE=quay.io/strimzi/kafka:1.2.0-kafka-4.3.1

# Partition lines of `--describe`: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
# A partition the group has not committed yet shows "-" as CURRENT: it still has to read everything (from 0).
AWK_LAG='$3 ~ /^[0-9]+$/ {c = ($4 == "-" ? 0 : $4); s += $5 - c} END {print s + 0}'
AWK_END='$3 ~ /^[0-9]+$/ {s += $5} END {print s + 0}'

# A helper pod that stays up, so each sample is a quick exec and not a new pod.
kubectl -n kafka delete pod kafka-tools --ignore-not-found --wait=true >/dev/null 2>&1
kubectl -n kafka run kafka-tools --image="$IMAGE" --restart=Never --command -- sleep 7200 >/dev/null
kubectl -n kafka wait pod/kafka-tools --for=condition=Ready --timeout=180s >/dev/null
tools() { kubectl -n kafka exec kafka-tools -- bin/kafka-consumer-groups.sh --bootstrap-server "$BOOT" --group "$GROUP" "$@"; }
lag() { tools --describe 2>/dev/null | awk "$AWK_LAG"; }
total() { tools --describe 2>/dev/null | awk "$AWK_END"; }
replicas() { kubectl -n "$NS" get deployment consumer -o jsonpath='{.status.readyReplicas}'; }
cleanup() { kubectl -n "$NS" annotate scaledobject consumer autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1 || true
            kubectl -n kafka delete pod kafka-tools --ignore-not-found --wait=false >/dev/null 2>&1 || true; }

if [ "$MODE" = lag ]; then
  echo "events behind: $(lag) of $(total)"
  kubectl -n kafka delete pod kafka-tools --ignore-not-found --wait=false >/dev/null 2>&1
  exit 0
fi
trap cleanup EXIT   # whatever happens, never leave KEDA paused

# Safety: the test is only meaningful if the consumer starts fully caught up.
BEHIND=$(lag)
if [ "$BEHIND" -gt 0 ]; then
  echo "The consumer is still $BEHIND events behind (it is still catching up). Wait until: make kafka-lag shows 0."
  exit 1
fi

echo "1/4 stopping the consumer (KEDA paused at 0, and the deployment scaled to 0)"
kubectl -n "$NS" annotate scaledobject consumer autoscaling.keda.sh/paused-replicas="0" --overwrite >/dev/null
kubectl -n "$NS" scale deployment consumer --replicas=0 >/dev/null
kubectl -n "$NS" wait --for=delete pod -l app=consumer --timeout=180s >/dev/null 2>&1 || true

echo "2/4 rewinding the group to the start (Kafka only allows it once the group is empty; this checks it really worked)"
END=$(total)
for attempt in $(seq 1 18); do
  tools --reset-offsets --to-earliest --all-topics --execute >/dev/null 2>&1 || true   # its exit code is 0 even when it refuses
  NOW_LAG=$(lag)
  if [ "$NOW_LAG" -eq "$END" ]; then break; fi
  [ "$attempt" -eq 18 ] && { echo "the rewind did not take effect (lag $NOW_LAG, expected $END): the group is still active"; exit 1; }
  sleep 10
done
echo "    events to catch up on: $(lag) (all of them, as expected)"

echo "3/4 starting ($MODE)"
if [ "$MODE" = baseline ]; then
  kubectl -n "$NS" annotate scaledobject consumer autoscaling.keda.sh/paused-replicas="1" --overwrite >/dev/null
  TAIL=60
else
  kubectl -n "$NS" annotate scaledobject consumer autoscaling.keda.sh/paused-replicas- >/dev/null
  TAIL=420   # keep watching after the drain, to see KEDA scale back in
fi

OUT=loadtest/${MODE}-$(date -u +%Y%m%dT%H%M%SZ).csv
mkdir -p loadtest
echo "4/4 sampling every 10 seconds into $OUT (stops $TAIL s after the lag reaches zero, or after 40 minutes)"
echo "elapsed_s,replicas,lag" > "$OUT"
T0=$(date +%s)
DRAINED_AT=""
while true; do
  NOW=$(( $(date +%s) - T0 ))
  R=$(replicas); R=${R:-0}
  L=$(lag)
  echo "$NOW,$R,$L" | tee -a "$OUT"
  if [ "$L" -eq 0 ] && [ "$R" -ge 1 ] && [ -z "$DRAINED_AT" ]; then DRAINED_AT=$NOW; fi
  [ -n "$DRAINED_AT" ] && [ $((NOW - DRAINED_AT)) -ge "$TAIL" ] && break
  [ "$NOW" -ge 2400 ] && { echo "stopping after 40 minutes"; break; }
  sleep 10
done

echo; echo "== summary =="
PYTHONPATH=src python3 -m loadtest.summary "$OUT"
