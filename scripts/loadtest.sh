#!/usr/bin/env bash
# Rewind the BigQuery consumer group to the start of Kafka and time how long it takes to catch up.
#   ./scripts/loadtest.sh baseline   pinned at 1 replica
#   ./scripts/loadtest.sh scaled     KEDA decides (1 to 6 replicas)
# Samples replicas and lag every 10 seconds into loadtest/<mode>-<time>.csv, then prints the summary.
set -euo pipefail
MODE=${1:?usage: loadtest.sh baseline|scaled}
NS=grid
BOOT=grid-kafka-kafka-bootstrap.kafka:9092
GROUP=bq-sink
IMAGE=quay.io/strimzi/kafka:1.2.0-kafka-4.3.1
OUT=loadtest/${MODE}-$(date -u +%Y%m%dT%H%M%SZ).csv
mkdir -p loadtest

# A helper pod that stays up, so each lag sample is a quick exec and not a new pod.
kubectl -n kafka delete pod kafka-tools --ignore-not-found --wait=true >/dev/null 2>&1
kubectl -n kafka run kafka-tools --image="$IMAGE" --restart=Never --command -- sleep 7200 >/dev/null
kubectl -n kafka wait pod/kafka-tools --for=condition=Ready --timeout=180s >/dev/null
tools() { kubectl -n kafka exec kafka-tools -- bin/kafka-consumer-groups.sh --bootstrap-server "$BOOT" --group "$GROUP" "$@"; }
lag() { tools --describe 2>/dev/null | awk '$6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}'; }
replicas() { kubectl -n "$NS" get deployment consumer -o jsonpath='{.status.readyReplicas}'; }

echo "1/4 stopping the consumer (KEDA paused at 0 replicas)"
kubectl -n "$NS" annotate scaledobject consumer autoscaling.keda.sh/paused-replicas="0" --overwrite >/dev/null
kubectl -n "$NS" wait --for=delete pod -l app=consumer --timeout=180s >/dev/null 2>&1 || true

echo "2/4 rewinding the group to the start (waits until Kafka sees the group as empty)"
for attempt in $(seq 1 12); do
  if tools --reset-offsets --to-earliest --all-topics --execute >/dev/null 2>&1; then break; fi
  [ "$attempt" -eq 12 ] && { echo "could not reset offsets"; exit 1; }
  sleep 10
done
echo "    events to catch up on: $(lag)"

echo "3/4 starting ($MODE)"
if [ "$MODE" = baseline ]; then
  kubectl -n "$NS" annotate scaledobject consumer autoscaling.keda.sh/paused-replicas="1" --overwrite >/dev/null
  TAIL=60
else
  kubectl -n "$NS" annotate scaledobject consumer autoscaling.keda.sh/paused-replicas- >/dev/null
  TAIL=420   # keep watching after the drain, to see KEDA scale back in
fi

echo "4/4 sampling every 10 seconds into $OUT (stops $TAIL s after the lag reaches zero, or after 40 minutes)"
echo "elapsed_s,replicas,lag" > "$OUT"
T0=$(date +%s)
DRAINED_AT=""
while true; do
  NOW=$(( $(date +%s) - T0 ))
  R=$(replicas); R=${R:-0}
  L=$(lag)
  echo "$NOW,$R,$L" | tee -a "$OUT"
  if [ "$L" -eq 0 ] && [ -z "$DRAINED_AT" ]; then DRAINED_AT=$NOW; fi
  [ -n "$DRAINED_AT" ] && [ $((NOW - DRAINED_AT)) -ge "$TAIL" ] && break
  [ "$NOW" -ge 2400 ] && { echo "stopping after 40 minutes"; break; }
  sleep 10
done

kubectl -n "$NS" annotate scaledobject consumer autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1 || true
kubectl -n kafka delete pod kafka-tools --ignore-not-found --wait=false >/dev/null
echo; echo "== summary =="
PYTHONPATH=src python3 -m loadtest.summary "$OUT"
