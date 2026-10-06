"""python -m loadtest.summary loadtest/scaled-20261007T140000Z.csv

Turns the samples a load test took (elapsed seconds, replicas, lag) into the numbers for the report.
"""
import csv
import json
import sys


def summarize(samples) -> dict:
    """samples: (elapsed_s, replicas, lag) in time order."""
    peak_lag = max(lag for _, _, lag in samples)
    drained = next((t for t, _, lag in samples if lag == 0 and t > 0), None)
    max_replicas = max(replicas for _, replicas, _ in samples)
    scaled_up = next((t for t, replicas, _ in samples if replicas >= 2), None)
    back_to_one = None
    if scaled_up is not None and drained is not None:
        peak_time = next(t for t, replicas, _ in samples if replicas == max_replicas)
        back_to_one = next((t for t, replicas, _ in samples if t > peak_time and replicas <= 1 and t >= drained), None)
    return {
        "peak_lag_events": peak_lag,
        "drain_seconds": drained,
        "events_per_second": round(peak_lag / drained, 1) if drained else None,
        "max_replicas": max_replicas,
        "first_scaled_up_at_seconds": scaled_up,
        "back_to_one_replica_at_seconds": back_to_one,
    }


def read(path):
    with open(path, newline="") as f:
        return [(int(r["elapsed_s"]), int(r["replicas"]), int(r["lag"])) for r in csv.DictReader(f)]


if __name__ == "__main__":
    print(json.dumps(summarize(read(sys.argv[1])), indent=1))
