"""python scripts/plot_loadtest.py loadtest/baseline-*.csv loadtest/scaled-*.csv -> docs/images/loadtest.png"""
import csv
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def read(path):
    with open(path, newline="") as f:
        rows = [(int(r["elapsed_s"]), int(r["replicas"]), int(r["lag"])) for r in csv.DictReader(f)]
    return [r[0] / 60 for r in rows], [r[1] for r in rows], [r[2] for r in rows]


baseline, scaled = read(sys.argv[1]), read(sys.argv[2])
figure, (top, bottom) = plt.subplots(2, 1, figsize=(8, 6), sharex=True, gridspec_kw={"height_ratios": [3, 1.4]})

top.plot(baseline[0], baseline[2], color="#c0392b", label="1 replica (baseline)")
top.plot(scaled[0], scaled[2], color="#1f6feb", label="KEDA, 1 to 6 replicas")
top.set_ylabel("Events waiting in Kafka")
top.set_title("Draining 57,900 events: 1 replica vs autoscaled")
top.legend()
top.grid(alpha=0.3)

bottom.step(scaled[0], scaled[1], where="post", color="#1f6feb")
bottom.set_ylabel("Consumer replicas\n(autoscaled run)")
bottom.set_xlabel("Minutes since the consumer group was rewound")
bottom.set_yticks(range(0, 7))
bottom.grid(alpha=0.3)

figure.tight_layout()
figure.savefig("docs/images/loadtest.png", dpi=130)
print("saved docs/images/loadtest.png")
