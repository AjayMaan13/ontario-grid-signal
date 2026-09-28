# Ontario Grid Signal: Full Build & Learning Guide

*Version 2, 24 Sep 2026. Expands the skeleton plan into a weekend-by-weekend guide. Everything marked **verified** was checked live against the IESO reports site or official docs tonight; anything marked **check** is a Weekend 0 task, not a fact.*

---

## How to use this document

Each weekend has the same shape:

1. **Goal**: the one thing that must be true by Sunday night.
2. **Learn first**: topics to study *before* touching the keyboard, with realistic hours for someone starting from foundational GKE.
3. **Concepts, in plain language**: the "why", so you can explain it in an interview without notes.
4. **Build**: the work, in order. Key commands are included where they teach something; this is not a copy-paste script.
5. **Testing thread**: what you test and how. This is deliberate: SDET is your #1 target, and the original skeleton had almost no testing story. Every weekend now produces test evidence.
6. **Done when**: a checklist. If it isn't all ticked, the weekend isn't done.
7. **Pitfalls** and **interview questions you should be able to answer**.

One mental model runs through the whole project and is worth learning once, deeply:

> **Desired state vs. actual state, reconciled in a loop.** Terraform compares your `.tf` files to real cloud resources and plans the difference. Kubernetes controllers compare a Deployment's spec to running pods and fix the gap. Strimzi (an operator) compares a `Kafka` custom resource to real brokers. Airflow compares "which DAG runs should exist" to "which have run". Your own revision-reconciliation DAG compares "what IESO has published" to "what BigQuery holds". Same idea, five times. If you can explain this pattern, you can explain the whole architecture.

---

## Part 1: What changed after tonight's verification

I checked the IESO public reports directory and several sub-directories live. Three findings change the plan, one of them substantially.

### 1.1 The core signal is demand, not price (substantial change)

ICI peaks are defined by **Ontario demand**, not price. The skeleton's producer ingested `RealtimeOntarioZonalPrice`, which is useful context but not the thing you're predicting. The demand source named in the skeleton, `RealtimeDemandZonal`, only appends once a day, so it can't drive a live signal. The directory listing turned up better sources:

| Report | What it is | Cadence / retention observed tonight | Role in project |
|---|---|---|---|
| `ICIDemand/` | IESO's "ICI Ontario Demand Report" | One cumulative CSV per ICI base period, re-issued roughly hourly as `_vNNNN` versions. Annual files back to 2021. **Verified.** | **Primary signal + historical backtest data.** This is the demand series the ICI program itself is measured on. |
| `ICIPeakTracker/` | IESO's own top-peak tracker | One XML per base period; finals for 2022–2025 plus current 2026. **Verified.** | **Ground truth** for validation. |
| `RealtimeTotals/` | Hourly totals report (5-min intervals inside) | One file per hour, published ~:54 past the hour, with `_vNN` versions. Hourly files retained **only ~30 days** (oldest tonight: 24 Aug). **Verified.** | Live, finer-grained demand for the streaming path. |
| `PredispTotals/` | Pre-dispatch (forecast) totals | One file per delivery day, re-issued ~hourly from ~20:00 the evening before (`_v1`) through the day (`_v25`–`_v27`). **Verified.** | The forward-looking "decide now" signal. |
| `PredispHourlyOntarioZonalPrice/` | Forecast price | Same daily-file, hourly-version pattern as above. **Verified.** | Optional context. Closes open item #3. |
| `PredispMktPrice/` | Old predispatch price report | **Empty.** Last touched 1 Jun 2025, i.e. retired with Market Renewal (May 2025). **Verified.** | Drop it. |
| `RealtimeDemandZonal/` | 5-min zonal demand, cumulative yearly CSV | Appended once daily (~07:31). | Backfill only. |

**Open item #3 is resolved:** predispatch price publishes one file per delivery day, reissued as a new version roughly every hour, starting the evening before. `PredispMktPrice` is dead.

### 1.2 "Revisions" are two different things, and your schema must treat them differently

The skeleton treats all revisions as corrections to be upserted over. In fact there are two kinds:

- **Restatements of actuals** (realtime/settlement data corrected days or weeks later, like the July price files you found). The newest version *replaces* the old one. `MERGE` is the right tool.
- **Forecast versions** (predispatch `_v1` → `_v27`). These are *not* corrections. Each version is a separate observation: "at 14:09, IESO forecast X for hour 17." If you upsert over them, you destroy exactly the information you need to answer "how early could a customer have known?"

The fix is a standard, very interviewable pattern: **two tables per report**.

- `raw_*_versions`: append-only, keyed `(report, interval_start, zone, version)`, with `published_at` (when IESO published it) and `ingested_at` (when you saw it). Nothing is ever overwritten.
- `current_*`: one row per `(interval_start, zone)`, maintained by `MERGE`, holding the latest version.

This is a lightweight form of **bitemporal modelling**: *valid time* (which interval the value describes) versus *knowledge time* (when the value became known). It's also what makes an honest backtest possible (Part 3, Weekend 6).

### 1.3 Correction #4 in your brief is only partly right

"No always-on component is needed because the archive goes back over a year" holds for **actuals**: annual cumulative CSVs (`ICIDemand`, `RealtimeDemandZonal`) cover years. It does **not** hold for:

- **Hourly files**: `RealtimeTotals` keeps ~30 days.
- **Forecast version history**: for predispatch days older than ~a month, the listing kept only the base file and the final version. The intermediate `_v1`–`_v26` versions are gone.

So intermediate forecast history **cannot be backfilled**. Every week without a collector running is forecast history lost for good. This leads to Decision A below.

### 1.4 Other verified facts that shape the design

- **Timestamps appear to be EST year-round.** At 23:04 EDT tonight, the newest `RealtimeTotals` file was stamped 22:04. **Check** this in the file headers. IESO also uses the *hour-ending* convention (HE1 = 00:00–01:00). Get both wrong and every peak lands in the wrong hour.
- **ICI base period is May 1 – April 30**, with Class A customers billed on their share of the top five peak hours in that window.
- **The current base period (May 2026 – Apr 2027) won't be final until after 30 April 2027.** You cannot validate against this year's final peaks inside the project timeline. The backtest must use the finished 2022–2025 base periods.
- **Your ground truth is small.** Five peaks per year × at most four finished years = **≤ 20 positive hours**. The `ICIPeakTracker_2024` file is 3.1 KB versus ~11 KB for the others, and `ICIDemand_2021` is 134 bytes, so some years may be incomplete or unusual. (The 2020–21 base period was an ICI "peak hiatus" during COVID.) **Check** each year before trusting it. Report accuracy with that sample size stated plainly.
- **Market Renewal (May 2025) changed report names.** Price history before May 2025 lives in different reports with different formats. Demand-based backtesting via `ICIDemand` avoids that discontinuity, which is another reason demand should be the core.
- Both ICI files showed an hourly version burst (Peak Tracker 15–17 Sep; ICIDemand 18–20 Sep) and then **no base-file update after 17 Sep / 20 Sep**, as of tonight. **Check** whether they update continuously, only during certain periods, or whether tonight's listing was stale.

### Decisions you need to make

**Decision A: a tiny always-on raw archiver, yes or no?** *(Recommended: yes, starting Weekend 1.)*
A Cloud Scheduler job triggers a small Cloud Run job hourly. It copies new files from 3–4 IESO directories into a GCS bucket, untouched. This is not part of the GKE stack, which still gets destroyed between sessions. It is a raw-data landing zone: the bucket becomes your replayable source of truth, and the Kafka producer can replay from GCS as easily as from IESO. One-sentence justification: *"IESO prunes intermediate forecast versions after ~30 days, so I archive raw files continuously and cheaply, and the expensive cluster only runs when I'm working."* It adds real GCP depth (Cloud Run, Scheduler, IAM) and is interview-defensible. Cost should sit within free-tier usage. **Check** in the pricing calculator.

**Decision B: GKE Standard (zonal) or Autopilot?** *(Recommended: Standard, zonal.)*
Both get the cluster management fee covered by the GKE free tier (one cluster). Standard makes you deal with nodes, requests and limits, and node pools, which is the Kubernetes depth you want to be able to talk about. Autopilot hides that.

**Decision C: schema-registry or not?** *(Recommended: not.)*
Use JSON events validated against a JSON Schema in code and in tests. A registry is another stateful service to run for little gain at this scale. Say so if asked.

---

## Part 2: Architecture v2

```mermaid
flowchart LR
  IESO[(IESO public reports<br/>reports-public.ieso.ca)]
  subgraph Always-on, tiny
    SCH[Cloud Scheduler<br/>hourly] --> ARC[Cloud Run job:<br/>raw archiver]
  end
  IESO --> ARC --> GCS[(GCS raw landing<br/>immutable files)]
  subgraph GKE cluster, up only during work sessions
    PROD[Producer<br/>parse + emit events] --> K[(Kafka via Strimzi<br/>KRaft, 1 node)]
    K --> CONS[BQ sink consumer<br/>micro-batch MERGE]
    K --> SIG[Signal evaluator<br/>peak-risk rule]
    AF[Airflow<br/>nightly reconciliation DAG] --> K
    KEDA[KEDA] -. scales on lag .-> CONS
    DD[Datadog agent] -. metrics .-> PROD & K & CONS & AF
  end
  GCS --> PROD
  IESO --> PROD
  CONS --> BQ[(BigQuery<br/>raw_versions + current)]
  SIG --> BQ
  AF --> GCS
  TF[Terraform<br/>state in GCS] -. provisions .-> GKE cluster & BQ & GCS
```

### Honest justification table

Every tool gets one sentence, plus the question an interviewer will poke at. If you can't answer the poke, cut the tool.

| Tool | One-sentence justification | The poke (have an answer ready) |
|---|---|---|
| Terraform | The whole environment is created and destroyed every session, so it has to be reproducible from code. | "Why not click-ops?" Show your bring-up time, and that `destroy` leaves nothing behind. |
| GKE | Hosts the stateful streaming stack (Kafka, Airflow) and the scalable consumers in one place. | "Why not Cloud Run for everything?" Kafka and Airflow are long-running and stateful. Cloud Run *is* used where it fits (the archiver). |
| Kafka (Strimzi) | Decouples ingestion from multiple consumers (BQ sink and signal evaluator) and lets me replay history by offset. | "Your volume is tiny, isn't Kafka overkill?" Yes on volume. Say so. The reason is replay plus fan-out plus decoupling. Don't claim throughput needs you don't have. |
| BigQuery | Serverless SQL warehouse for the time-series and the backtest. | "Why MERGE, not append?" Restatements. "Why not MERGE per message?" DML is built for batches (Weekend 4). |
| Airflow | A scheduled, dependency-ordered batch job (reconciliation) that is a different kind of work from streaming. | "Why not a cron job?" Retries, backfill over date ranges, run history, visibility. |
| KEDA | Scales consumers on Kafka lag during backfill bursts, and back to one at rest. | "Does it ever scale in steady state?" No: ~12 events/hour. It earns its place during replay. Say that. |
| Datadog | Freshness, lag and failure alerting. A silent producer means a missed peak, not a missing chart. | "What would you alert on?" Data freshness first, then lag, then errors. |
| Cloud Run + Scheduler | Cheap continuous raw capture, because forecast versions get pruned. | "Why separate from GKE?" Cluster lifecycle ≠ data lifecycle. |

**Volume reality check** (use these numbers when asked): 5-minute Ontario demand is 288 values per day, ~105,000 per year. Zonal adds roughly a factor of ten. The load test replays real history at high speed; it does not simulate volume you don't have.

---

## Part 3: Cost and safety rails (do these before Weekend 1)

- **Billing alerts first.** Create budget alerts (for example $25 / $50 / $100) the moment the billing account exists. Alerts don't stop spending, so also note where to find the "disable billing" switch.
- **The $300 trial clock starts when you create the billing account.** Don't create it until Weekend 1. The 8-weekend plan (~56 days) fits the ~90-day window, but not with much slack if you add the always-on archiver early.
- **GKE free tier** covers the management fee for **one** zonal or Autopilot cluster per billing account. It doesn't cover nodes, disks, load balancers or egress. Never run two clusters.
- **Orphan resources are the #1 surprise bill.** Deleting a cluster can leave behind persistent disks created by PVCs (Kafka and Airflow's Postgres both create them) and load balancers from `Service type: LoadBalancer`. After every `terraform destroy`, run your own check: `gcloud compute disks list`, `gcloud compute forwarding-rules list`, `gcloud compute addresses list`. Better still, script it into `make down`.
- **Spot VMs** for the node pool cut compute cost substantially. Preemption can kill Kafka mid-session. That's acceptable in dev, and it's a good talking point about resilience.
- **Datadog:** the GitHub Student Developer Pack includes Datadog Pro (10 hosts, 2 years). Activate it with your Seneca email *before* Weekend 7, since verification can take days.

---

## Part 4: Weekend-by-weekend guide

### Weekend 0: Data investigation and design (no cloud spend)

**Goal:** You understand the IESO data well enough to write a parser with golden-file tests, you have a working peak-risk rule in pandas, and your BigQuery schema is designed. You will have spent $0.

**Learn first (~6–8 h)**

| Topic | Hours | Why now |
|---|---|---|
| ICI mechanics: base period, top-5 peaks, peak demand factor | 1 | You need to explain the business problem in 60 seconds. |
| IESO report conventions: hour-ending, 5-min intervals, EST, version suffixes | 1.5 | Every downstream bug traces back here. |
| Time-series handling in pandas: tz-aware timestamps, resampling, rolling windows | 2 | For the rule prototype. |
| BigQuery fundamentals: datasets, partitioning, clustering, cost model (bytes scanned) | 2 | Schema design this weekend. |
| pytest fixtures and golden-file testing | 1 | Starts your testing thread. |

**Concepts**

- **Hour-ending vs hour-beginning.** "HE17" means the hour that *ends* at 17:00 (16:00–17:00). Normalise everything to `interval_start` (UTC) at parse time, and keep the original HE and interval fields for traceability. Convert once, at the edge, never in the middle of the pipeline.
- **Why EST-year-round matters.** On DST-change days an EST-based market day is not 24 local hours in Toronto time. Store UTC internally. Test the two DST-transition days explicitly.
- **Golden files.** Save real IESO files (including an old version and a restated version of the same hour) into `tests/fixtures/`. Your parser tests assert exact expected output for those files. They're real data, frozen, so the "no fake data" rule still holds.

**Build**

1. Download (by hand or with a small script, run locally) for **one** recent week: `RealtimeTotals` hourly files, `PredispTotals` versions for 2–3 days, the current `ICIDemand` and `ICIPeakTracker` files, and the 2022–2025 annual `ICIDemand` and `ICIPeakTracker` files.
2. Answer the **check** items from Part 1 and write the answers into `docs/data-notes.md`:
   - Timezone and hour-ending convention, confirmed from file headers or IESO docs.
   - What the ICI demand column is called and exactly what it measures.
   - Why the 2024 Peak Tracker file is small, and whether 2021 is usable.
   - Whether the ICI files update continuously or in bursts.
   - Whether the IESO server returns `ETag`/`Last-Modified` headers. This decides your polling design in Weekend 3.
   - Whether a restated actuals file changes values, or only metadata.
3. Write parsers (`parsers/realtime_totals.py`, etc.) that output a list of typed records (dataclasses or pydantic).
4. Prototype the peak-risk rule in a notebook against 2022–2025 `ICIDemand` (see Weekend 6 for the rule design). You're only checking that it's feasible, not tuning it.
5. Design the BigQuery schema (below) and write it as SQL DDL in the repo.
6. Scaffold the repo: `infra/` (Terraform), `k8s/` (manifests/Helm values), `src/`, `dags/`, `tests/`, `docs/`, and a `Makefile` with `up`, `down`, `test` targets (empty for now). Set up GitHub Actions to run `pytest` on every push.

**Schema sketch**

```sql
-- Append-only: every version of every interval, forever.
CREATE TABLE grid.raw_demand_versions (
  report          STRING,     -- 'RealtimeTotals', 'PredispTotals', 'ICIDemand'
  interval_start  TIMESTAMP,  -- UTC, normalised
  zone            STRING,     -- 'ONTARIO' for totals
  version         INT64,      -- from _vNN; base file = resolved to its version
  value_mw        FLOAT64,
  published_at    TIMESTAMP,  -- IESO Last-Modified
  ingested_at     TIMESTAMP,  -- when you saw it
  source_uri      STRING,     -- gs:// path of the raw file (lineage)
  content_sha256  STRING
)
PARTITION BY DATE(interval_start)
CLUSTER BY report, zone;

-- Latest known value per interval, maintained by MERGE.
CREATE TABLE grid.current_demand ( ... same columns ... )
PARTITION BY DATE(interval_start)
CLUSTER BY report, zone;
```

**Testing thread:** parser golden-file tests, including one restated pair, the two DST days, and one malformed or truncated file (the parser must fail loudly, not silently return zero rows).

**Done when**
- [ ] `docs/data-notes.md` answers every check item, with evidence.
- [ ] Parsers pass golden tests in CI.
- [ ] Notebook shows the rule running over at least one full historical base period.
- [ ] DDL committed. Decisions A, B and C recorded in `docs/decisions.md` (a short ADR per decision).

**Pitfalls:** parsing with naive datetimes; assuming the base file and the highest `_vNN` are identical without checking; tuning the rule on all four years (that's leakage — see Weekend 6).

**Interview questions:** *"How do you handle time zones in a pipeline?"* *"What's a golden-file test and when is it better than a mock?"* *"Why partition by interval date and not ingestion date?"*

---

### Weekend 1: Infrastructure foundation with Terraform

**Goal:** `make up` creates the GCP environment from nothing and `make down` removes it completely, both repeatably. If you chose Decision A, the raw archiver is running.

**Learn first (~9–11 h)**

| Topic | Hours |
|---|---|
| Terraform core: providers, resources, data sources, variables, outputs, `plan`/`apply`/`destroy` | 3 |
| Terraform state: what it is, remote backends, locking, why you never edit it by hand | 1.5 |
| Terraform modules and project layout | 1 |
| GCP IAM: principals, roles, service accounts, least privilege | 2 |
| VPC basics: subnets, secondary ranges (pods/services), private clusters vs public endpoint | 1.5 |
| GKE: control plane vs nodes, node pools, Workload Identity | 2 |

**Concepts**

- **Terraform is a reconciler.** Your `.tf` files are desired state. The state file is Terraform's memory of what it created. `plan` diffs desired against remembered-and-refreshed actual. Lose the state file and Terraform no longer knows it owns those resources, so it will try to create duplicates. That's why state lives in a GCS bucket with versioning, and why that bucket is **never** destroyed.
- **The bootstrap problem.** The state bucket can't be managed by the same Terraform config that stores its state in it. Standard answer: a tiny separate `infra/bootstrap/` config with *local* state creates the state bucket once; everything else uses the GCS backend. The GCS backend supports state locking, so two `apply`s can't collide.
- **Workload Identity** lets a Kubernetes service account act as a Google service account, with no JSON key files. Your consumer pod gets BigQuery write access through identity, not a secret. Key files in repos are a classic security finding; say you avoided them and why.
- **VPC-native clusters** give pods real VPC IPs from a secondary range. You need to size those ranges up front, so you're learning subnetting for a real reason.

**Build**

1. `infra/bootstrap/`: state bucket (versioning on, uniform access). Apply once.
2. `infra/main/` with the GCS backend. Resources:
   - VPC and subnet with secondary ranges for pods and services.
   - GKE Standard zonal cluster; remove the default node pool; add a managed node pool (2–3 × e2-medium or similar, optionally Spot). Workload Identity enabled.
   - Service accounts: `sa-producer`, `sa-consumer` (BigQuery dataEditor on the dataset only), `sa-airflow`, `sa-archiver` (GCS writer on the raw bucket only).
   - BigQuery dataset (tables can come later via DDL or Terraform; pick one and be consistent).
   - GCS raw-landing bucket (**persistent**: not destroyed with the cluster; use `prevent_destroy` or a separate config).
   - If Decision A: Cloud Run job and Cloud Scheduler trigger for the archiver.
3. `Makefile`: `up` = `terraform apply` + `gcloud container clusters get-credentials`; `down` = `terraform destroy` + the orphan-resource check script.
4. Time a full `up`→`down` cycle. Write the number down; it's your first real metric.

**Testing thread:** in CI, run `terraform fmt -check`, `terraform validate`, and a static analyser (`tflint`, and `checkov` or `trivy config` for security misconfigurations). Infrastructure-as-code testing is a real SDET/platform skill and is easy to show.

**Done when**
- [ ] Two consecutive clean `up`/`down` cycles, with no orphans reported.
- [ ] `kubectl get nodes` works after `up`.
- [ ] No service-account key files anywhere.
- [ ] Budget alerts active. Archiver writing files to GCS (if chosen).

**Pitfalls:** forgetting `deletion_protection` defaults on the GKE resource (destroy fails); pod CIDR too small; granting `roles/editor` "for now".

**Interview questions:** *"What happens if two people run `terraform apply` at once?"* *"How do pods authenticate to GCP without keys?"* *"How do you stop Terraform deleting something important?"*

---

### Weekend 2: Kafka on Kubernetes with Strimzi

**Goal:** A single-node KRaft Kafka cluster runs on GKE, managed by Strimzi. Topics are declared as YAML. You can produce and consume a test message from inside the cluster and explain every moving part.

**Learn first (~11–13 h). The heaviest learning weekend; start reading midweek.**

| Topic | Hours |
|---|---|
| K8s objects at depth: Pod, Deployment, **StatefulSet**, Service (ClusterIP/headless), ConfigMap, Secret, Namespace | 3 |
| Storage: PersistentVolume, PVC, StorageClass, why StatefulSets pair with PVCs | 1.5 |
| Requests/limits, QoS classes, OOMKilled, scheduling basics | 1.5 |
| **Operator pattern** and Custom Resource Definitions (CRDs) | 1.5 |
| Kafka fundamentals: log, topic, partition, offset, producer acks, consumer group, rebalancing, retention vs **compaction** | 3.5 |
| KRaft: controllers vs brokers, why ZooKeeper is gone | 1 |
| Helm basics (charts, values, releases) | 1 |

**Concepts**

- **Kafka is a log, not a queue.** A topic is split into partitions. Each partition is an append-only, ordered log, and each message gets an increasing **offset**. Consumers don't delete messages; they remember how far they've read. That's what makes **replay** possible: reset a consumer group's offset and reprocess history. That property is your main justification for Kafka.
- **Ordering is per partition only.** Messages with the same key go to the same partition, so they stay ordered. Key your events by `(report, zone)` so versions of the same series arrive in order.
- **Consumer groups.** Consumers sharing a `group.id` split a topic's partitions between them, and each partition is read by exactly one consumer in the group. So **max useful consumers = number of partitions**. This is why KEDA can't scale past your partition count (Weekend 7). Choose partitions now (6 is plenty) with that in mind.
- **Compaction.** A compacted topic keeps at least the *latest* message per key and eventually discards older ones. A compacted topic keyed by `(report, interval, zone)` naturally models "current value per interval": a Kafka-native version of your `current_*` table. You don't have to use it, but understanding why it exists is a strong interview answer.
- **KRaft.** Kafka used to keep cluster metadata in a separate ZooKeeper ensemble. KRaft moves that into Kafka itself using the Raft consensus protocol, run by *controller* nodes. Kafka 4.0 is KRaft-only, and Strimzi removed ZooKeeper support in 0.46. For your cluster, one node plays **both** controller and broker roles (a "dual-role" node pool). Fine for dev; say plainly it isn't fault-tolerant.
- **Operators.** A CRD teaches Kubernetes a new noun (`Kafka`, `KafkaTopic`, `KafkaNodePool`). An operator is a controller that watches those nouns and does the operational work: creating StatefulSet-like pods, certificates, config and rolling restarts. It's the desired-vs-actual loop again, with Kafka expertise encoded in software.

**Build**

1. Install the Strimzi operator (Helm or its install YAML) into namespace `kafka`. Watch its pod logs to see a reconciler starting up.
2. Apply a `KafkaNodePool` (1 replica, roles: controller + broker, persistent-claim storage, modest size) and a `Kafka` resource that uses it. Keep replication factors at 1, since there's one broker.
3. Declare topics as `KafkaTopic` resources, for example `ieso.demand.realtime`, `ieso.demand.predispatch`, `ieso.demand.ici`, `ieso.reconciliation.republish`, and `grid.signals`. Set partitions, retention and cleanup policy in YAML. Topics now live in Git with everything else.
4. Smoke test from a throwaway pod using the Kafka console producer and consumer. Then deliberately kill the broker pod and watch Strimzi and the PVC bring it back **with data intact**.
5. Set resource requests and limits on the broker. Watch what happens if the limits are too low; you'll meet `OOMKilled` on purpose rather than by surprise.

**Testing thread:** a small pytest integration suite that runs against the in-cluster Kafka (through `kubectl port-forward`) to produce a keyed message, consume it, and assert key, partition stability and payload. Locally, the same tests can run against **Testcontainers**' Kafka module, so CI can exercise Kafka code without GKE.

**Done when**
- [ ] Kafka and topics declared in Git; `make up` installs them.
- [ ] Broker survives pod deletion with messages retained.
- [ ] Integration tests pass locally (Testcontainers) and against GKE.
- [ ] You can draw topic → partitions → consumer group on a whiteboard from memory.

**Pitfalls:** replication factor > 1 with one broker (topic creation fails); PVCs orphaned on destroy (Part 3); confusing Strimzi versions in blog posts written before 0.46 (they use ZooKeeper YAML).

**Interview questions:** *"How does Kafka guarantee ordering?"* *"What happens when a consumer joins a group?"* *"Retention vs compaction?"* *"What does an operator do that a Helm chart doesn't?"* (A chart installs once; an operator keeps managing, continuously.)

---

### Weekend 3: Producer and historical backfill

**Goal:** A producer on GKE detects new IESO files (live, or from the GCS archive), parses them, and emits one event per interval to Kafka, without duplicating work after a restart. A backfill job replays history through the same code path.

**Learn first (~6–7 h)**

| Topic | Hours |
|---|---|
| Polling design: conditional GETs (`ETag`, `If-Modified-Since`), backoff, jitter, being polite to a public server | 1.5 |
| Producer semantics: `acks`, retries, **idempotent producer**, delivery guarantees | 2 |
| Event design: envelope vs payload, schema versioning, JSON Schema | 1.5 |
| Containerising Python well: slim images, non-root user, config via env | 1 |

**Concepts**

- **Delivery semantics.** *At-most-once* can lose data. *At-least-once* can duplicate. *Exactly-once* end-to-end is hard. The practical industry answer, and yours, is **at-least-once delivery plus idempotent processing**: duplicates are harmless because the sink upserts by a natural key (Weekend 4). Be able to say this in one breath.
- **Idempotent producer** (`enable.idempotence=true`) stops Kafka *itself* writing duplicates when the producer retries after a network blip. It doesn't stop *your code* re-sending a file after a crash. That's what the checkpoint below is for.
- **Checkpointing.** The producer must remember which `(file, version, sha256)` it has already emitted. Store this outside the pod (a small BigQuery table or a GCS object), because pods are disposable. That's why the job survives restarts.
- **Event envelope.** Each event carries metadata alongside the measurement:

```json
{
  "schema_version": 1,
  "report": "RealtimeTotals",
  "interval_start": "2026-09-24T21:05:00Z",
  "zone": "ONTARIO",
  "version": 12,
  "value_mw": 17234.5,
  "published_at": "2026-09-24T22:04:00Z",
  "ingested_at": "2026-09-24T22:06:13Z",
  "source_uri": "gs://.../PUB_RealtimeTotals_2026092422_v12.xml",
  "content_sha256": "…",
  "reason": "live"          // or "backfill" / "reconciliation"
}
```

`published_at` versus `ingested_at` gives you **ingestion latency** as a real, measured metric. `reason` lets you tell backfill traffic apart from live traffic on every dashboard.

**Build**

1. Producer as a K8s **Deployment** (one replica): loop → list new files → conditional-GET → sha check → parse (Weekend 0 parsers) → validate against JSON Schema → produce (keyed by `report|zone`) → write checkpoint.
2. **Source abstraction:** the same producer reads from either `IESOSource` (HTTPS) or `GCSSource` (archive). Backfill = run it as a K8s **Job** pointed at the archive and the annual CSVs. One code path means backfill genuinely tests the live path.
3. Backfill order: annual `ICIDemand` files 2022–2025 first (needed for Weekend 6), then whatever hourly and forecast history the archiver has captured.
4. Emit metrics now, even before Datadog exists: counters for files seen, parsed, skipped, failed, and events produced, plus a latency histogram. Logging structured JSON lines is enough for this weekend.

**Testing thread:**
- Unit: checkpoint logic ("same sha → skip", "new version → emit", "same version, different sha → emit and flag").
- Contract: every produced event validates against the JSON Schema, and the schema file is versioned.
- Integration (Testcontainers Kafka): feed golden files through the full producer and assert the exact set of events on the topic. Run it twice and assert **no new events** the second time. That's your idempotency test.

**Done when**
- [ ] Killing the producer mid-file and restarting produces no duplicates beyond the at-least-once window, and your tests prove it.
- [ ] Historical `ICIDemand` for four base periods is on Kafka.
- [ ] Live mode picks up a newly published hourly file within minutes of its `Last-Modified` time, measured.

**Pitfalls:** hammering IESO (poll every few minutes, not every second); emitting from inside the parsing loop before the checkpoint design is settled; treating the unversioned base file as a separate version from its `_vNN` twin.

**Interview questions:** *"Walk me through what happens if your producer crashes halfway through a file."* *"How do you know you didn't lose data?"*

---

### Weekend 4: Consumer and BigQuery sink

**Goal:** A consumer reads events and lands them in BigQuery: every version in `raw_demand_versions`, latest-per-interval in `current_demand`. Known-restated intervals are verified to hold the right final values.

**Learn first (~6–7 h)**

| Topic | Hours |
|---|---|
| Consumer offset management: auto vs manual commit, commit-after-write | 1.5 |
| BigQuery ingestion paths: load jobs, Storage Write API, legacy streaming; tradeoffs | 2 |
| `MERGE` semantics; BigQuery DML behaviour and quotas (read the current quota page) | 1.5 |
| Micro-batching: size- and time-based flushing | 1 |

**Concepts**

- **Commit after write.** The consumer commits its Kafka offset **only after** the BigQuery write succeeds. A crash between write and commit means the batch is reprocessed, which is harmless because the MERGE is idempotent. Commit before writing and a crash loses data silently. This ordering is the single most important line in the consumer.
- **Don't MERGE per message.** BigQuery DML is built for set-based batch work, and per-row statements will hit limits and cost you. The pattern: accumulate a micro-batch (for example 500 events or 60 seconds, whichever comes first), write it to a staging table (or use the Storage Write API into `raw_*`), then run **one** `MERGE` from staging into `current_*`.
- **The MERGE key and the tie-breaker.** Key on `(report, interval_start, zone)`. Only overwrite when the incoming `version` is **greater** than the stored one. Otherwise an out-of-order older version, which *will* happen during backfill plus live overlap, would overwrite newer data. That `WHEN MATCHED AND S.version > T.version` clause is a great interview detail.

```sql
MERGE grid.current_demand T
USING (
  -- de-duplicate within the batch first: highest version wins
  SELECT * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY report, interval_start, zone ORDER BY version DESC) rn
    FROM grid._staging_batch)
  WHERE rn = 1
) S
ON  T.report = S.report AND T.interval_start = S.interval_start AND T.zone = S.zone
WHEN MATCHED AND S.version > T.version THEN UPDATE SET
  value_mw = S.value_mw, version = S.version, published_at = S.published_at,
  ingested_at = S.ingested_at, source_uri = S.source_uri, content_sha256 = S.content_sha256
WHEN NOT MATCHED THEN INSERT ROW;
```

**Build**

1. Consumer Deployment, group `bq-sink`, manual commits, micro-batching, Workload Identity for BigQuery access.
2. Append to `raw_demand_versions`, MERGE into `current_demand`.
3. Replay the whole backfill topic from offset 0 with a **fresh consumer group**. Replay is the Kafka justification made concrete, so screen-record it.
4. Verification query: pick intervals you *know* were restated (from Weekend 0 notes). Assert `current_demand` holds the final version and `raw_demand_versions` holds all of them.

**Testing thread: data quality becomes a first-class test suite**
- SQL assertions run after each load, failing the pipeline on violation: no duplicate `(report, interval_start, zone)` in `current_*`; no gaps in 5-min series inside a closed day; `value_mw` inside a plausible physical range for Ontario (derive the range from history, not a guess); `current.version = MAX(raw.version)` for every key.
- These can be plain pytest + BigQuery queries now, then move into Airflow tasks in Weekend 5. (dbt tests are the same idea if you add dbt later.)
- Consumer unit tests: "older version arrives after newer → no overwrite"; "crash before commit → reprocess yields identical table".

**Done when**
- [ ] Full replay from zero reproduces an identical `current_demand` (compare row counts and a checksum query).
- [ ] Restated intervals verified.
- [ ] Data-quality suite green; one deliberately corrupted fixture makes it go red.

**Pitfalls:** timestamp precision mismatches in the MERGE key; forgetting that a table partitioned on `interval_start` needs a partition filter in the MERGE for cost; auto-commit left on by default in your client library.

**Interview questions:** *"How do you get effectively-once into a warehouse?"* *"What happens with out-of-order versions?"* *"How do you test data, as opposed to code?"*

---

### Weekend 5: Revision reconciliation with Airflow

**Goal:** A nightly Airflow DAG finds restated files published in the last ~30 days that the live path missed or saw an older version of, republishes them to Kafka with `reason=reconciliation`, and records what it found.

**Learn first (~7–8 h)**

| Topic | Hours |
|---|---|
| Airflow core: DAG, task, operator, scheduler, executor, task instance states | 2 |
| **Data intervals**, logical dates, `catchup`, backfill; idempotent tasks | 2 |
| Deploying Airflow on K8s via the official Helm chart; executor choice; DAG delivery (git-sync) | 2 |
| Sensors vs deferrable operators; XCom limits (small metadata only) | 1 |

**Concepts**

- **Airflow schedules batches over time windows; it doesn't move data.** A DAG run is tied to a *data interval* (for example 2026-09-23 00:00 → 2026-09-24 00:00). Tasks should compute from that interval, never from "now". That makes a run **rerunnable**: rerun last Tuesday and you get last Tuesday's answer. This is the idempotency idea again, at the orchestration layer.
- **Why this is not forced.** Streaming handles "new file appeared". Reconciliation handles "an old file changed quietly", which needs a scheduled, look-back scan with retries and run history. That's a different job shape, so it's a different tool.
- **Executor choice.** LocalExecutor runs tasks as processes in the scheduler pod: simplest, and enough here. KubernetesExecutor runs each task as its own pod: more isolation, more moving parts. Pick Local, and be able to say when you'd switch.
- **Airflow is heavy.** The chart brings a scheduler, API/web server, triggerer and a Postgres metadata DB, and it will compete with Kafka for memory on small nodes. Budget resources consciously, or add a second small node pool. Use Airflow 3.x (**check** the current chart version).

**The DAG (`reconcile_ieso_revisions`, daily, look-back 30 days)**

```
list_remote_versions   → for each tracked report dir: (file, version, Last-Modified, size)
compare_with_ingested  → join against raw_demand_versions: which (file, version) are new or
                          have a different sha than what was ingested?
fetch_and_archive      → download changed files to GCS (immutable, new object per version)
republish              → emit events with reason='reconciliation' via the same producer code
verify_landed          → wait until those keys show the new version in current_demand
record_run_summary     → write counts to a grid.reconciliation_runs table + emit metrics
```

Version suffixes give you a cheap first comparison (did a higher `_vNN` appear?). The sha256 check is defence in depth: it catches a file changing *without* a version bump. Record how often each case happens; that's a finding worth mentioning.

**Build:** Helm install into namespace `airflow`, DAGs synced from your repo, connections via Workload Identity (no stored keys). Move the Weekend 4 data-quality assertions in as the DAG's final tasks.

**Testing thread:**
- DAG integrity test in CI: every DAG file imports, has no cycles, sets `catchup` explicitly, and has owners and retries. This is a standard, cheap Airflow test.
- Task-logic unit tests (the compare function is pure Python; test it with golden inputs).
- End-to-end: seed BigQuery with an *old* version of a known-restated file, run the DAG for that interval, and assert the new version lands and `reconciliation_runs` records exactly one fix.

**Done when**
- [ ] The DAG runs green for at least 3 scheduled nights (or 3 backfilled intervals).
- [ ] The seeded restatement is detected and fixed end-to-end.
- [ ] A real count, "N restated files found over M days", is recorded from actual runs. That's a defensible metric.

**Pitfalls:** tasks using `datetime.now()` (breaks reruns); pushing file contents through XCom; DAG changes that need a pod restart because git-sync isn't set up.

**Interview questions:** *"Why Airflow and not a cron job?"* *"What makes a task idempotent?"* *"What's the difference between logical date and run time?"*

---

### Weekend 6: Peak-risk logic and honest validation

**Goal:** A rule-based signal flags hours at risk of being top-5 ICI peaks. It is backtested causally against IESO's own Peak Tracker for finished base periods, with results reported honestly, small sample included.

**Learn first (~5–6 h)**

| Topic | Hours |
|---|---|
| Evaluation for rare events: recall, precision, and why accuracy is meaningless here | 1.5 |
| **Look-ahead bias** and point-in-time ("as-of") data | 1.5 |
| Train/validation splits for time series (walk-forward) | 1 |
| Communicating results with small n | 1 |

**Concepts**

- **Accuracy is useless here.** A base period has ~8,760 hours and five peaks. "Never alert" is 99.94% accurate and worthless. What a Class A customer actually cares about is a **tradeoff**: *peak capture* (how many of the 5 did you flag?) against *alert burden* (how many hours did you ask them to curtail?). Report both, always together.
- **Causality (no peeking).** When the rule evaluates hour *h*, it may only use information that existed before *h*. Two traps:
  1. Using the *final* top-5 of the year to set a threshold. You didn't know that in July.
  2. Using *final restated* demand when only the first version existed at decision time. That's why `raw_*_versions` with `published_at` exists: an as-of query (`WHERE published_at <= decision_time`) reconstructs what was knowable.
- **A sensible first rule** (for you to tune, not a recommendation of final parameters): at each hour, compute the 5th-highest daily peak observed *so far* this base period. Flag the hour if demand (actual, or predispatch forecast when you're looking ahead) exceeds that value, or comes within a margin of it. Restrict to plausible peak windows if the history supports that (for example summer weekday afternoons). Then derive restrictions from the data; don't assume them.
- **Walk-forward validation.** Tune the margin on 2022–2023, then evaluate *once* on 2024–2025. If you tune on all four years, your reported number measures memory, not skill.

**Build**

1. `signals/peak_risk.py`: a pure function `(history_as_of_t, candidate_hour) → risk_flag, reason`. Pure means trivially unit-testable and replayable.
2. A signal-evaluator consumer on `ieso.demand.*` that writes flags to `grid.signals` (topic and table).
3. `backtest/`: replays each finished base period hour by hour through the **same** function using as-of queries, and outputs a table (base period, peaks captured /5, alert hours, median lead time before each peak).
4. A short results write-up, `docs/validation.md`, containing: method, split, results table, and limitations (n ≤ 20; incomplete years excluded, and why; forecast-based lead time measured only over the period the archiver captured).

**What an honest result sentence looks like** *(fill with real numbers only)*: "On held-out base periods 2024–25, the rule flagged X of Y IESO-confirmed peak hours while issuing Z alert-hours per base period, measured against IESO's ICI Peak Tracker. Small sample; see limitations."

**Testing thread:**
- Unit tests with hand-built history: the rule must not change its output for hour *h* when you append data *after* *h*. This is a **no-look-ahead property test**, a strong SDET talking point.
- Regression test: the backtest output for a frozen dataset is committed, and CI fails if a code change silently alters it.

**Done when**
- [ ] Backtest reproducible with one command.
- [ ] `validation.md` written, with limitations stated.
- [ ] No-look-ahead test in CI.

**Pitfalls:** quietly dropping the awkward year; reporting only capture without burden; comparing against your own computed top-5 instead of IESO's tracker.

**Interview questions:** *"How did you validate it?"* *"How do you know it isn't overfit?"* *"What's look-ahead bias?"*

---

### Weekend 7: Observability, autoscaling, load test, ship

**Goal:** Datadog shows freshness, lag and errors, with alerts. KEDA scales consumers during an accelerated replay and back down after. The repo is interview-ready, with a recorded demo, since the infrastructure won't be up when a recruiter looks.

This is the most crowded weekend. Weekend 8 exists to absorb its overflow; plan on using it.

**Learn first (~6–7 h)**

| Topic | Hours |
|---|---|
| Datadog on K8s: agent/operator via Helm, integrations (Kafka), custom metrics via DogStatsD, monitors | 2.5 |
| SLI/SLO basics: pick 2–3 indicators that matter | 1 |
| KEDA: ScaledObject, the Kafka scaler, lag thresholds, the partition ceiling | 1.5 |
| Load-test design: what you're measuring, and when you'd stop | 1 |

**Concepts**

- **Freshness is the SLI that matters.** For a peak signal, "the newest interval in `current_demand` is more than 90 minutes old" is an outage, even if every pod is green. Alert on **data freshness** first, then **consumer lag**, then error rates. Being able to say that ordering, and why, is worth more than any dashboard screenshot.
- **Consumer lag** = latest offset minus committed offset, per partition. Lag rising while the producer is healthy means consumers can't keep up; lag at zero while freshness is stale means the *producer* stopped. Two metrics together localise the fault.
- **KEDA** watches lag and adjusts the consumer Deployment's replicas between min and max. The ceiling is the partition count (Weekend 2), because extra consumers would sit idle. With 6 partitions, max useful replicas is 6.
- **The load test is accelerated replay of real history.** Reset a fresh consumer group to offset 0 on the backfill topics and let it rip; or have the backfill Job re-emit a month of archived files as fast as it can. You measure: peak lag, time to drain, replicas over time, and BigQuery write throughput. Real data at accelerated speed stays within your "no fake producers" rule.

**Build**

1. Activate Datadog Pro via the Student Pack. Install the agent with Helm; enable the Kafka integration; send your existing app metrics via DogStatsD.
2. Monitors: freshness (no new interval for over N minutes while the producer should be running), consumer lag above threshold for over M minutes, and a producer-error spike. Route them to email.
3. KEDA install; `ScaledObject` on the `bq-sink` consumer (min 1, max 6, lag threshold tuned by experiment).
4. Run the accelerated replay. Record: replicas over time, lag curve, drain time. Screenshot the Datadog dashboard. These are the **measured** metrics for your resume.
5. Ship:
   - README: problem (60-second version), architecture diagram, justification table, how to run, results with limitations, and "what I'd do next".
   - A 3–5 minute recorded demo: `make up` → live file ingested → Datadog dashboard → replay with autoscaling → signal output → `make down`.
   - Final `make down`, plus the orphan check.

**Testing thread:** the load test itself is the performance-testing evidence you listed as a gap. Write it up as a test report: objective, setup, measurements, observed bottleneck, and what you'd change. SDET interviewers read test reports; give them one.

**Done when**
- [ ] Three monitors exist, and each has been triggered on purpose at least once (stop the producer; pause the consumer).
- [ ] Autoscaling observed up and back down, with a recording.
- [ ] README, demo video, validation doc and load-test report are all in the repo.
- [ ] Environment destroyed; no orphans; billing checked.

**Interview questions:** *"What would page you at 3 a.m.?"* *"Why didn't more replicas make it faster?"* *"What was your bottleneck?"*

---

### Weekend 8: Buffer

Reserved for overflow, almost certainly from Weekend 7, possibly 2 or 5. If you truly have nothing left, spend it on the second-project decision gate (Part 6) or a written post-mortem of the hardest bug. Don't use it for new scope.

---

## Part 5: Resume-metric candidates (all must come from your own runs)

These are the numbers this project *can* honestly produce. No placeholder ever goes on a resume; only measured values do.

- Environment bring-up and teardown time via Terraform + Helm (Weekends 1 and 7).
- Median and p95 ingestion latency: IESO `Last-Modified` → row in BigQuery (Weekends 3–4).
- Restated files detected by reconciliation over *M* nights (Weekend 5).
- Backtest result: peaks captured versus alert-hours on held-out base periods, against IESO's tracker (Weekend 6).
- Replay drain time and max replicas during the accelerated load test (Weekend 7).
- Test counts by layer (unit / contract / integration / data-quality / IaC) and CI runtime.

---

## Part 6: The second project, "Entry-Level Hiring Collapse Tracker"

**Recommendation: don't decide now. Decide at a gate after Weekend 4.**

Reasoning:

- One deep, finished, well-tested project is worth more than two half-finished ones. Weekend 7 is already crowded.
- This project, as now revised, already covers GCP, Kubernetes, Terraform, Kafka, Airflow, Datadog, data-quality testing, IaC testing and performance testing. The second project's main *new* coverage would be dbt, plus a second domain story.
- Your #1 target is SDET/QA, and the revised testing thread here serves that directly. A second data project adds less for SDET roles than, say, a small, sharp test-automation repo would.

**Gate question after Weekend 4:** *Are Weekends 1–4 done, and the budget intact?* If yes, and you still want dbt evidence, scope the tracker as a 2–3 weekend project that reuses this project's Terraform and Airflow patterns, with no Kafka. If no, park it and finish this one properly.

---

## Part 7: Consolidated check list for Weekend 0

1. Confirm the timezone (EST year-round?) and hour-ending convention from file contents.
2. Identify the exact ICI demand field and its definition.
3. Explain the small 2024 Peak Tracker file and the tiny 2021 ICIDemand file; decide which base periods are valid ground truth.
4. Determine whether the ICI files update continuously or in bursts.
5. Confirm `ETag`/`Last-Modified` support on the IESO server.
6. Confirm how long each report keeps hourly files and intermediate versions (tonight: ~30 days for `RealtimeTotals`, and intermediate predispatch versions pruned after ~a month).
7. Check whether `PredispTotals` is still publishing current days. Tonight's listing showed its unversioned latest file dated 7 Aug 2026, and the listing was truncated before it could be confirmed.
8. Confirm the current Airflow Helm chart version, and BigQuery DML/quotas from the current docs.

---

## Part 8: Reference docs (official)

- Kubernetes concepts: kubernetes.io/docs/concepts
- GKE: cloud.google.com/kubernetes-engine/docs (Workload Identity, VPC-native clusters, pricing)
- Terraform: developer.hashicorp.com/terraform (GCS backend, Google provider)
- Strimzi: strimzi.io/docs (use docs for 0.46+ only; older posts use ZooKeeper)
- Apache Kafka: kafka.apache.org/documentation (design section: log, partitions, compaction)
- Apache Airflow: airflow.apache.org/docs (data intervals, Helm chart)
- KEDA: keda.sh/docs (Apache Kafka scaler)
- Datadog: docs.datadoghq.com (Kubernetes install, Kafka integration, DogStatsD)
- BigQuery: cloud.google.com/bigquery/docs (MERGE, Storage Write API, quotas)
- IESO: reports-public.ieso.ca/public/ and the IESO Class A / ICI pages on ieso.ca

## Sources verified on 24 Sep 2026

- IESO public reports root and sub-directories: `PredispHourlyOntarioZonalPrice`, `PredispMktPrice`, `ICIPeakTracker`, `ICIDemand`, `RealtimeTotals`, `PredispTotals`. Listings read live.
- GKE pricing: the free tier gives $74.40/month per billing account, covering one zonal or Autopilot cluster's management fee; compute is not covered. (cloud.google.com/kubernetes-engine/pricing)
- Strimzi 0.46.0 release notes: ZooKeeper support removed; Kafka 4.0 supported; KRaft requires `KafkaNodePool`. (github.com/strimzi/strimzi-kafka-operator/releases/tag/0.46.0)
- GitHub Student Developer Pack: Datadog Pro, 10 servers, free for 2 years. (education.github.com/pack)
- ICI base period runs May 1 – April 30; Class A GA is based on share of the top five peak hours. (Hydro Ottawa ICI page)
