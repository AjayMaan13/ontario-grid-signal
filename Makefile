PROJECT = ontario-grid-signal
CLUSTER = grid-signal
ZONE    = northamerica-northeast2-a

STRIMZI_VERSION = 1.2.0

.PHONY: up down test kafka-up kafka-test kafka-count producer-build producer-backfill producer-live consumer-up consumer-replay bq-test bq-check bq-summary bq-reset bq-runs airflow-image airflow-up airflow-ui airflow-backfill airflow-down seed-restatement dag-test

# Create the cluster and point kubectl at it.
up:
	cd infra/main && GOOGLE_OAUTH_ACCESS_TOKEN=$$(gcloud auth print-access-token) terraform init -input=false
	cd infra/main && GOOGLE_OAUTH_ACCESS_TOKEN=$$(gcloud auth print-access-token) terraform apply
	gcloud container clusters get-credentials $(CLUSTER) --zone $(ZONE) --project $(PROJECT)

# Destroy the cluster, then check nothing billable was left behind.
down:
	cd infra/main && GOOGLE_OAUTH_ACCESS_TOKEN=$$(gcloud auth print-access-token) terraform destroy
	./scripts/check_orphans.sh

test:
	uv run --with pytest --with jsonschema --with google-cloud-bigquery --with hypothesis --with confluent-kafka --with "testcontainers[kafka]" pytest

# Install Strimzi (the Kafka operator), then the cluster and topics it manages.
kafka-up:
	helm repo add strimzi https://strimzi.io/charts/ --force-update
	helm upgrade --install strimzi strimzi/strimzi-kafka-operator \
		--namespace kafka --create-namespace --version $(STRIMZI_VERSION) -f k8s/kafka/strimzi-values.yaml
	kubectl -n kafka apply -f k8s/kafka/nodepool.yaml -f k8s/kafka/cluster.yaml
	kubectl -n kafka wait kafka/grid-kafka --for=condition=Ready --timeout=300s
	kubectl -n kafka apply -f k8s/kafka/topics.yaml

# Produce one message and read it back, proving the cluster and topics work.
kafka-test:
	kubectl -n kafka run kafka-smoke-test --rm -i --restart=Never \
		--image=quay.io/strimzi/kafka:$(STRIMZI_VERSION)-kafka-4.3.1 -- bash -c \
		"echo hello-from-make | bin/kafka-console-producer.sh --bootstrap-server grid-kafka-kafka-bootstrap:9092 --topic grid.signals \
		&& bin/kafka-console-consumer.sh --bootstrap-server grid-kafka-kafka-bootstrap:9092 --topic grid.signals --from-beginning --max-messages 1 --timeout-ms 20000"

IMAGE = northamerica-northeast2-docker.pkg.dev/$(PROJECT)/grid/producer:latest

# Build the producer image in Google Cloud Build and push it to Artifact Registry.
producer-build:
	gcloud builds submit . --tag $(IMAGE) --project $(PROJECT)

# Replay the archived files into Kafka. Run before producer-live.
producer-backfill:
	kubectl apply -f k8s/producer/serviceaccount.yaml
	kubectl -n grid delete job producer-backfill --ignore-not-found
	kubectl apply -f k8s/producer/backfill-job.yaml
	kubectl -n grid wait job/producer-backfill --for=condition=complete --timeout=1800s

# Start the live producer (polls IESO every 10 minutes).
producer-live:
	kubectl apply -f k8s/producer/serviceaccount.yaml -f k8s/producer/deployment.yaml
	kubectl -n grid rollout status deployment/producer

# How many events each topic holds. Run it twice, a minute apart, to watch it grow.
kafka-count:
	kubectl -n kafka run kafka-count --rm -i --restart=Never --image=quay.io/strimzi/kafka:$(STRIMZI_VERSION)-kafka-4.3.1 -- bash -c \
		'for t in ieso.demand.ici ieso.demand.predispatch ieso.demand.realtime; do echo -n "$$t: "; bin/kafka-get-offsets.sh --bootstrap-server grid-kafka-kafka-bootstrap:9092 --topic $$t | awk -F: "{s+=\$$3} END{print s}"; done'

# --- BigQuery sink ---
BQ_PYTHON = PYTHONPATH=src uv run --quiet --with google-cloud-bigquery --with jsonschema python -m consumer.cli

# Start the live consumer (group bq-sink).
consumer-up:
	kubectl apply -f k8s/consumer/serviceaccount.yaml -f k8s/consumer/deployment.yaml
	kubectl -n grid rollout status deployment/consumer

# Read every event again with a brand-new consumer group, then exit. See docs/data-notes or the plan for the full procedure.
consumer-replay:
	kubectl apply -f k8s/consumer/serviceaccount.yaml
	kubectl -n grid delete job consumer-replay --ignore-not-found
	sed "s/__GROUP__/replay-$$(date +%s)/" k8s/consumer/replay-job.yaml | kubectl apply -f -
	kubectl -n grid wait job/consumer-replay --for=condition=complete --timeout=3600s

# Row counts and a checksum of both tables (compare before and after a replay).
bq-summary:
	$(BQ_PYTHON) summary

# The data-quality checks. Exits with an error if any check finds a violation.
bq-check:
	$(BQ_PYTHON) check

# Empty both tables, for a clean replay.
bq-reset:
	$(BQ_PYTHON) reset

# The MERGE statements and quality checks on real BigQuery, in a throwaway dataset (takes a few minutes).
bq-test:
	RUN_BQ_TESTS=1 uv run --with pytest --with jsonschema --with google-cloud-bigquery pytest tests/test_bigquery.py

# --- Airflow (nightly reconciliation) ---
AIRFLOW_IMAGE = northamerica-northeast2-docker.pkg.dev/$(PROJECT)/grid/airflow:latest

# The official Airflow image plus the libraries our tasks import (our code arrives through git-sync).
airflow-image:
	gcloud builds submit airflow-image --tag $(AIRFLOW_IMAGE) --project $(PROJECT)

airflow-up:
	helm repo add apache-airflow https://airflow.apache.org --force-update
	helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace --version 1.22.0 \
		-f k8s/airflow/values.yaml
	kubectl -n airflow rollout status deployment/airflow-api-server --timeout=15m
	kubectl -n airflow rollout status statefulset/airflow-scheduler --timeout=15m

# Opens the Airflow screen on this Mac. Leave it running; login admin / admin.
airflow-ui:
	@echo "Open http://localhost:8080  (login: admin / admin)"
	kubectl -n airflow port-forward svc/airflow-api-server 8080:8080

# Run the DAG for past days (days that already ran are run again):  make airflow-backfill FROM=2026-10-03 TO=2026-10-05
airflow-backfill:
	kubectl -n airflow exec -c scheduler $$(kubectl -n airflow get pods -l component=scheduler -o name | head -1) -- \
		airflow backfill create --dag-id reconcile_ieso_revisions --from-date $(FROM) --to-date $(TO) --reprocess-behavior completed

# Remove Airflow and its database disk (do this before make down, or the disk is left behind).
airflow-down:
	helm uninstall airflow --namespace airflow --ignore-not-found
	kubectl -n airflow delete pvc --all

# For the end-to-end test: make the producer "forget" one finished hour, so the DAG has something to find.
seed-restatement:
	PYTHONPATH=src uv run --quiet --with google-cloud-bigquery --with google-cloud-storage --with jsonschema python -m reconcile.cli seed

# Checks the DAG files without running them (needs Airflow, which needs Python 3.12).
dag-test:
	AIRFLOW_HOME=/tmp/airflow-test uv run --quiet --python 3.12 --with "apache-airflow==3.2.2" --with pytest pytest tests/test_dags.py

# What each reconciliation run found (one row per run).
bq-runs:
	$(BQ_PYTHON) runs

# Walk-forward backtest: tune on 2022-23, score 2024-25 once, write backtest/results.json. Runs offline in seconds.
backtest:
	PYTHONPATH=src uv run --quiet python -m signals.backtest

# --- live signal evaluator ---
# Start it (needs the Terraform apply for its table and account first).
signals-up:
	kubectl apply -f k8s/signals/serviceaccount.yaml -f k8s/signals/deployment.yaml
	kubectl -n grid rollout status deployment/signals

# What would the live rule have flagged around a real past day?  make signals-replay DAY=2025-06-24
signals-replay:
	PYTHONPATH=src uv run --quiet python -m signals.replay $(DAY)

# The live evaluator's latest decisions.
bq-signals:
	$(BQ_PYTHON) signals

# Events held by each partition of each topic (partition:end offset). Shows whether a topic is spread or stuck on one.
kafka-spread:
	kubectl -n kafka run kafka-spread --rm -i --restart=Never --image=quay.io/strimzi/kafka:$(STRIMZI_VERSION)-kafka-4.3.1 -- bash -c \
		'for t in ieso.demand.ici ieso.demand.predispatch ieso.demand.realtime; do echo "$$t"; bin/kafka-get-offsets.sh --bootstrap-server grid-kafka-kafka-bootstrap:9092 --topic $$t; done'

# --- Datadog and KEDA ---
# Keys come from your terminal environment (see the read -s lines); nothing is written to a file.
datadog-secret:
	@test -n "$$DD_API_KEY" || (echo "set DD_API_KEY first"; exit 1)
	kubectl create namespace datadog --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n datadog create secret generic datadog-secret --from-literal=api-key="$$DD_API_KEY" --dry-run=client -o yaml | kubectl apply -f -

datadog-up:
	@test -n "$$DD_SITE" || (echo "set DD_SITE first, for example datadoghq.com"; exit 1)
	helm repo add datadog https://helm.datadoghq.com --force-update
	helm upgrade --install datadog datadog/datadog --namespace datadog --create-namespace --version 3.253.0 \
		-f k8s/datadog/values.yaml --set datadog.site=$$DD_SITE
	kubectl -n datadog rollout status daemonset/datadog --timeout=10m

keda-up:
	helm repo add kedacore https://kedacore.github.io/charts --force-update
	helm upgrade --install keda kedacore/keda --namespace keda --create-namespace --version 2.21.0 -f k8s/keda/values.yaml
	kubectl -n keda rollout status deployment/keda-operator --timeout=5m
	kubectl -n keda rollout status deployment/keda-operator-metrics-apiserver --timeout=5m
	kubectl apply -f k8s/keda/scaledobject.yaml

# Rewind the consumer group to the start and time the drain. baseline = pinned at 1 replica; scaled = KEDA decides.
loadtest-baseline:
	./scripts/loadtest.sh baseline
loadtest-scaled:
	./scripts/loadtest.sh scaled
