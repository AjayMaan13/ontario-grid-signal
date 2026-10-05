PROJECT = ontario-grid-signal
CLUSTER = grid-signal
ZONE    = northamerica-northeast2-a

STRIMZI_VERSION = 1.2.0

.PHONY: up down test kafka-up kafka-test kafka-count producer-build producer-backfill producer-live

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
	uv run --with pytest --with jsonschema --with confluent-kafka --with "testcontainers[kafka]" pytest

# Install Strimzi (the Kafka operator), then the cluster and topics it manages.
kafka-up:
	helm repo add strimzi https://strimzi.io/charts/ --force-update
	helm upgrade --install strimzi strimzi/strimzi-kafka-operator \
		--namespace kafka --create-namespace --version $(STRIMZI_VERSION)
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
