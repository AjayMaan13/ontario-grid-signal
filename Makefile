PROJECT = ontario-grid-signal
CLUSTER = grid-signal
ZONE    = northamerica-northeast2-a

.PHONY: up down test

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
	uv run --with pytest pytest
