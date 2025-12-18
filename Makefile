# Makefile for HyperFleet GCP Validation Adapter
#
# Usage:
#   make help              - Show this help
#   make helm-lint         - Lint helm chart
#   make helm-template     - Render helm templates
#   make helm-test         - Run all helm tests

.PHONY: help helm-lint helm-template helm-template-broker helm-template-rabbitmq helm-test helm-dry-run helm-package \
        helm-template-full helm-template-fake helm-template-real test-helm \
        helm-install helm-upgrade helm-uninstall helm-status \
        run-local validate-adapter-yaml validate-broker-pubsub validate-broker-rabbitmq validate-fake-job validate

# Default values
RELEASE_NAME ?= validation-gcp
NAMESPACE ?= hyperfleet-system
CHART_DIR := ./charts

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

##@ General

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Helm Chart Testing

helm-lint: ## Lint the helm chart
	@echo "$(GREEN)Linting helm chart...$(NC)"
	helm lint $(CHART_DIR)

helm-template: ## Render helm templates with default values
	@echo "$(GREEN)Rendering helm templates (default values)...$(NC)"
	helm template $(RELEASE_NAME) $(CHART_DIR)

helm-template-broker: ## Render helm templates with Google Pub/Sub broker
	@echo "$(GREEN)Rendering helm templates (Google Pub/Sub broker)...$(NC)"
	helm template $(RELEASE_NAME) $(CHART_DIR) \
		--set broker.type=googlepubsub \
		--set broker.googlepubsub.projectId=test-project \
		--set broker.googlepubsub.topic=test-topic \
		--set broker.googlepubsub.subscription=test-subscription

helm-template-rabbitmq: ## Render helm templates with RabbitMQ broker
	@echo "$(GREEN)Rendering helm templates (RabbitMQ broker)...$(NC)"
	helm template $(RELEASE_NAME) $(CHART_DIR) \
		--set broker.type=rabbitmq \
		--set broker.rabbitmq.url="amqp://guest:guest@rabbitmq:5672/"

helm-template-fake: ## Render helm templates with fake validation mode
	@echo "$(GREEN)Rendering helm templates (fake validation mode)...$(NC)"
	helm template $(RELEASE_NAME) $(CHART_DIR) \
		--namespace $(NAMESPACE) \
		--set deploymentMode=fake \
		--set broker.type=googlepubsub \
		--set broker.googlepubsub.projectId=my-project \
		--set broker.googlepubsub.topic=my-topic \
		--set broker.googlepubsub.subscription=my-subscription \
		--set broker.googlepubsub.deadLetterTopic=my-dlq \
		--set broker.subscriber.parallelism=10 \
		--set hyperfleetApi.baseUrl=https://api.hyperfleet.example.com \
		--set validation.fake.simulateResult=success \
		--set rbac.create=true

# Real mode (when available in future)
#helm-template-real: ## Render helm templates with real validation mode (future)
#	@echo "$(GREEN)Rendering helm templates (real validation mode)...$(NC)"
#	helm template $(RELEASE_NAME) $(CHART_DIR) \
#		--namespace $(NAMESPACE) \
#		--set deploymentMode=real \
#		--set broker.type=googlepubsub \
#		--set broker.googlepubsub.projectId=my-project \
#		--set broker.googlepubsub.topic=my-topic \
#		--set broker.googlepubsub.subscription=my-subscription \
#		--set broker.googlepubsub.deadLetterTopic=my-dlq \
#		--set broker.subscriber.parallelism=20 \
#		--set hyperfleetApi.baseUrl=https://api.hyperfleet.example.com \
#		--set rbac.create=true

helm-template-full: helm-template-fake ## Alias for helm-template-fake (full fake configuration)

test-helm: helm-lint helm-template-broker helm-template-rabbitmq helm-template-fake ## Run all helm chart tests (lint + template rendering)
	@echo "$(GREEN)All helm chart tests passed!$(NC)"

helm-dry-run: ## Simulate helm install (requires cluster connection)
	@echo "$(YELLOW)Simulating helm install (dry-run)...$(NC)"
	helm install $(RELEASE_NAME) $(CHART_DIR) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--dry-run \
		--debug \
		--set deploymentMode=fake \
		--set broker.type=googlepubsub \
		--set broker.googlepubsub.projectId=test-project \
		--set broker.googlepubsub.topic=test-topic \
		--set broker.googlepubsub.subscription=test-subscription

helm-package: ## Package helm chart
	@echo "$(GREEN)Packaging helm chart...$(NC)"
	helm package $(CHART_DIR)

##@ Helm Chart Deployment

helm-install: ## Install helm chart to cluster
	@echo "$(GREEN)Installing helm chart...$(NC)"
	helm install $(RELEASE_NAME) $(CHART_DIR) \
		--namespace $(NAMESPACE) \
		--create-namespace

helm-upgrade: ## Upgrade helm chart
	@echo "$(GREEN)Upgrading helm chart...$(NC)"
	helm upgrade $(RELEASE_NAME) $(CHART_DIR) \
		--namespace $(NAMESPACE)

helm-uninstall: ## Uninstall helm chart
	@echo "$(YELLOW)Uninstalling helm chart...$(NC)"
	helm uninstall $(RELEASE_NAME) --namespace $(NAMESPACE)

helm-status: ## Show helm release status
	helm status $(RELEASE_NAME) --namespace $(NAMESPACE)

##@ Local Development

run-local: ## Run adapter locally (auto-sources .env if exists)
	@./run-local.sh

##@ Validation

validate-adapter-yaml: ## Validate charts/configs/adapter-fake-validation-gcp.yaml syntax
	@echo "$(GREEN)Validating charts/configs/adapter-fake-validation-gcp.yaml...$(NC)"
	@yq '.' charts/configs/adapter-fake-validation-gcp.yaml >/dev/null && echo "adapter-fake-validation-gcp.yaml is valid YAML"

validate-fake-job: ## Validate charts/configs/fake-validation-gcp-job.yaml syntax
	@echo "$(GREEN)Validating charts/configs/fake-validation-gcp-job.yaml...$(NC)"
	@yq '.' charts/configs/fake-validation-gcp-job.yaml >/dev/null && echo "fake-validation-gcp-job.yaml is valid YAML"

validate: validate-adapter-yaml validate-fake-job ## Validate all YAML files
