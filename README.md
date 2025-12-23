# HyperFleet GCP Validation Adapter

Event-driven adapter for HyperFleet GCP cluster validation. Validates GCP cluster configurations and prerequisites before provisioning. Consumes CloudEvents from message brokers (GCP Pub/Sub, RabbitMQ), processes AdapterConfig, manages validation jobs in Kubernetes, and reports status via API.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Deployment Modes](#deployment-modes)
- [Local Development](#local-development)
- [Helm Chart Installation](#helm-chart-installation)
- [Configuration](#configuration)
- [Examples](#examples)

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- GCP Workload Identity (for Pub/Sub access)
- `gcloud` CLI configured with appropriate permissions

## Deployment Modes

This adapter supports two deployment modes via the `deploymentMode` parameter:

### Dummy Mode (Current)
- **Value**: `deploymentMode: "dummy"`
- **Description**: Simulates GCP validation for testing and development
- **Config File**: Uses `charts/configs/validation-dummy-adapter.yaml`
- **Features**:
  - Configurable simulation results (success, failure, hang, crash, invalid-json, missing-status)
  - No actual GCP API calls
  - Fast validation cycles for testing

### Real Mode (Future)
- **Value**: `deploymentMode: "real"`
- **Description**: Performs actual GCP validation checks
- **Config File**: Will use `charts/configs/validation-gcp-adapter.yaml` (to be created)
- **Features**:
  - Real GCP API validation
  - Production-ready validation checks
  - Comprehensive error reporting

## Local Development

Run the adapter locally for development and testing.

### Prerequisites

- `hyperfleet-adapter` binary installed and in PATH
- GCP service account key for Pub/Sub access
- Access to a GKE cluster (for applying Kubernetes resources)
- `podman` or `docker` for RabbitMQ (if `BROKER_TYPE=rabbitmq`)

### Setup

1. Copy environment template:

```bash
cp env.example .env
```

2. Edit `.env` with your configuration:

```bash
# Required for Google Pub/Sub (default)
GCP_PROJECT_ID="your-gcp-project-id"
BROKER_TOPIC="hyperfleet-adapter-topic"
BROKER_SUBSCRIPTION_ID="hyperfleet-adapter-validation-gcp-subscription"

# Required for all broker types
HYPERFLEET_API_BASE_URL="https://localhost:8000"

# Optional (defaults provided)
SUBSCRIBER_PARALLELISM="1"
HYPERFLEET_API_VERSION="v1"

# Validation-specific settings
STATUS_REPORTER_IMAGE="<The image built by https://github.com/openshift-hyperfleet/status-reporter>"
SIMULATE_RESULT="success"  # success, failure, hang, crash, invalid-json, missing-status
RESULTS_PATH="/results/adapter-result.json"
MAX_WAIT_TIME_SECONDS="300"

# Required for RabbitMQ (if BROKER_TYPE=rabbitmq)
# RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
```

3. Set up GCP authentication:

```bash
# Create service account key and set in .env
export GOOGLE_APPLICATION_CREDENTIALS="./sa-key.json"
```

4. Connect to your GKE cluster:

```bash
gcloud container clusters get-credentials "$GKE_CLUSTER_NAME" \
  --region "$GKE_CLUSTER_REGION" \
  --project "$GCP_PROJECT_ID"

kubectl cluster-info
```

### Run

```bash
# For Google Pub/Sub (default)
./run-local.sh

# For RabbitMQ
BROKER_TYPE=rabbitmq ./run-local.sh
```

## Helm Chart Installation

### Installing the Chart

**Dummy Validation Mode (Default):**

```bash
helm install validation-gcp ./charts/ \
  --set broker.type=googlepubsub \
  --set broker.googlepubsub.projectId=my-gcp-project \
  --set broker.googlepubsub.topic=my-topic \
  --set broker.googlepubsub.subscription=my-subscription \
  --set hyperfleetApi.baseUrl=https://api.hyperfleet.example.com
```

**With Specific Deployment Mode:**

```bash
# Dummy mode (simulated validation)
helm install validation-gcp ./charts/ \
  --set deploymentMode=dummy \
  --set validation.dummy.simulateResult=success \
  --set broker.type=googlepubsub \
  --set broker.googlepubsub.projectId=my-gcp-project \
  --set broker.googlepubsub.topic=my-topic \
  --set broker.googlepubsub.subscription=my-subscription

# Real mode (not yet available; keep commented until implemented — see HYPERFLEET-267)
# helm install validation-gcp ./charts/ \
#   --set deploymentMode=real \
#   --set broker.type=googlepubsub \
#   ...
```

### Install to a Specific Namespace

```bash
helm install validation-gcp ./charts/ \
  --namespace hyperfleet-system \
  --create-namespace \
  --set broker.type=googlepubsub \
  --set broker.googlepubsub.projectId=my-gcp-project \
  --set broker.googlepubsub.topic=my-topic \
  --set broker.googlepubsub.subscription=my-subscription
```

### Uninstalling the Chart

```bash
helm delete validation-gcp

# Or with namespace
helm delete validation-gcp --namespace hyperfleet-system
```

## Configuration

All configurable parameters are in `values.yaml`. For advanced customization, modify the templates directly.

### Deployment Mode

| Parameter | Description | Default |
|-----------|-------------|---------|
| `deploymentMode` | Deployment mode: "dummy" or "real" | `"dummy"` |

### Image & Replica

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.registry` | Image registry | `quay.io/openshift-hyperfleet` |
| `image.repository` | Image repository | `hyperfleet-adapter` |
| `image.tag` | Image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `Always` |
| `imagePullSecrets` | Image pull secrets | `[]` |

### Naming

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nameOverride` | Override chart name | `""` |
| `fullnameOverride` | Override full release name | `""` |

### ServiceAccount & RBAC

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create ServiceAccount | `true` |
| `serviceAccount.name` | ServiceAccount name (auto-generated if empty) | `""` |
| `serviceAccount.annotations` | ServiceAccount annotations (for Workload Identity) | `{}` |
| `rbac.create` | Create ClusterRole and ClusterRoleBinding | `false` |

When `rbac.create=true`, the adapter gets **minimal permissions** needed for validation:
- **Namespaces**: `get`, `list`, `watch` (read-only, to verify target namespace exists)
- **ServiceAccounts**: Full management (`create`, `update`, `patch`, `delete`, `get`, `list`, `watch`)
- **Roles/RoleBindings**: Full management (for validation job RBAC)
- **Jobs**: Full management (for validation job lifecycle)
- **Jobs/status**: `get`, `update`, `patch` (for status reporter sidecar)
- **Pods**: `get`, `list`, `watch` (read-only, to check validation job pod status)

### Logging

| Parameter | Description | Default |
|-----------|-------------|---------|
| `logging.verbosity` | Log verbosity level (0-10, higher = more verbose) | `2` |
| `logging.logtostderr` | Log to stderr instead of files | `true` |

### Scheduling

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | `{}` |

### Broker Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `broker.type` | Broker type: `googlepubsub` or `rabbitmq` (**required**) | `""` |
| `broker.subscriber.parallelism` | Number of parallel workers | `1` |
| `broker.yaml` | Raw YAML override (advanced use) | `""` |

#### Google Pub/Sub (when `broker.type=googlepubsub`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `broker.googlepubsub.projectId` | GCP project ID (**required**) | `""` |
| `broker.googlepubsub.topic` | Pub/Sub topic name (**required**) | `""` |
| `broker.googlepubsub.subscription` | Pub/Sub subscription ID (**required**) | `""` |
| `broker.googlepubsub.deadLetterTopic` | Dead letter topic name (optional) | `""` |

#### RabbitMQ (when `broker.type=rabbitmq`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `broker.rabbitmq.url` | RabbitMQ connection URL (**required**) | `""` |

### HyperFleet API

| Parameter | Description | Default |
|-----------|-------------|---------|
| `hyperfleetApi.baseUrl` | HyperFleet API base URL | `""` |
| `hyperfleetApi.version` | API version | `v1` |

### Validation Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `validation.statusReporterImage` | Status reporter sidecar image | `<The image built by https://github.com/openshift-hyperfleet/status-reporter>` |

#### Dummy Validation Mode Settings

| Parameter | Description | Default |
|-----------|-------------|---------|
| `validation.dummy.simulateResult` | Simulated result (success, failure, hang, crash, invalid-json, missing-status) | `"success"` |
| `validation.dummy.resultsPath` | Path where validation results are written | `"/results/adapter-result.json"` |
| `validation.dummy.maxWaitTimeSeconds` | Maximum time to wait for validation completion | `"300"` |

### Environment Variables

| Parameter | Description | Default |
|-----------|-------------|---------|
| `env` | Additional environment variables | `[]` |

Example:
```yaml
env:
  - name: MY_VAR
    value: "my-value"
  - name: MY_SECRET
    valueFrom:
      secretKeyRef:
        name: my-secret
        key: key
```

## Examples

### Basic Dummy Validation with Google Pub/Sub

```bash
helm install validation-gcp ./charts/ \
  --set deploymentMode=dummy \
  --set broker.type=googlepubsub \
  --set broker.googlepubsub.projectId=my-gcp-project \
  --set broker.googlepubsub.topic=my-topic \
  --set broker.googlepubsub.subscription=my-subscription \
  --set hyperfleetApi.baseUrl=https://api.hyperfleet.example.com
```

### Dummy Validation with Different Simulation Results

```bash
# Simulate failure
helm install validation-gcp ./charts/ \
  --set validation.dummy.simulateResult=failure \
  --set broker.type=googlepubsub \
  --set broker.googlepubsub.projectId=my-gcp-project \
  --set broker.googlepubsub.topic=my-topic \
  --set broker.googlepubsub.subscription=my-subscription

# Simulate hang (for timeout testing)
helm install validation-gcp ./charts/ \
  --set validation.dummy.simulateResult=hang \
  --set validation.dummy.maxWaitTimeSeconds=60 \
  --set broker.type=googlepubsub \
  ...
```

### With RabbitMQ

```bash
helm install validation-gcp ./charts/ \
  --set deploymentMode=dummy \
  --set broker.type=rabbitmq \
  --set broker.rabbitmq.url="amqp://user:password@rabbitmq.svc:5672/"
```

### With GCP Workload Identity and RBAC

First, grant Pub/Sub permissions to the KSA (Kubernetes Service Account) :

```bash
# Get project number
PROJECT_NUMBER=$(gcloud projects describe my-gcp-project --format="value(projectNumber)")

# Grant permissions using direct principal binding
gcloud projects add-iam-policy-binding my-gcp-project \
  --role="roles/pubsub.subscriber" \
  --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/my-gcp-project.svc.id.goog/subject/ns/hyperfleet-system/sa/validation-gcp" \
  --condition=None

gcloud projects add-iam-policy-binding my-gcp-project \
  --role="roles/pubsub.viewer" \
  --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/my-gcp-project.svc.id.goog/subject/ns/hyperfleet-system/sa/validation-gcp" \
  --condition=None
```

Then deploy:

```bash
helm install validation-gcp ./charts/ \
  --namespace hyperfleet-system \
  --create-namespace \
  --set deploymentMode=dummy \
  --set image.registry=us-central1-docker.pkg.dev/my-project/my-repo \
  --set image.repository=hyperfleet-adapter \
  --set image.tag=v0.1.0 \
  --set broker.type=googlepubsub \
  --set broker.googlepubsub.projectId=my-gcp-project \
  --set broker.googlepubsub.topic=my-topic \
  --set broker.googlepubsub.subscription=my-subscription \
  --set hyperfleetApi.baseUrl=https://api.hyperfleet.example.com \
  --set rbac.create=true
```

### With Values File

<details>
<summary>Example <code>my-values.yaml</code></summary>

```yaml
deploymentMode: dummy

replicaCount: 1

image:
  registry: quay.io/openshift-hyperfleet
  repository: hyperfleet-adapter
  tag: latest

serviceAccount:
  create: true

rbac:
  create: true

logging:
  verbosity: 3
  logtostderr: true

hyperfleetApi:
  baseUrl: https://api.hyperfleet.example.com
  version: v1

broker:
  type: googlepubsub
  googlepubsub:
    projectId: my-gcp-project
    topic: hyperfleet-events
    subscription: hyperfleet-validation-subscription
  subscriber:
    parallelism: 1

validation:
  statusReporterImage: <The image built by https://github.com/openshift-hyperfleet/status-reporter>
  dummy:
    simulateResult: success
    resultsPath: /results/adapter-result.json
    maxWaitTimeSeconds: "300"
```

</details>

Install with values file
```bash
helm install validation-gcp ./charts/ -f my-values.yaml
```
> Note: If you encounter a `PermissionDenied` related subscription error in the Pod, refer to [With GCP Workload Identity and RBAC](#with-gcp-workload-identity-and-rbac) to grant the required permissions first.

## Deployment Environment Variables

The deployment sets these environment variables automatically:

| Variable | Value | Condition |
|----------|-------|-----------|
| `HYPERFLEET_API_BASE_URL` | From `hyperfleetApi.baseUrl` | When set |
| `HYPERFLEET_API_VERSION` | From `hyperfleetApi.version` | Always (default: v1) |
| `ADAPTER_CONFIG_PATH` | `/etc/adapter/adapter.yaml` | Always |
| `BROKER_CONFIG_FILE` | `/etc/broker/broker.yaml` | When `broker.type` is set |
| `BROKER_SUBSCRIPTION_ID` | From `broker.googlepubsub.subscription` | When `broker.type=googlepubsub` |
| `BROKER_TOPIC` | From `broker.googlepubsub.topic` | When `broker.type=googlepubsub` |
| `GCP_PROJECT_ID` | From `broker.googlepubsub.projectId` | When `broker.type=googlepubsub` |
| `STATUS_REPORTER_IMAGE` | From `validation.statusReporterImage` | When `deploymentMode=dummy` |
| `SIMULATE_RESULT` | From `validation.dummy.simulateResult` | When `deploymentMode=dummy` |
| `RESULTS_PATH` | From `validation.dummy.resultsPath` | When `deploymentMode=dummy` |
| `MAX_WAIT_TIME_SECONDS` | From `validation.dummy.maxWaitTimeSeconds` | When `deploymentMode=dummy` |

## License

See [LICENSE](LICENSE) for details.
