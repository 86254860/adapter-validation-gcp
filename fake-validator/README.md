# Fake GCP Validator

A simulated GCP validator for testing the adapter framework without making actual GCP API calls. This tool is designed for local development, testing, and CI/CD pipelines.

## Overview

The fake GCP validator mimics the behavior of a real GCP validation adapter by:
- Writing validation results in the expected JSON format
- Supporting multiple test scenarios (success, failure, hang, crash, etc.)
- Running in a Kubernetes Job with a status-reporter sidecar
- Using the same contract as the real validator
- Embedding the validator script directly in the Job YAML (no custom container image required)

## Features

- **Multiple Test Scenarios**: Simulate different validation outcomes
- **No External Dependencies**: No actual GCP API calls or custom container images required
- **Quick Feedback**: Instant results for testing the validation framework
- **Easy Configuration**: Simple environment variable configuration
- **Zero Build Overhead**: Script embedded directly in Job YAML - no container build/push needed

## Supported Scenarios

The validator supports the following simulation scenarios via the `SIMULATE_RESULT` environment variable:

| Scenario | Description | Exit Code | Result File |
|----------|-------------|-----------|-------------|
| `success` | Validation passes successfully | 0 | Valid JSON with `status: "success"` |
| `failure` | Validation fails (e.g., missing permissions) | 1 | Valid JSON with `status: "failure"` |
| `hang` | Validator hangs indefinitely | N/A | No result file written |
| `crash` | Validator crashes without writing results | 1 | No result file written |
| `invalid-json` | Writes malformed JSON | 0 | Invalid JSON |
| `missing-status` | Writes JSON missing required `status` field | 0 | Valid JSON but missing `status` |

## Quick Start

### Prerequisites

- Kubernetes cluster (for running jobs)
- kubectl configured

### Deploying to Kubernetes

1. **Apply RBAC configuration** (replace `<namespace>` with your actual namespace):
   ```bash
   sed 's/<namespace>/your-namespace/g' rbac.yaml | kubectl apply -f -
   ```

2. **Run a test job using the template**:
   Replace `<scenario>` with `success`, `failure`, `hang`, `crash`, `invalid-json`, or `missing-status`:
   ```bash
   # Replace placeholders and apply
   sed -e 's|<scenario>|success|g' \
       -e 's|<namespace>|your-namespace|g' \
       -e 's|<status-reporter-image>|quay.io/rh-ee-dawang/status-reporter:dev-04e8d0a|g' \
       job-template.yaml | kubectl apply -f -
   ```

Note: No custom container image is required. The Job uses the standard `alpine:3.19` image with an embedded validation script.

## Configuration

### Environment Variables

The fake validator accepts the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SIMULATE_RESULT` | Scenario to simulate (see table above) | `success` |
| `RESULTS_PATH` | Path where result JSON will be written | `/results/adapter-result.json` |

### Job Template Placeholders

The `job-template.yaml` file includes the following placeholders that should be replaced:

| Placeholder | Description | Example Values |
|-------------|-------------|----------------|
| `<namespace>` | Your Kubernetes namespace | `default`, `validation-testing` |
| `<scenario>` | The test scenario to run | `success`, `failure`, `hang`, `crash`, `invalid-json`, `missing-status` |
| `<status-reporter-image>` | The status-reporter container image | `quay.io/rh-ee-dawang/status-reporter:dev-04e8d0a` |

The `<scenario>` placeholder is used in multiple places:
- Job name: `fake-validator-<scenario>`
- Job labels: `job-name: fake-validator-<scenario>`
- Environment variable: `SIMULATE_RESULT: <scenario>`

## Example Results

### Success Result Example
```json
{
  "status": "success",
  "reason": "ValidationPassed",
  "message": "GCP environment validated successfully (simulated)",
  "details": {
    "simulation": true,
    "checks_run": 5,
    "checks_passed": 5,
    "timestamp": "2025-12-15T10:30:00Z"
  }
}
```

### Failure Result Example
```json
{
  "status": "failure",
  "reason": "MissingPermissions",
  "message": "Service account lacks required IAM permissions (simulated)",
  "details": {
    "simulation": true,
    "missing_permissions": [
      "compute.instances.list",
      "iam.serviceAccounts.get"
    ],
    "service_account": "fake-sa@project.iam.gserviceaccount.com",
    "timestamp": "2025-12-15T10:30:00Z"
  }
}
```

## Monitoring Job Status

Check job status:
```bash
kubectl get job fake-validator-<scenario> -n <namespace>
```

View job logs:
```bash
# Validator container logs
kubectl logs -n <namespace> -l job-name=fake-validator-<scenario> -c fake-validator

# Status reporter logs
kubectl logs -n <namespace> -l job-name=fake-validator-<scenario> -c status-reporter
```

Check job conditions (set by status-reporter):
```bash
kubectl get job fake-validator-<scenario> -n <namespace> -o jsonpath='{.status.conditions}' | jq
```

## Architecture

```
┌─────────────────────────────────────────┐
│           Kubernetes Job                │
├─────────────────────────────────────────┤
│                                         │
│  ┌──────────────────┐  ┌─────────────┐ │
│  │ fake-validator   │  │   status-   │ │
│  │                  │  │   reporter  │ │
│  │ - Writes result  │  │             │ │
│  │   to shared vol  │  │ - Monitors  │ │
│  │                  │  │   result    │ │
│  │ - Simulates      │  │ - Updates   │ │
│  │   scenarios      │  │   job status│ │
│  └────────┬─────────┘  └──────┬──────┘ │
│           │                   │        │
│           └────────┬──────────┘        │
│                    │                   │
│           ┌────────▼─────────┐         │
│           │  Shared Volume   │         │
│           │  /results/       │         │
│           └──────────────────┘         │
└─────────────────────────────────────────┘
```

## Files

- `job-template.yaml`: Kubernetes Job template with embedded validation script
- `rbac.yaml`: Kubernetes RBAC configuration (ServiceAccount, Role, RoleBinding)
- `README.md`: This file

## License

Copyright Red Hat

## Contributing

This is a testing tool for internal use. For issues or enhancements, please contact the development team.
