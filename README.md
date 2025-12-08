# adapter-validation-gcp

This repository provides the foundation for validating GCP environments before cluster deployment.

## Overview

This repository contains components for validating GCP prerequisites and reporting validation results in Kubernetes environments. It serves as the foundational infrastructure for all future GCP validators.

## Components

### 1. Status Reporter (Implemented ✅)

A **cloud-agnostic**, **reusable** Kubernetes sidecar container that monitors adapter operation results and updates Job status. It works with any adapter container (validation, DNS, pull secret, etc.) that follows the defined result contract.

**Key Features:**
- Monitors adapter container execution via file polling and container state watching
- Handles various failure scenarios (OOMKilled, crashes, timeouts, invalid results)
- Updates Kubernetes Job status with detailed condition information
- Zero-dependency on adapter implementation - uses simple JSON contract
- Always exits successfully (never fails the parent Job)

**Location:** `status-reporter/`

### 2. Fake GCP Validator (Planned 🚧)

A **simulated** GCP validator that mimics real validation behavior without making actual GCP API calls. This component is essential for:
- Local development and testing
- CI/CD pipeline validation
- Integration testing without GCP credentials
- Rapid iteration on validation logic

**Planned Features:**
- Configurable success/failure scenarios
- Deterministic test cases for all validation types
- No GCP credentials or API quotas required

**Status:** Not yet implemented

### 3. Minimal Real GCP Validator (Planned 🚧)

A **minimal production** GCP validator that performs actual API calls to validate the foundational requirements before cluster creation.

**Planned Features:**
- Workload Identity Federation (WIF) configuration validation
- Minimal required GCP API enablement checks (e.g., `compute.googleapis.com`, `iam.googleapis.com`)
- Service account permissions verification
- Real GCP API integration with proper error handling
- Serves as reference implementation for future validators

**Validation Scope (Minimal Set):**
- ✓ Workload Identity configured correctly
- ✓ Essential GCP APIs enabled
- ✓ Service account has minimum required permissions

**Status:** Not yet implemented

## Adapter Contract

The status reporter works with any adapter container that follows this simple JSON contract:

1. **Result File Requirements:**
    - **Location:** Write results to `/results/adapter-result.json` (configurable via `RESULTS_PATH` env var)
    - **Format:** Valid JSON file (max size: 1MB)
    - **Timing:** Must be written before the adapter container exits or within the configured timeout (default: 300 seconds)

2. **JSON Schema:**
   ```json
   {
     "status": "success",           // Required: "success" or "failure"
     "reason": "AllChecksPassed",   // Required: Machine-readable identifier (max 128 chars)
     "message": "All validation checks passed successfully",  // Required: Human-readable description (max 1024 chars)
     "details": {                   // Optional: Adapter-specific data (any valid JSON), this information will not be reflected in k8s Job Status
       "checks_run": 5,
       "duration_ms": 1234
     }
   }
   ```

3. **Field Validation:**
    - `status`: Must be exactly `"success"` or `"failure"` (case-sensitive)
    - `reason`: Trimmed and truncated to 128 characters. Defaults to `"NoReasonProvided"` if empty/missing
    - `message`: Trimmed and truncated to 1024 characters. Defaults to `"No message provided"` if empty/missing
    - `details`: Optional JSON object containing any adapter-specific information

4. **Examples:**

   **Success result:**

   Adapter writes to `/results/adapter-result.json`:
   ```json
   {
     "status": "success",
     "reason": "ValidationPassed",
     "message": "GCP environment validated successfully"
   }
   ```

   Resulting Kubernetes Job status:
   ```yaml
   status:
     conditions:
     - type: Available
       status: "True"
       reason: ValidationPassed
       message: GCP environment validated successfully
       lastTransitionTime: "2024-01-15T10:30:00Z"
   ```

   **Failure result with details:**

   Adapter writes to `/results/adapter-result.json`:
   ```json
   {
     "status": "failure",
     "reason": "MissingPermissions",
     "message": "Service account lacks required IAM permissions",
     "details": {
       "missing_permissions": ["compute.instances.list", "iam.serviceAccounts.get"],
       "service_account": "my-sa@project.iam.gserviceaccount.com"
     }
   }
   ```

   Resulting Kubernetes Job status:
   ```yaml
   status:
     conditions:
     - type: Available
       status: "False"
       reason: MissingPermissions
       message: Service account lacks required IAM permissions
       lastTransitionTime: "2024-01-15T10:30:00Z"
   ```

   **Timeout scenario:**

   If adapter doesn't write result file within timeout, Job status will be:
   ```yaml
   status:
     conditions:
     - type: Available
       status: "False"
       reason: AdapterTimeout
       message: "Adapter did not produce results within 5m0s"
       lastTransitionTime: "2024-01-15T10:30:00Z"
   ```

   **Container crash scenario:**

   If adapter container exits with non-zero code, Job status will be:
   ```yaml
   status:
     conditions:
     - type: Available
       status: "False"
       reason: AdapterExitedWithError
       message: "Adapter container exited with code 1: Error"
       lastTransitionTime: "2024-01-15T10:30:00Z"
   ```

   **OOMKilled scenario:**

   If adapter container is killed due to memory limits:
   ```yaml
   status:
     conditions:
     - type: Available
       status: "False"
       reason: AdapterOOMKilled
       message: "Adapter container was killed due to out of memory (OOMKilled)"
       lastTransitionTime: "2024-01-15T10:30:00Z"
   ```

   **Invalid result format:**

   If adapter writes invalid JSON or schema:
   ```yaml
   status:
     conditions:
     - type: Available
       status: "False"
       reason: InvalidResultFormat
       message: "Failed to parse adapter result: status: must be either 'success' or 'failure'"
       lastTransitionTime: "2024-01-15T10:30:00Z"
   ```

5. **Shared Volume Configuration:**

   Both adapter and status reporter containers must share a volume mounted at `/results`:

   ```yaml
   volumes:
   - name: results
     emptyDir: {}

   containers:
   - name: adapter
     volumeMounts:
     - name: results
       mountPath: /results

   - name: status-reporter
     volumeMounts:
     - name: results
       mountPath: /results
   ```

## Repository Structure

```text
adapter-validation-gcp/
├── status-reporter/          # ✅ Cloud-agnostic Kubernetes status reporter
│   ├── cmd/reporter/         # Main entry point
│   ├── pkg/                  # Core packages (reporter, k8s, result parser)
│   ├── Dockerfile            # Container image definition
│   ├── Makefile              # Build, test, and image targets
│   └── README.md             # Component-specific documentation
├── fake-validator/           # 🚧 Simulated GCP validator (planned)
├── validator/                # 🚧 Real GCP validator (planned)
└── README.md                 # This file
```

## Quick Start

### Status Reporter

The status reporter is production-ready and can be used with any adapter container.

#### Makefile Usage

```bash
$ make
Available targets:
binary               Build binary
clean                Clean build artifacts and test coverage files
fmt                  Format code with gofmt and goimports
help                 Display this help message
image-dev            Build and push to personal Quay registry (requires QUAY_USER)
image-push           Build and push container image to registry
image                Build container image with Docker or Podman
lint                 Run golangci-lint
mod-tidy             Tidy Go module dependencies
test-coverage-html   Generate HTML coverage report
test-coverage        Run unit tests with coverage report
test                 Run unit tests with race detection
verify               Run all verification checks (lint + test)
```

## License

See LICENSE file for details.

## Contact

For questions or issues, please open a GitHub issue in this repository.
