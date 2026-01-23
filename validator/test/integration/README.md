# Integration Tests

This directory contains **real integration tests** that interact with actual GCP APIs to validate the validator implementation.

## ⚠️ Requirements

These tests **require**:

1. **Real GCP Project** - with a valid PROJECT_ID
2. **Valid GCP Authentication** - one of:
   - Workload Identity Federation (WIF) in Kubernetes
   - Service Account key file
   - Application Default Credentials (ADC) via `gcloud auth`
3. **Network Access** - to GCP APIs (*.googleapis.com)
4. **IAM Permissions** - on the target GCP project:
   - `serviceusage.services.get` (Service Usage Viewer)
   - `resourcemanager.projects.get` (Project Viewer)
   - `compute.projects.get` (Compute Viewer)
   - `iam.roles.get` (IAM Role Viewer)

## 🚀 Running Tests Locally

### Step 1: Authenticate with GCP

```bash
# Option A: Use your user credentials (recommended for local dev)
gcloud auth application-default login

# Option B: Use a service account key file
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

### Step 2: Set Required Environment Variables

```bash
export PROJECT_ID="your-gcp-project-id"

# Optional: Customize API list (defaults are provided)
export REQUIRED_APIS="compute.googleapis.com,iam.googleapis.com,cloudresourcemanager.googleapis.com"

# Optional: Set log level
export LOG_LEVEL="info"
```

### Step 3: Run Integration Tests

```bash
make test-integration
```
