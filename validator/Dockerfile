ARG BASE_IMAGE=gcr.io/distroless/static-debian12:nonroot

# Build stage
FROM golang:1.25-alpine AS builder

# Build arguments passed from build machine
ARG VERSION=0.0.1
ARG GIT_COMMIT=unknown

# Install build dependencies
RUN apk add --no-cache make

WORKDIR /build

# Copy source code
COPY . .

# Tidy and verify Go module dependencies
RUN go mod tidy && go mod verify

# Build binary using make to include version, commit, and build date
RUN make binary VERSION=${VERSION} GIT_COMMIT=${GIT_COMMIT}

# Runtime stage
FROM ${BASE_IMAGE}

# Build arguments for labels (must be redeclared after FROM)
ARG VERSION=0.0.1

WORKDIR /app

# Copy binary from builder (make binary outputs to bin/)
COPY --from=builder /build/bin/validator /app/validator

ENTRYPOINT ["/app/validator"]

LABEL name="gcp-validator" \
      vendor="Red Hat" \
      version="${VERSION}" \
      summary="GCP Validator - Pre-provisioning validation for GCP clusters" \
      description="Validates GCP prerequisites before cluster provisioning, including API enablement and quota checks"
