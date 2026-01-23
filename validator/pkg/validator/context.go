package validator

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/monitoring/v3"
	"google.golang.org/api/serviceusage/v1"

	"validator/pkg/config"
	"validator/pkg/gcp"
)

// Context provides shared resources and configuration to all validators
// Implements least-privilege principle through lazy initialization:
// - Services are only created when first requested by validators
// - OAuth scopes are only requested for services that are actually used
// - Disabled validators never trigger authentication for their services
type Context struct {
	// Configuration
	Config *config.Config

	// Client factory for creating GCP service clients
	clientFactory *gcp.ClientFactory

	// GCP Clients (lazily initialized, shared across validators)
	// These are private to enforce use of getter methods
	computeService          *compute.Service
	iamService              *iam.Service
	cloudResourceManagerSvc *cloudresourcemanager.Service
	serviceUsageService     *serviceusage.Service
	monitoringService       *monitoring.Service

	// Shared state between validators
	ProjectNumber int64

	// Results from previous validators (for dependency checking)
	Results map[string]*Result
}

// NewContext creates a new validation context with a client factory
func NewContext(cfg *config.Config, logger *slog.Logger) *Context {
	return &Context{
		Config:        cfg,
		clientFactory: gcp.NewClientFactory(cfg.ProjectID, logger),
		Results:       make(map[string]*Result),
	}
}

// GetComputeService returns the Compute Engine service, creating it lazily on first use
// Only requests compute.readonly scope when a validator actually needs it
func (c *Context) GetComputeService(ctx context.Context) (*compute.Service, error) {
	if c.computeService == nil {
		svc, err := c.clientFactory.CreateComputeService(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create compute service: %w", err)
		}
		c.computeService = svc
	}
	return c.computeService, nil
}

// GetIAMService returns the IAM service, creating it lazily on first use
// Only requests cloud-platform.read-only scope when a validator actually needs it
func (c *Context) GetIAMService(ctx context.Context) (*iam.Service, error) {
	if c.iamService == nil {
		svc, err := c.clientFactory.CreateIAMService(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create IAM service: %w", err)
		}
		c.iamService = svc
	}
	return c.iamService, nil
}

// GetCloudResourceManagerService returns the Cloud Resource Manager service, creating it lazily on first use
// Only requests cloudresourcemanager.readonly scope when a validator actually needs it
func (c *Context) GetCloudResourceManagerService(ctx context.Context) (*cloudresourcemanager.Service, error) {
	if c.cloudResourceManagerSvc == nil {
		svc, err := c.clientFactory.CreateCloudResourceManagerService(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create cloud resource manager service: %w", err)
		}
		c.cloudResourceManagerSvc = svc
	}
	return c.cloudResourceManagerSvc, nil
}

// GetServiceUsageService returns the Service Usage service, creating it lazily on first use
// Only requests serviceusage.readonly scope when a validator actually needs it
func (c *Context) GetServiceUsageService(ctx context.Context) (*serviceusage.Service, error) {
	if c.serviceUsageService == nil {
		svc, err := c.clientFactory.CreateServiceUsageService(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create service usage service: %w", err)
		}
		c.serviceUsageService = svc
	}
	return c.serviceUsageService, nil
}

// GetMonitoringService returns the Monitoring service, creating it lazily on first use
// Only requests monitoring.read scope when a validator actually needs it
func (c *Context) GetMonitoringService(ctx context.Context) (*monitoring.Service, error) {
	if c.monitoringService == nil {
		svc, err := c.clientFactory.CreateMonitoringService(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create monitoring service: %w", err)
		}
		c.monitoringService = svc
	}
	return c.monitoringService, nil
}
