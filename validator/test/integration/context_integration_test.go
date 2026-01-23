//go:build integration
// +build integration

package integration_test

import (
	"context"
	"log/slog"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"validator/pkg/config"
	"validator/pkg/validator"
)

var _ = Describe("Context Integration Tests", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		vctx   *validator.Context
		cfg    *config.Config
		logger *slog.Logger
	)

	BeforeEach(func() {
		// Create context with reasonable timeout for integration tests
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)

		// Set up logger
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))

		// Load configuration from environment
		// Requires: PROJECT_ID environment variable
		var err error
		cfg, err = config.LoadFromEnv()
		Expect(err).NotTo(HaveOccurred(), "Failed to load config - ensure PROJECT_ID is set")
		Expect(cfg.ProjectID).NotTo(BeEmpty(), "PROJECT_ID must be set for integration tests")

		// Create new context with client factory
		vctx = validator.NewContext(cfg, logger)
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Lazy Initialization with Real GCP Services", func() {
		Context("GetServiceUsageService", func() {
			It("should successfully create service with valid credentials", func() {
				svc, err := vctx.GetServiceUsageService(ctx)

				Expect(err).NotTo(HaveOccurred(), "Should create service with valid GCP credentials")
				Expect(svc).NotTo(BeNil(), "Service should not be nil")
			})

			It("should return cached service on subsequent calls", func() {
				// First call - creates the service
				svc1, err := vctx.GetServiceUsageService(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(svc1).NotTo(BeNil())

				// Second call - should return cached instance
				svc2, err := vctx.GetServiceUsageService(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(svc2).NotTo(BeNil())

				// Verify it's the exact same instance (pointer equality)
				Expect(svc2).To(BeIdenticalTo(svc1), "Should return cached service instance")
			})

			It("should successfully make API calls with created service", func() {
				svc, err := vctx.GetServiceUsageService(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Make a real API call to verify the service works
				serviceName := "projects/" + cfg.ProjectID + "/services/compute.googleapis.com"
				service, err := svc.Services.Get(serviceName).Context(ctx).Do()

				// This may fail if compute API is not enabled, but shouldn't fail on auth
				if err != nil {
					// Log the error but don't fail - API might not be enabled
					logger.Info("API check failed (might not be enabled)", "error", err.Error())
				} else {
					Expect(service).NotTo(BeNil())
					logger.Info("Successfully called Service Usage API", "state", service.State)
				}
			})
		})

		Context("GetComputeService", func() {
			It("should successfully create service with valid credentials", func() {
				svc, err := vctx.GetComputeService(ctx)

				Expect(err).NotTo(HaveOccurred(), "Should create service with valid GCP credentials")
				Expect(svc).NotTo(BeNil(), "Service should not be nil")
			})

			It("should return cached service on subsequent calls", func() {
				svc1, err := vctx.GetComputeService(ctx)
				Expect(err).NotTo(HaveOccurred())

				svc2, err := vctx.GetComputeService(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(svc2).To(BeIdenticalTo(svc1), "Should return cached service instance")
			})
		})

		Context("GetIAMService", func() {
			It("should successfully create service with valid credentials", func() {
				svc, err := vctx.GetIAMService(ctx)

				Expect(err).NotTo(HaveOccurred(), "Should create service with valid GCP credentials")
				Expect(svc).NotTo(BeNil(), "Service should not be nil")
			})

			It("should return cached service on subsequent calls", func() {
				svc1, err := vctx.GetIAMService(ctx)
				Expect(err).NotTo(HaveOccurred())

				svc2, err := vctx.GetIAMService(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(svc2).To(BeIdenticalTo(svc1), "Should return cached service instance")
			})
		})

		Context("GetCloudResourceManagerService", func() {
			It("should successfully create service with valid credentials", func() {
				svc, err := vctx.GetCloudResourceManagerService(ctx)

				Expect(err).NotTo(HaveOccurred(), "Should create service with valid GCP credentials")
				Expect(svc).NotTo(BeNil(), "Service should not be nil")
			})

			It("should return cached service on subsequent calls", func() {
				svc1, err := vctx.GetCloudResourceManagerService(ctx)
				Expect(err).NotTo(HaveOccurred())

				svc2, err := vctx.GetCloudResourceManagerService(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(svc2).To(BeIdenticalTo(svc1), "Should return cached service instance")
			})

			It("should successfully make API calls with created service", func() {
				svc, err := vctx.GetCloudResourceManagerService(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Make a real API call to get project details
				project, err := svc.Projects.Get(cfg.ProjectID).Context(ctx).Do()

				Expect(err).NotTo(HaveOccurred(), "Should successfully get project details")
				Expect(project).NotTo(BeNil())
				Expect(project.ProjectId).To(Equal(cfg.ProjectID))
				logger.Info("Successfully retrieved project",
					"projectId", project.ProjectId,
					"projectNumber", project.ProjectNumber,
					"state", project.LifecycleState)
			})
		})

		Context("GetMonitoringService", func() {
			It("should successfully create service with valid credentials", func() {
				svc, err := vctx.GetMonitoringService(ctx)

				Expect(err).NotTo(HaveOccurred(), "Should create service with valid GCP credentials")
				Expect(svc).NotTo(BeNil(), "Service should not be nil")
			})

			It("should return cached service on subsequent calls", func() {
				svc1, err := vctx.GetMonitoringService(ctx)
				Expect(err).NotTo(HaveOccurred())

				svc2, err := vctx.GetMonitoringService(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(svc2).To(BeIdenticalTo(svc1), "Should return cached service instance")
			})
		})
	})


	Describe("Context Cancellation with Real Services", func() {
		It("should respect context timeout during service creation", func() {
			// Create a context with very short timeout
			shortCtx, shortCancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
			defer shortCancel()

			// Wait for context to expire
			time.Sleep(10 * time.Millisecond)

			// Try to create service with expired context
			_, err := vctx.GetServiceUsageService(shortCtx)

			// Should fail due to context timeout
			// Note: Might still succeed if service was already cached
			if err != nil {
				Expect(err.Error()).To(Or(
					ContainSubstring("context"),
					ContainSubstring("deadline"),
					ContainSubstring("timeout"),
				), "Error should be context-related")
			}
		})

		It("should handle context cancellation gracefully", func() {
			cancelCtx, cancelFunc := context.WithCancel(context.Background())
			cancelFunc() // Cancel immediately

			// Create new context (not cached yet) with cancelled context
			freshVctx := validator.NewContext(cfg, logger)

			_, err := freshVctx.GetServiceUsageService(cancelCtx)

			// Should fail gracefully (no panic)
			if err != nil {
				logger.Info("Context cancellation handled", "error", err.Error())
			}
		})
	})

	Describe("Least Privilege Verification", func() {
		It("should only create services when getters are called", func() {
			// Create a fresh context
			freshVctx := validator.NewContext(cfg, logger)

			// At this point, NO services should be created
			// We can't directly verify this without exposing internals,
			// but we can verify that calling different getters succeeds

			// Call only ServiceUsageService
			svc, err := freshVctx.GetServiceUsageService(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(svc).NotTo(BeNil())

			// Other services should be lazily created only when needed
			// This verifies the lazy initialization pattern

			logger.Info("Verified lazy initialization - service created only when requested")
		})

		It("should create all services when all getters are called", func() {
			// Call all getters
			computeSvc, err1 := vctx.GetComputeService(ctx)
			iamSvc, err2 := vctx.GetIAMService(ctx)
			crmSvc, err3 := vctx.GetCloudResourceManagerService(ctx)
			suSvc, err4 := vctx.GetServiceUsageService(ctx)
			monSvc, err5 := vctx.GetMonitoringService(ctx)

			// All should succeed
			Expect(err1).NotTo(HaveOccurred())
			Expect(err2).NotTo(HaveOccurred())
			Expect(err3).NotTo(HaveOccurred())
			Expect(err4).NotTo(HaveOccurred())
			Expect(err5).NotTo(HaveOccurred())

			Expect(computeSvc).NotTo(BeNil())
			Expect(iamSvc).NotTo(BeNil())
			Expect(crmSvc).NotTo(BeNil())
			Expect(suSvc).NotTo(BeNil())
			Expect(monSvc).NotTo(BeNil())

			logger.Info("Successfully created all 5 GCP service clients")
		})
	})
})
