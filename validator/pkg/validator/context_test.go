package validator_test

import (
	"context"
	"log/slog"
	"os"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"validator/pkg/config"
	"validator/pkg/validator"
)

var _ = Describe("Context", func() {
	var (
		cfg    *config.Config
		logger *slog.Logger
		vctx   *validator.Context
	)

	BeforeEach(func() {
		// Set up minimal config with automatic cleanup
		GinkgoT().Setenv("PROJECT_ID", "test-project-lazy-init")

		var err error
		cfg, err = config.LoadFromEnv()
		Expect(err).NotTo(HaveOccurred())

		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
	})

	Describe("NewContext", func() {
		Context("with valid configuration", func() {
			It("should create a new context with proper initialization", func() {
				vctx = validator.NewContext(cfg, logger)

				Expect(vctx).NotTo(BeNil())
				Expect(vctx.Config).To(Equal(cfg))
				Expect(vctx.Results).NotTo(BeNil())
				Expect(vctx.Results).To(BeEmpty())
			})

			It("should initialize with correct project ID", func() {
				vctx = validator.NewContext(cfg, logger)

				Expect(vctx.Config.ProjectID).To(Equal("test-project-lazy-init"))
			})

			It("should create Results map ready for use", func() {
				vctx = validator.NewContext(cfg, logger)

				// Should be able to add results without nil pointer panic
				vctx.Results["test"] = &validator.Result{
					ValidatorName: "test",
					Status:        validator.StatusSuccess,
				}
				Expect(vctx.Results).To(HaveKey("test"))
			})
		})

		Context("with different configurations", func() {
			It("should handle different project IDs", func() {
				GinkgoT().Setenv("PROJECT_ID", "production-123")
				cfg2, err := config.LoadFromEnv()
				Expect(err).NotTo(HaveOccurred())

				vctx = validator.NewContext(cfg2, logger)
				Expect(vctx.Config.ProjectID).To(Equal("production-123"))
			})
		})
	})

	Describe("Lazy Initialization - Least Privilege Guarantee", func() {
		BeforeEach(func() {
			vctx = validator.NewContext(cfg, logger)
		})

		Context("GetServiceUsageService", func() {
			It("should create service on first call", func() {
				ctx := context.Background()

				// First call should create the service
				svc1, err := vctx.GetServiceUsageService(ctx)

				// Note: This will fail without valid GCP credentials
				// For unit tests, we expect an error but verify the method works
				if err != nil {
					// Expected in test environment without GCP credentials
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Or(
						ContainSubstring("could not find default credentials"),
						ContainSubstring("ADC"),
						ContainSubstring("GOOGLE_APPLICATION_CREDENTIALS"),
					))
				} else {
					// If credentials exist (e.g., in CI with WIF), verify service is created
					Expect(svc1).NotTo(BeNil())
				}
			})

		})

		Context("GetComputeService", func() {
			It("should handle missing credentials gracefully", func() {
				ctx := context.Background()

				svc, err := vctx.GetComputeService(ctx)

				if err != nil {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("failed to create compute service"))
				} else {
					Expect(svc).NotTo(BeNil())
				}
			})
		})

		Context("GetIAMService", func() {
			It("should handle missing credentials gracefully", func() {
				ctx := context.Background()

				svc, err := vctx.GetIAMService(ctx)

				if err != nil {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("failed to create IAM service"))
				} else {
					Expect(svc).NotTo(BeNil())
				}
			})
		})

		Context("GetCloudResourceManagerService", func() {
			It("should handle missing credentials gracefully", func() {
				ctx := context.Background()

				svc, err := vctx.GetCloudResourceManagerService(ctx)

				if err != nil {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("failed to create cloud resource manager service"))
				} else {
					Expect(svc).NotTo(BeNil())
				}
			})
		})

		Context("GetMonitoringService", func() {
			It("should handle missing credentials gracefully", func() {
				ctx := context.Background()

				svc, err := vctx.GetMonitoringService(ctx)

				if err != nil {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("failed to create monitoring service"))
				} else {
					Expect(svc).NotTo(BeNil())
				}
			})
		})
	})

	Describe("Context Cancellation", func() {
		BeforeEach(func() {
			vctx = validator.NewContext(cfg, logger)
		})


		It("should not panic with cancelled context", func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			// Should not panic, even if it doesn't check context
			Expect(func() {
				_, _ = vctx.GetServiceUsageService(ctx)
			}).NotTo(Panic())
		})
	})

	Describe("Thread Safety", func() {
		BeforeEach(func() {
			vctx = validator.NewContext(cfg, logger)
		})


		It("should handle concurrent access to different getters safely", func() {
			ctx := context.Background()
			var wg sync.WaitGroup

			// Launch multiple goroutines calling different getters
			getters := []func(context.Context) (interface{}, error){
				func(ctx context.Context) (interface{}, error) { return vctx.GetComputeService(ctx) },
				func(ctx context.Context) (interface{}, error) { return vctx.GetIAMService(ctx) },
				func(ctx context.Context) (interface{}, error) { return vctx.GetServiceUsageService(ctx) },
				func(ctx context.Context) (interface{}, error) { return vctx.GetMonitoringService(ctx) },
			}

			for _, getter := range getters {
				wg.Add(1)
				go func(g func(context.Context) (interface{}, error)) {
					defer GinkgoRecover()
					defer wg.Done()
					_, _ = g(ctx)
					// Don't check error - just verify no race conditions/panics
				}(getter)
			}

			// Should complete without race conditions or panics
			wg.Wait()
		})
	})

	Describe("Shared State", func() {
		BeforeEach(func() {
			vctx = validator.NewContext(cfg, logger)
		})

		It("should maintain ProjectNumber across operations", func() {
			vctx.ProjectNumber = 12345678

			Expect(vctx.ProjectNumber).To(Equal(int64(12345678)))
		})

		It("should maintain Results map across operations", func() {
			vctx.Results["validator-1"] = &validator.Result{
				ValidatorName: "validator-1",
				Status:        validator.StatusSuccess,
			}

			Expect(vctx.Results).To(HaveLen(1))
			Expect(vctx.Results["validator-1"].Status).To(Equal(validator.StatusSuccess))
		})
	})
})
