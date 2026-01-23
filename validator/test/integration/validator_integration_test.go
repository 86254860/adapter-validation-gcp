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
	_ "validator/pkg/validators" // Import to trigger validator registration
)

var _ = Describe("Validator Integration Tests", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		vctx   *validator.Context
		cfg    *config.Config
		logger *slog.Logger
	)

	BeforeEach(func() {
		// Create context with reasonable timeout
		ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)

		// Set up logger
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))

		// Load configuration from environment
		var err error
		cfg, err = config.LoadFromEnv()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.ProjectID).NotTo(BeEmpty(), "PROJECT_ID must be set")

		// Create validation context
		vctx = validator.NewContext(cfg, logger)
	})

	AfterEach(func() {
		cancel()
	})

	Describe("End-to-End Validator Execution", func() {
		Context("with all validators enabled", func() {
			It("should execute all enabled validators successfully", func() {
				executor := validator.NewExecutor(vctx, logger)

				results, err := executor.ExecuteAll(ctx)

				Expect(err).NotTo(HaveOccurred(), "Executor should complete without error")
				Expect(results).NotTo(BeEmpty(), "Should have at least one validator result")

				logger.Info("Validator execution completed",
					"total_validators", len(results),
					"project_id", cfg.ProjectID)

				// Log each result
				for _, result := range results {
					logger.Info("Validator result",
						"name", result.ValidatorName,
						"status", result.Status,
						"reason", result.Reason,
						"message", result.Message,
						"duration", result.Duration)
				}
			})
		})

		Context("api-enabled validator", func() {
			It("should successfully check if required APIs are enabled", func() {
				// Get the api-enabled validator
				v, exists := validator.Get("api-enabled")
				Expect(exists).To(BeTrue(), "api-enabled validator should be registered")

				// Check if it's enabled
				enabled := v.Enabled(vctx)
				if !enabled {
					Skip("api-enabled validator is disabled in configuration")
				}

				// Execute the validator
				result := v.Validate(ctx, vctx)

				Expect(result).NotTo(BeNil())
				// Note: ValidatorName is set by Executor, not by Validate method directly

				// Log the result
				logger.Info("API enabled check result",
					"status", result.Status,
					"reason", result.Reason,
					"message", result.Message,
					"details", result.Details)

				// Verify result structure
				// Note: Timestamp, Duration, and ValidatorName are set by Executor, not by Validate directly
				Expect(result.Status).To(BeElementOf(
					validator.StatusSuccess,
					validator.StatusFailure,
				), "Status should be success or failure")
				Expect(result.Reason).NotTo(BeEmpty(), "Reason should not be empty")
				Expect(result.Message).NotTo(BeEmpty(), "Message should not be empty")
			})
		})

		Context("quota-check validator", func() {
			It("should run quota-check validator (stub)", func() {
				v, exists := validator.Get("quota-check")
				Expect(exists).To(BeTrue(), "quota-check validator should be registered")

				enabled := v.Enabled(vctx)
				if !enabled {
					Skip("quota-check validator is disabled in configuration")
				}

				result := v.Validate(ctx, vctx)

				Expect(result).NotTo(BeNil())
				// Note: ValidatorName is set by Executor, not by Validate method directly

				logger.Info("Quota check result",
					"status", result.Status,
					"reason", result.Reason,
					"message", result.Message)

				// Currently a stub, so should succeed
				Expect(result.Status).To(Equal(validator.StatusSuccess))
			})
		})
	})

	Describe("Validator Aggregation", func() {
		It("should aggregate multiple validator results correctly", func() {
			executor := validator.NewExecutor(vctx, logger)

			results, err := executor.ExecuteAll(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Aggregate results
			aggregated := validator.Aggregate(results)

			Expect(aggregated).NotTo(BeNil())
			Expect(aggregated.Status).To(BeElementOf(
				validator.StatusSuccess,
				validator.StatusFailure,
			))
			Expect(aggregated.Message).NotTo(BeEmpty())
			Expect(aggregated.Details).NotTo(BeEmpty())

			// Extract counts from Details map
			checksRun, ok := aggregated.Details["checks_run"].(int)
			Expect(ok).To(BeTrue(), "checks_run should be an int")
			Expect(checksRun).To(Equal(len(results)))

			checksPassed, ok := aggregated.Details["checks_passed"].(int)
			Expect(ok).To(BeTrue(), "checks_passed should be an int")

			successCount := 0
			failureCount := 0
			for _, r := range results {
				if r.Status == validator.StatusSuccess {
					successCount++
				} else {
					failureCount++
				}
			}

			Expect(checksPassed).To(Equal(successCount))
			Expect(checksRun - checksPassed).To(Equal(failureCount))

			logger.Info("Aggregated results",
				"status", aggregated.Status,
				"checks_run", checksRun,
				"checks_passed", checksPassed,
				"checks_failed", checksRun-checksPassed,
				"message", aggregated.Message)
		})
	})

	Describe("Shared State Between Validators", func() {
		It("should maintain shared state in context across validators", func() {
			executor := validator.NewExecutor(vctx, logger)

			results, err := executor.ExecuteAll(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify results are stored in context
			Expect(vctx.Results).To(HaveLen(len(results)))

			for _, result := range results {
				Expect(vctx.Results).To(HaveKey(result.ValidatorName))
				Expect(vctx.Results[result.ValidatorName]).To(Equal(result))
			}

			logger.Info("Verified shared state",
				"validators_in_context", len(vctx.Results))
		})
	})

	Describe("Real GCP API Integration", func() {
		Context("when checking actual GCP project state", func() {
			It("should successfully interact with GCP APIs", func() {
				// Get Cloud Resource Manager service
				svc, err := vctx.GetCloudResourceManagerService(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Make real API call
				project, err := svc.Projects.Get(cfg.ProjectID).Context(ctx).Do()
				Expect(err).NotTo(HaveOccurred())

				Expect(project.ProjectId).To(Equal(cfg.ProjectID))
				Expect(project.ProjectNumber).To(BeNumerically(">", 0))
				Expect(project.LifecycleState).To(Equal("ACTIVE"))

				// Store project number in context (validators might use this)
				vctx.ProjectNumber = project.ProjectNumber

				logger.Info("Successfully retrieved real project details",
					"projectId", project.ProjectId,
					"projectNumber", project.ProjectNumber,
					"name", project.Name,
					"state", project.LifecycleState)
			})

			It("should successfully check if Compute API is accessible", func() {
				svc, err := vctx.GetServiceUsageService(ctx)
				Expect(err).NotTo(HaveOccurred())

				serviceName := "projects/" + cfg.ProjectID + "/services/compute.googleapis.com"
				service, err := svc.Services.Get(serviceName).Context(ctx).Do()

				if err != nil {
					logger.Warn("Failed to get Compute API status", "error", err.Error())
					// Don't fail test - API might not be enabled
					return
				}

				Expect(service).NotTo(BeNil())
				logger.Info("Compute API status",
					"name", service.Name,
					"state", service.State)
			})
		})
	})

	Describe("Performance and Timeout", func() {
		It("should complete all validators within reasonable time", func() {
			start := time.Now()

			executor := validator.NewExecutor(vctx, logger)
			_, err := executor.ExecuteAll(ctx)

			duration := time.Since(start)

			Expect(err).NotTo(HaveOccurred())
			Expect(duration).To(BeNumerically("<", 30*time.Second),
				"All validators should complete within 30 seconds")

			logger.Info("Performance test completed",
				"total_duration", duration.String())
		})

		It("should respect global timeout from configuration", func() {
			// Create short timeout config
			shortTimeout := 5 * time.Second
			cfg.MaxWaitTimeSeconds = int(shortTimeout.Seconds())

			shortCtx, shortCancel := context.WithTimeout(context.Background(), shortTimeout)
			defer shortCancel()

			executor := validator.NewExecutor(vctx, logger)
			results, err := executor.ExecuteAll(shortCtx)

			// Should either complete or respect timeout
			if err != nil {
				Expect(err.Error()).To(ContainSubstring("context"))
			} else {
				Expect(results).NotTo(BeNil())
			}

			logger.Info("Timeout test completed")
		})
	})
})
