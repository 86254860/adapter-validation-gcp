package validator_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"validator/pkg/config"
	"validator/pkg/validator"
)

var _ = Describe("Executor", func() {
	var (
		ctx      context.Context
		vctx     *validator.Context
		executor *validator.Executor
		logger   *slog.Logger
	)

	BeforeEach(func() {
		ctx = context.Background()
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelWarn, // Reduce noise in test output
		}))

		// Clear the global registry before each test
		validator.ClearRegistry()

		// Set up minimal config with automatic cleanup
		GinkgoT().Setenv("PROJECT_ID", "test-project")

		cfg, err := config.LoadFromEnv()
		Expect(err).NotTo(HaveOccurred())

		// Use NewContext constructor for proper initialization
		vctx = validator.NewContext(cfg, logger)
	})

	Describe("ExecuteAll", func() {
		Context("with no validators registered", func() {
			It("should return error when no validators are enabled", func() {
				executor = validator.NewExecutor(vctx, logger)
				results, err := executor.ExecuteAll(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no validators enabled"))
				Expect(results).To(BeNil())
			})
		})

		Context("with single validator", func() {
			var mockValidator *MockValidator

			BeforeEach(func() {
				mockValidator = &MockValidator{
					name:    "test-validator",
					enabled: true,
					validateFunc: func(ctx context.Context, vctx *validator.Context) *validator.Result {
						return &validator.Result{
							ValidatorName: "test-validator",
							Status:        validator.StatusSuccess,
							Reason:        "TestPassed",
							Message:       "Test validation successful",
						}
					},
				}
				validator.Register(mockValidator)
			})

			It("should execute the validator", func() {
				executor = validator.NewExecutor(vctx, logger)
				results, err := executor.ExecuteAll(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(results).To(HaveLen(1))
				Expect(results[0].ValidatorName).To(Equal("test-validator"))
				Expect(results[0].Status).To(Equal(validator.StatusSuccess))
			})

			It("should store result in context", func() {
				executor = validator.NewExecutor(vctx, logger)
				_, err := executor.ExecuteAll(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(vctx.Results).To(HaveKey("test-validator"))
			})

			It("should set timestamp and duration", func() {
				executor = validator.NewExecutor(vctx, logger)
				results, err := executor.ExecuteAll(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(results[0].Timestamp).NotTo(BeZero())
				Expect(results[0].Duration).To(BeNumerically(">", 0))
			})
		})

		Context("with disabled validator", func() {
			var mockValidator *MockValidator

			BeforeEach(func() {
				mockValidator = &MockValidator{
					name:    "disabled-validator",
					enabled: false,
				}
				validator.Register(mockValidator)
			})

			It("should skip disabled validators", func() {
				executor = validator.NewExecutor(vctx, logger)
				_, err := executor.ExecuteAll(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("no validators enabled"))
			})
		})

		Context("with multiple independent validators", func() {
			BeforeEach(func() {
				for i := 1; i <= 3; i++ {
					name := "validator-" + string(rune('a'+i-1))
					n := name // Capture loop variable for closure
					validator.Register(&MockValidator{
						name:    n,
						enabled: true,
						validateFunc: func(ctx context.Context, vctx *validator.Context) *validator.Result {
							time.Sleep(10 * time.Millisecond) // Simulate work
							return &validator.Result{
								ValidatorName: n,
								Status:        validator.StatusSuccess,
								Reason:        "Success",
								Message:       "Passed",
							}
						},
					})
				}
			})

			It("should execute all independent validators successfully", func() {
				executor = validator.NewExecutor(vctx, logger)
				results, err := executor.ExecuteAll(ctx)

				Expect(err).NotTo(HaveOccurred())
				Expect(results).To(HaveLen(3))
				// All validators should complete successfully
				for _, result := range results {
					Expect(result.Status).To(Equal(validator.StatusSuccess))
				}
			})

			It("should store all results in context", func() {
				executor = validator.NewExecutor(vctx, logger)
				_, err := executor.ExecuteAll(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(vctx.Results).To(HaveLen(3))
			})
		})

		Context("with dependent validators", func() {
			var executionOrder []string
			var mu sync.Mutex

			BeforeEach(func() {
				executionOrder = []string{}

				// Level 0 validator
				validator.Register(&MockValidator{
					name:     "validator-a",
					runAfter: []string{},
					enabled:  true,
					validateFunc: func(ctx context.Context, vctx *validator.Context) *validator.Result {
						mu.Lock()
						executionOrder = append(executionOrder, "validator-a")
						mu.Unlock()
						return &validator.Result{
							ValidatorName: "validator-a",
							Status:        validator.StatusSuccess,
						}
					},
				})

				// Level 1 validators (depend on validator-a)
				for _, name := range []string{"validator-b", "validator-c"} {
					n := name
					validator.Register(&MockValidator{
						name:     n,
						runAfter: []string{"validator-a"},
						enabled:  true,
						validateFunc: func(ctx context.Context, vctx *validator.Context) *validator.Result {
							mu.Lock()
							executionOrder = append(executionOrder, n)
							mu.Unlock()
							return &validator.Result{
								ValidatorName: n,
								Status:        validator.StatusSuccess,
							}
						},
					})
				}
			})

			It("should execute validators in dependency order", func() {
				executor = validator.NewExecutor(vctx, logger)
				results, err := executor.ExecuteAll(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(results).To(HaveLen(3))

				// validator-a should execute before b and c
				Expect(executionOrder[0]).To(Equal("validator-a"))
				Expect(executionOrder[1:]).To(ConsistOf("validator-b", "validator-c"))
			})
		})

		Context("with StopOnFirstFailure enabled", func() {
			BeforeEach(func() {
				vctx.Config.StopOnFirstFailure = true

				// First validator fails
				validator.Register(&MockValidator{
					name:    "failing-validator",
					enabled: true,
					validateFunc: func(ctx context.Context, vctx *validator.Context) *validator.Result {
						return &validator.Result{
							ValidatorName: "failing-validator",
							Status:        validator.StatusFailure,
							Reason:        "TestFailure",
							Message:       "Intentional failure",
						}
					},
				})

				// Second validator should not run
				validator.Register(&MockValidator{
					name:     "should-not-run",
					runAfter: []string{"failing-validator"},
					enabled:  true,
					validateFunc: func(ctx context.Context, vctx *validator.Context) *validator.Result {
						Fail("This validator should not execute")
						return nil
					},
				})
			})

			It("should stop execution after first failure", func() {
				executor = validator.NewExecutor(vctx, logger)
				results, err := executor.ExecuteAll(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(results).To(HaveLen(1))
				Expect(results[0].Status).To(Equal(validator.StatusFailure))
			})
		})

		Context("with validator that returns failure", func() {
			BeforeEach(func() {
				validator.Register(&MockValidator{
					name:    "failing-validator",
					enabled: true,
					validateFunc: func(ctx context.Context, vctx *validator.Context) *validator.Result {
						return &validator.Result{
							ValidatorName: "failing-validator",
							Status:        validator.StatusFailure,
							Reason:        "ValidationFailed",
							Message:       "Validation check failed",
							Details: map[string]interface{}{
								"error": "Test error",
							},
						}
					},
				})
			})

			It("should return the failure result", func() {
				executor = validator.NewExecutor(vctx, logger)
				results, err := executor.ExecuteAll(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(results).To(HaveLen(1))
				Expect(results[0].Status).To(Equal(validator.StatusFailure))
				Expect(results[0].Reason).To(Equal("ValidationFailed"))
			})
		})
	})
})
