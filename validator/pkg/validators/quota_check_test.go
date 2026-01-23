package validators_test

import (
	"context"
	"log/slog"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"validator/pkg/config"
	"validator/pkg/validator"
	"validator/pkg/validators"
)

var _ = Describe("QuotaCheckValidator", func() {
	var (
		v    *validators.QuotaCheckValidator
		vctx *validator.Context
	)

	BeforeEach(func() {
		v = &validators.QuotaCheckValidator{}

		// Set up minimal config with automatic cleanup
		GinkgoT().Setenv("PROJECT_ID", "test-project")

		cfg, err := config.LoadFromEnv()
		Expect(err).NotTo(HaveOccurred())

		// Use NewContext constructor for proper initialization
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
		vctx = validator.NewContext(cfg, logger)
	})

	Describe("Metadata", func() {
		It("should return correct metadata", func() {
			meta := v.Metadata()
			Expect(meta.Name).To(Equal("quota-check"))
			Expect(meta.Description).To(ContainSubstring("quota"))
			Expect(meta.Description).To(ContainSubstring("stub"))
			Expect(meta.RunAfter).To(ConsistOf("api-enabled")) // Depends on api-enabled
			Expect(meta.Tags).To(ContainElement("post-mvp"))
			Expect(meta.Tags).To(ContainElement("quota"))
			Expect(meta.Tags).To(ContainElement("stub"))
		})

		It("should depend on api-enabled (Level 1)", func() {
			meta := v.Metadata()
			Expect(meta.RunAfter).To(ConsistOf("api-enabled"))
		})
	})

	Describe("Enabled", func() {
		Context("when validator is not explicitly disabled", func() {
			It("should be enabled by default", func() {
				enabled := v.Enabled(vctx)
				Expect(enabled).To(BeTrue())
			})
		})

		Context("when validator is explicitly disabled", func() {
			BeforeEach(func() {
				GinkgoT().Setenv("DISABLED_VALIDATORS", "quota-check")
				cfg, err := config.LoadFromEnv()
				Expect(err).NotTo(HaveOccurred())
				vctx.Config = cfg
			})

			It("should be disabled", func() {
				enabled := v.Enabled(vctx)
				Expect(enabled).To(BeFalse())
			})
		})

	})

	Describe("Validate", func() {
		It("should return success with stub message", func() {
			ctx := context.Background()
			result := v.Validate(ctx, vctx)
			Expect(result).NotTo(BeNil())
			Expect(result.Status).To(Equal(validator.StatusSuccess))
			Expect(result.Reason).To(Equal("QuotaCheckStub"))
			Expect(result.Message).To(ContainSubstring("not yet implemented"))
		})

		It("should include stub metadata in details", func() {
			ctx := context.Background()
			result := v.Validate(ctx, vctx)
			Expect(result.Details).To(HaveKey("stub"))
			Expect(result.Details["stub"]).To(BeTrue())
			Expect(result.Details).To(HaveKey("implemented"))
			Expect(result.Details["implemented"]).To(BeFalse())
		})
	})
})
