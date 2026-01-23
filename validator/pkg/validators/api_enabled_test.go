package validators_test

import (
	"log/slog"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"validator/pkg/config"
	"validator/pkg/validator"
	"validator/pkg/validators"
)

var _ = Describe("APIEnabledValidator", func() {
	var (
		v    *validators.APIEnabledValidator
		vctx *validator.Context
	)

	BeforeEach(func() {
		v = &validators.APIEnabledValidator{}

		// Set up minimal config with automatic cleanup
		GinkgoT().Setenv("PROJECT_ID", "test-project")
		GinkgoT().Setenv("REQUIRED_APIS", "")

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
			Expect(meta.Name).To(Equal("api-enabled"))
			Expect(meta.Description).To(ContainSubstring("GCP APIs"))
			Expect(meta.RunAfter).To(BeEmpty()) // No dependencies - WIF is implicitly validated
			Expect(meta.Tags).To(ContainElement("mvp"))
			Expect(meta.Tags).To(ContainElement("gcp-api"))
		})

		It("should have no dependencies (Level 0)", func() {
			meta := v.Metadata()
			Expect(meta.RunAfter).To(BeEmpty())
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
				GinkgoT().Setenv("DISABLED_VALIDATORS", "api-enabled")
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

	Describe("Configuration", func() {
		It("should use default required APIs", func() {
			Expect(vctx.Config.RequiredAPIs).To(ConsistOf(
				"compute.googleapis.com",
				"iam.googleapis.com",
				"cloudresourcemanager.googleapis.com",
			))
		})

		Context("with custom required APIs", func() {
			BeforeEach(func() {
				GinkgoT().Setenv("REQUIRED_APIS", "storage.googleapis.com,bigquery.googleapis.com")
				cfg, err := config.LoadFromEnv()
				Expect(err).NotTo(HaveOccurred())
				vctx.Config = cfg
			})

			It("should use custom APIs list", func() {
				Expect(vctx.Config.RequiredAPIs).To(ConsistOf(
					"storage.googleapis.com",
					"bigquery.googleapis.com",
				))
			})
		})

		Context("with APIs containing whitespace", func() {
			BeforeEach(func() {
				GinkgoT().Setenv("REQUIRED_APIS", " storage.googleapis.com , bigquery.googleapis.com ")
				cfg, err := config.LoadFromEnv()
				Expect(err).NotTo(HaveOccurred())
				vctx.Config = cfg
			})

			It("should trim whitespace from API names", func() {
				Expect(vctx.Config.RequiredAPIs).To(ConsistOf(
					"storage.googleapis.com",
					"bigquery.googleapis.com",
				))
			})
		})
	})

	Describe("GCP Project Configuration", func() {
		It("should have GCP project ID from config", func() {
			Expect(vctx.Config.ProjectID).To(Equal("test-project"))
		})

		Context("with different project ID", func() {
			BeforeEach(func() {
				GinkgoT().Setenv("PROJECT_ID", "production-project-456")
				cfg, err := config.LoadFromEnv()
				Expect(err).NotTo(HaveOccurred())
				vctx.Config = cfg
			})

			It("should use the specified project ID", func() {
				Expect(vctx.Config.ProjectID).To(Equal("production-project-456"))
			})
		})
	})

	// Note: Testing Validate() method requires either:
	// 1. A real GCP project with Service Usage API enabled (integration test)
	// 2. Mocked GCP client (complex setup)
	// These tests would be added in integration test suite
})
