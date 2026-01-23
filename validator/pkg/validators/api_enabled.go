package validators

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/api/googleapi"
	"validator/pkg/validator"
)

const (
	// Timeout for overall API validation
	apiValidationTimeout = 2 * time.Minute
	// Timeout for individual API check requests
	apiRequestTimeout = 30 * time.Second
)

// extractErrorReason extracts a structured error reason from GCP API errors
// Prioritizes GCP-specific error reasons, falls back to HTTP status code
func extractErrorReason(err error, fallbackReason string) string {
	if err == nil {
		return fallbackReason
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		// First, try to get GCP-specific reason (more detailed)
		if len(apiErr.Errors) > 0 && apiErr.Errors[0].Reason != "" {
			return apiErr.Errors[0].Reason
		}

		// No specific reason provided, return generic HTTP code
		return fmt.Sprintf("HTTP_%d", apiErr.Code)
	}

	// Not a GCP API error, use fallback
	return fallbackReason
}

// APIEnabledValidator checks if required GCP APIs are enabled
type APIEnabledValidator struct{}

// init registers the APIEnabledValidator with the global validator registry
func init() {
	validator.Register(&APIEnabledValidator{})
}

// Metadata returns the validator configuration including name, description, and dependencies
func (v *APIEnabledValidator) Metadata() validator.ValidatorMetadata {
	return validator.ValidatorMetadata{
		Name:        "api-enabled",
		Description: "Verify required GCP APIs are enabled in the target project",
		RunAfter:    []string{}, // No dependencies - WIF is implicitly validated when API calls succeed
		Tags:        []string{"mvp", "gcp-api"},
	}
}

// Enabled determines if this validator should run based on configuration
func (v *APIEnabledValidator) Enabled(ctx *validator.Context) bool {
	return ctx.Config.IsValidatorEnabled("api-enabled")
}

// Validate performs the actual validation logic to check if required GCP APIs are enabled
func (v *APIEnabledValidator) Validate(ctx context.Context, vctx *validator.Context) *validator.Result {
	slog.Info("Checking if required GCP APIs are enabled")

	// Add timeout for overall validation
	ctx, cancel := context.WithTimeout(ctx, apiValidationTimeout)
	defer cancel()

	// Get Service Usage client from context (lazy initialization with least privilege)
	// Only requests serviceusage.readonly scope when this validator actually runs
	svc, err := vctx.GetServiceUsageService(ctx)
	if err != nil {
		// Log full error for debugging
		slog.Error("Failed to get Service Usage client",
			"error", err.Error(),
			"project_id", vctx.Config.ProjectID)

		// Extract structured reason
		reason := extractErrorReason(err, "ServiceUsageClientError")

		return &validator.Result{
			Status:  validator.StatusFailure,
			Reason:  reason,
			Message: fmt.Sprintf("Failed to get Service Usage client (check WIF configuration): %v", err),
			Details: map[string]interface{}{
				//"error":       err.Error(),
				"error_type": fmt.Sprintf("%T", err),
				"project_id": vctx.Config.ProjectID,
				"hint":       "Verify WIF annotation on KSA and IAM bindings for GSA",
			},
		}
	}

	// Check each required API
	requiredAPIs := vctx.Config.RequiredAPIs
	enabledAPIs := []string{}
	disabledAPIs := []string{}

	for _, apiName := range requiredAPIs {
		// Add per-request timeout
		reqCtx, reqCancel := context.WithTimeout(ctx, apiRequestTimeout)

		serviceName := fmt.Sprintf("projects/%s/services/%s", vctx.Config.ProjectID, apiName)

		slog.Debug("Checking API", "api", apiName)
		service, err := svc.Services.Get(serviceName).Context(reqCtx).Do()
		reqCancel() // Clean up context

		if err != nil {
			// Log full error for debugging
			slog.Error("Failed to check API",
				"api", apiName,
				"error", err.Error(),
				"project_id", vctx.Config.ProjectID,
				"service_name", serviceName)

			// Extract structured reason
			reason := extractErrorReason(err, "APICheckFailed")

			return &validator.Result{
				Status:  validator.StatusFailure,
				Reason:  reason,
				Message: fmt.Sprintf("Failed to check API %s: %v", apiName, err),
				Details: map[string]interface{}{
					"api": apiName,
					//"error":        err.Error(),
					"error_type":   fmt.Sprintf("%T", err),
					"project_id":   vctx.Config.ProjectID,
					"service_name": serviceName,
				},
			}
		}

		if service.State == "ENABLED" {
			enabledAPIs = append(enabledAPIs, apiName)
			slog.Debug("API is enabled", "api", apiName)
		} else {
			disabledAPIs = append(disabledAPIs, apiName)
			slog.Warn("API is NOT enabled", "api", apiName, "state", service.State)
		}
	}

	// Check if any APIs are disabled
	if len(disabledAPIs) > 0 {
		return &validator.Result{
			Status:  validator.StatusFailure,
			Reason:  "RequiredAPIsDisabled",
			Message: fmt.Sprintf("%d required API(s) are not enabled", len(disabledAPIs)),
			Details: map[string]interface{}{
				"disabled_apis": disabledAPIs,
				"enabled_apis":  enabledAPIs,
				"project_id":    vctx.Config.ProjectID,
				"hint":          "Enable APIs with: gcloud services enable <api-name>",
			},
		}
	}

	// Build success message based on whether APIs were checked
	message := fmt.Sprintf("All %d required APIs are enabled", len(enabledAPIs))
	if len(enabledAPIs) == 0 {
		message = "No required APIs to validate"
	}
	slog.Info(message)

	return &validator.Result{
		Status:  validator.StatusSuccess,
		Reason:  "AllAPIsEnabled",
		Message: message,
		Details: map[string]interface{}{
			"enabled_apis": enabledAPIs,
			"project_id":   vctx.Config.ProjectID,
		},
	}
}
