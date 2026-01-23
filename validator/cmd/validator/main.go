package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"validator/pkg/config"
	"validator/pkg/validator"
	_ "validator/pkg/validators" // Import to trigger init() registration
)


// main is the entry point for the GCP validator application.
// It loads configuration, executes all enabled validators, aggregates results,
// and writes the output to a JSON file.
func main() {
	// Load configuration first to get log level
	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("Configuration error", "error", err)
		os.Exit(1)
	}

	// Set up structured logger based on log level
	logLevel := parseLogLevel(cfg.LogLevel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	logger.Info("Starting GCP Validator")
	logger.Info("Loaded configuration",
		"gcp_project", cfg.ProjectID,
		"results_path", cfg.ResultsPath,
		"log_level", cfg.LogLevel,
		"max_wait_time_seconds", cfg.MaxWaitTimeSeconds)

	// Validate disabled validators against registry
	if len(cfg.DisabledValidators) > 0 {
		logger.Info("Disabled validators", "validators", cfg.DisabledValidators)
		for _, name := range cfg.DisabledValidators {
			if _, exists := validator.Get(name); !exists {
				logger.Warn("Unknown validator in DISABLED_VALIDATORS - will be ignored",
					"validator", name,
					"hint", "Check for typos. Run without DISABLED_VALIDATORS to see available validators.")
			}
		}
	}

	// Create validation context with lazy client initialization
	// Services will only be created when validators actually need them (least privilege)
	vctx := validator.NewContext(cfg, logger)

	// Create context with timeout (max time for all validators)
	validationTimeout := time.Duration(cfg.MaxWaitTimeSeconds) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), validationTimeout)
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Warn("Received shutdown signal, cancelling validation", "signal", sig)
		cancel()
	}()

	// Execute all validators
	executor := validator.NewExecutor(vctx, logger)

	results, err := executor.ExecuteAll(ctx)
	if err != nil {
		logger.Error("Validator execution failed", "error", err)
		os.Exit(1)
	}

	// Aggregate results
	aggregated := validator.Aggregate(results)

	// Write to output file
	outputFile := cfg.ResultsPath
	logger.Info("Writing results", "path", outputFile)

	data, err := json.MarshalIndent(aggregated, "", "  ")
	if err != nil {
		logger.Error("Failed to marshal results", "error", err)
		os.Exit(1)
	}

	// Ensure output directory exists
	// Note: In Kubernetes, the /results directory should be pre-created via volumeMounts
	if err := os.WriteFile(outputFile, data, 0644); err != nil {
		logger.Error("Failed to write results", "error", err, "path", outputFile)
		os.Exit(1)
	}

	// Log the results content for easy access via logs (useful in containerized environments)
	logger.Info("Results written successfully",
		"path", outputFile,
		"content", string(data))

	logger.Info("Validation completed",
		"status", aggregated.Status,
		"message", aggregated.Message)

	// Exit with appropriate code
	if aggregated.Status == validator.StatusFailure {
		logger.Warn("Validation FAILED - exiting with code 1")
		os.Exit(1)
	}

	logger.Info("Validation PASSED - exiting with code 0")
}

// parseLogLevel converts string log level to slog.Level
func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
