package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/openshift-hyperfleet/adapter-validation-gcp/status-reporter/pkg/config"
	"github.com/openshift-hyperfleet/adapter-validation-gcp/status-reporter/pkg/reporter"
)

const (
	shutdownTimeout = 5 * time.Second
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Status Reporter starting...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	logConfig(cfg)

	rep, err := reporter.NewReporter(
		cfg.ResultsPath,
		cfg.GetPollInterval(),
		cfg.GetMaxWaitTime(),
		cfg.ConditionType,
		cfg.PodName,
		cfg.AdapterContainerName,
		cfg.JobName,
		cfg.JobNamespace,
	)
	if err != nil {
		log.Fatalf("Failed to create reporter: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	doneChan := make(chan struct{})

	go func() {
		defer close(doneChan)
		errChan <- rep.Run(ctx)
	}()

	select {
	case sig := <-sigChan:
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()

		shutdownTimer := time.NewTimer(shutdownTimeout)
		defer shutdownTimer.Stop()

		select {
		case <-doneChan:
			log.Println("Reporter finished during shutdown")
		case <-shutdownTimer.C:
			log.Printf("Shutdown timeout (%s) exceeded", shutdownTimeout)
		}

		// Check for errors even during signal shutdown
		if err := <-errChan; err != nil {
			log.Printf("Reporter finished with error: %v", err)
			os.Exit(1)
		}

		log.Println("Shutdown complete")
		os.Exit(0)

	case <-doneChan:
		if err := <-errChan; err != nil {
			log.Printf("Reporter finished with error: %v", err)
			os.Exit(1)
		}
		log.Println("Reporter finished successfully")
		os.Exit(0)
	}
}

// logConfig logs the loaded configuration
func logConfig(cfg *config.Config) {
	log.Println("Configuration:")
	log.Printf("  JOB_NAME: %s", cfg.JobName)
	log.Printf("  JOB_NAMESPACE: %s", cfg.JobNamespace)
	log.Printf("  POD_NAME: %s", cfg.PodName)
	if cfg.AdapterContainerName != "" {
		log.Printf("  ADAPTER_CONTAINER_NAME: %s", cfg.AdapterContainerName)
	} else {
		log.Printf("  ADAPTER_CONTAINER_NAME: (auto-detect)")
	}
	log.Printf("  RESULTS_PATH: %s", cfg.ResultsPath)
	log.Printf("  POLL_INTERVAL_SECONDS: %d", cfg.PollIntervalSeconds)
	log.Printf("  MAX_WAIT_TIME_SECONDS: %d", cfg.MaxWaitTimeSeconds)
	log.Printf("  CONDITION_TYPE: %s", cfg.ConditionType)
	log.Printf("  LOG_LEVEL: %s", cfg.LogLevel)
}
