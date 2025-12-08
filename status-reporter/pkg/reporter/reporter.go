package reporter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/openshift-hyperfleet/adapter-validation-gcp/status-reporter/pkg/k8s"
	"github.com/openshift-hyperfleet/adapter-validation-gcp/status-reporter/pkg/result"
)

const (
	DefaultConditionType = "Available"

	ConditionStatusTrue  = "True"
	ConditionStatusFalse = "False"

	ReasonAdapterCrashed         = "AdapterCrashed"
	ReasonAdapterOOMKilled       = "AdapterOOMKilled"
	ReasonAdapterExitedWithError = "AdapterExitedWithError"
	ReasonAdapterTimeout         = "AdapterTimeout"
	ReasonInvalidResultFormat    = "InvalidResultFormat"

	ContainerReasonOOMKilled = "OOMKilled"

	containerStatusCheckTimeout = 10 * time.Second
)

// K8sClientInterface defines the k8s operations needed by StatusReporter
type K8sClientInterface interface {
	UpdateJobStatus(ctx context.Context, condition k8s.JobCondition) error
	GetAdapterContainerStatus(ctx context.Context, podName, containerName string) (*corev1.ContainerStatus, error)
}

// StatusReporter is the main status reporter
type StatusReporter struct {
	resultsPath          string
	pollInterval         time.Duration
	maxWaitTime          time.Duration
	conditionType        string
	podName              string
	adapterContainerName string
	k8sClient            K8sClientInterface
	parser               *result.Parser
}

// NewReporter creates a new status reporter
func NewReporter(resultsPath string, pollInterval, maxWaitTime time.Duration, conditionType, podName, adapterContainerName, jobName, jobNamespace string) (*StatusReporter, error) {
	if conditionType == "" {
		conditionType = DefaultConditionType
	}

	k8sClient, err := k8s.NewClient(jobNamespace, jobName)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	return newReporterWithClient(resultsPath, pollInterval, maxWaitTime, conditionType, podName, adapterContainerName, k8sClient), nil
}

// NewReporterWithClient creates a new status reporter with a custom k8s client (for testing)
func NewReporterWithClient(resultsPath string, pollInterval, maxWaitTime time.Duration, conditionType, podName, adapterContainerName string, k8sClient K8sClientInterface) *StatusReporter {
	return newReporterWithClient(resultsPath, pollInterval, maxWaitTime, conditionType, podName, adapterContainerName, k8sClient)
}

func newReporterWithClient(resultsPath string, pollInterval, maxWaitTime time.Duration, conditionType, podName, adapterContainerName string, k8sClient K8sClientInterface) *StatusReporter {
	if conditionType == "" {
		conditionType = DefaultConditionType
	}

	return &StatusReporter{
		resultsPath:          resultsPath,
		pollInterval:         pollInterval,
		maxWaitTime:          maxWaitTime,
		conditionType:        conditionType,
		podName:              podName,
		adapterContainerName: adapterContainerName,
		k8sClient:            k8sClient,
		parser:               result.NewParser(),
	}
}

// Run starts the reporter and blocks until completion
func (r *StatusReporter) Run(ctx context.Context) error {
	log.Printf("Status reporter starting...")
	log.Printf("  Pod: %s", r.podName)
	log.Printf("  Results path: %s", r.resultsPath)
	log.Printf("  Poll interval: %s", r.pollInterval)
	log.Printf("  Max wait time: %s", r.maxWaitTime)

	timeoutCtx, cancel := context.WithTimeout(ctx, r.maxWaitTime)

	resultChan := make(chan *result.AdapterResult, 1)
	errorChan := make(chan error, 1)
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go r.pollForResults(timeoutCtx, resultChan, errorChan, done, &wg)

	var reportErr error
	select {
	case adapterResult := <-resultChan:
		reportErr = r.UpdateFromResult(ctx, adapterResult)
	case err := <-errorChan:
		reportErr = r.UpdateFromError(ctx, err)
	case <-timeoutCtx.Done():
		// Give precedence to results/errors that may have arrived just before timeout
		select {
		case adapterResult := <-resultChan:
			reportErr = r.UpdateFromResult(ctx, adapterResult)
		case err := <-errorChan:
			reportErr = r.UpdateFromError(ctx, err)
		default:
			reportErr = r.UpdateFromTimeout(ctx)
		}
	}

	close(done)
	cancel()
	wg.Wait()

	return reportErr
}

// pollForResults polls for the result file
func (r *StatusReporter) pollForResults(ctx context.Context, resultChan chan<- *result.AdapterResult, errorChan chan<- error, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	log.Printf("Polling for results at %s...", r.resultsPath)

	for {
		select {
		case <-done:
			log.Printf("Polling stopped by shutdown signal")
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := os.Stat(r.resultsPath); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				select {
				case errorChan <- fmt.Errorf("failed to stat result file path=%s: %w", r.resultsPath, err):
				case <-done:
					return
				}
				return
			}

			log.Printf("Result file found, parsing...")
			adapterResult, err := r.parser.ParseFile(r.resultsPath)
			if err != nil {
				select {
				case errorChan <- err:
				case <-done:
					return
				}
				return
			}

			log.Printf("Result parsed successfully: status=%s, reason=%s", adapterResult.Status, adapterResult.Reason)
			select {
			case resultChan <- adapterResult:
			case <-done:
				return
			}
			return
		}
	}
}

// UpdateFromResult updates Job status from adapter result
func (r *StatusReporter) UpdateFromResult(ctx context.Context, adapterResult *result.AdapterResult) error {
	log.Printf("Updating Job status from adapter result...")

	conditionStatus := ConditionStatusTrue
	if !adapterResult.IsSuccess() {
		conditionStatus = ConditionStatusFalse
	}

	condition := k8s.JobCondition{
		Type:    r.conditionType,
		Status:  conditionStatus,
		Reason:  adapterResult.Reason,
		Message: adapterResult.Message,
	}

	if err := r.k8sClient.UpdateJobStatus(ctx, condition); err != nil {
		return fmt.Errorf("failed to update job status: pod=%s condition=%s: %w", r.podName, r.conditionType, err)
	}

	log.Printf("Job status updated successfully: %s=%s (reason: %s)", r.conditionType, conditionStatus, adapterResult.Reason)
	return nil
}

// UpdateFromError updates Job status when parsing fails
func (r *StatusReporter) UpdateFromError(ctx context.Context, err error) error {
	log.Printf("Failed to parse result file: %v", err)

	condition := k8s.JobCondition{
		Type:    r.conditionType,
		Status:  ConditionStatusFalse,
		Reason:  ReasonInvalidResultFormat,
		Message: fmt.Sprintf("Failed to parse adapter result: %v", err),
	}

	if updateErr := r.k8sClient.UpdateJobStatus(ctx, condition); updateErr != nil {
		return fmt.Errorf("failed to update job status: %w", updateErr)
	}

	log.Printf("Job status updated: %s=False (reason: %s)", r.conditionType, ReasonInvalidResultFormat)
	return err
}

// UpdateFromTimeout updates Job status when timeout occurs
func (r *StatusReporter) UpdateFromTimeout(ctx context.Context) error {
	log.Printf("Timeout waiting for adapter results")
	log.Printf("Checking adapter container status in pod %s...", r.podName)

	checkCtx, cancel := context.WithTimeout(context.Background(), containerStatusCheckTimeout)
	defer cancel()

	containerStatus, err := r.k8sClient.GetAdapterContainerStatus(checkCtx, r.podName, r.adapterContainerName)
	if err != nil {
		log.Printf("Warning: failed to get adapter container status: %v", err)
	} else if containerStatus != nil && containerStatus.State.Terminated != nil {
		return r.UpdateFromTerminatedContainer(ctx, containerStatus.State.Terminated)
	}

	condition := k8s.JobCondition{
		Type:    r.conditionType,
		Status:  ConditionStatusFalse,
		Reason:  ReasonAdapterTimeout,
		Message: fmt.Sprintf("Adapter did not produce results within %s", r.maxWaitTime),
	}

	if err := r.k8sClient.UpdateJobStatus(ctx, condition); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	log.Printf("Job status updated: %s=False (reason: %s)", r.conditionType, ReasonAdapterTimeout)
	return errors.New("timeout waiting for adapter results")
}

// UpdateFromTerminatedContainer updates Job status from container termination state
func (r *StatusReporter) UpdateFromTerminatedContainer(ctx context.Context, terminated *corev1.ContainerStateTerminated) error {
	var reason, message string

	if terminated.Reason == ContainerReasonOOMKilled {
		reason = ReasonAdapterOOMKilled
		message = "Adapter container was killed due to out of memory (OOMKilled)"
	} else if terminated.ExitCode != 0 {
		reason = ReasonAdapterExitedWithError
		message = fmt.Sprintf("Adapter container exited with code %d: %s", terminated.ExitCode, terminated.Reason)
	} else {
		reason = ReasonAdapterCrashed
		message = fmt.Sprintf("Adapter container terminated: %s", terminated.Reason)
	}

	log.Printf("Adapter container terminated: reason=%s, exitCode=%d", terminated.Reason, terminated.ExitCode)

	condition := k8s.JobCondition{
		Type:    r.conditionType,
		Status:  ConditionStatusFalse,
		Reason:  reason,
		Message: message,
	}

	if err := r.k8sClient.UpdateJobStatus(ctx, condition); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	log.Printf("Job status updated: %s=False (reason: %s)", r.conditionType, reason)
	return fmt.Errorf("adapter container terminated: %s", message)
}
