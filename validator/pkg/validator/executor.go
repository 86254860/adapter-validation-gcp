package validator

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// Executor orchestrates validator execution
type Executor struct {
	ctx    *Context
	logger *slog.Logger
	mu     sync.Mutex // Protects results map during parallel execution
}

// NewExecutor creates a new executor
func NewExecutor(ctx *Context, logger *slog.Logger) *Executor {
	return &Executor{
		ctx:    ctx,
		logger: logger,
	}
}

// ExecuteAll runs validators with dependency resolution and parallel execution
func (e *Executor) ExecuteAll(ctx context.Context) ([]*Result, error) {
	// 1. Get all registered validators
	allValidators := GetAll()

	// 2. Filter enabled validators
	enabledValidators := []Validator{}
	for _, v := range allValidators {
		if v.Enabled(e.ctx) {
			enabledValidators = append(enabledValidators, v)
		} else {
			meta := v.Metadata()
			e.logger.Info("Validator disabled, skipping", "validator", meta.Name)
		}
	}

	if len(enabledValidators) == 0 {
		return nil, fmt.Errorf("no validators enabled")
	}

	e.logger.Info("Found enabled validators", "count", len(enabledValidators))

	// 3. Resolve dependencies and build execution plan
	resolver := NewDependencyResolver(enabledValidators)
	groups, err := resolver.ResolveExecutionGroups()
	if err != nil {
		return nil, fmt.Errorf("dependency resolution failed: %w", err)
	}

	e.logger.Info("Execution plan created", "groups", len(groups))

	// Log dependency graphs
	e.logger.Debug("Validator dependency graph (raw dependencies):\n" + resolver.ToMermaid())
	e.logger.Info("Validator execution plan (with levels):\n" + resolver.ToMermaidWithLevels(groups))

	for _, group := range groups {
		e.logger.Debug("Execution group",
			"level", group.Level,
			"validators", len(group.Validators),
			"mode", "parallel")
	}

	// 4. Execute validators group by group
	allResults := []*Result{}
	for _, group := range groups {
		e.logger.Info("Executing level",
			"level", group.Level,
			"validators", len(group.Validators))

		groupResults := e.executeGroup(ctx, group)
		allResults = append(allResults, groupResults...)

		// Check stop on failure
		if e.ctx.Config.StopOnFirstFailure {
			for _, result := range groupResults {
				if result.Status == StatusFailure {
					e.logger.Warn("Stopping due to failure", "validator", result.ValidatorName)
					return allResults, nil
				}
			}
		}
	}

	return allResults, nil
}

// executeGroup runs all validators in a group in parallel
func (e *Executor) executeGroup(ctx context.Context, group ExecutionGroup) []*Result {
	var wg sync.WaitGroup
	results := make([]*Result, len(group.Validators))

	for i, v := range group.Validators {
		wg.Add(1)
		go func(index int, validator Validator) {
			defer wg.Done()

			// Add panic recovery to prevent one validator from crashing all validators
			defer func() {
				if r := recover(); r != nil {
					stack := string(debug.Stack())
					meta := validator.Metadata()
					e.logger.Error("Validator panicked",
						"validator", meta.Name,
						"panic", r,
						"stack", stack)

					// Create failure result for panicked validator
					panicResult := &Result{
						ValidatorName: meta.Name,
						Status:        StatusFailure,
						Reason:        "ValidatorPanic",
						Message:       fmt.Sprintf("Validator crashed: %v", r),
						Details: map[string]interface{}{
							"panic":      fmt.Sprint(r),
							"panic_type": fmt.Sprintf("%T", r),
							"stack":      stack,
						},
						Duration:  0,
						Timestamp: time.Now().UTC(),
					}

					// Thread-safe result storage
					e.mu.Lock()
					e.ctx.Results[meta.Name] = panicResult
					results[index] = panicResult
					e.mu.Unlock()
				}
			}()

			meta := validator.Metadata()
			e.logger.Info("Running validator", "validator", meta.Name)

			start := time.Now()
			result := validator.Validate(ctx, e.ctx)

			// Defensive nil check - validator.Validate should never return nil,
			// but handle it to prevent nil pointer panics
			if result == nil {
				e.logger.Error("Validator returned nil result",
					"validator", meta.Name)
				result = &Result{
					ValidatorName: meta.Name,
					Status:        StatusFailure,
					Reason:        "NilResult",
					Message:       "Validator returned nil result (this is a validator implementation bug)",
					Duration:      time.Since(start),
					Timestamp:     time.Now().UTC(),
				}
			} else {
				result.Duration = time.Since(start)
				result.Timestamp = time.Now().UTC()
				result.ValidatorName = meta.Name
			}

			// Thread-safe result storage
			e.mu.Lock()
			e.ctx.Results[meta.Name] = result
			e.mu.Unlock()

			results[index] = result

			// Log based on result status
			logAttrs := []any{
				"validator", meta.Name,
				"status", result.Status,
				"duration", result.Duration,
			}
			switch result.Status {
			case StatusFailure:
				// Add reason and message for failures to help with debugging
				logAttrs = append(logAttrs,
					"reason", result.Reason,
					"message", result.Message)
				e.logger.Warn("Validator completed with failure", logAttrs...)
			case StatusSkipped:
				// Add reason for skipped validators
				logAttrs = append(logAttrs, "reason", result.Reason)
				e.logger.Info("Validator skipped", logAttrs...)
			default:
				e.logger.Info("Validator completed", logAttrs...)
			}
		}(i, v)
	}

	wg.Wait() // Wait for all validators in this group
	return results
}
