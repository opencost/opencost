package diagnostics

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/opencost/opencost/core/pkg/util/maputil"
	"github.com/opencost/opencost/core/pkg/util/worker"
)

// basic composite type for diagnostics and the runner function
type runner struct {
	diagnostic Diagnostic
	run        DiagnosticRunner
}

// OpencostDiagnosticsService is an implementation of the `DiagnosticService` contract that provides concurrent diagnostic
// execution and result collection.
type OpencostDiagnosticService struct {
	lock    sync.RWMutex
	runners map[string]map[string]*runner
	count   int
}

func NewDiagnosticService() DiagnosticService {
	return &OpencostDiagnosticService{
		runners: make(map[string]map[string]*runner),
		count:   0,
	}
}

// Register registers a new diagnostic runner implementation with the service that will run the next time diagnostics are requested.
// An error is returned if a runner failed to register. Note that category _and_ name must be a unique combination.
func (ocds *OpencostDiagnosticService) Register(name string, description string, category string, r DiagnosticRunner) error {
	ocds.lock.Lock()
	defer ocds.lock.Unlock()

	categoryRunners, exists := ocds.runners[category]
	if !exists {
		categoryRunners = make(map[string]*runner)
		ocds.runners[category] = categoryRunners
	}

	if _, exists := categoryRunners[name]; exists {
		return fmt.Errorf("runner with name %s already exists in category %s", name, category)
	}

	categoryRunners[name] = &runner{
		diagnostic: Diagnostic{
			Name:        name,
			Description: description,
			Category:    category,
		},
		run: r,
	}

	ocds.count += 1

	return nil
}

// Unregister unregisters a diagnostic runner implementation with the service. True is returned if the runner was unregistered successfully,
// false otherwise.
func (ocds *OpencostDiagnosticService) Unregister(name string, category string) bool {
	ocds.lock.Lock()
	defer ocds.lock.Unlock()

	categoryRunners, exists := ocds.runners[category]
	if !exists {
		return false
	}

	if _, exists := categoryRunners[name]; !exists {
		return false
	}

	delete(categoryRunners, name)
	if len(categoryRunners) == 0 {
		delete(ocds.runners, category)
	}

	ocds.count -= 1

	return true
}

// Run executes all registered diagnostics and returns the results.
func (ocds *OpencostDiagnosticService) Run(ctx context.Context) []*DiagnosticResult {
	ocds.lock.RLock()
	defer ocds.lock.RUnlock()

	return runAll(ctx, maputil.Flatten(ocds.runners))
}

// RunCategory executes all registered diagnostics in the provided category.
func (ocds *OpencostDiagnosticService) RunCategory(ctx context.Context, category string) []*DiagnosticResult {
	ocds.lock.RLock()
	defer ocds.lock.RUnlock()

	categoryRunners, exists := ocds.runners[category]
	if !exists {
		return nil
	}

	return runAll(ctx, maps.Values(categoryRunners))
}

// RunDiagnostic executes a specific diagnostic by category and name. If the diagnostic does not exist, nil is returned.
func (ocds *OpencostDiagnosticService) RunDiagnostic(ctx context.Context, category, name string) *DiagnosticResult {
	ocds.lock.RLock()
	defer ocds.lock.RUnlock()

	categoryRunners, exists := ocds.runners[category]
	if !exists {
		return nil
	}

	r, exists := categoryRunners[name]
	if !exists {
		return nil
	}

	diagRunner := diagRunnerFor(ctx)

	return diagRunner(r)
}

// runAll executes all runners in the provided iterator with a specific worker pool size,
// and returns the results when all diagnostic runners have completed.
func runAll(ctx context.Context, runners iter.Seq[*runner]) []*DiagnosticResult {
	allContext, cancel := context.WithCancel(ctx)
	defer cancel()

	return worker.ConcurrentIterCollect(5, diagRunnerFor(allContext), runners)
}

// diagRunnerFor returns a diagnostic runner function that executes the diagnostic and creates the DiagnosticResult
// leveraging the provided context as a parent.
func diagRunnerFor(ctx context.Context) func(*runner) *DiagnosticResult {
	return func(r *runner) *DiagnosticResult {
		result := &DiagnosticResult{
			ID:          uuid.Must(uuid.NewV7()).String(),
			Name:        r.diagnostic.Name,
			Description: r.diagnostic.Description,
			Category:    r.diagnostic.Category,
		}

		c, cancelDiag := context.WithTimeout(ctx, 5*time.Second)
		defer cancelDiag()

		details, err := r.run(c)
		if err != nil {
			result.Error = err.Error()
		} else {
			result.Details = details
		}

		result.Timestamp = time.Now().UTC()
		return result
	}
}

// Diagnostics returns a list of all registered diagnostics.
func (ocds *OpencostDiagnosticService) Diagnostics() []Diagnostic {
	ocds.lock.RLock()
	defer ocds.lock.RUnlock()

	diagnostics := maputil.FlatMap(ocds.runners, func(r *runner) Diagnostic {
		return r.diagnostic
	})

	return slices.Collect(diagnostics)
}

// Total returns the total number of registered diagnostics.
func (ocds *OpencostDiagnosticService) Total() int {
	ocds.lock.RLock()
	defer ocds.lock.RUnlock()

	return ocds.count
}
