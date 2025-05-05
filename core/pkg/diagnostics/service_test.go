package diagnostics

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/util/json"
)

const (
	TestDiagnosticNameA       = "TestDiagnosticA"
	TestDiagnosticNameB       = "TestDiagnosticB"
	TestDiagnosticNameC       = "TestDiagnosticC"
	TestDiagnosticNameD       = "TestDiagnosticD"
	TestDiagnosticNameE       = "TestDiagnosticE"
	TestDiagnosticNameF       = "TestDiagnosticF"
	TestDiagnosticNameTimeout = "TestDiagnosticTimeout"

	TestDiagnosticDescriptionA       = "Diagnostic A Description..."
	TestDiagnosticDescriptionB       = "Diagnostic B Description..."
	TestDiagnosticDescriptionC       = "Diagnostic C Description..."
	TestDiagnosticDescriptionD       = "Diagnostic D Description..."
	TestDiagnosticDescriptionE       = "Diagnostic E Description..."
	TestDiagnosticDescriptionF       = "Diagnostic F Description..."
	TestDiagnosticDescriptionTimeout = "Diagnostic Timeout will run for longer than 5 seconds..."

	TestDiagnosticCategoryBlue  = "TestCategoryBlue"
	TestDiagnosticCategoryRed   = "TestCategoryRed"
	TestDiagnosticCategoryGreen = "TestCategoryGreen"
)

// TestDiagnostic is a general structure used to capture test diagnostic data
type TestDiagnostic struct {
	Name        string
	Description string
	Category    string
	Run         DiagnosticRunner
}

// generate a runner func that will run for the provided duration and return a map with the key: "test"
// and the value of testName provided.
func runnerFor(testName string, duration time.Duration) DiagnosticRunner {
	return func(ctx context.Context) (map[string]any, error) {
		fmt.Printf("Running Diagnostic: %s\n", testName)
		defer fmt.Printf("Finished Diagnostic: %s\n", testName)

		select {
		case <-ctx.Done():
			fmt.Printf("context cancelled: %v\n", ctx.Err())
			return nil, ctx.Err()
		case <-time.After(duration):
			return map[string]any{
				"test": testName,
			}, nil
		}
	}
}

var (
	TestDiagnosticA = TestDiagnostic{
		Name:        TestDiagnosticNameA,
		Description: TestDiagnosticDescriptionA,
		Category:    TestDiagnosticCategoryRed,
		Run:         runnerFor(TestDiagnosticNameA, 250*time.Millisecond),
	}
	TestDiagnosticB = TestDiagnostic{
		Name:        TestDiagnosticNameB,
		Description: TestDiagnosticDescriptionB,
		Category:    TestDiagnosticCategoryRed,
		Run:         runnerFor(TestDiagnosticNameB, 150*time.Millisecond),
	}
	TestDiagnosticC = TestDiagnostic{
		Name:        TestDiagnosticNameC,
		Description: TestDiagnosticDescriptionC,
		Category:    TestDiagnosticCategoryBlue,
		Run:         runnerFor(TestDiagnosticNameC, 350*time.Millisecond),
	}
	TestDiagnosticD = TestDiagnostic{
		Name:        TestDiagnosticNameD,
		Description: TestDiagnosticDescriptionD,
		Category:    TestDiagnosticCategoryBlue,
		Run:         runnerFor(TestDiagnosticNameD, 450*time.Millisecond),
	}
	TestDiagnosticE = TestDiagnostic{
		Name:        TestDiagnosticNameE,
		Description: TestDiagnosticDescriptionE,
		Category:    TestDiagnosticCategoryGreen,
		Run:         runnerFor(TestDiagnosticNameE, 550*time.Millisecond),
	}
	TestDiagnosticF = TestDiagnostic{
		Name:        TestDiagnosticNameF,
		Description: TestDiagnosticDescriptionF,
		Category:    TestDiagnosticCategoryGreen,
		Run:         runnerFor(TestDiagnosticNameF, 650*time.Millisecond),
	}
	TestDiagnosticTimeout = TestDiagnostic{
		Name:        TestDiagnosticNameTimeout,
		Description: TestDiagnosticDescriptionTimeout,
		Category:    TestDiagnosticCategoryGreen,
		Run:         runnerFor(TestDiagnosticNameTimeout, 6*time.Second),
	}
)

func TestDiagnosticsRegisterAndRun(t *testing.T) {
	t.Parallel()

	d := NewDiagnosticService()

	diags := []TestDiagnostic{
		TestDiagnosticA,
		TestDiagnosticB,
		TestDiagnosticC,
		TestDiagnosticD,
		TestDiagnosticE,
		TestDiagnosticF,
	}

	for _, diag := range diags {
		if err := d.Register(diag.Name, diag.Description, diag.Category, diag.Run); err != nil {
			t.Fatalf("failed to register diagnostic %s: %v", diag.Name, err)
		}
	}

	// Register a duplicate diagnostic and expect an error
	err := d.Register(TestDiagnosticA.Name, TestDiagnosticA.Description, TestDiagnosticA.Category, TestDiagnosticA.Run)
	if err == nil {
		t.Fatalf("expected error when registering duplicate diagnostic %s", TestDiagnosticA.Name)
	}

	c := context.Background()
	results := d.Run(c)

	if len(results) != len(diags) {
		t.Fatalf("expected %d results, got %d", len(diags), len(results))
	}

	for _, result := range results {
		if result.Error != "" {
			t.Errorf("expected no error, got %s", result.Error)
		}

		if result.Category == "" {
			t.Errorf("expected category, got empty")
		}

		if result.Name == "" {
			t.Errorf("expected name, got empty")
		}

		if result.Timestamp.IsZero() {
			t.Errorf("expected timestamp, got zero")
		}

		if result.Details == nil {
			t.Errorf("expected details, got nil")
		}

		if result.Details["test"] != result.Name {
			t.Errorf("expected test name %s, got %s", result.Name, result.Details["test"])
		}

		j, err := json.Marshal(result)
		if err != nil {
			t.Errorf("failed to marshal result: %v", err)
		}
		js := string(j)
		if js == "" {
			t.Errorf("expected non-empty JSON, got empty")
		}

		t.Logf("%s", js)
	}
}

func TestDiagnosticsServiceTimeout(t *testing.T) {
	t.Parallel()

	d := NewDiagnosticService()

	diags := []TestDiagnostic{
		TestDiagnosticA,
		TestDiagnosticB,
		TestDiagnosticC,
		TestDiagnosticTimeout,
	}

	for _, diag := range diags {
		if err := d.Register(diag.Name, diag.Description, diag.Category, diag.Run); err != nil {
			t.Fatalf("failed to register diagnostic %s: %v", diag.Name, err)
		}
	}

	c := context.Background()
	results := d.Run(c)

	if len(results) != len(diags) {
		t.Fatalf("expected %d results, got %d", len(diags), len(results))
	}

	foundTimeoutDiagnostic := false

	for _, result := range results {
		if result.Name == TestDiagnosticNameTimeout {
			foundTimeoutDiagnostic = true
			if result.Error == "" {
				t.Errorf("expected timeout error, but got empty error")
			} else {
				t.Logf("Diagnostic %s/%s completed with error as expected: %s", result.Category, result.Name, result.Error)
			}
		} else {
			t.Logf("Diagnostic %s/%s completed successfully", result.Category, result.Name)
		}
	}

	if !foundTimeoutDiagnostic {
		t.Errorf("expected to find timeout diagnostic, but it was not found")
	}
}

func TestDiagnosticsList(t *testing.T) {
	t.Parallel()

	d := NewDiagnosticService()

	diags := []TestDiagnostic{
		TestDiagnosticA,
		TestDiagnosticB,
		TestDiagnosticC,
		TestDiagnosticD,
		TestDiagnosticE,
		TestDiagnosticF,
	}

	for _, diag := range diags {
		if err := d.Register(diag.Name, diag.Description, diag.Category, diag.Run); err != nil {
			t.Fatalf("failed to register diagnostic %s: %v", diag.Name, err)
		}
	}

	diagList := d.Diagnostics()
	slices.SortFunc(diagList, func(a, b Diagnostic) int {
		return cmp.Compare(a.Category+"/"+a.Name, b.Category+"/"+b.Name)
	})

	slices.SortFunc(diags, func(a, b TestDiagnostic) int {
		return cmp.Compare(a.Category+"/"+a.Name, b.Category+"/"+b.Name)
	})

	if !slices.EqualFunc(diags, diagList, isEqual) {
		t.Errorf("expected diagnostics list to match registered diagnostics")
	}

	for _, diagItem := range diagList {
		t.Logf("Diagnostic: %+v", diagItem)
	}
}

func TestUnregisterDiagnostic(t *testing.T) {
	t.Parallel()

	d := NewDiagnosticService()

	diags := []TestDiagnostic{
		TestDiagnosticA,
		TestDiagnosticB,
		TestDiagnosticC,
		TestDiagnosticD,
		TestDiagnosticE,
		TestDiagnosticF,
	}

	for _, diag := range diags {
		if err := d.Register(diag.Name, diag.Description, diag.Category, diag.Run); err != nil {
			t.Fatalf("failed to register diagnostic %s: %v", diag.Name, err)
		}
	}

	if !d.Unregister(TestDiagnosticNameA, TestDiagnosticCategoryRed) {
		t.Errorf("failed to unregister diagnostic %s/%s", TestDiagnosticCategoryRed, TestDiagnosticNameA)
	}

	if d.Unregister(TestDiagnosticNameA, TestDiagnosticCategoryRed) {
		t.Errorf("unregistering diagnostic %s/%s again should fail", TestDiagnosticCategoryRed, TestDiagnosticNameA)
	}

	if d.Unregister(TestDiagnosticNameB, "nonexistent") {
		t.Errorf("unregistering nonexistent diagnostic should fail")
	}

	results := d.Run(context.Background())
	if len(results) != len(diags)-1 {
		t.Fatalf("expected %d results, got %d", len(diags)-1, len(results))
	}

	for _, result := range results {
		if result.Name == TestDiagnosticNameA {
			t.Errorf("expected diagnostic %s/%s to be unregistered", TestDiagnosticCategoryRed, TestDiagnosticNameA)
		}
	}
}

func TestUnregisterAllFromCategory(t *testing.T) {
	t.Parallel()

	d := NewDiagnosticService()

	diags := []TestDiagnostic{
		TestDiagnosticA,
		TestDiagnosticB,
		TestDiagnosticC,
		TestDiagnosticD,
		TestDiagnosticE,
		TestDiagnosticF,
	}

	for _, diag := range diags {
		if err := d.Register(diag.Name, diag.Description, diag.Category, diag.Run); err != nil {
			t.Fatalf("failed to register diagnostic %s: %v", diag.Name, err)
		}
	}

	if !d.Unregister(TestDiagnosticNameA, TestDiagnosticCategoryRed) {
		t.Errorf("failed to unregister diagnostic %s/%s", TestDiagnosticCategoryRed, TestDiagnosticNameA)
	}

	if !d.Unregister(TestDiagnosticNameB, TestDiagnosticCategoryRed) {
		t.Errorf("failed to unregister diagnostic %s/%s", TestDiagnosticCategoryRed, TestDiagnosticNameB)
	}

	results := d.RunCategory(context.Background(), TestDiagnosticCategoryRed)
	if len(results) != 0 {
		t.Fatalf("expected 0 results for category %s, got %d", TestDiagnosticCategoryRed, len(results))
	}
}

func TestRunCategoryDiagnostics(t *testing.T) {
	t.Parallel()

	d := NewDiagnosticService()

	diags := []TestDiagnostic{
		TestDiagnosticA,
		TestDiagnosticB,
		TestDiagnosticC,
		TestDiagnosticD,
		TestDiagnosticE,
		TestDiagnosticF,
	}

	for _, diag := range diags {
		if err := d.Register(diag.Name, diag.Description, diag.Category, diag.Run); err != nil {
			t.Fatalf("failed to register diagnostic %s: %v", diag.Name, err)
		}
	}

	c := context.Background()
	results := d.RunCategory(c, TestDiagnosticCategoryBlue)

	if len(results) != 2 {
		t.Fatalf("expected 2 results for category %s, got %d", TestDiagnosticCategoryBlue, len(results))
	}

	for _, result := range results {
		if result.Category != TestDiagnosticCategoryBlue {
			t.Errorf("expected category %s, got %s", TestDiagnosticCategoryBlue, result.Category)
		}
	}
}

func TestRunSingleDiagnostic(t *testing.T) {
	t.Parallel()

	d := NewDiagnosticService()

	diags := []TestDiagnostic{
		TestDiagnosticA,
		TestDiagnosticB,
		TestDiagnosticC,
		TestDiagnosticD,
		TestDiagnosticE,
		TestDiagnosticF,
	}

	for _, diag := range diags {
		if err := d.Register(diag.Name, diag.Description, diag.Category, diag.Run); err != nil {
			t.Fatalf("failed to register diagnostic %s: %v", diag.Name, err)
		}
	}

	c := context.Background()
	result := d.RunDiagnostic(c, TestDiagnosticCategoryGreen, TestDiagnosticNameF)

	if result == nil {
		t.Fatalf("expected a result for diagnostic %s, got nil", TestDiagnosticNameF)
	}

	if result.Name != TestDiagnosticNameF {
		t.Errorf("expected name %s, got %s", TestDiagnosticNameF, result.Name)
	}

	// Run category without name
	result = d.RunDiagnostic(c, TestDiagnosticCategoryGreen, "not-a-valid-diagnostic-name")
	if result != nil {
		t.Fatalf("expected nil result for invalid diagnostic name, got %v", result)
	}

	// Run without category
	result = d.RunDiagnostic(c, "not-a-valid-category", TestDiagnosticNameF)
	if result != nil {
		t.Fatalf("expected nil result for invalid category, got %v", result)
	}

}

func isEqual(a TestDiagnostic, b Diagnostic) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Description != b.Description {
		return false
	}
	if a.Category != b.Category {
		return false
	}
	return true
}
