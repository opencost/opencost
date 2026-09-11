package allocation

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestQueryAllocationAutocompleteFromSetRange(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	as := opencost.NewAllocationSet(start, start.Add(24*time.Hour))
	as.Set(opencost.NewMockUnitAllocation("a1", start, 24*time.Hour, &opencost.AllocationProperties{
		Cluster:         "cluster-a",
		Namespace:       "ns-a",
		Pod:             "pod-a",
		Container:       "container-a",
		ControllerKind:  "deployment",
		Controller:      "deploy-a",
		Node:            "node-a",
		Labels:          map[string]string{"Team": "platform", "app": "api"},
		NamespaceLabels: map[string]string{"owner": "sre"},
	}))
	as.Set(opencost.NewMockUnitAllocation("a2", start, 24*time.Hour, &opencost.AllocationProperties{
		Cluster:         "cluster-b",
		Namespace:       "ns-b",
		Pod:             "pod-b",
		Container:       "container-b",
		ControllerKind:  "statefulset",
		Controller:      "db-a",
		Node:            "node-b",
		Labels:          map[string]string{"Team": "data", "app": "db"},
		NamespaceLabels: map[string]string{"owner": "db"},
	}))

	asr := opencost.NewAllocationSetRange(as)
	window := opencost.NewClosedWindow(start, start.Add(24*time.Hour))

	resp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "label",
		Limit:  10,
		Window: window,
		Filter: &ast.VoidOp{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Data) != 2 || resp.Data[0] != "Team" || resp.Data[1] != "app" {
		t.Fatalf("unexpected label autocomplete response: %+v", resp.Data)
	}

	valueResp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "label:team",
		Search: "plat",
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(valueResp.Data) != 1 || valueResp.Data[0] != "platform" {
		t.Fatalf("unexpected label value autocomplete response: %+v", valueResp.Data)
	}

	mixedCaseResp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "label:Team",
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mixedCaseResp.Data) != 2 || mixedCaseResp.Data[0] != "data" || mixedCaseResp.Data[1] != "platform" {
		t.Fatalf("expected label:team to match Team label values, got %+v", mixedCaseResp.Data)
	}

	accountResp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "account",
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error for account field: %v", err)
	}
	if len(accountResp.Data) != 0 {
		t.Fatalf("expected empty account autocomplete response, got %+v", accountResp.Data)
	}

	_, err = QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "namespace",
		Limit:  autocomplete.MaxResultLimit + 1,
		Window: window,
	})
	if err == nil {
		t.Fatal("expected error for excessive limit")
	}
	if !autocomplete.IsBadRequest(err) {
		t.Fatalf("expected bad request error, got: %v", err)
	}
}

func TestQueryAllocationAutocompleteFromSetRange_Alias(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	as := opencost.NewAllocationSet(start, start.Add(24*time.Hour))
	// label-backed alias value
	as.Set(opencost.NewMockUnitAllocation("a1", start, 24*time.Hour, &opencost.AllocationProperties{
		Namespace: "ns-a",
		Labels:    map[string]string{"team": "platform"},
	}))
	// annotation-backed alias value: GenerateKey and the alias filter pass
	// both fall back to annotations, so autocomplete must too
	as.Set(opencost.NewMockUnitAllocation("a2", start, 24*time.Hour, &opencost.AllocationProperties{
		Namespace:   "ns-b",
		Annotations: map[string]string{"team": "data"},
	}))
	// label wins over annotation when both are present
	as.Set(opencost.NewMockUnitAllocation("a3", start, 24*time.Hour, &opencost.AllocationProperties{
		Namespace:   "ns-c",
		Labels:      map[string]string{"team": "sre"},
		Annotations: map[string]string{"team": "ignored"},
	}))
	// second configured key, needing sanitization (cost-center -> cost_center)
	as.Set(opencost.NewMockUnitAllocation("a4", start, 24*time.Hour, &opencost.AllocationProperties{
		Namespace: "ns-d",
		Labels:    map[string]string{"cost_center": "eng-123"},
	}))

	asr := opencost.NewAllocationSetRange(as)
	window := opencost.NewClosedWindow(start, start.Add(24*time.Hour))

	// default LabelConfig: team -> "team"
	resp, err := QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "team",
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"data", "platform", "sre"}
	if !equalStrings(resp.Data, want) {
		t.Fatalf("team autocomplete = %v, want %v", resp.Data, want)
	}

	// search applies to alias values
	resp, err = QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:  "Team",
		Search: "dat",
		Window: window,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !equalStrings(resp.Data, []string{"data"}) {
		t.Fatalf("team autocomplete with search = %v, want [data]", resp.Data)
	}

	// custom LabelConfig with comma-separated keys
	cfg := opencost.NewLabelConfig()
	cfg.DepartmentLabel = "squad, cost-center"
	resp, err = QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:       "department",
		Window:      window,
		LabelConfig: cfg,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !equalStrings(resp.Data, []string{"eng-123"}) {
		t.Fatalf("department autocomplete = %v, want [eng-123]", resp.Data)
	}

	// alias configured to a key no allocation carries
	cfg.OwnerLabel = "missing"
	resp, err = QueryAllocationAutocompleteFromSetRange(asr, autocomplete.Request{
		Field:       "owner",
		Window:      window,
		LabelConfig: cfg,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Fatalf("owner autocomplete = %v, want empty", resp.Data)
	}
}

func TestAliasLabelKeys(t *testing.T) {
	cfg := &opencost.LabelConfig{
		DepartmentLabel:  "cost-center",
		EnvironmentLabel: "stage, tier",
		OwnerLabel:       "owner",
		ProductLabel:     "",
		TeamLabel:        "squad",
	}

	tests := []struct {
		field string
		want  []string
	}{
		{"department", []string{"cost_center"}},
		{"environment", []string{"stage", "tier"}},
		{"owner", []string{"owner"}},
		{"product", nil},
		{"team", []string{"squad"}},
		{"namespace", nil},
		{"label:team", nil},
	}
	for _, tt := range tests {
		got := aliasLabelKeys(tt.field, cfg)
		if !equalStrings(got, tt.want) {
			t.Errorf("aliasLabelKeys(%q) = %v, want %v", tt.field, got, tt.want)
		}
	}

	defaults := opencost.NewLabelConfig()
	if got := aliasLabelKeys("product", defaults); !equalStrings(got, []string{"app"}) {
		t.Errorf("aliasLabelKeys(product, defaults) = %v, want [app]", got)
	}
}

func TestAliasLabelValues(t *testing.T) {
	tests := []struct {
		name  string
		props *opencost.AllocationProperties
		keys  []string
		want  []string
	}{
		{
			name:  "label match",
			props: &opencost.AllocationProperties{Labels: map[string]string{"team": "platform"}},
			keys:  []string{"team"},
			want:  []string{"platform"},
		},
		{
			name:  "annotation fallback",
			props: &opencost.AllocationProperties{Annotations: map[string]string{"team": "platform"}},
			keys:  []string{"team"},
			want:  []string{"platform"},
		},
		{
			name: "label wins over annotation",
			props: &opencost.AllocationProperties{
				Labels:      map[string]string{"team": "from-label"},
				Annotations: map[string]string{"team": "from-annotation"},
			},
			keys: []string{"team"},
			want: []string{"from-label"},
		},
		{
			name: "multiple keys, mixed sources",
			props: &opencost.AllocationProperties{
				Labels:      map[string]string{"cost_center": "eng-123"},
				Annotations: map[string]string{"squad": "core"},
			},
			keys: []string{"squad", "cost_center", "missing"},
			want: []string{"core", "eng-123"},
		},
		{
			name:  "exact-case only, matching GenerateKey",
			props: &opencost.AllocationProperties{Labels: map[string]string{"Team": "platform"}},
			keys:  []string{"team"},
			want:  nil,
		},
		{
			name:  "no keys",
			props: &opencost.AllocationProperties{Labels: map[string]string{"team": "platform"}},
			keys:  nil,
			want:  nil,
		},
		{
			name:  "nil maps",
			props: &opencost.AllocationProperties{},
			keys:  []string{"team"},
			want:  nil,
		},
	}

	for _, tt := range tests {
		got := aliasLabelValues(tt.props, tt.keys)
		if !equalStrings(got, tt.want) {
			t.Errorf("case %q: aliasLabelValues = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
