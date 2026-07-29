package huawei

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud"
)

func TestCostConfiguration_Validate(t *testing.T) {
	cases := map[string]struct {
		config  CostConfiguration
		wantErr bool
	}{
		"valid": {
			config:  CostConfiguration{ProjectID: "proj-1", Region: "la-south-2"},
			wantErr: false,
		},
		"missing projectID": {
			config:  CostConfiguration{Region: "la-south-2"},
			wantErr: true,
		},
		"missing region": {
			config:  CostConfiguration{ProjectID: "proj-1"},
			wantErr: true,
		},
		"missing both": {
			config:  CostConfiguration{},
			wantErr: true,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := c.config.Validate()
			if c.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCostConfiguration_Equals(t *testing.T) {
	base := &CostConfiguration{ProjectID: "proj-1", Region: "la-south-2"}

	if !base.Equals(&CostConfiguration{ProjectID: "proj-1", Region: "la-south-2"}) {
		t.Fatalf("expected equal configs to be equal")
	}
	if base.Equals(&CostConfiguration{ProjectID: "proj-2", Region: "la-south-2"}) {
		t.Fatalf("expected different projectID to not be equal")
	}
	if base.Equals(&CostConfiguration{ProjectID: "proj-1", Region: "cn-north-1"}) {
		t.Fatalf("expected different region to not be equal")
	}
	if base.Equals(nil) {
		t.Fatalf("expected nil config to not be equal")
	}
	var other cloud.Config = &struct {
		cloud.Config
	}{}
	if base.Equals(other) {
		t.Fatalf("expected mismatched config type to not be equal")
	}
}

func TestCostConfiguration_Sanitize(t *testing.T) {
	c := &CostConfiguration{ProjectID: "proj-1", Region: "la-south-2"}
	sanitized, ok := c.Sanitize().(*CostConfiguration)
	if !ok {
		t.Fatalf("expected *CostConfiguration from Sanitize")
	}
	if sanitized.ProjectID != c.ProjectID || sanitized.Region != c.Region {
		t.Fatalf("Sanitize should preserve non-secret fields, got %+v", sanitized)
	}
}

func TestCostConfiguration_KeyAndProvider(t *testing.T) {
	c := &CostConfiguration{ProjectID: "proj-1", Region: "la-south-2"}
	if c.Key() != "proj-1" {
		t.Fatalf("expected key proj-1, got %s", c.Key())
	}
	if c.Provider() != "Huawei" {
		t.Fatalf("expected provider Huawei, got %s", c.Provider())
	}
}

func TestCostConfiguration_JSONRoundTrip(t *testing.T) {
	c := CostConfiguration{ProjectID: "proj-1", Region: "la-south-2"}

	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var unmarshalled CostConfiguration
	if err := json.Unmarshal(data, &unmarshalled); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if !c.Equals(&unmarshalled) {
		t.Fatalf("round-tripped config %+v does not equal original %+v", unmarshalled, c)
	}
}

func TestCostConfiguration_UnmarshalJSON_MissingFields(t *testing.T) {
	var c CostConfiguration
	if err := json.Unmarshal([]byte(`{"region":"la-south-2"}`), &c); err == nil {
		t.Fatalf("expected error for missing projectID")
	}

	var c2 CostConfiguration
	if err := json.Unmarshal([]byte(`{"projectID":"proj-1"}`), &c2); err == nil {
		t.Fatalf("expected error for missing region")
	}
}
