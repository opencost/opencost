package asset

import (
	"testing"

	coreasset "github.com/opencost/opencost/core/pkg/autocomplete/asset"
	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func TestParseAssetAutocompleteRequest(t *testing.T) {
	windowStr := "2023-01-01T00:00:00Z,2023-01-02T00:00:00Z"
	tests := map[string]struct {
		values  map[string][]string
		wantErr bool
	}{
		"missing window": {
			values:  map[string][]string{"field": {"cluster"}},
			wantErr: true,
		},
		"valid with default window": {
			values: map[string][]string{
				"field": {"cluster"},
			},
			wantErr: false,
		},
		"valid": {
			values: map[string][]string{
				"window": {windowStr},
				"field":  {"label:App"},
				"search": {"  foo  "},
			},
			wantErr: false,
		},
		"assettype alias": {
			values: map[string][]string{
				"window": {windowStr},
				"field":  {"assettype"},
			},
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			qp := httputil.NewQueryParams(tt.values)
			opts := autocomplete.ParseOptions{DefaultTenantID: "tenant-1"}
			if name == "valid with default window" {
				opts.DefaultWindow = "30d"
			}
			got, err := coreasset.ParseRequest(qp, opts)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.Field == "assettype" {
				t.Fatalf("field should be normalized from assettype")
			}
			if name == "valid" && got.Search != "foo" {
				t.Fatalf("search = %q, want foo", got.Search)
			}
			if name == "assettype alias" && got.Field != "type" {
				t.Fatalf("field = %q, want type", got.Field)
			}
		})
	}
}

func TestValidateAutocompleteField_assettype(t *testing.T) {
	got, err := coreasset.ValidateField("assettype")
	if err != nil || got != "type" {
		t.Fatalf("ValidateField(assettype) = %q, %v", got, err)
	}
}
