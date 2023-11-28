package cloudcost

import (
	"testing"
)

func TestParseSortDirection(t *testing.T) {
	tests := map[string]struct {
		input   string
		want    SortDirection
		wantErr bool
	}{
		"Empty String": {
			input:   "",
			want:    SortDirectionNone,
			wantErr: true,
		},
		"invalid input": {
			input:   "invalid",
			want:    SortDirectionNone,
			wantErr: true,
		},
		"upper case ascending": {
			input:   "ASC",
			want:    SortDirectionAscending,
			wantErr: false,
		},
		"lower case ascending": {
			input:   "asc",
			want:    SortDirectionAscending,
			wantErr: false,
		},
		"upper case descending": {
			input:   "DESC",
			want:    SortDirectionDescending,
			wantErr: false,
		},
		"lower case descending": {
			input:   "desc",
			want:    SortDirectionDescending,
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ParseSortDirection(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSortDirection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseSortDirection() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseSortField(t *testing.T) {

	tests := map[string]struct {
		input   string
		want    SortField
		wantErr bool
	}{
		"Empty String": {
			input:   "",
			want:    SortFieldNone,
			wantErr: true,
		},
		"invalid input": {
			input:   "invalid",
			want:    SortFieldNone,
			wantErr: true,
		},
		"upper case cost": {
			input:   "Cost",
			want:    SortFieldCost,
			wantErr: false,
		},
		"lower case cost": {
			input:   "cost",
			want:    SortFieldCost,
			wantErr: false,
		},
		"upper case k8s %": {
			input:   "KubernetesPercent",
			want:    SortFieldKubernetesPercent,
			wantErr: false,
		},
		"lower case k8s %": {
			input:   "kubernetesPercent",
			want:    SortFieldKubernetesPercent,
			wantErr: false,
		},
		"upper case name": {
			input:   "Name",
			want:    SortFieldName,
			wantErr: false,
		},
		"lower case Name": {
			input:   "name",
			want:    SortFieldName,
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ParseSortField(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSortField() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseSortField() got = %v, want %v", got, tt.want)
			}
		})
	}
}
