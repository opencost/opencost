package aws

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
)

func TestAthenaIntegration_GetCloudCost(t *testing.T) {
	athenaConfigPath := os.Getenv("ATHENA_CONFIGURATION")
	if athenaConfigPath == "" {
		t.Skip("skipping integration test, set environment variable ATHENA_CONFIGURATION")
	}
	athenaConfigBin, err := os.ReadFile(athenaConfigPath)
	if err != nil {
		t.Fatalf("failed to read config file: %s", err.Error())
	}
	var athenaConfig AthenaConfiguration
	err = json.Unmarshal(athenaConfigBin, &athenaConfig)
	if err != nil {
		t.Fatalf("failed to unmarshal config from JSON: %s", err.Error())
	}
	testCases := map[string]struct {
		integration *AthenaIntegration
		start       time.Time
		end         time.Time
		expected    bool
	}{
		// No CUR data is expected within 2 days of now
		"too_recent_window": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: athenaConfig,
				},
			},
			end:      time.Now(),
			start:    time.Now().Add(-timeutil.Day),
			expected: true,
		},
		// CUR data should be available
		"last week window": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: athenaConfig,
				},
			},
			end:      time.Now().Add(-7 * timeutil.Day),
			start:    time.Now().Add(-8 * timeutil.Day),
			expected: false,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := testCase.integration.GetCloudCost(testCase.start, testCase.end)
			if err != nil {
				t.Errorf("Other error during testing %s", err)
			} else if actual.IsEmpty() != testCase.expected {
				t.Errorf("Incorrect result, actual emptiness: %t, expected: %t", actual.IsEmpty(), testCase.expected)
			}
		})
	}
}

func Test_athenaRowToCloudCost(t *testing.T) {
	aqi := AthenaQueryIndexes{
		ColumnIndexes: map[string]int{
			"ListCostColumn":              0,
			"NetCostColumn":               1,
			"AmortizedNetCostColumn":      2,
			"AmortizedCostColumn":         3,
			"IsK8sColumn":                 4,
			AthenaDateTruncColumn:         5,
			"line_item_resource_id":       6,
			"bill_payer_account_id":       7,
			"line_item_usage_account_id":  8,
			"line_item_product_code":      9,
			"line_item_usage_type":        10,
			"product_region_code":         11,
			"line_item_availability_zone": 12,
			"resource_tags_user_test":     13,
			"resource_tags_aws_test":      14,
		},
		TagColumns:             []string{"resource_tags_user_test"},
		AWSTagColumns:          []string{"resource_tags_aws_test"},
		ListCostColumn:         "ListCostColumn",
		NetCostColumn:          "NetCostColumn",
		AmortizedNetCostColumn: "AmortizedNetCostColumn",
		AmortizedCostColumn:    "AmortizedCostColumn",
		IsK8sColumn:            "IsK8sColumn",
	}

	tests := []struct {
		name    string
		row     []string
		aqi     AthenaQueryIndexes
		want    *opencost.CloudCost
		wantErr bool
	}{
		{
			name:    "incorrect row length",
			row:     []string{"not enough elements"},
			aqi:     aqi,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid list cost",
			row:     []string{"invalid", "2", "3", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqi,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid net cost",
			row:     []string{"1", "invalid", "3", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqi,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid amortized net cost",
			row:     []string{"1", "2", "invalid", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqi,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid amortized cost",
			row:     []string{"1", "2", "3", "invalid", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqi,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid date",
			row:     []string{"1", "2", "3", "4", "true", "invalid", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqi,
			want:    nil,
			wantErr: true,
		},
		{
			name: "valid kubernetes with labels",
			row:  []string{"1", "2", "3", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:  aqi,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "resourceID",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "productCode",
					Category:          opencost.OtherCategory,
					Labels: opencost.CloudCostLabels{
						"test":     "userTagTestValue",
						"aws_test": "awsTagTestValue",
					},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 1,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 1,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              3,
					KubernetesPercent: 1,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 1,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              4,
					KubernetesPercent: 1,
				},
			},
			wantErr: false,
		},
		{
			name: "valid non-kubernetes, no labels",
			row:  []string{"1", "2", "3", "4", "false", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "", ""},
			aqi:  aqi,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "resourceID",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "productCode",
					Category:          opencost.OtherCategory,
					Labels:            opencost.CloudCostLabels{},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 0,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              3,
					KubernetesPercent: 0,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              4,
					KubernetesPercent: 0,
				},
			},
			wantErr: false,
		},
		{
			name: "valid load balancer product code",
			row:  []string{"1", "2", "3", "4", "false", "2024-09-01 00:00:00.000", "resourceID/lbID", "payerAccountID", "usageAccountID", "AWSELB", "usageType", "regionCode", "availabilityZone", "", ""},
			aqi:  aqi,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "lbID",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "AWSELB",
					Category:          opencost.NetworkCategory,
					Labels:            opencost.CloudCostLabels{},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 0,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              3,
					KubernetesPercent: 0,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              4,
					KubernetesPercent: 0,
				},
			},
			wantErr: false,
		},
		{
			name: "valid non-kubernetes, Fargate CPU",
			row:  []string{"1", "2", "3", "4", "false", "2024-09-01 00:00:00.000", "123:pod/resource", "payerAccountID", "usageAccountID", "AmazonEKS", "CPU", "regionCode", "availabilityZone", "", ""},
			aqi:  aqi,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "123:pod/resource/CPU",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "AmazonEKS",
					Category:          opencost.ComputeCategory,
					Labels:            opencost.CloudCostLabels{},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 0,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              3,
					KubernetesPercent: 0,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              4,
					KubernetesPercent: 0,
				},
			},
			wantErr: false,
		},
		{
			name: "valid non-kubernetes, Fargate RAM",
			row:  []string{"1", "2", "3", "4", "false", "2024-09-01 00:00:00.000", "123:pod/resource", "payerAccountID", "usageAccountID", "AmazonEKS", "GB", "regionCode", "availabilityZone", "", ""},
			aqi:  aqi,
			want: &opencost.CloudCost{
				Properties: &opencost.CloudCostProperties{
					ProviderID:        "123:pod/resource/RAM",
					Provider:          "AWS",
					AccountID:         "usageAccountID",
					AccountName:       "usageAccountID",
					InvoiceEntityID:   "payerAccountID",
					InvoiceEntityName: "payerAccountID",
					RegionID:          "regionCode",
					AvailabilityZone:  "availabilityZone",
					Service:           "AmazonEKS",
					Category:          opencost.ComputeCategory,
					Labels:            opencost.CloudCostLabels{},
				},
				Window: opencost.NewClosedWindow(
					time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 9, 2, 0, 0, 0, 0, time.UTC),
				),
				ListCost: opencost.CostMetric{
					Cost:              1,
					KubernetesPercent: 0,
				},
				NetCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost:              3,
					KubernetesPercent: 0,
				},
				InvoicedCost: opencost.CostMetric{
					Cost:              2,
					KubernetesPercent: 0,
				},
				AmortizedCost: opencost.CostMetric{
					Cost:              4,
					KubernetesPercent: 0,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := stringsToRow(tt.row)
			got, err := athenaRowToCloudCost(row, tt.aqi)
			if (err != nil) != tt.wantErr {
				t.Errorf("RowToCloudCost() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RowToCloudCost() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAthenaIntegration_GetListCostColumn(t *testing.T) {
	ai := &AthenaIntegration{}

	result := ai.GetListCostColumn()

	expected := "SUM(CASE line_item_line_item_type WHEN 'EdpDiscount' THEN 0 WHEN 'PrivateRateDiscount' THEN 0 ELSE line_item_unblended_cost END) as list_cost"

	if result != expected {
		t.Errorf("GetListCostColumn() returned %s, expected %s", result, expected)
	}
}

func TestAthenaIntegration_GetNetCostColumn(t *testing.T) {
	ai := &AthenaIntegration{}

	testCases := map[string]struct {
		allColumns map[string]bool
		expected   string
	}{
		"with net pricing column": {
			allColumns: map[string]bool{
				"line_item_net_unblended_cost": true,
			},
			expected: "SUM(COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0)) as net_cost",
		},
		"without net pricing column": {
			allColumns: map[string]bool{
				"line_item_net_unblended_cost": false,
			},
			expected: "SUM(line_item_unblended_cost) as net_cost",
		},
		"empty columns map": {
			allColumns: map[string]bool{},
			expected:   "SUM(line_item_unblended_cost) as net_cost",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ai.GetNetCostColumn(testCase.allColumns)
			if result != testCase.expected {
				t.Errorf("GetNetCostColumn() returned %s, expected %s", result, testCase.expected)
			}
		})
	}
}

func TestAthenaIntegration_GetAmortizedCostColumn(t *testing.T) {
	ai := &AthenaIntegration{}

	testCases := map[string]struct {
		allColumns map[string]bool
		expected   string
	}{
		"with RI and SP columns": {
			allColumns: map[string]bool{
				"reservation_effective_cost":               true,
				"savings_plan_savings_plan_effective_cost": true,
			},
			expected: "SUM(CASE line_item_line_item_type WHEN 'DiscountedUsage' THEN reservation_effective_cost WHEN 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost ELSE line_item_unblended_cost END) as amortized_cost",
		},
		"with only RI column": {
			allColumns: map[string]bool{
				"reservation_effective_cost":               true,
				"savings_plan_savings_plan_effective_cost": false,
			},
			expected: "SUM(CASE line_item_line_item_type WHEN 'DiscountedUsage' THEN reservation_effective_cost ELSE line_item_unblended_cost END) as amortized_cost",
		},
		"with only SP column": {
			allColumns: map[string]bool{
				"reservation_effective_cost":               false,
				"savings_plan_savings_plan_effective_cost": true,
			},
			expected: "SUM(CASE line_item_line_item_type WHEN 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost ELSE line_item_unblended_cost END) as amortized_cost",
		},
		"without RI or SP columns": {
			allColumns: map[string]bool{
				"reservation_effective_cost":               false,
				"savings_plan_savings_plan_effective_cost": false,
			},
			expected: "SUM(line_item_unblended_cost) as amortized_cost",
		},
		"empty columns map": {
			allColumns: map[string]bool{},
			expected:   "SUM(line_item_unblended_cost) as amortized_cost",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ai.GetAmortizedCostColumn(testCase.allColumns)
			if result != testCase.expected {
				t.Errorf("GetAmortizedCostColumn() returned %s, expected %s", result, testCase.expected)
			}
		})
	}
}

func TestAthenaIntegration_GetAmortizedNetCostColumn(t *testing.T) {
	ai := &AthenaIntegration{}

	testCases := map[string]struct {
		allColumns map[string]bool
		expected   string
	}{
		"with net pricing and RI/SP columns": {
			allColumns: map[string]bool{
				"line_item_net_unblended_cost":                 true,
				"reservation_net_effective_cost":               true,
				"savings_plan_net_savings_plan_effective_cost": true,
			},
			expected: "SUM(CASE line_item_line_item_type WHEN 'DiscountedUsage' THEN COALESCE(reservation_net_effective_cost, reservation_effective_cost, 0) WHEN 'SavingsPlanCoveredUsage' THEN COALESCE(savings_plan_net_savings_plan_effective_cost, savings_plan_savings_plan_effective_cost, 0) ELSE COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0) END) as amortized_net_cost",
		},
		"with net pricing but no RI/SP columns": {
			allColumns: map[string]bool{
				"line_item_net_unblended_cost":                 true,
				"reservation_net_effective_cost":               false,
				"savings_plan_net_savings_plan_effective_cost": false,
			},
			expected: "SUM(COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0)) as amortized_net_cost",
		},
		"without net pricing": {
			allColumns: map[string]bool{
				"line_item_net_unblended_cost": false,
			},
			expected: "SUM(line_item_unblended_cost) as amortized_net_cost",
		},
		"empty columns map": {
			allColumns: map[string]bool{},
			expected:   "SUM(line_item_unblended_cost) as amortized_net_cost",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ai.GetAmortizedNetCostColumn(testCase.allColumns)
			if result != testCase.expected {
				t.Errorf("GetAmortizedNetCostColumn() returned %s, expected %s", result, testCase.expected)
			}
		})
	}
}

func TestAthenaIntegration_GetAmortizedCostCase(t *testing.T) {
	ai := &AthenaIntegration{}

	testCases := map[string]struct {
		allColumns map[string]bool
		expected   string
	}{
		"with RI and SP columns": {
			allColumns: map[string]bool{
				"reservation_effective_cost":               true,
				"savings_plan_savings_plan_effective_cost": true,
			},
			expected: "CASE line_item_line_item_type WHEN 'DiscountedUsage' THEN reservation_effective_cost WHEN 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost ELSE line_item_unblended_cost END",
		},
		"with only RI column": {
			allColumns: map[string]bool{
				"reservation_effective_cost":               true,
				"savings_plan_savings_plan_effective_cost": false,
			},
			expected: "CASE line_item_line_item_type WHEN 'DiscountedUsage' THEN reservation_effective_cost ELSE line_item_unblended_cost END",
		},
		"with only SP column": {
			allColumns: map[string]bool{
				"reservation_effective_cost":               false,
				"savings_plan_savings_plan_effective_cost": true,
			},
			expected: "CASE line_item_line_item_type WHEN 'SavingsPlanCoveredUsage' THEN savings_plan_savings_plan_effective_cost ELSE line_item_unblended_cost END",
		},
		"without RI or SP columns": {
			allColumns: map[string]bool{
				"reservation_effective_cost":               false,
				"savings_plan_savings_plan_effective_cost": false,
			},
			expected: "line_item_unblended_cost",
		},
		"empty columns map": {
			allColumns: map[string]bool{},
			expected:   "line_item_unblended_cost",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ai.GetAmortizedCostCase(testCase.allColumns)
			if result != testCase.expected {
				t.Errorf("GetAmortizedCostCase() returned %s, expected %s", result, testCase.expected)
			}
		})
	}
}

func TestAthenaIntegration_GetAmortizedNetCostCase(t *testing.T) {
	ai := &AthenaIntegration{}

	testCases := map[string]struct {
		allColumns map[string]bool
		expected   string
	}{
		"with net RI and SP columns": {
			allColumns: map[string]bool{
				"reservation_net_effective_cost":               true,
				"savings_plan_net_savings_plan_effective_cost": true,
			},
			expected: "CASE line_item_line_item_type WHEN 'DiscountedUsage' THEN COALESCE(reservation_net_effective_cost, reservation_effective_cost, 0) WHEN 'SavingsPlanCoveredUsage' THEN COALESCE(savings_plan_net_savings_plan_effective_cost, savings_plan_savings_plan_effective_cost, 0) ELSE COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0) END",
		},
		"with only net RI column": {
			allColumns: map[string]bool{
				"reservation_net_effective_cost":               true,
				"savings_plan_net_savings_plan_effective_cost": false,
			},
			expected: "CASE line_item_line_item_type WHEN 'DiscountedUsage' THEN COALESCE(reservation_net_effective_cost, reservation_effective_cost, 0) ELSE COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0) END",
		},
		"with only net SP column": {
			allColumns: map[string]bool{
				"reservation_net_effective_cost":               false,
				"savings_plan_net_savings_plan_effective_cost": true,
			},
			expected: "CASE line_item_line_item_type WHEN 'SavingsPlanCoveredUsage' THEN COALESCE(savings_plan_net_savings_plan_effective_cost, savings_plan_savings_plan_effective_cost, 0) ELSE COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0) END",
		},
		"without net RI or SP columns": {
			allColumns: map[string]bool{
				"reservation_net_effective_cost":               false,
				"savings_plan_net_savings_plan_effective_cost": false,
			},
			expected: "COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0)",
		},
		"empty columns map": {
			allColumns: map[string]bool{},
			expected:   "COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0)",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ai.GetAmortizedNetCostCase(testCase.allColumns)
			if result != testCase.expected {
				t.Errorf("GetAmortizedNetCostCase() returned %s, expected %s", result, testCase.expected)
			}
		})
	}
}

func TestAthenaIntegration_RemoveColumnAliases(t *testing.T) {
	ai := &AthenaIntegration{}

	testCases := map[string]struct {
		input    []string
		expected []string
	}{
		"columns with aliases": {
			input: []string{
				"SUM(cost) as total_cost",
				"COUNT(*) as count",
				"AVG(price) as avg_price",
			},
			expected: []string{
				"SUM(cost)",
				"COUNT(*)",
				"AVG(price)",
			},
		},
		"columns without aliases": {
			input: []string{
				"SUM(cost)",
				"COUNT(*)",
				"AVG(price)",
			},
			expected: []string{
				"SUM(cost)",
				"COUNT(*)",
				"AVG(price)",
			},
		},
		"mixed columns": {
			input: []string{
				"SUM(cost) as total_cost",
				"COUNT(*)",
				"AVG(price) as avg_price",
				"MAX(value)",
			},
			expected: []string{
				"SUM(cost)",
				"COUNT(*)",
				"AVG(price)",
				"MAX(value)",
			},
		},
		"empty slice": {
			input:    []string{},
			expected: []string{},
		},
		"single column with alias": {
			input: []string{
				"SUM(cost) as total_cost",
			},
			expected: []string{
				"SUM(cost)",
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// Create a copy of the input slice to avoid modifying the original
			columns := make([]string, len(testCase.input))
			copy(columns, testCase.input)

			ai.RemoveColumnAliases(columns)

			if len(columns) != len(testCase.expected) {
				t.Errorf("RemoveColumnAliases() returned slice of length %d, expected %d", len(columns), len(testCase.expected))
				return
			}

			for i, expected := range testCase.expected {
				if columns[i] != expected {
					t.Errorf("RemoveColumnAliases() at index %d returned %s, expected %s", i, columns[i], expected)
				}
			}
		})
	}
}

func TestAthenaIntegration_ConvertLabelToAWSTag(t *testing.T) {
	ai := &AthenaIntegration{}

	testCases := map[string]struct {
		label    string
		expected string
	}{
		"already has prefix": {
			label:    "resource_tags_user_kubernetes_io_app_name",
			expected: "resource_tags_user_kubernetes_io_app_name",
		},
		"simple label": {
			label:    "app",
			expected: "resource_tags_user_app",
		},
		"label with dots": {
			label:    "kubernetes.io/app.name",
			expected: "resource_tags_user_kubernetes_io_app_name",
		},
		"label with slashes": {
			label:    "kubernetes/io/app/name",
			expected: "resource_tags_user_kubernetes_io_app_name",
		},
		"label with colons": {
			label:    "kubernetes:io:app:name",
			expected: "resource_tags_user_kubernetes_io_app_name",
		},
		"label with hyphens": {
			label:    "kubernetes-io-app-name",
			expected: "resource_tags_user_kubernetes_io_app_name",
		},
		"label with mixed separators": {
			label:    "kubernetes.io/app-name:test",
			expected: "resource_tags_user_kubernetes_io_app_name_test",
		},
		"empty label": {
			label:    "",
			expected: "resource_tags_user_",
		},
		"label with multiple consecutive separators": {
			label:    "app..name",
			expected: "resource_tags_user_app__name",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ai.ConvertLabelToAWSTag(testCase.label)
			if result != testCase.expected {
				t.Errorf("ConvertLabelToAWSTag() returned %s, expected %s", result, testCase.expected)
			}
		})
	}
}

func stringsToRow(strings []string) types.Row {
	var data []types.Datum
	for _, str := range strings {
		varChar := str
		data = append(data, types.Datum{VarCharValue: &varChar})
	}
	return types.Row{Data: data}
}
