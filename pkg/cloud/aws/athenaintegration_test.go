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
	aqiCur10 := AthenaQueryIndexes{
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

	aqiCur20 := AthenaQueryIndexes{
		ColumnIndexes: map[string]int{
			"ListCostColumn":                   0,
			"NetCostColumn":                    1,
			"AmortizedNetCostColumn":           2,
			"AmortizedCostColumn":              3,
			"IsK8sColumn":                      4,
			AthenaDateTruncColumn:              5,
			"line_item_resource_id":            6,
			"bill_payer_account_id":            7,
			"line_item_usage_account_id":       8,
			"line_item_product_code":           9,
			"line_item_usage_type":             10,
			"product_region_code":              11,
			"line_item_availability_zone":      12,
			AthenaResourceTagsCastToJsonColumn: 13,
		},
		TagColumns:             []string{},
		AWSTagColumns:          []string{},
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
			name:    "incorrect row length CUR 1.0",
			row:     []string{"not enough elements"},
			aqi:     aqiCur10,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid list cost CUR 1.0",
			row:     []string{"invalid", "2", "3", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqiCur10,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid net cost CUR 1.0",
			row:     []string{"1", "invalid", "3", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqiCur10,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid amortized net cost CUR 1.0",
			row:     []string{"1", "2", "invalid", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqiCur10,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid amortized cost CUR 1.0",
			row:     []string{"1", "2", "3", "invalid", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqiCur10,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "invalid date CUR 1.0",
			row:     []string{"1", "2", "3", "4", "true", "invalid", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:     aqiCur10,
			want:    nil,
			wantErr: true,
		},
		{
			name: "valid kubernetes with labels CUR 1.0",
			row:  []string{"1", "2", "3", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", "userTagTestValue", "awsTagTestValue"},
			aqi:  aqiCur10,
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
			aqi:  aqiCur10,
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
			name: "valid load balancer product code CUR 1.0",
			row:  []string{"1", "2", "3", "4", "false", "2024-09-01 00:00:00.000", "resourceID/lbID", "payerAccountID", "usageAccountID", "AWSELB", "usageType", "regionCode", "availabilityZone", "", ""},
			aqi:  aqiCur10,
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
			name: "valid non-kubernetes, Fargate CPU CUR 1.0",
			row:  []string{"1", "2", "3", "4", "false", "2024-09-01 00:00:00.000", "123:pod/resource", "payerAccountID", "usageAccountID", "AmazonEKS", "CPU", "regionCode", "availabilityZone", "", ""},
			aqi:  aqiCur10,
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
			name: "valid non-kubernetes, Fargate RAM CUR 1.0",
			row:  []string{"1", "2", "3", "4", "false", "2024-09-01 00:00:00.000", "123:pod/resource", "payerAccountID", "usageAccountID", "AmazonEKS", "GB", "regionCode", "availabilityZone", "", ""},
			aqi:  aqiCur10,
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
		{
			name: "valid kubernetes with labels CUR 2.0",
			row:  []string{"1", "2", "3", "4", "true", "2024-09-01 00:00:00.000", "resourceID", "payerAccountID", "usageAccountID", "productCode", "usageType", "regionCode", "availabilityZone", `{"test": "userTagTestValue", "aws_test": "awsTagTestValue"}`},
			aqi:  aqiCur20,
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

func stringsToRow(strings []string) types.Row {
	var data []types.Datum
	for _, str := range strings {
		varChar := str
		data = append(data, types.Datum{VarCharValue: &varChar})
	}
	return types.Row{Data: data}
}

func TestAthenaIntegration_GetPartitionWhere(t *testing.T) {
	testCases := map[string]struct {
		integration        *AthenaIntegration
		start              time.Time
		end                time.Time
		resourceTagsColumn bool
		expected           string
	}{
		"CUR 1.0 single month": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: AthenaConfiguration{
						Bucket:     "bucket",
						Region:     "region",
						Database:   "database",
						Table:      "table",
						Workgroup:  "workgroup",
						Account:    "account",
						Authorizer: &ServiceAccount{},
					},
				},
			},
			start:              time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 25, 0, 0, 0, 0, time.UTC),
			resourceTagsColumn: false,
			expected:           "((year = '2024' AND month = '1'))",
		},
		"CUR 2.0 single month": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: AthenaConfiguration{
						Bucket:     "bucket",
						Region:     "region",
						Database:   "database",
						Table:      "table",
						Workgroup:  "workgroup",
						Account:    "account",
						Authorizer: &ServiceAccount{},
					},
				},
			},
			start:              time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 1, 25, 0, 0, 0, 0, time.UTC),
			resourceTagsColumn: true,
			expected:           "((billing_period = '2024-01'))",
		},
		"CUR 1.0 multiple months": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: AthenaConfiguration{
						Bucket:     "bucket",
						Region:     "region",
						Database:   "database",
						Table:      "table",
						Workgroup:  "workgroup",
						Account:    "account",
						Authorizer: &ServiceAccount{},
					},
				},
			},
			start:              time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC),
			resourceTagsColumn: false,
			expected:           "((year = '2024' AND month = '1') OR (year = '2024' AND month = '2') OR (year = '2024' AND month = '3'))",
		},
		"CUR 2.0 multiple months": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: AthenaConfiguration{
						Bucket:     "bucket",
						Region:     "region",
						Database:   "database",
						Table:      "table",
						Workgroup:  "workgroup",
						Account:    "account",
						Authorizer: &ServiceAccount{},
					},
				},
			},
			start:              time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC),
			resourceTagsColumn: true,
			expected:           "((billing_period = '2024-01') OR (billing_period = '2024-02') OR (billing_period = '2024-03'))",
		},
		"CUR 2.0 across year boundary": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: AthenaConfiguration{
						Bucket:     "bucket",
						Region:     "region",
						Database:   "database",
						Table:      "table",
						Workgroup:  "workgroup",
						Account:    "account",
						Authorizer: &ServiceAccount{},
					},
				},
			},
			start:              time.Date(2023, 12, 15, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 2, 10, 0, 0, 0, 0, time.UTC),
			resourceTagsColumn: true,
			expected:           "((billing_period = '2023-12') OR (billing_period = '2024-01') OR (billing_period = '2024-02'))",
		},
		"CUR 1.0 across year boundary": {
			integration: &AthenaIntegration{
				AthenaQuerier: AthenaQuerier{
					AthenaConfiguration: AthenaConfiguration{
						Bucket:     "bucket",
						Region:     "region",
						Database:   "database",
						Table:      "table",
						Workgroup:  "workgroup",
						Account:    "account",
						Authorizer: &ServiceAccount{},
					},
				},
			},
			start:              time.Date(2023, 12, 15, 0, 0, 0, 0, time.UTC),
			end:                time.Date(2024, 2, 10, 0, 0, 0, 0, time.UTC),
			resourceTagsColumn: false,
			expected:           "((year = '2023' AND month = '12') OR (year = '2024' AND month = '1') OR (year = '2024' AND month = '2'))",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.integration.GetPartitionWhere(testCase.start, testCase.end, testCase.resourceTagsColumn)
			if actual != testCase.expected {
				t.Errorf("GetPartitionWhere() mismatch:\nActual:   %s\nExpected: %s", actual, testCase.expected)
			}
		})
	}
}
