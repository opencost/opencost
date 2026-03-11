package aws

import (
	"fmt"
	"os"
	"reflect"
	"strings"
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

func stringsToRow(strings []string) types.Row {
	var data []types.Datum
	for _, str := range strings {
		varChar := str
		data = append(data, types.Datum{VarCharValue: &varChar})
	}
	return types.Row{Data: data}
}

// mockAthenaQuerier is a mock that overrides HasBillingPeriodPartitions for testing
type mockAthenaQuerier struct {
	AthenaQuerier
	hasBillingPeriodPartitions bool
}

func (m *mockAthenaQuerier) HasBillingPeriodPartitions() (bool, error) {
	return m.hasBillingPeriodPartitions, nil
}

// mockAthenaIntegration is a mock that uses mockAthenaQuerier
type mockAthenaIntegration struct {
	*mockAthenaQuerier
}

func (m *mockAthenaIntegration) GetPartitionWhere(start, end time.Time) string {
	// The partition logic using our mock's HasBillingPeriodPartitions result
	month := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	endMonth := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	var disjuncts []string

	// Using our mock's result for billing period partitions
	useBillingPeriodPartitions := false
	if m.mockAthenaQuerier.AthenaConfiguration.CURVersion != "1.0" {
		useBillingPeriodPartitions = m.mockAthenaQuerier.hasBillingPeriodPartitions
	}

	for !month.After(endMonth) {
		if m.mockAthenaQuerier.AthenaConfiguration.CURVersion == "1.0" {
			// CUR 1.0 uses year and month columns for partitioning
			disjuncts = append(disjuncts, fmt.Sprintf("(year = '%d' AND month = '%d')", month.Year(), month.Month()))
		} else if useBillingPeriodPartitions {
			// CUR 2.0 with billing_period partitions
			disjuncts = append(disjuncts, fmt.Sprintf("(billing_period = '%d-%02d')", month.Year(), month.Month()))
		} else {
			// CUR 2.0 fallback - use date_format functions
			disjuncts = append(disjuncts, fmt.Sprintf("(date_format(line_item_usage_start_date, '%%Y') = '%d' AND date_format(line_item_usage_start_date, '%%m') = '%02d')",
				month.Year(), month.Month()))
		}
		month = month.AddDate(0, 1, 0)
	}
	return fmt.Sprintf("(%s)", strings.Join(disjuncts, " OR "))
}

func TestAthenaIntegration_GetPartitionWhere(t *testing.T) {
	testCases := map[string]struct {
		integration interface {
			GetPartitionWhere(time.Time, time.Time) string
		}
		start    time.Time
		end      time.Time
		expected string
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
						CURVersion: "1.0",
					},
				},
			},
			start:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 1, 25, 0, 0, 0, 0, time.UTC),
			expected: "((year = '2024' AND month = '1'))",
		},
		"CUR 2.0 single month": {
			integration: &mockAthenaIntegration{
				mockAthenaQuerier: &mockAthenaQuerier{
					AthenaQuerier: AthenaQuerier{
						AthenaConfiguration: AthenaConfiguration{
							Bucket:     "bucket",
							Region:     "region",
							Database:   "database",
							Table:      "table",
							Workgroup:  "workgroup",
							Account:    "account",
							Authorizer: &ServiceAccount{},
							CURVersion: "2.0",
						},
					},
					hasBillingPeriodPartitions: true,
				},
			},
			start:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 1, 25, 0, 0, 0, 0, time.UTC),
			expected: "((billing_period = '2024-01'))",
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
						CURVersion: "1.0",
					},
				},
			},
			start:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC),
			expected: "((year = '2024' AND month = '1') OR (year = '2024' AND month = '2') OR (year = '2024' AND month = '3'))",
		},
		"CUR 2.0 multiple months": {
			integration: &mockAthenaIntegration{
				mockAthenaQuerier: &mockAthenaQuerier{
					AthenaQuerier: AthenaQuerier{
						AthenaConfiguration: AthenaConfiguration{
							Bucket:     "bucket",
							Region:     "region",
							Database:   "database",
							Table:      "table",
							Workgroup:  "workgroup",
							Account:    "account",
							Authorizer: &ServiceAccount{},
							CURVersion: "2.0",
						},
					},
					hasBillingPeriodPartitions: true,
				},
			},
			start:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC),
			expected: "((billing_period = '2024-01') OR (billing_period = '2024-02') OR (billing_period = '2024-03'))",
		},
		"CUR 2.0 across year boundary": {
			integration: &mockAthenaIntegration{
				mockAthenaQuerier: &mockAthenaQuerier{
					AthenaQuerier: AthenaQuerier{
						AthenaConfiguration: AthenaConfiguration{
							Bucket:     "bucket",
							Region:     "region",
							Database:   "database",
							Table:      "table",
							Workgroup:  "workgroup",
							Account:    "account",
							Authorizer: &ServiceAccount{},
							CURVersion: "2.0",
						},
					},
					hasBillingPeriodPartitions: true,
				},
			},
			start:    time.Date(2023, 12, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 2, 10, 0, 0, 0, 0, time.UTC),
			expected: "((billing_period = '2023-12') OR (billing_period = '2024-01') OR (billing_period = '2024-02'))",
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
						CURVersion: "1.0",
					},
				},
			},
			start:    time.Date(2023, 12, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 2, 10, 0, 0, 0, 0, time.UTC),
			expected: "((year = '2023' AND month = '12') OR (year = '2024' AND month = '1') OR (year = '2024' AND month = '2'))",
		},
		"Default CUR version (empty string defaults to 2.0)": {
			integration: &mockAthenaIntegration{
				mockAthenaQuerier: &mockAthenaQuerier{
					AthenaQuerier: AthenaQuerier{
						AthenaConfiguration: AthenaConfiguration{
							Bucket:     "bucket",
							Region:     "region",
							Database:   "database",
							Table:      "table",
							Workgroup:  "workgroup",
							Account:    "account",
							Authorizer: &ServiceAccount{},
							CURVersion: "",
						},
					},
					hasBillingPeriodPartitions: true,
				},
			},
			start:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 1, 25, 0, 0, 0, 0, time.UTC),
			expected: "((billing_period = '2024-01'))",
		},
		"CUR 2.0 fallback when no billing_period partitions": {
			integration: &mockAthenaIntegration{
				mockAthenaQuerier: &mockAthenaQuerier{
					AthenaQuerier: AthenaQuerier{
						AthenaConfiguration: AthenaConfiguration{
							Bucket:     "bucket",
							Region:     "region",
							Database:   "database",
							Table:      "table",
							Workgroup:  "workgroup",
							Account:    "account",
							Authorizer: &ServiceAccount{},
							CURVersion: "2.0",
						},
					},
					hasBillingPeriodPartitions: false, // No billing_period partitions
				},
			},
			start:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 1, 25, 0, 0, 0, 0, time.UTC),
			expected: "((date_format(line_item_usage_start_date, '%Y') = '2024' AND date_format(line_item_usage_start_date, '%m') = '01'))",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.integration.GetPartitionWhere(testCase.start, testCase.end)
			if actual != testCase.expected {
				t.Errorf("GetPartitionWhere() mismatch:\nActual:   %s\nExpected: %s", actual, testCase.expected)
			}
		})
	}
}
