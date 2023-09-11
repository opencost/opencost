package azure

import (
	"encoding/csv"
	"os"
	"testing"
	"time"
)

const billingExportPath = "./resources/billingexports/"
const headerSetPath = billingExportPath + "headersets/"
const valueCasesPath = billingExportPath + "values/"

type TestCSVRetriever struct {
	CSVName string
}

func (tcr TestCSVRetriever) getCSVReaders(start, end time.Time) ([]*csv.Reader, error) {
	csvFile, err := os.Open(tcr.CSVName)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(csvFile)
	return append([]*csv.Reader{}, reader), nil
}

func Test_NewBillingExportParser(t *testing.T) {
	loc, _ := time.LoadLocation("UTC")
	start := time.Date(2021, 2, 1, 00, 00, 00, 00, loc)
	end := time.Date(2021, 2, 3, 00, 00, 00, 00, loc)
	tests := map[string]struct {
		input    string
		expected BillingExportParser
	}{
		"English Headers": {
			input: "PayAsYouGo.csv",
			expected: BillingExportParser{
				Date:            3,
				MeterCategory:   4,
				InvoiceEntityID: 0,
				SubscriptionID:  0,
				InstanceID:      14,
				Service:         12,
				Tags:            15,
				AdditionalInfo:  17,
				Cost:            11,
				NetCost:         11,
				DateFormat:      azureDateLayout,
			},
		},
		"Enterprise Camel Headers": {
			input: "EnterpriseCamel.csv",
			expected: BillingExportParser{
				Date:            11,
				MeterCategory:   18,
				InvoiceEntityID: 0,
				SubscriptionID:  23,
				InstanceID:      29,
				Service:         15,
				Tags:            45,
				AdditionalInfo:  44,
				Cost:            38,
				NetCost:         38,
				DateFormat:      AzureEnterpriseDateLayout,
			},
		},
		"Enterprise Headers": {
			input: "Enterprise.csv",
			expected: BillingExportParser{
				Date:            7,
				MeterCategory:   9,
				InvoiceEntityID: 39,
				SubscriptionID:  3,
				InstanceID:      20,
				Service:         19,
				Tags:            21,
				AdditionalInfo:  23,
				Cost:            17,
				NetCost:         17,
				DateFormat:      AzureEnterpriseDateLayout,
			},
		},
		"German Headers": {
			input: "German.csv",
			expected: BillingExportParser{
				Date:            3,
				MeterCategory:   4,
				InvoiceEntityID: 0,
				SubscriptionID:  0,
				InstanceID:      14,
				Service:         12,
				Tags:            15,
				AdditionalInfo:  17,
				Cost:            11,
				NetCost:         11,
				DateFormat:      azureDateLayout,
			},
		},
		"YA Headers": {
			input: "YA.csv",
			expected: BillingExportParser{
				Date:            3,
				MeterCategory:   4,
				InvoiceEntityID: 0,
				SubscriptionID:  0,
				InstanceID:      14,
				Service:         12,
				Tags:            15,
				AdditionalInfo:  17,
				Cost:            11,
				NetCost:         11,
				DateFormat:      AzureEnterpriseDateLayout,
			},
		},
		"BOM Prefixed Headers": {
			input: "BOM.csv",
			expected: BillingExportParser{
				Date:            3,
				MeterCategory:   4,
				InvoiceEntityID: 0,
				SubscriptionID:  0,
				InstanceID:      14,
				Service:         12,
				Tags:            15,
				AdditionalInfo:  17,
				Cost:            11,
				NetCost:         11,
				DateFormat:      azureDateLayout,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			csvRetriever := TestCSVRetriever{
				CSVName: headerSetPath + tc.input,
			}
			csvs, err := csvRetriever.getCSVReaders(start, end)
			if err != nil {
				t.Errorf("Failed to read specified CSV: %s", err.Error())
			}
			reader := csvs[0]
			headers, _ := reader.Read()
			abp, err := NewBillingParseSchema(headers)
			if err != nil {
				t.Errorf("failed to create Azure Billing Parser from headers with error: %s", err.Error())
			}

			if abp.DateFormat != tc.expected.DateFormat {
				t.Errorf("Azure Billing Parser does not have expected DateFormat index. Expected: %s, Actual: %s", tc.expected.DateFormat, abp.DateFormat)
			}

			if abp.Date != tc.expected.Date {
				t.Errorf("Azure Billing Parser does not have expected Date index. Expected: %d, Actual: %d", tc.expected.Date, abp.Date)
			}

			if abp.MeterCategory != tc.expected.MeterCategory {
				t.Errorf("Azure Billing Parser does not have expected MeterCategory index. Expected: %d, Actual: %d", tc.expected.MeterCategory, abp.MeterCategory)
			}

			if abp.InvoiceEntityID != tc.expected.InvoiceEntityID {
				t.Errorf("Azure Billing Parser does not have expected InvoiceEntityID index. Expected: %d, Actual: %d", tc.expected.InvoiceEntityID, abp.InvoiceEntityID)
			}

			if abp.SubscriptionID != tc.expected.SubscriptionID {
				t.Errorf("Azure Billing Parser does not have expected SubscriptionID index. Expected: %d, Actual: %d", tc.expected.SubscriptionID, abp.SubscriptionID)
			}

			if abp.InstanceID != tc.expected.InstanceID {
				t.Errorf("Azure Billing Parser does not have expected InstanceID index. Expected: %d, Actual: %d", tc.expected.InstanceID, abp.InstanceID)
			}

			if abp.Service != tc.expected.Service {
				t.Errorf("Azure Billing Parser does not have expected Service index. Expected: %d, Actual: %d", tc.expected.Service, abp.Service)
			}

			if abp.Tags != tc.expected.Tags {
				t.Errorf("Azure Billing Parser does not have expected Tags index. Expected: %d, Actual: %d", tc.expected.Tags, abp.Tags)
			}

			if abp.AdditionalInfo != tc.expected.AdditionalInfo {
				t.Errorf("Azure Billing Parser does not have expected AdditionalInfo index. Expected: %d, Actual: %d", tc.expected.AdditionalInfo, abp.AdditionalInfo)
			}

			if abp.Cost != tc.expected.Cost {
				t.Errorf("Azure Billing Parser does not have expected Cost index. Expected: %d, Actual: %d", tc.expected.Cost, abp.Cost)
			}

			if abp.NetCost != tc.expected.NetCost {
				t.Errorf("Azure Billing Parser does not have expected NetCost index. Expected: %d, Actual: %d", tc.expected.NetCost, abp.NetCost)
			}
		})
	}
}
