package azure

import (
	"testing"
	"time"
)

func TestAzureStorageBillingParser_getMonthStrings(t *testing.T) {
	asbp := AzureStorageBillingParser{}
	loc, _ := time.LoadLocation("UTC")
	testCases := map[string]struct {
		start    time.Time
		end      time.Time
		expected []string
	}{
		"Single Month": {
			start: time.Date(2021, 2, 1, 00, 00, 00, 00, loc),
			end:   time.Date(2021, 2, 3, 00, 00, 00, 00, loc),
			expected: []string{
				"20210201-20210228",
			},
		},
		"Two Month": {
			start: time.Date(2021, 2, 1, 00, 00, 00, 00, loc),
			end:   time.Date(2021, 3, 3, 00, 00, 00, 00, loc),
			expected: []string{
				"20210201-20210228",
				"20210301-20210331",
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			months, err := asbp.getMonthStrings(tc.start, tc.end)
			if err != nil {
				t.Errorf("Could not retrieve month strings %v", err)
			}

			if len(months) != len(tc.expected) {
				t.Errorf("Did not create the expected number of month strings. Expected: %d, Actual: %d", len(tc.expected), len(months))
			}

			for i, monthStr := range months {
				if monthStr != tc.expected[i] {
					t.Errorf("Incorrect month string at index %d. Expected: %s, Actual: %s", i, tc.expected[i], monthStr)
				}
			}
		})
	}
}

func TestAzureStorageBillingParser_parseCSV(t *testing.T) {
	loc, _ := time.LoadLocation("UTC")
	start := time.Date(2021, 2, 1, 00, 00, 00, 00, loc)
	end := time.Date(2021, 2, 3, 00, 00, 00, 00, loc)
	tests := map[string]struct {
		input    string
		expected []BillingRowValues
	}{
		"Virtual Machine": {
			input: "VirtualMachine.csv",
			expected: []BillingRowValues{
				{
					Date:            start,
					MeterCategory:   "Virtual Machines",
					SubscriptionID:  "11111111-12ab-34dc-56ef-123456abcdef",
					InvoiceEntityID: "11111111-12ab-34dc-56ef-123456billing",
					InstanceID:      "/subscriptions/11111111-12ab-34dc-56ef-123456abcdef/resourceGroups/Example-Resource-Group/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-12345678-vmss",
					Service:         "Microsoft.Compute",
					Tags: map[string]string{
						"resourceNameSuffix": "12345678",
						"aksEngineVersion":   "aks-release-v0.47.0-1-aks",
						"creationSource":     "aks-aks-nodepool1-12345678-vmss",
					},
					AdditionalInfo: map[string]any{
						"ServiceType": "Standard_DS2_v2",
						"VMName":      "aks-nodepool1-12345678-vmss_0",
						"VCPUs":       2.0,
					},
					Cost:    5,
					NetCost: 4,
				},
			},
		},
		"Missing Brackets": {
			input: "MissingBrackets.csv",
			expected: []BillingRowValues{
				{
					Date:            start,
					MeterCategory:   "Virtual Machines",
					SubscriptionID:  "11111111-12ab-34dc-56ef-123456abcdef",
					InvoiceEntityID: "11111111-12ab-34dc-56ef-123456abcdef",
					InstanceID:      "/subscriptions/11111111-12ab-34dc-56ef-123456abcdef/resourceGroups/Example-Resource-Group/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-12345678-vmss",
					Service:         "Microsoft.Compute",
					Tags: map[string]string{
						"resourceNameSuffix": "12345678",
						"aksEngineVersion":   "aks-release-v0.47.0-1-aks",
						"creationSource":     "aks-aks-nodepool1-12345678-vmss",
					},
					AdditionalInfo: map[string]any{
						"ServiceType": "Standard_DS2_v2",
						"VMName":      "aks-nodepool1-12345678-vmss_0",
						"VCPUs":       2.0,
					},
					Cost:    5,
					NetCost: 4,
				},
			},
		},
	}
	asbp := &AzureStorageBillingParser{}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			csvRetriever := &TestCSVRetriever{
				CSVName: valueCasesPath + tc.input,
			}
			csvs, err := csvRetriever.getCSVReaders(start, end)
			if err != nil {
				t.Errorf("Failed to read specified CSV: %s", err.Error())
			}
			reader := csvs[0]

			var actual []*BillingRowValues
			resultFn := func(abv *BillingRowValues) error {
				actual = append(actual, abv)
				return nil
			}

			err = asbp.parseCSV(start, end, reader, resultFn)
			if err != nil {
				t.Errorf("Error generating BillingRowValues: %s", err.Error())
			}

			if len(actual) != len(tc.expected) {
				t.Errorf("Actual output length did not match expected. Expected: %d, Actual: %d", len(tc.expected), len(actual))
			}

			for i, this := range actual {
				that := tc.expected[i]

				if !this.Date.Equal(that.Date) {
					t.Errorf("Parsed data at index %d has incorrect Date value. Expected: %s, Actual: %s", i, this.Date.String(), that.Date.String())
				}

				if this.MeterCategory != that.MeterCategory {
					t.Errorf("Parsed data at index %d has incorrect MeterCategroy value. Expected: %s, Actual: %s", i, this.MeterCategory, that.MeterCategory)
				}

				if this.SubscriptionID != that.SubscriptionID {
					t.Errorf("Parsed data at index %d has incorrect SubscriptionID value. Expected: %s, Actual: %s", i, this.SubscriptionID, that.SubscriptionID)
				}

				if this.InvoiceEntityID != that.InvoiceEntityID {
					t.Errorf("Parsed data at index %d has incorrect InvoiceEntityID value. Expected: %s, Actual: %s", i, this.InvoiceEntityID, that.InvoiceEntityID)
				}

				if this.InstanceID != that.InstanceID {
					t.Errorf("Parsed data at index %d has incorrect InstanceID value. Expected: %s, Actual: %s", i, this.InstanceID, that.InstanceID)
				}

				if this.Service != that.Service {
					t.Errorf("Parsed data at index %d has incorrect Service value. Expected: %s, Actual: %s", i, this.Service, that.Service)
				}

				if this.Cost != that.Cost {
					t.Errorf("Parsed data at index %d has incorrect Cost value. Expected: %f, Actual: %f", i, this.Cost, that.Cost)
				}

				if this.NetCost != that.NetCost {
					t.Errorf("Parsed data at index %d has incorrect NetCost value. Expected: %f, Actual: %f", i, this.NetCost, that.NetCost)
				}

				if len(this.Tags) != len(that.Tags) {
					t.Errorf("Parsed data at index %d did not have the expected number of tags. Expected: %d, Actual: %d", i, len(that.Tags), len(this.Tags))
				}

				for key, thisTag := range this.Tags {
					thatTag, ok := that.Tags[key]
					if !ok {
						t.Errorf("Parsed data at index %d is has unexpected entry in Tags with key: %s", i, key)
					}

					if thisTag != thatTag {
						t.Errorf("Parsed data at index %d is has unexpected value in Tags for key: %s. Expected: %s, Actual: %s", i, key, thatTag, thisTag)
					}
				}

				for key, thisAI := range this.AdditionalInfo {
					thatAI, ok := that.AdditionalInfo[key]
					if !ok {
						t.Errorf("Parsed data at index %d is has unexpected entry in Additional Inforamation with key: %s", i, key)
					}

					if thisAI != thatAI {
						t.Errorf("Parsed data at index %d is has unexpected value in Tags for key: %s. Expected: %v, Actual: %v", i, key, thisAI, thatAI)
					}
				}
			}

		})

	}
}
