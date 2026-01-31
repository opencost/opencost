package azure

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"
	"strings"
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

func TestAzureStorageBillingParser_parseCSV_GzippedFile(t *testing.T) {
	// Integration test with gzipped Azure billing export
	loc, _ := time.LoadLocation("UTC")
	start := time.Date(2024, 10, 1, 00, 00, 00, 00, loc)
	end := time.Date(2024, 11, 30, 00, 00, 00, 00, loc)

	asbp := &AzureStorageBillingParser{}

	// Open the gzipped test file
	gzFile, err := os.Open(valueCasesPath + "test_azure_billing.csv.gz")
	if err != nil {
		t.Fatalf("Failed to open test gzipped file: %v", err)
	}
	defer gzFile.Close()

	// Use decompressIfGzipped to decompress
	reader, err := decompressIfGzipped(gzFile, "test_azure_billing.csv.gz")
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer reader.Close()

	// Parse the CSV
	var rowCount int
	var totalCost float64
	resultFn := func(abv *BillingRowValues) error {
		rowCount++
		// Validate that we're getting valid data
		if abv == nil {
			t.Error("Received nil BillingRowValues")
			return nil
		}
		totalCost += abv.Cost
		return nil
	}

	err = asbp.parseCSV(start, end, csv.NewReader(reader), resultFn)
	if err != nil {
		t.Fatalf("Error parsing gzipped CSV: %v", err)
	}

	// We expect 5 data rows (excluding header)
	expectedRows := 5
	if rowCount != expectedRows {
		t.Errorf("Expected %d rows, got %d rows", expectedRows, rowCount)
	}

	// Verify we got some cost data
	if totalCost == 0 {
		t.Error("Total cost is 0, expected some cost data")
	}

	t.Logf("Successfully parsed %d rows from gzipped Azure billing export, total cost: $%.2f", rowCount, totalCost)
}

func TestAzureStorageBillingParser_parseCSV_NonGzippedFile(t *testing.T) {
	// Test backward compatibility with non-gzipped files
	loc, _ := time.LoadLocation("UTC")
	start := time.Date(2024, 10, 1, 00, 00, 00, 00, loc)
	end := time.Date(2024, 11, 30, 00, 00, 00, 00, loc)

	asbp := &AzureStorageBillingParser{}

	// Open the non-gzipped test file
	csvFile, err := os.Open(valueCasesPath + "test_azure_billing.csv")
	if err != nil {
		t.Fatalf("Failed to open test CSV file: %v", err)
	}
	defer csvFile.Close()

	// Use decompressIfGzipped - should return NopCloser for non-gz files
	reader, err := decompressIfGzipped(csvFile, "test_azure_billing.csv")
	if err != nil {
		t.Fatalf("Failed to wrap reader: %v", err)
	}
	defer reader.Close()

	// Parse the CSV
	var rowCount int
	resultFn := func(abv *BillingRowValues) error {
		rowCount++
		if abv == nil {
			t.Error("Received nil BillingRowValues")
		}
		return nil
	}

	err = asbp.parseCSV(start, end, csv.NewReader(reader), resultFn)
	if err != nil {
		t.Fatalf("Error parsing non-gzipped CSV: %v", err)
	}

	// We expect 5 data rows (excluding header)
	expectedRows := 5
	if rowCount != expectedRows {
		t.Errorf("Expected %d rows, got %d rows", expectedRows, rowCount)
	}

	t.Logf("Successfully parsed %d rows from non-gzipped Azure billing export", rowCount)
}

func TestDecompressIfGzipped(t *testing.T) {
	testCases := map[string]struct {
		blobName    string
		content     string
		shouldGzip  bool
		expectError bool
	}{
		"Gzipped file with .gz extension": {
			blobName:    "billing_export.csv.gz",
			content:     "test,data\n1,2\n",
			shouldGzip:  true,
			expectError: false,
		},
		"Gzipped file with .GZ extension (case insensitive)": {
			blobName:    "billing_export.CSV.GZ",
			content:     "test,data\n1,2\n",
			shouldGzip:  true,
			expectError: false,
		},
		"Non-gzipped CSV file": {
			blobName:    "billing_export.csv",
			content:     "test,data\n1,2\n",
			shouldGzip:  false,
			expectError: false,
		},
		"Non-gzipped file without extension": {
			blobName:    "billing_export",
			content:     "test,data\n1,2\n",
			shouldGzip:  false,
			expectError: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var inputReader io.Reader

			if tc.shouldGzip {
				// Create gzipped content
				var buf bytes.Buffer
				gw := gzip.NewWriter(&buf)
				_, err := gw.Write([]byte(tc.content))
				if err != nil {
					t.Fatalf("Failed to write gzip content: %v", err)
				}
				gw.Close()
				inputReader = &buf
			} else {
				// Use plain content
				inputReader = strings.NewReader(tc.content)
			}

			// Call decompressIfGzipped
			reader, err := decompressIfGzipped(inputReader, tc.blobName)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer reader.Close()

			// Read and verify content
			output, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("Failed to read from reader: %v", err)
			}

			if string(output) != tc.content {
				t.Errorf("Content mismatch. Expected: %q, Got: %q", tc.content, string(output))
			}
		})
	}
}

func TestDecompressIfGzipped_InvalidGzip(t *testing.T) {
	// Test with invalid gzip data
	blobName := "invalid.csv.gz"
	invalidData := strings.NewReader("this is not gzipped data")

	reader, err := decompressIfGzipped(invalidData, blobName)
	if err == nil {
		if reader != nil {
			reader.Close()
		}
		t.Error("Expected error for invalid gzip data, but got none")
	}
}

func TestDecompressIfGzipped_EmptyGzipFile(t *testing.T) {
	// Test with empty gzipped file
	blobName := "empty.csv.gz"
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Close()

	reader, err := decompressIfGzipped(&buf, blobName)
	if err != nil {
		t.Fatalf("Unexpected error for empty gzip file: %v", err)
	}
	defer reader.Close()

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read empty gzip file: %v", err)
	}

	if len(output) != 0 {
		t.Errorf("Expected empty output, got %d bytes", len(output))
	}
}
