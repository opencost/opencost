package azure

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAzureStorageBillingParser_getMonthStrings(t *testing.T) {
	asbp := AzureStorageBillingParser{}
	loc := time.UTC // Use time.UTC constant instead of LoadLocation
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
	loc := time.UTC // Use time.UTC constant instead of LoadLocation
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

func TestAzureStorageBillingParser_processLocalBillingFile(t *testing.T) {
	loc := time.UTC
	start := time.Date(2024, 10, 1, 0, 0, 0, 0, loc)
	end := time.Date(2024, 11, 30, 0, 0, 0, 0, loc)

	testCases := map[string]struct {
		fileName     string
		expectedRows int
		expectError  bool
	}{
		"Gzipped file": {
			fileName:     "test_azure_billing.csv.gz",
			expectedRows: 5,
			expectError:  false,
		},
		"Non-gzipped file": {
			fileName:     "test_azure_billing.csv",
			expectedRows: 5,
			expectError:  false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			asbp := &AzureStorageBillingParser{}
			filePath := valueCasesPath + tc.fileName

			var rowCount int
			resultFn := func(abv *BillingRowValues) error {
				rowCount++
				if abv == nil {
					t.Error("Received nil BillingRowValues")
				}
				return nil
			}

			err := asbp.processLocalBillingFile(filePath, tc.fileName, start, end, resultFn)
			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if rowCount != tc.expectedRows {
				t.Errorf("Expected %d rows, got %d rows", tc.expectedRows, rowCount)
			}
		})
	}
}

func TestAzureStorageBillingParser_processStreamBillingData(t *testing.T) {
	loc := time.UTC
	start := time.Date(2024, 10, 1, 0, 0, 0, 0, loc)
	end := time.Date(2024, 11, 30, 0, 0, 0, 0, loc)

	testCases := map[string]struct {
		fileName     string
		expectedRows int
	}{
		"Gzipped stream": {
			fileName:     "test_azure_billing.csv.gz",
			expectedRows: 5,
		},
		"Non-gzipped stream": {
			fileName:     "test_azure_billing.csv",
			expectedRows: 5,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			asbp := &AzureStorageBillingParser{}

			// Read file into memory to simulate stream
			data, err := os.ReadFile(valueCasesPath + tc.fileName)
			if err != nil {
				t.Fatalf("Failed to read test file: %v", err)
			}
			streamReader := bytes.NewReader(data)

			var rowCount int
			resultFn := func(abv *BillingRowValues) error {
				rowCount++
				if abv == nil {
					t.Error("Received nil BillingRowValues")
				}
				return nil
			}

			err = asbp.processStreamBillingData(streamReader, tc.fileName, start, end, resultFn)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if rowCount != tc.expectedRows {
				t.Errorf("Expected %d rows, got %d rows", tc.expectedRows, rowCount)
			}
		})
	}
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

// TestDecompressIfGzipped_MultipleFiles tests processing multiple files in sequence
// to ensure proper resource cleanup between iterations
func TestDecompressIfGzipped_MultipleFiles(t *testing.T) {
	testFiles := []struct {
		name       string
		content    string
		shouldGzip bool
	}{
		{"file1.csv.gz", "data1,data2\nvalue1,value2\n", true},
		{"file2.csv", "data3,data4\nvalue3,value4\n", false},
		{"file3.csv.GZ", "data5,data6\nvalue5,value6\n", true},
	}

	for _, tf := range testFiles {
		t.Run(tf.name, func(t *testing.T) {
			var input io.Reader
			if tf.shouldGzip {
				var buf bytes.Buffer
				gw := gzip.NewWriter(&buf)
				_, err := gw.Write([]byte(tf.content))
				if err != nil {
					t.Fatalf("Failed to write gzip data: %v", err)
				}
				gw.Close()
				input = &buf
			} else {
				input = strings.NewReader(tf.content)
			}

			reader, err := decompressIfGzipped(input, tf.name)
			if err != nil {
				t.Fatalf("Failed to decompress %s: %v", tf.name, err)
			}
			defer reader.Close()

			output, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("Failed to read from reader for %s: %v", tf.name, err)
			}

			if string(output) != tf.content {
				t.Errorf("Content mismatch for %s. Expected: %q, Got: %q", tf.name, tf.content, string(output))
			}
		})
	}
}

// TestDecompressIfGzipped_CaseInsensitiveExtension tests various case combinations
func TestDecompressIfGzipped_CaseInsensitiveExtension(t *testing.T) {
	testCases := []string{
		"file.gz",
		"file.GZ",
		"file.Gz",
		"file.gZ",
	}

	content := "test,data\n1,2\n"
	for _, blobName := range testCases {
		t.Run(blobName, func(t *testing.T) {
			var buf bytes.Buffer
			gw := gzip.NewWriter(&buf)
			_, err := gw.Write([]byte(content))
			if err != nil {
				t.Fatalf("Failed to write gzip data: %v", err)
			}
			gw.Close()

			reader, err := decompressIfGzipped(&buf, blobName)
			if err != nil {
				t.Fatalf("Failed to decompress %s: %v", blobName, err)
			}
			defer reader.Close()

			output, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("Failed to read from reader: %v", err)
			}

			if string(output) != content {
				t.Errorf("Content mismatch. Expected: %q, Got: %q", content, string(output))
			}
		})
	}
}

// TestDecompressIfGzipped_LargeFile tests handling of larger gzipped files
func TestDecompressIfGzipped_LargeFile(t *testing.T) {
	// Create a larger CSV content (1000 rows)
	var contentBuilder strings.Builder
	contentBuilder.WriteString("col1,col2,col3,col4\n")
	for i := 0; i < 1000; i++ {
		contentBuilder.WriteString("value1,value2,value3,value4\n")
	}
	content := contentBuilder.String()

	// Gzip the content
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write([]byte(content))
	if err != nil {
		t.Fatalf("Failed to write gzip data: %v", err)
	}
	gw.Close()

	blobName := "large_file.csv.gz"
	reader, err := decompressIfGzipped(&buf, blobName)
	if err != nil {
		t.Fatalf("Failed to decompress large file: %v", err)
	}
	defer reader.Close()

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read large file: %v", err)
	}

	if string(output) != content {
		t.Errorf("Content mismatch for large file. Expected %d bytes, got %d bytes", len(content), len(output))
	}

	t.Logf("Successfully processed large gzipped file with %d bytes", len(output))
}
