package azure

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCredential implements azcore.TokenCredential for testing
type mockCredential struct{}

func (m *mockCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{
		Token:     "mock-token",
		ExpiresOn: time.Now().Add(time.Hour),
	}, nil
}

// TestPriceSheetClient_CreateRequest tests the core HTTP method fix
// This test directly validates the createRequest method to ensure POST is used
func TestPriceSheetClient_CreateRequest_HTTPMethod(t *testing.T) {
	tests := []struct {
		name              string
		billingAccountID  string
		billingPeriodName string
		expectedMethod    string
		expectedPath      string
	}{
		{
			name:              "POST method for valid billing account",
			billingAccountID:  "test-billing-account",
			billingPeriodName: "202308",
			expectedMethod:    "POST",
			expectedPath:      "/providers/Microsoft.Billing/billingAccounts/test-billing-account/billingPeriods/202308/providers/Microsoft.Consumption/pricesheets/download",
		},
		{
			name:              "POST method for different billing period",
			billingAccountID:  "another-account",
			billingPeriodName: "202401",
			expectedMethod:    "POST",
			expectedPath:      "/providers/Microsoft.Billing/billingAccounts/another-account/billingPeriods/202401/providers/Microsoft.Consumption/pricesheets/download",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cred := &mockCredential{}
			client, err := NewPriceSheetClient(tt.billingAccountID, cred, nil)
			require.NoError(t, err)

			// Test the createRequest method directly
			req, err := client.downloadByBillingPeriodCreateRequest(context.Background(), tt.billingPeriodName)
			require.NoError(t, err)

			// Validate HTTP method - this is the core fix validation
			assert.Equal(t, tt.expectedMethod, req.Raw().Method, "HTTP method must be POST, not GET (fix for Azure billing API issue #3326)")

			// Validate URL path construction
			assert.Contains(t, req.Raw().URL.Path, tt.expectedPath)

			// Validate query parameters
			assert.Equal(t, "2022-06-01", req.Raw().URL.Query().Get("api-version"))
			assert.Equal(t, "en", req.Raw().URL.Query().Get("ln"))

			// Validate Accept header
			assert.Equal(t, []string{"*/*"}, req.Raw().Header["Accept"])
		})
	}
}

// TestPriceSheetClient_Validation tests parameter validation
func TestPriceSheetClient_Validation(t *testing.T) {
	cred := &mockCredential{}

	tests := []struct {
		name              string
		billingAccountID  string
		billingPeriodName string
		expectError       bool
		expectedError     string
	}{
		{
			name:              "empty billing account ID",
			billingAccountID:  "",
			billingPeriodName: "202308",
			expectError:       true,
			expectedError:     "parameter client.billingAccountID cannot be empty",
		},
		{
			name:              "empty billing period name",
			billingAccountID:  "test-account",
			billingPeriodName: "",
			expectError:       true,
			expectedError:     "parameter billingPeriodName cannot be empty",
		},
		{
			name:              "valid parameters",
			billingAccountID:  "test-account",
			billingPeriodName: "202308",
			expectError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewPriceSheetClient(tt.billingAccountID, cred, nil)
			require.NoError(t, err)

			// Test the createRequest method directly for validation
			_, err = client.downloadByBillingPeriodCreateRequest(context.Background(), tt.billingPeriodName)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestPriceSheetClient_URLConstruction tests URL path construction
func TestPriceSheetClient_URLConstruction(t *testing.T) {
	tests := []struct {
		name              string
		billingAccountID  string
		billingPeriodName string
		expectedPath      string
	}{
		{
			name:              "standard billing account and period",
			billingAccountID:  "123456789",
			billingPeriodName: "202308",
			expectedPath:      "/providers/Microsoft.Billing/billingAccounts/123456789/billingPeriods/202308/providers/Microsoft.Consumption/pricesheets/download",
		},
		{
			name:              "billing account with dashes",
			billingAccountID:  "account-with-dashes",
			billingPeriodName: "202308",
			expectedPath:      "/providers/Microsoft.Billing/billingAccounts/account-with-dashes/billingPeriods/202308/providers/Microsoft.Consumption/pricesheets/download",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cred := &mockCredential{}
			client, err := NewPriceSheetClient(tt.billingAccountID, cred, nil)
			require.NoError(t, err)

			req, err := client.downloadByBillingPeriodCreateRequest(context.Background(), tt.billingPeriodName)
			require.NoError(t, err)

			// Verify URL path construction
			assert.Contains(t, req.Raw().URL.Path, tt.expectedPath)
		})
	}
}

// TestPriceSheetClient_MethodRegression ensures the HTTP method fix doesn't regress
// This test would fail if someone accidentally changed POST back to GET
func TestPriceSheetClient_MethodRegression(t *testing.T) {
	// This test specifically prevents regression of issue #3326
	// where Azure billing API was incorrectly using GET instead of POST
	cred := &mockCredential{}
	client, err := NewPriceSheetClient("test-billing-account", cred, nil)
	require.NoError(t, err)

	req, err := client.downloadByBillingPeriodCreateRequest(context.Background(), "202308")
	require.NoError(t, err)

	// Critical assertion: must be POST, never GET
	// This is the core fix for Azure billing account pricing API
	assert.Equal(t, "POST", req.Raw().Method, "REGRESSION: HTTP method changed back to GET - this will cause 404 errors with Azure billing API")
	assert.NotEqual(t, "GET", req.Raw().Method, "HTTP method must not be GET for Azure pricesheet download endpoint")
}
