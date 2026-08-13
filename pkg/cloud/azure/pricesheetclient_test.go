package azure

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
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

// testClientOptions points the SDK at a local test server instead of ARM.
func testClientOptions(endpoint string) *arm.ClientOptions {
	return &arm.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			// httptest serves plain HTTP, which the bearer token policy
			// otherwise refuses to send a credential over.
			InsecureAllowCredentialWithHTTP: true,
			Cloud: cloud.Configuration{
				Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
					cloud.ResourceManager: {
						Endpoint: endpoint,
						Audience: "https://management.azure.com",
					},
				},
			},
		},
	}
}

// priceSheetServer fakes the Cost Management price sheet long running operation:
// a POST that returns 202 with a Location header, then an operationResults URL
// that answers 200 with the supplied body.
type priceSheetServer struct {
	server *httptest.Server

	mu          sync.Mutex
	downloadReq *http.Request
	pollCount   int
}

func newPriceSheetServer(t *testing.T, resultBody string) *priceSheetServer {
	t.Helper()
	ps := &priceSheetServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		switch {
		case r.Method == http.MethodPost:
			ps.downloadReq = r.Clone(context.Background())
			w.Header().Set("Location", ps.server.URL+"/operationResults/test-operation?api-version=2023-09-01")
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodGet:
			ps.pollCount++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, resultBody)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	ps.server = httptest.NewServer(mux)
	t.Cleanup(ps.server.Close)
	return ps
}

func (ps *priceSheetServer) downloadRequest() *http.Request {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.downloadReq
}

// TestPriceSheetDownloadURL_UsesCostManagementAPI checks that we call the current
// Cost Management endpoint. The Microsoft.Consumption pricesheets/download API
// this replaced was retired on 01 June 2026 and had already started rejecting
// EnrollmentReader service principals with AuthorizationFailed (issue #3993).
func TestPriceSheetDownloadURL_UsesCostManagementAPI(t *testing.T) {
	const downloadURL = "https://example.blob.core.windows.net/pricesheet.zip?sig=abc"
	server := newPriceSheetServer(t, fmt.Sprintf(`{"status":"Completed","properties":{"reportUrl":%q,"validUntil":"2026-09-30T17:32:28Z"}}`, downloadURL))

	url, err := priceSheetDownloadURL(context.Background(), &mockCredential{}, testClientOptions(server.server.URL), "test-billing-account", "202308")
	require.NoError(t, err)
	assert.Equal(t, downloadURL, url)

	req := server.downloadRequest()
	require.NotNil(t, req, "expected a download request to be made")

	assert.Equal(t, http.MethodPost, req.Method, "the price sheet download is a POST")
	assert.Equal(t,
		"/providers/microsoft.Billing/billingAccounts/test-billing-account/billingPeriods/202308/providers/Microsoft.CostManagement/pricesheets/default/download",
		req.URL.Path,
		"must target Microsoft.CostManagement, not the retired Microsoft.Consumption API")
	assert.NotContains(t, req.URL.Path, "Microsoft.Consumption")
	assert.Equal(t, "2025-03-01", req.URL.Query().Get("api-version"))
}

// TestPriceSheetDownloadURL_AcceptsDownloadURLField covers the documentation
// disagreement over the result field name: the 2025-03-01 spec (and so the
// generated SDK model) says "reportUrl", while Microsoft's migration guide for
// the same endpoint says "downloadUrl". We have to handle both.
func TestPriceSheetDownloadURL_AcceptsDownloadURLField(t *testing.T) {
	const downloadURL = "https://example.blob.core.windows.net/pricesheet.zip?sig=xyz"
	server := newPriceSheetServer(t, fmt.Sprintf(`{"status":"Completed","properties":{"downloadUrl":%q,"validTill":"2026-09-30T17:32:28Z"}}`, downloadURL))

	url, err := priceSheetDownloadURL(context.Background(), &mockCredential{}, testClientOptions(server.server.URL), "test-billing-account", "202308")
	require.NoError(t, err)
	assert.Equal(t, downloadURL, url)
}

func TestPriceSheetDownloadURL_MissingURL(t *testing.T) {
	server := newPriceSheetServer(t, `{"status":"Completed","properties":{}}`)

	_, err := priceSheetDownloadURL(context.Background(), &mockCredential{}, testClientOptions(server.server.URL), "test-billing-account", "202308")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "returned no download URL")
	assert.Contains(t, err.Error(), "Completed")
}

func TestPriceSheetDownloadURL_Validation(t *testing.T) {
	tests := []struct {
		name              string
		billingAccountID  string
		billingPeriodName string
		expectedError     string
	}{
		{
			name:              "empty billing account ID",
			billingAccountID:  "",
			billingPeriodName: "202308",
			expectedError:     "parameter billingAccountID cannot be empty",
		},
		{
			name:              "empty billing period name",
			billingAccountID:  "test-account",
			billingPeriodName: "",
			expectedError:     "parameter billingPeriodName cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No server: validation must fail before any request is made.
			_, err := priceSheetDownloadURL(context.Background(), &mockCredential{}, testClientOptions("https://management.invalid"), tt.billingAccountID, tt.billingPeriodName)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestExtractDownloadURL(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		expected string
	}{
		{
			name:     "reportUrl",
			body:     `{"properties":{"reportUrl":"https://example.com/a"}}`,
			expected: "https://example.com/a",
		},
		{
			name:     "downloadUrl",
			body:     `{"properties":{"downloadUrl":"https://example.com/b"}}`,
			expected: "https://example.com/b",
		},
		{
			name:     "reportUrl wins when both are set",
			body:     `{"properties":{"reportUrl":"https://example.com/a","downloadUrl":"https://example.com/b"}}`,
			expected: "https://example.com/a",
		},
		{
			name:     "no properties",
			body:     `{"status":"Running"}`,
			expected: "",
		},
		{
			name:     "not json",
			body:     `<html>nope</html>`,
			expected: "",
		},
		{
			name:     "empty",
			body:     ``,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, extractDownloadURL([]byte(tt.body)))
		})
	}
}

// The capture policy must not consume the body it inspects, otherwise the SDK
// can't unmarshal the response afterwards.
func TestResponseCaptureLeavesBodyReadable(t *testing.T) {
	const downloadURL = "https://example.blob.core.windows.net/pricesheet.zip"
	server := newPriceSheetServer(t, fmt.Sprintf(`{"status":"Completed","properties":{"reportUrl":%q}}`, downloadURL))

	// A reportUrl response is only resolvable via the typed SDK model, so
	// getting it back proves the body survived the capture policy.
	url, err := priceSheetDownloadURL(context.Background(), &mockCredential{}, testClientOptions(server.server.URL), "test-billing-account", "202308")
	require.NoError(t, err)
	require.Equal(t, downloadURL, url)
}

func TestWithResponseCaptureDoesNotMutateCallerOptions(t *testing.T) {
	original := &arm.ClientOptions{}
	updated := withResponseCapture(original, &responseCapture{})

	assert.Empty(t, original.PerRetryPolicies, "caller's options must be left alone")
	assert.Len(t, updated.PerRetryPolicies, 1)

	// A nil input is valid too.
	assert.Len(t, withResponseCapture(nil, &responseCapture{}).PerRetryPolicies, 1)
}
