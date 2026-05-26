package nodestats

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// MockHttpClient is a mock implementation of HttpClient interface for testing
type MockHttpClient struct {
	// capturedRequest stores the last request that was sent
	capturedRequest *http.Request
	// responseToReturn is the response that will be returned by Do
	responseToReturn *http.Response
	// errorToReturn is the error that will be returned by Do
	errorToReturn error
}

// Do captures the request and returns the configured response/error
func (m *MockHttpClient) Do(req *http.Request) (*http.Response, error) {
	m.capturedRequest = req
	return m.responseToReturn, m.errorToReturn
}

// GetCapturedRequest returns the last captured request
func (m *MockHttpClient) GetCapturedRequest() *http.Request {
	return m.capturedRequest
}

func TestNodeHttpClient_BearerTokenCapitalization(t *testing.T) {
	tests := []struct {
		name        string
		bearerToken string
		wantHeader  string
	}{
		{
			name:        "Bearer token with correct capitalization",
			bearerToken: "test-token-123",
			wantHeader:  "Bearer test-token-123",
		},
		{
			name:        "Empty bearer token should not set header",
			bearerToken: "",
			wantHeader:  "",
		},
		{
			name:        "Bearer token with special characters",
			bearerToken: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test",
			wantHeader:  "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP client
			mockClient := &MockHttpClient{
				responseToReturn: &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader("")),
				},
			}

			// Create NodeHttpClient with mock
			nodeClient := NewNodeHttpClient(mockClient)

			// Make request through the client
			_, err := nodeClient.AttemptEndPoint("GET", "http://example.com/test", tt.bearerToken)
			if err != nil {
				t.Fatalf("AttemptEndPoint returned error: %v", err)
			}

			// Get the captured request
			capturedReq := mockClient.GetCapturedRequest()
			if capturedReq == nil {
				t.Fatal("Expected request to be captured, but got nil")
			}

			// Verify the Authorization header
			authHeader := capturedReq.Header.Get("Authorization")
			if tt.wantHeader == "" {
				// Empty token case - should not have Authorization header
				if authHeader != "" {
					t.Errorf("Expected no Authorization header, but got: %s", authHeader)
				}
			} else {
				// Verify exact header value including capitalization
				if authHeader != tt.wantHeader {
					t.Errorf("Authorization header = %q, want %q", authHeader, tt.wantHeader)
				}

				// Specifically verify "Bearer" capitalization (capital B, lowercase rest)
				if !strings.HasPrefix(authHeader, "Bearer ") {
					t.Errorf("Authorization header does not start with 'Bearer ' (with capital B): %s", authHeader)
				}

				// Check for common incorrect capitalizations
				if strings.HasPrefix(authHeader, "bearer ") {
					t.Error("Authorization header uses lowercase 'bearer' instead of 'Bearer'")
				}
				if strings.HasPrefix(authHeader, "BEARER ") {
					t.Error("Authorization header uses uppercase 'BEARER' instead of 'Bearer'")
				}
			}
		})
	}
}

func TestNodeHttpClient_BearerCapitalization_HappyPath(t *testing.T) {
	// HAPPY PATH TEST: Verify "Bearer" uses correct capitalization (capital B, lowercase e-a-r-e-r)
	// According to RFC 6750, the correct format is "Bearer" not "bearer" or "BEARER"

	mockClient := &MockHttpClient{
		responseToReturn: &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("")),
		},
	}

	nodeClient := NewNodeHttpClient(mockClient)
	bearerToken := "test-token-123"

	_, err := nodeClient.makeRequest("GET", "http://example.com/api", bearerToken)
	if err != nil {
		t.Fatalf("makeRequest returned error: %v", err)
	}

	capturedReq := mockClient.GetCapturedRequest()
	authHeader := capturedReq.Header.Get("Authorization")

	// PRIMARY ASSERTION: Verify exact string match with correct capitalization
	expectedHeader := "Bearer test-token-123"
	if authHeader != expectedHeader {
		t.Errorf("FAILED: Authorization header = %q, want %q", authHeader, expectedHeader)
		t.Errorf("  This means 'Bearer' capitalization is incorrect")
	}

	// EXPLICIT CHECK: Verify "Bearer" has capital B
	if !strings.HasPrefix(authHeader, "Bearer ") {
		t.Errorf("FAILED: Authorization header must start with 'Bearer ' (capital B, lowercase e-a-r-e-r)")
		t.Errorf("  Got: %q", authHeader)
	}

	// SUCCESS: Log happy path success
	if authHeader == expectedHeader {
		t.Logf("✓ PASS: Bearer token has correct capitalization: %q", authHeader)
	}
}

func TestNodeHttpClient_MakeRequestWithBearerToken(t *testing.T) {
	// Create a mock HTTP client
	mockClient := &MockHttpClient{
		responseToReturn: &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
		},
	}

	// Create NodeHttpClient with mock
	nodeClient := NewNodeHttpClient(mockClient)

	// Test with a bearer token
	bearerToken := "my-secret-token"
	_, err := nodeClient.makeRequest("GET", "http://example.com/api", bearerToken)
	if err != nil {
		t.Fatalf("makeRequest returned error: %v", err)
	}

	// Verify the Authorization header
	capturedReq := mockClient.GetCapturedRequest()
	if capturedReq == nil {
		t.Fatal("Expected request to be captured")
	}

	authHeader := capturedReq.Header.Get("Authorization")
	expectedHeader := "Bearer my-secret-token"
	if authHeader != expectedHeader {
		t.Errorf("Authorization header = %q, want %q", authHeader, expectedHeader)
	}

	// Verify the exact capitalization of "Bearer"
	if !strings.HasPrefix(authHeader, "Bearer ") {
		t.Errorf("Expected 'Bearer ' with capital B, got: %s", authHeader)
	}
}

func TestNodeHttpClient_NoAuthorizationHeaderWhenTokenEmpty(t *testing.T) {
	// Create a mock HTTP client
	mockClient := &MockHttpClient{
		responseToReturn: &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("")),
		},
	}

	// Create NodeHttpClient with mock
	nodeClient := NewNodeHttpClient(mockClient)

	// Test with empty bearer token
	_, err := nodeClient.makeRequest("GET", "http://example.com/api", "")
	if err != nil {
		t.Fatalf("makeRequest returned error: %v", err)
	}

	// Verify no Authorization header is set
	capturedReq := mockClient.GetCapturedRequest()
	if capturedReq == nil {
		t.Fatal("Expected request to be captured")
	}

	authHeader := capturedReq.Header.Get("Authorization")
	if authHeader != "" {
		t.Errorf("Expected no Authorization header when token is empty, got: %s", authHeader)
	}
}

func TestNodeHttpClient_RequestMethod(t *testing.T) {
	tests := []struct {
		name   string
		method string
	}{
		{"GET request", "GET"},
		{"POST request", "POST"},
		{"PUT request", "PUT"},
		{"DELETE request", "DELETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &MockHttpClient{
				responseToReturn: &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader("")),
				},
			}

			nodeClient := NewNodeHttpClient(mockClient)
			_, err := nodeClient.makeRequest(tt.method, "http://example.com/test", "token123")
			if err != nil {
				t.Fatalf("makeRequest returned error: %v", err)
			}

			capturedReq := mockClient.GetCapturedRequest()
			if capturedReq.Method != tt.method {
				t.Errorf("Request method = %s, want %s", capturedReq.Method, tt.method)
			}

			// Also verify Bearer token is set correctly regardless of HTTP method
			authHeader := capturedReq.Header.Get("Authorization")
			if authHeader != "Bearer token123" {
				t.Errorf("Authorization header = %q, want %q", authHeader, "Bearer token123")
			}
		})
	}
}
