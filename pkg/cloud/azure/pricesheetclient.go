package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/costmanagement/armcostmanagement/v3"

	"github.com/opencost/opencost/core/pkg/log"
)

// The EA price sheet is fetched through the Cost Management price sheet API:
//
//	POST /providers/Microsoft.Billing/billingAccounts/{id}/billingPeriods/{yyyymm}
//	     /providers/Microsoft.CostManagement/pricesheets/default/download
//
// It replaces the Microsoft.Consumption pricesheets/download API, which was
// retired on 01 June 2026. Consumption only ever granted access via the
// Microsoft.Consumption/pricesheets/action permission, which the EnrollmentReader
// billing role no longer carries, so calls against it now fail with
// AuthorizationFailed even for correctly configured service principals.
//
// The request is a long running operation: it returns 202 plus a Location header
// pointing at an operationResults URL that eventually answers 200 with the
// generated download link.

// pollFrequency is how often we poll the operationResults URL. Generating the
// sheet can take several minutes, and the service asks for 60s in Retry-After,
// so there is no point polling aggressively.
const pollFrequency = 30 * time.Second

// priceSheetDownloadURL asks Cost Management to generate the price sheet for the
// given billing period (formatted yyyymm) and returns a URL to download it from.
// The returned URL is time limited.
func priceSheetDownloadURL(ctx context.Context, credential azcore.TokenCredential, options *arm.ClientOptions, billingAccountID, billingPeriodName string) (string, error) {
	if billingAccountID == "" {
		return "", errors.New("parameter billingAccountID cannot be empty")
	}
	if billingPeriodName == "" {
		return "", errors.New("parameter billingPeriodName cannot be empty")
	}

	// The generated SDK only understands "reportUrl" in the operation result,
	// but Microsoft's own migration guide for this endpoint documents
	// "downloadUrl". Keep a copy of the raw body so we can cope with either.
	capture := &responseCapture{}
	options = withResponseCapture(options, capture)

	client, err := armcostmanagement.NewPriceSheetClient(credential, options)
	if err != nil {
		return "", fmt.Errorf("creating price sheet client: %w", err)
	}

	poller, err := client.BeginDownloadByBillingAccount(ctx, billingAccountID, billingPeriodName, nil)
	if err != nil {
		return "", fmt.Errorf("beginning price sheet download: %w", err)
	}

	resp, err := poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{
		Frequency: pollFrequency,
	})
	if err != nil {
		return "", fmt.Errorf("polling for price sheet: %w", err)
	}

	if resp.Properties != nil {
		if resp.Properties.ReportURL != nil && *resp.Properties.ReportURL != "" {
			logValidUntil(resp.Properties.ValidUntil)
			return string(*resp.Properties.ReportURL), nil
		}
		logValidUntil(resp.Properties.ValidUntil)
	}

	// Fall back to reading the URL out of the raw operation result.
	if url := extractDownloadURL(capture.Body()); url != "" {
		return url, nil
	}

	status := ""
	if resp.Status != nil {
		status = string(*resp.Status)
	}
	return "", fmt.Errorf("price sheet operation finished with status %q but returned no download URL", status)
}

func logValidUntil(validUntil *time.Time) {
	if validUntil != nil {
		log.Debugf("price sheet download URL valid until %s", validUntil.Format(time.RFC3339))
	}
}

// extractDownloadURL pulls the download link out of a raw operation result body,
// accepting either of the two field names the service has been documented to
// use. It returns "" if the body isn't a recognisable operation result.
func extractDownloadURL(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var result struct {
		Properties struct {
			ReportURL   string `json:"reportUrl"`
			DownloadURL string `json:"downloadUrl"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return ""
	}
	if result.Properties.ReportURL != "" {
		return result.Properties.ReportURL
	}
	return result.Properties.DownloadURL
}

// withResponseCapture returns a copy of options with capture installed as a
// pipeline policy, leaving the caller's options untouched.
func withResponseCapture(options *arm.ClientOptions, capture *responseCapture) *arm.ClientOptions {
	var updated arm.ClientOptions
	if options != nil {
		updated = *options
	}
	updated.PerRetryPolicies = append(append([]policy.Policy{}, updated.PerRetryPolicies...), capture)
	return &updated
}

// responseCapture keeps hold of the body of the most recent successful JSON
// response to pass through the pipeline, so we can read fields the generated
// models don't know about. It is safe for concurrent use because the poller may
// run on a different goroutine to the one that reads the body.
type responseCapture struct {
	mu   sync.Mutex
	body []byte
}

func (c *responseCapture) Do(req *policy.Request) (*http.Response, error) {
	resp, err := req.Next()
	if err != nil || resp == nil {
		return resp, err
	}
	if resp.StatusCode != http.StatusOK {
		return resp, nil
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "json") {
		return resp, nil
	}
	// runtime.Payload buffers the body and rewinds it, so the SDK can still
	// unmarshal the response after we've looked at it.
	body, payloadErr := runtime.Payload(resp)
	if payloadErr != nil {
		log.Debugf("could not buffer price sheet response body: %s", payloadErr)
		return resp, nil
	}
	c.mu.Lock()
	c.body = body
	c.mu.Unlock()
	return resp, nil
}

func (c *responseCapture) Body() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.body
}
