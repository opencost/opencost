package prom

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/kubecost/cost-model/pkg/util"
	prometheus "github.com/prometheus/client_golang/api"
)

// ResponseAndBody is just a test objet used to hold predefined responses
// and response bodies
type ResponseAndBody struct {
	Response *http.Response
	Body     []byte
}

// MockPromClient accepts a slice of responses and bodies to return on requests made.
// It will cycle these responses linearly, then reset back to the first.
// Also works with concurrent requests.
type MockPromClient struct {
	sync.Mutex
	responses []*ResponseAndBody
	current   int
}

// prometheus.Client URL()
func (mpc *MockPromClient) URL(ep string, args map[string]string) *url.URL {
	return nil
}

// prometheus.Client Do
func (mpc *MockPromClient) Do(context.Context, *http.Request) (*http.Response, []byte, prometheus.Warnings, error) {
	// fake latency
	time.Sleep(250 * time.Millisecond)

	mpc.Lock()
	defer mpc.Unlock()
	rnb := mpc.responses[mpc.current]
	mpc.current++
	if mpc.current >= len(mpc.responses) {
		mpc.current = 0
	}

	return rnb.Response, rnb.Body, nil, nil
}

// Creates a new mock prometheus client
func newMockPromClientWith(responses []*ResponseAndBody) prometheus.Client {
	return &MockPromClient{
		responses: responses,
		current:   0,
	}
}

// creates a ResponseAndBody representing a 200 status code
func newSuccessfulResponse() *ResponseAndBody {
	body := []byte("Success")

	return &ResponseAndBody{
		Response: &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader(body)),
		},
		Body: body,
	}
}

// creates a ResponseAndBody representing a 429 status code and 'Retry-After' header
func newNormalRateLimitedResponse(retryAfter string) *ResponseAndBody {
	body := []byte("Rate Limitted")

	return &ResponseAndBody{
		Response: &http.Response{
			StatusCode: 429,
			Header: http.Header{
				"Retry-After": []string{retryAfter},
			},
			Body: io.NopCloser(bytes.NewReader(body)),
		},
		Body: body,
	}
}

// creates a ResponseAndBody representing some amazon services ThrottlingException 400 status
func newHackyAmazonRateLimitedResponse() *ResponseAndBody {
	body := []byte("<ThrottlingException>\n  <Message>Rate exceeded</Message>\n</ThrottlingException>\n")

	return &ResponseAndBody{
		Response: &http.Response{
			StatusCode: 400,
			Body:       io.NopCloser(bytes.NewReader(body)),
		},
		Body: body,
	}
}

func TestRateLimitedOnceAndSuccess(t *testing.T) {
	t.Parallel()

	// creates a prom client with hard coded responses for any requests that
	// are issued
	promClient := newMockPromClientWith([]*ResponseAndBody{
		newNormalRateLimitedResponse("2"),
		newSuccessfulResponse(),
	})

	client, err := NewRateLimitedClient(
		"TestClient",
		promClient,
		1,
		nil,
		nil,
		true,
		"",
	)

	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// we just need to execute this  once to see retries in effect
	res, body, _, err := client.Do(context.Background(), req)

	if res.StatusCode != 200 {
		t.Fatalf("200 StatusCode expected. Got: %d", res.StatusCode)
	}

	if string(body) != "Success" {
		t.Fatalf("Expected 'Success' message body. Got: %s", string(body))
	}
}

func TestRateLimitedResponses(t *testing.T) {
	t.Parallel()

	dateRetry := time.Now().Add(5 * time.Second).Format(time.RFC1123)

	// creates a prom client with hard coded responses for any requests that
	// are issued
	promClient := newMockPromClientWith([]*ResponseAndBody{
		newNormalRateLimitedResponse("2"),
		newNormalRateLimitedResponse(dateRetry),
		newHackyAmazonRateLimitedResponse(),
		newHackyAmazonRateLimitedResponse(),
		newNormalRateLimitedResponse("3"),
	})

	client, err := NewRateLimitedClient(
		"TestClient",
		promClient,
		1,
		nil,
		nil,
		true,
		"",
	)

	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// we just need to execute this  once to see retries in effect
	_, _, _, err = client.Do(context.Background(), req)

	if err == nil {
		t.Fatal("Expected a RateLimitedResponseError. Err was nil.")
	}

	rateLimitErr, ok := err.(*RateLimitedResponseError)
	if !ok {
		t.Fatal("Expected a RateLimitedResponseError. Got unexpected type.")
	}

	t.Logf("%s\n", rateLimitErr.Error())

	// RateLimitedResponseStatus checks just ensure that wait times were close configuration
	rateLimitRetries := rateLimitErr.RateLimitStatus

	if len(rateLimitRetries) != 5 {
		t.Fatalf("Expected 5 retries. Got: %d", len(rateLimitRetries))
	}

	// check 2s wait after
	seconds := rateLimitRetries[0].WaitTime.Seconds()
	if !util.IsApproximately(seconds, 2.0) {
		t.Fatalf("Expected 2.0 seconds. Got %.2f", seconds)
	}

	// check to see if fuzzed wait time for datetime parsing
	seconds = rateLimitRetries[1].WaitTime.Seconds()
	if math.Abs(seconds-2.0) > 3.0 {
		t.Fatalf("Expected delta between 2s and resulting wait time to be within 3s. Seconds: %.2f, Delta: %.2f", seconds, math.Abs(seconds-2.0))
	}

	// check 1s wait
	seconds = rateLimitRetries[2].WaitTime.Seconds()
	if !util.IsApproximately(seconds, 1.0) {
		t.Fatalf("Expected 1.0 seconds. Got %.2f", seconds)
	}

	// check 1s wait
	seconds = rateLimitRetries[3].WaitTime.Seconds()
	if !util.IsApproximately(seconds, 1.0) {
		t.Fatalf("Expected 1.0 seconds. Got %.2f", seconds)
	}

	// check 3s wait
	seconds = rateLimitRetries[4].WaitTime.Seconds()
	if !util.IsApproximately(seconds, 3.0) {
		t.Fatalf("Expected 3.0 seconds. Got %.2f", seconds)
	}

}

func TestConcurrentRateLimiting(t *testing.T) {
	t.Parallel()

	// Set QueryConcurrency to 3 here, then test double that
	const QueryConcurrency = 3
	const TotalRequests = QueryConcurrency * 2

	dateRetry := time.Now().Add(5 * time.Second).Format(time.RFC1123)

	// creates a prom client with hard coded responses for any requests that
	// are issued
	promClient := newMockPromClientWith([]*ResponseAndBody{
		newNormalRateLimitedResponse("2"),
		newNormalRateLimitedResponse(dateRetry),
		newHackyAmazonRateLimitedResponse(),
		newHackyAmazonRateLimitedResponse(),
		newNormalRateLimitedResponse("3"),
	})

	client, err := NewRateLimitedClient(
		"TestClient",
		promClient,
		QueryConcurrency,
		nil,
		nil,
		true,
		"",
	)

	if err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, TotalRequests)

	for i := 0; i < TotalRequests; i++ {
		go func() {
			req, err := http.NewRequest(http.MethodPost, "", nil)
			if err != nil {
				errs <- err
				return
			}

			// we just need to execute this  once to see retries in effect
			_, _, _, err = client.Do(context.Background(), req)

			errs <- err
		}()
	}

	for i := 0; i < TotalRequests; i++ {
		err := <-errs
		if err == nil {
			t.Fatal("Expected a RateLimitedResponseError. Err was nil.")
		}

		rateLimitErr, ok := err.(*RateLimitedResponseError)
		if !ok {
			t.Fatal("Expected a RateLimitedResponseError. Got unexpected type.")
		}

		t.Logf("%s\n", rateLimitErr.Error())
	}
}
