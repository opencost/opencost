package nodestats

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

// HttpClient is an interface that captures the Do method of the http.Client. We use this interface to allow
// mocking in tests.
type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type NodeHttpClient struct {
	client HttpClient
}

// NewNodeHttpClient creates a new NodeHttpClient with the provided HttpClient.
func NewNodeHttpClient(client HttpClient) *NodeHttpClient {
	return &NodeHttpClient{
		client: client,
	}
}

// AttemptEndPoint will hit a specified endpoint with as many retries as it is allotted.
func (c *NodeHttpClient) AttemptEndPoint(method string, URL string, bearerToken string) (*http.Response, error) {
	attempts := uint(1)

	for i := uint(0); i < attempts; i++ {
		if i > 0 {
			time.Sleep(time.Duration(int64(math.Pow(2, float64(i)))) * time.Second)
		}

		data, err := c.makeRequest(method, URL, bearerToken)
		if err == nil {
			return data, nil
		}
		log.Warnf("Error making request to %s: %s", URL, err)
	}

	return nil, fmt.Errorf("requests to %v failed", URL)
}

// makeRequest will call out to an endpoint and attempt to decode the body into an existing
// data type.
func (c *NodeHttpClient) makeRequest(method string, URL string, bearerToken string) (*http.Response, error) {
	request, err := http.NewRequest(method, URL, nil)
	if err != nil {
		return nil, err
	}

	if bearerToken != "" {
		request.Header.Add("Authorization", "Bearer "+bearerToken)
	}

	resp, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode <= 299) {
		if resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil, fmt.Errorf("invalid response %s", strconv.Itoa(resp.StatusCode))
	}

	return resp, nil
}

// NodeHttpConnect is a struct that represents a connection to a node using an http client and endpoint formatter.
type NodeHttpConnection struct {
	formatter NodeEndpointFormatter
	client    *NodeHttpClient
}

// NewNodeHttpConnection creates a new HttpConnection with the provided NodeHttpClient and NodeEndpointFormatter.
func NewNodeHttpConnection(client *NodeHttpClient, formatter NodeEndpointFormatter) *NodeHttpConnection {
	return &NodeHttpConnection{
		formatter: formatter,
		client:    client,
	}
}

// AttemptEndPoint will hit a specified endpoint leveraging the internal http client and formatter for the endpoint.
func (nhc *NodeHttpConnection) AttemptEndPoint(method string, url string, bearerToken string) (*http.Response, error) {
	return nhc.client.AttemptEndPoint(method, nhc.formatter.FormatEndpoint(url), bearerToken)
}
