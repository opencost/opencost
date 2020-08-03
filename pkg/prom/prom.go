package prom

import (
	"context"
	"net/http"
	"net/url"

	"github.com/kubecost/cost-model/pkg/util"
	prometheus "github.com/prometheus/client_golang/api"
)

// NewRateLimitedClient creates a prometheus client which limits the number of concurrent outbound
// prometheus requests.
func NewRateLimitedClient(config prometheus.Config, maxConcurrency int, username, password string) (prometheus.Client, error) {
	c, err := prometheus.NewClient(config)
	if err != nil {
		return nil, err
	}

	limiter := util.NewSemaphore(maxConcurrency)

	return &RateLimitedPrometheusClient{
		client:   c,
		limiter:  limiter,
		username: username,
		password: password,
	}, nil
}

// Creates a new prometheus client which limits the total number of concurrent outbound requests
// allowed at a given moment.
type RateLimitedPrometheusClient struct {
	client   prometheus.Client
	limiter  *util.Semaphore
	username string
	password string
}

// Passthrough to the prometheus client API
func (rlpc *RateLimitedPrometheusClient) URL(ep string, args map[string]string) *url.URL {
	return rlpc.client.URL(ep, args)
}

// Rate limit and passthrough to prometheus client API
func (rlpc *RateLimitedPrometheusClient) Do(ctx context.Context, req *http.Request) (*http.Response, []byte, prometheus.Warnings, error) {
	if rlpc.username != "" {
		req.SetBasicAuth(rlpc.username, rlpc.password)
	}
	rlpc.limiter.Acquire()
	defer rlpc.limiter.Return()

	return rlpc.client.Do(ctx, req)
}
