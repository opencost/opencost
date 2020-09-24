package prom

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"time"

	golog "log"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util"
	prometheus "github.com/prometheus/client_golang/api"
)

// Creates a new prometheus client which limits the total number of concurrent outbound requests
// allowed at a given moment.
type RateLimitedPrometheusClient struct {
	client      prometheus.Client
	limiter     *util.Semaphore
	requests    *util.AtomicInt32
	outbound    *util.AtomicInt32
	username    string
	password    string
	bearerToken string
	fileLogger  *golog.Logger
}

// requestCounter is used to determine if the prometheus client keeps track of
// the concurrent outbound requests
type requestCounter interface {
	TotalRequests() int32
	TotalOutboundRequests() int32
}

// NewRateLimitedClient creates a prometheus client which limits the number of concurrent outbound
// prometheus requests.
func NewRateLimitedClient(config prometheus.Config, maxConcurrency int, username, password, bearerToken string, queryLogFile string) (prometheus.Client, error) {
	c, err := prometheus.NewClient(config)
	if err != nil {
		return nil, err
	}

	limiter := util.NewSemaphore(maxConcurrency)
	requests := util.NewAtomicInt32(0)
	outbound := util.NewAtomicInt32(0)

	var logger *golog.Logger
	if queryLogFile != "" {
		exists, err := util.FileExists(queryLogFile)
		if exists {
			os.Remove(queryLogFile)
		}

		f, err := os.OpenFile(queryLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Infof("Failed to open queryLogFile: %s for query logging: %s", queryLogFile, err)
		} else {
			logger = golog.New(f, "query-log", golog.LstdFlags)
		}
	}

	return &RateLimitedPrometheusClient{
		client:      c,
		limiter:     limiter,
		requests:    requests,
		outbound:    outbound,
		username:    username,
		password:    password,
		bearerToken: bearerToken,
		fileLogger:  logger,
	}, nil
}

// LogPrometheusClientState logs the current state, with respect to outbound requests, if that
// information is available.
func LogPrometheusClientState(client prometheus.Client) {
	if rc, ok := client.(requestCounter); ok {
		total := rc.TotalRequests()
		outbound := rc.TotalOutboundRequests()
		queued := total - outbound

		log.Infof("Outbound Requests: %d, Queued Requests: %d, Total Requests: %d", outbound, queued, total)
	}
}

// LogQueryRequest logs the query that was send to prom/thanos with the time in queue and total time after being sent
func LogQueryRequest(l *golog.Logger, req *http.Request, queueTime time.Duration, sendTime time.Time) {
	if l == nil {
		return
	}
	qp := util.NewQueryParams(req.URL.Query())
	query := qp.Get("query", "<Unknown>")

	totalSendDuration := time.Since(sendTime)
	l.Printf("[Queue: %fs, Outbound: %fs][Query: %s]\n", queueTime.Seconds(), totalSendDuration.Seconds(), query)
}

// TotalRequests returns the total number of requests that are either waiting to be sent and/or
// are currently outbound.
func (rlpc *RateLimitedPrometheusClient) TotalRequests() int32 {
	return rlpc.requests.Get()
}

// TotalOutboundRequests returns the total number of concurrent outbound requests, which have been
// sent to the server and are awaiting response.
func (rlpc *RateLimitedPrometheusClient) TotalOutboundRequests() int32 {
	return rlpc.outbound.Get()
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
	if rlpc.bearerToken != "" {
		token := "Bearer " + rlpc.bearerToken
		req.Header.Add("Authorization", token)
	}

	start := time.Now()

	// Increment the total request counter first
	rlpc.requests.Increment()
	defer rlpc.requests.Decrement()

	// Acquire mutex based on concurrency limiter
	rlpc.limiter.Acquire()
	defer rlpc.limiter.Return()

	timeInQueue := time.Since(start)
	roundTripStart := time.Now()
	defer LogQueryRequest(rlpc.fileLogger, req, timeInQueue, roundTripStart)

	// Increment outbound once mutex acquired
	rlpc.outbound.Increment()
	defer rlpc.outbound.Decrement()

	return rlpc.client.Do(ctx, req)
}
