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
	queue       util.BlockingQueue
	outbound    *util.AtomicInt32
	username    string
	password    string
	bearerToken string
	fileLogger  *golog.Logger
}

// requestCounter is used to determine if the prometheus client keeps track of
// the concurrent outbound requests
type requestCounter interface {
	TotalRequests() int
	TotalOutboundRequests() int
}

// NewRateLimitedClient creates a prometheus client which limits the number of concurrent outbound
// prometheus requests.
func NewRateLimitedClient(config prometheus.Config, maxConcurrency int, username, password, bearerToken string, queryLogFile string) (prometheus.Client, error) {
	c, err := prometheus.NewClient(config)
	if err != nil {
		return nil, err
	}

	queue := util.NewBlockingQueue()
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

	rlpc := &RateLimitedPrometheusClient{
		client:      c,
		queue:       queue,
		outbound:    outbound,
		username:    username,
		password:    password,
		bearerToken: bearerToken,
		fileLogger:  logger,
	}

	// Start concurrent request processing
	for i := 0; i < maxConcurrency; i++ {
		go rlpc.worker()
	}

	return rlpc, nil
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
func LogQueryRequest(l *golog.Logger, req *http.Request, queueTime time.Duration, sendTime time.Duration) {
	if l == nil {
		return
	}
	qp := util.NewQueryParams(req.URL.Query())
	query := qp.Get("query", "<Unknown>")

	l.Printf("[Queue: %fs, Outbound: %fs][Query: %s]\n", queueTime.Seconds(), sendTime.Seconds(), query)
}

// TotalRequests returns the total number of requests that are either waiting to be sent and/or
// are currently outbound.
func (rlpc *RateLimitedPrometheusClient) TotalRequests() int {
	return rlpc.queue.Length()
}

// TotalOutboundRequests returns the total number of concurrent outbound requests, which have been
// sent to the server and are awaiting response.
func (rlpc *RateLimitedPrometheusClient) TotalOutboundRequests() int {
	return int(rlpc.outbound.Get())
}

// Passthrough to the prometheus client API
func (rlpc *RateLimitedPrometheusClient) URL(ep string, args map[string]string) *url.URL {
	return rlpc.client.URL(ep, args)
}

// workRequest is used to queue requests
type workRequest struct {
	ctx      context.Context
	req      *http.Request
	start    time.Time
	respChan chan *workResponse
	// used as a sentinel value to close the worker goroutine
	closer bool
}

// workResponse is the response payload returned to the Do method
type workResponse struct {
	res      *http.Response
	body     []byte
	warnings prometheus.Warnings
	err      error
}

// worker is used as a consumer goroutine to pull workRequest from the blocking queue and execute them
func (rlpc *RateLimitedPrometheusClient) worker() {
	for {
		// blocks until there is an item available
		item := rlpc.queue.Dequeue()

		// Ensure the dequeued item was a workRequest
		if we, ok := item.(*workRequest); ok {
			// if we need to shut down all workers, we'll need to submit sentinel values
			// that will force the worker to return
			if we.closer {
				return
			}

			ctx := we.ctx
			req := we.req

			// measure time in queue
			timeInQueue := time.Since(we.start)

			// Increment outbound counter
			rlpc.outbound.Increment()

			// Execute Request
			roundTripStart := time.Now()
			res, body, warnings, err := rlpc.client.Do(ctx, req)

			// Decrement outbound counter
			rlpc.outbound.Decrement()
			LogQueryRequest(rlpc.fileLogger, req, timeInQueue, time.Since(roundTripStart))

			// Pass back response data over channel to caller
			we.respChan <- &workResponse{
				res:      res,
				body:     body,
				warnings: warnings,
				err:      err,
			}
		}
	}
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

	respChan := make(chan *workResponse)
	defer close(respChan)

	rlpc.queue.Enqueue(&workRequest{
		ctx:      ctx,
		req:      req,
		respChan: respChan,
		closer:   false,
	})

	workRes := <-respChan
	return workRes.res, workRes.body, workRes.warnings, workRes.err
}
