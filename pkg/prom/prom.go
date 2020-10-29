package prom

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util"

	golog "log"

	prometheus "github.com/prometheus/client_golang/api"
)

//--------------------------------------------------------------------------
//  QueryParamsDecorator
//--------------------------------------------------------------------------

// QueryParamsDecorator is used to decorate and return query parameters for
// outgoing requests
type QueryParamsDecorator = func(path string, values url.Values) url.Values

//--------------------------------------------------------------------------
//  ClientAuth
//--------------------------------------------------------------------------

// ClientAuth is used to authenticate outgoing client requests.
type ClientAuth struct {
	Username    string
	Password    string
	BearerToken string
}

// Apply Applies the authentication data to the request headers
func (auth *ClientAuth) Apply(req *http.Request) {
	if auth == nil {
		return
	}

	if auth.Username != "" {
		req.SetBasicAuth(auth.Username, auth.Password)
	}

	if auth.BearerToken != "" {
		token := "Bearer " + auth.BearerToken
		req.Header.Add("Authorization", token)
	}
}

//--------------------------------------------------------------------------
//  RateLimitedPrometheusClient
//--------------------------------------------------------------------------

// RateLimitedPrometheusClient is a prometheus client which limits the total number of
// concurrent outbound requests allowed at a given moment.
type RateLimitedPrometheusClient struct {
	id         string
	client     prometheus.Client
	auth       *ClientAuth
	queue      util.BlockingQueue
	decorator  QueryParamsDecorator
	outbound   *util.AtomicInt32
	fileLogger *golog.Logger
}

// requestCounter is used to determine if the prometheus client keeps track of
// the concurrent outbound requests
type requestCounter interface {
	TotalRequests() int
	TotalOutboundRequests() int
}

// NewRateLimitedClient creates a prometheus client which limits the number of concurrent outbound
// prometheus requests.
func NewRateLimitedClient(id string, config prometheus.Config, maxConcurrency int, auth *ClientAuth, decorator QueryParamsDecorator, queryLogFile string) (prometheus.Client, error) {
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
		id:         id,
		client:     c,
		queue:      queue,
		decorator:  decorator,
		outbound:   outbound,
		auth:       auth,
		fileLogger: logger,
	}

	// Start concurrent request processing
	for i := 0; i < maxConcurrency; i++ {
		go rlpc.worker()
	}

	return rlpc, nil
}

// ID is used to identify the type of client
func (rlpc *RateLimitedPrometheusClient) ID() string {
	return rlpc.id
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

			// decorate the raw query parameters
			if rlpc.decorator != nil {
				req.URL.RawQuery = rlpc.decorator(req.URL.Path, req.URL.Query()).Encode()
			}

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
	rlpc.auth.Apply(req)

	respChan := make(chan *workResponse)
	defer close(respChan)

	rlpc.queue.Enqueue(&workRequest{
		ctx:      ctx,
		req:      req,
		start:    time.Now(),
		respChan: respChan,
		closer:   false,
	})

	workRes := <-respChan
	return workRes.res, workRes.body, workRes.warnings, workRes.err
}

//--------------------------------------------------------------------------
//  Client Helpers
//--------------------------------------------------------------------------

func NewPrometheusClient(address string, timeout, keepAlive time.Duration, queryConcurrency int, queryLogFile string) (prometheus.Client, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: env.GetInsecureSkipVerify()}

	// may be necessary for long prometheus queries. TODO: make this configurable
	pc := prometheus.Config{
		Address: address,
		RoundTripper: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: keepAlive,
			}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			TLSClientConfig:     tlsConfig,
		},
	}

	auth := &ClientAuth{
		Username:    env.GetDBBasicAuthUsername(),
		Password:    env.GetDBBasicAuthUserPassword(),
		BearerToken: env.GetDBBearerToken(),
	}

	return NewRateLimitedClient(PrometheusClientID, pc, queryConcurrency, auth, nil, queryLogFile)
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
