package prom

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/kubecost/cost-model/pkg/errors"
	prometheus "github.com/prometheus/client_golang/api"
	"k8s.io/klog"
)

const (
	apiPrefix = "/api/v1"
	epQuery   = apiPrefix + "/query"
)

// Context wraps a Prometheus client and provides methods for querying and
// parsing query responses and errors.
type Context struct {
	Client         prometheus.Client
	ErrorCollector *errors.ErrorCollector
}

// NewContext creates a new Promethues querying context from the given client
func NewContext(client prometheus.Client) *Context {
	var ec errors.ErrorCollector

	return &Context{
		Client:         client,
		ErrorCollector: &ec,
	}
}

// Errors returns the errors collected from the Context's ErrorCollector
func (ctx *Context) Errors() []error {
	return ctx.ErrorCollector.Errors()
}

// TODO SetMaxConcurrency

// QueryAll returns one QueryResultsChan for each query provided, then runs
// each query concurrently and returns results on each channel, respectively,
// in the order they were provided; i.e. the response to queries[1] will be
// sent on channel resChs[1].
func (ctx *Context) QueryAll(queries ...string) []QueryResultsChan {
	resChs := []QueryResultsChan{}

	for _, q := range queries {
		resChs = append(resChs, ctx.Query(q))
	}

	return resChs
}

// Query returns a QueryResultsChan, then runs the given query and sends the
// results on the provided channel. Receiver is responsible for closing the
// channel, preferably using the Read method.
func (ctx *Context) Query(query string) QueryResultsChan {
	resCh := make(QueryResultsChan)

	go func(ctx *Context, resCh QueryResultsChan) {
		defer errors.HandlePanic()

		raw, promErr := ctx.query(query)
		ctx.ErrorCollector.Report(promErr)

		results, parseErr := NewQueryResults(query, raw)
		ctx.ErrorCollector.Report(parseErr)

		resCh <- results
	}(ctx, resCh)

	return resCh
}

func (ctx *Context) query(query string) (interface{}, error) {
	u := ctx.Client.URL(epQuery, nil)
	q := u.Query()
	q.Set("query", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, body, warnings, err := ctx.Client.Do(context.Background(), req)
	for _, w := range warnings {
		klog.V(3).Infof("Warning '%s' fetching query '%s'", w, query)
	}
	if err != nil {
		if resp == nil {
			return nil, fmt.Errorf("Error %s fetching query %s", err.Error(), query)
		}

		return nil, fmt.Errorf("%d Error %s fetching query %s", resp.StatusCode, err.Error(), query)
	}
	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		return nil, fmt.Errorf("Error %s fetching query %s", err.Error(), query)
	}
	return toReturn, nil
}
