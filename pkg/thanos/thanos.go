package thanos

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/prom"

	prometheus "github.com/prometheus/client_golang/api"
)

// MaxSourceResulution is the query parameter key used to designate the resolution
// to use when executing a query.
const MaxSourceResulution = "max_source_resolution"

var (
	lock           = new(sync.Mutex)
	enabled        = env.IsThanosEnabled()
	queryUrl       = env.GetThanosQueryUrl()
	offset         = env.GetThanosOffset()
	maxSourceRes   = env.GetThanosMaxSourceResolution()
	offsetDuration *time.Duration
	queryOffset    = fmt.Sprintf(" offset %s", offset)
)

// IsEnabled returns true if Thanos is enabled.
func IsEnabled() bool {
	return enabled
}

// QueryURL returns true if Thanos is enabled.
func QueryURL() string {
	return queryUrl
}

// Offset returns the duration string for the query offset that should be applied to thanos
func Offset() string {
	return offset
}

// OffsetDuration returns the Offset as a parsed duration
func OffsetDuration() time.Duration {
	lock.Lock()
	defer lock.Unlock()

	if offsetDuration == nil {
		d, err := time.ParseDuration(offset)
		if err != nil {
			d = 0
		}

		offsetDuration = &d
	}

	return *offsetDuration
}

// QueryOffset returns a string in the format: " offset %s" substituting in the Offset() string.
func QueryOffset() string {
	return queryOffset
}

func NewThanosClient(address string, config *prom.PrometheusClientConfig) (prometheus.Client, error) {
	tc := prometheus.Config{
		Address: address,
		RoundTripper: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   config.Timeout,
				KeepAlive: config.KeepAlive,
			}).DialContext,
			TLSHandshakeTimeout: config.TLSHandshakeTimeout,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: config.TLSInsecureSkipVerify,
			},
		},
	}

	client, err := prometheus.NewClient(tc)
	if err != nil {
		return nil, err
	}

	// max source resolution decorator
	maxSourceDecorator := func(path string, queryParams url.Values) url.Values {
		if strings.Contains(path, "query") {
			queryParams.Set(MaxSourceResulution, maxSourceRes)
		}
		return queryParams
	}

	return prom.NewRateLimitedClient(
		prom.ThanosClientID,
		client,
		config.QueryConcurrency,
		config.Auth,
		maxSourceDecorator,
		config.RateLimitRetryOpts,
		config.QueryLogFile,
	)
}
