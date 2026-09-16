package prom

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type testClusterInfoProvider struct{}

func (testClusterInfoProvider) GetClusterInfo() map[string]string {
	return map[string]string{"id": "test-cluster"}
}

func TestPrometheusDataSourceDetectsRangeSemanticsForNonSemverVersions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		probeValue string
		wantOffset bool
	}{
		{name: "left-open range", probeValue: "1", wantOffset: true},
		{name: "inclusive range", probeValue: "2", wantOffset: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/query":
					if err := r.ParseForm(); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					value := "1"
					if r.Form.Get("query") == rangeSemanticsProbe {
						value = tc.probeValue
					}
					_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"kubecost"},"value":[0,%q]}]}}`, value)
				case "/api/v1/status/buildinfo":
					_, _ = fmt.Fprint(w, `{"status":"success","data":{"version":"r409-4d1cd395"}}`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			config := &OpenCostPrometheusConfig{
				ServerEndpoint: server.URL,
				ClientConfig: &PrometheusClientConfig{
					Timeout:             time.Second,
					KeepAlive:           time.Second,
					TLSHandshakeTimeout: time.Second,
					Auth:                DefaultClientAuth(),
					QueryConcurrency:    1,
				},
				ScrapeInterval: time.Hour,
				JobName:        "kubecost",
			}

			datasource, err := NewPrometheusDataSource(testClusterInfoProvider{}, config)
			if err != nil {
				t.Fatal(err)
			}
			if refresher, ok := datasource.ClusterMap().(interface{ StopRefresh() }); ok {
				refresher.StopRefresh()
			}

			if config.IsOffsetResolution != tc.wantOffset {
				t.Errorf("IsOffsetResolution = %t for probe value %s, want %t", config.IsOffsetResolution, tc.probeValue, tc.wantOffset)
			}
		})
	}
}
