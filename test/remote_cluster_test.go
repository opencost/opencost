package costmodel_test

import (
	"log"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/kubecost/cost-model/cloud"
	costModel "github.com/kubecost/cost-model/costmodel"
	"gotest.tools/assert"

	prometheusClient "github.com/prometheus/client_golang/api"

	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

func TestClusterConvergence(t *testing.T) {
	rclient, err := getKubernetesClient()
	if err != nil {
		panic(err)
	}
	var LongTimeoutRoundTripper http.RoundTripper = &http.Transport{ // may be necessary for long prometheus queries. TODO: make this configurable
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   120 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	pc := prometheusClient.Config{
		Address:      address,
		RoundTripper: LongTimeoutRoundTripper,
	}
	promCli, err := prometheusClient.NewClient(pc)
	if err != nil {
		panic(err)
	}
	cm := costModel.NewCostModel(rclient)

	provider := &cloud.CustomProvider{
		Clientset: rclient,
	}
	loc, _ := time.LoadLocation("UTC")
	endTime := time.Now().In(loc)
	d, _ := time.ParseDuration("24h")
	startTime := endTime.Add(-1 * d)
	layout := "2006-01-02T15:04:05.000Z"
	startStr := startTime.Format(layout)
	endStr := endTime.Format(layout)
	log.Printf("Starting at %s \n", startStr)
	log.Printf("Ending at %s \n", endStr)
	provider.DownloadPricingData()

	data, err := cm.ComputeCostDataRange(promCli, rclient, provider, startStr, endStr, "1h", "", false)
	if err != nil {
		panic(err)
	}

	os.Setenv("SQL_ADDRESS", "ab5cfc235d64e11e9b8280265f54018f-778641917.us-east-2.elb.amazonaws.com")
	os.Setenv("REMOTE_WRITE_PASSWORD", "savemoney123")

	data2, err := cm.ComputeCostDataRange(promCli, rclient, provider, startStr, endStr, "1h", "", true)
	if err != nil {
		panic(err)
	}

	agg := costModel.AggregateCostModel(data, 0.0, 1.0, nil, "namespace", "")
	agg2 := costModel.AggregateCostModel(data2, 0.0, 1.0, nil, "namespace", "")

	assert.Equal(t, agg["kubecost"].TotalCost, agg2["kubecost"].TotalCost)

}
