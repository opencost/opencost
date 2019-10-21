package costmodel_test

import (
	"context"
	"encoding/json"
	"fmt"

	//	"math"
	//	"net"
	"net/http"
	"strconv"

	//	"testing"
	//	"time"

	//	"gotest.tools/assert"

	prometheusClient "github.com/prometheus/client_golang/api"

	//	v1 "k8s.io/api/core/v1"
	//	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"log"
)

const apiPrefix = "/api/v1"

const epQuery = apiPrefix + "/query"

// The integration test assumes a GKE cluster in us-central-1 or an AWS cluster in us-east-2, with the following instance types
// and storage classes.
var prices = map[string]float64{
	"n1standardRAM":   0.004237,
	"n1standardCPU":   0.031611,
	"t2.medium":       0.0464,
	"t2.small":        0.023,
	"t2.micro":        0.0116,
	"c4.large":        0.1,
	"gp2":             0.000137,
	"ssd":             0.170,
	"Standard_DS2_v2": 0.252,
	"g1smallCPU":      0.025643,
	"g1smallRAM":      0.000034,
	"n1-highmem-2":    0.1171,
}

func parseQuery(qr interface{}) (float64, error) {
	data, ok := qr.(map[string]interface{})["data"]
	if !ok {
		return 0, fmt.Errorf("Improperly formatted response from prometheus, response %+v has no data field", data)
	}
	r, ok := data.(map[string]interface{})["result"]
	if !ok {
		return 0, fmt.Errorf("Improperly formatted data from prometheus, data has no result field")
	}
	results, ok := r.([]interface{})
	if !ok {
		return 0, fmt.Errorf("Improperly formatted results from prometheus, result field is not a slice")
	}
	val, ok := results[0].(map[string]interface{})["value"]
	if !ok {
		return 0, fmt.Errorf("Improperly formatted results from prometheus, value is not a field in the vector")
	}
	dataPoint, ok := val.([]interface{})
	if !ok || len(dataPoint) != 2 {
		return 0, fmt.Errorf("Improperly formatted datapoint from Prometheus")
	}

	return strconv.ParseFloat(dataPoint[1].(string), 64)

}

func query(cli prometheusClient.Client, query string) (interface{}, error) {
	u := cli.URL(epQuery, nil)
	q := u.Query()
	q.Set("query", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	_, body, _, err := cli.Do(context.Background(), req)
	if err != nil {
		return nil, err
	}
	var toReturn interface{}
	err = json.Unmarshal(body, &toReturn)
	if err != nil {
		log.Printf("ERROR" + err.Error())
	}
	return toReturn, err
}

/*
func TestKubernetesPVCosts(t *testing.T) {
	cli, err := getKubernetesClient()
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

	pvs, err := cli.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		panic(err)
	}
	for _, pv := range pvs.Items {
		name := pv.Name
		class := pv.Spec.StorageClassName

		q := fmt.Sprintf(`pv_hourly_cost{persistentvolume="%s"}`, name)
		qt, err := query(promCli, q)
		total, err := parseQuery(qt)
		if err != nil {
			log.Printf(err.Error())
		}
		if price, ok := prices[class]; ok {
			assert.Equal(t, math.Round(total*1000000)/1000000, price)
		}

	}

}

func TestKubernetesClusterCosts(t *testing.T) {
	prices["n1-standard-1"] = math.Round((prices["n1standardCPU"]+3.61219*prices["n1standardRAM"])*10000) / 10000
	prices["g1-small"] = math.Round(((prices["g1smallCPU"] + 0.216998*prices["g1smallRAM"]) * 10000)) / 10000
	cli, err := getKubernetesClient()
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

	nodes, err := cli.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		panic(err)
	}
	for _, n := range nodes.Items {
		name := n.GetObjectMeta().GetName()
		q := fmt.Sprintf(`node_total_hourly_cost{instance="%s"}`, name)
		labels := n.GetObjectMeta().GetLabels()
		instanceType := labels[v1.LabelInstanceType]

		qt, err := query(promCli, q)
		if err != nil {
			panic(err)
		}
		total, err := parseQuery(qt)
		if err != nil {
			panic(err)
		}

		if price, ok := prices[instanceType]; ok {
			assert.Equal(t, math.Round(total*10000)/10000, price)
		}

	}
}
*/
