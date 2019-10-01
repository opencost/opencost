package costmodel_test

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/kubecost/cost-model/cloud"
	costModel "github.com/kubecost/cost-model/costmodel"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	prometheusClient "github.com/prometheus/client_golang/api"

	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

func getDynamicKubernetesClient() (dynamic.Interface, error) {
	var kubeconfig string

	if home := homeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	} else {
		return nil, fmt.Errorf("Unable to find home directory")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	return dynamic.NewForConfig(config)

}

func TestPodUpDown(t *testing.T) {
	client, err := getDynamicKubernetesClient()
	if err != nil {
		panic(err)
	}
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

	deployment := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name": "demo-deployment",
			},
			"spec": map[string]interface{}{
				"replicas": 2,
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"app": "demo",
					},
				},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "demo",
						},
					},

					"spec": map[string]interface{}{
						"containers": []map[string]interface{}{
							{
								"name":  "web",
								"image": "nginx:1.12",
								"resources": map[string]interface{}{
									"requests": map[string]interface{}{
										"memory": "64Mi",
										"cpu":    "250m",
									},
								},
								"ports": []map[string]interface{}{
									{
										"name":          "http",
										"protocol":      "TCP",
										"containerPort": 80,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	deploymentRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	labels := make(map[string]string)
	labels["testaggregation"] = "foo"
	namespace := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "test",
			Labels: labels,
		},
	}
	rclient.CoreV1().Namespaces().Create(namespace)
	_, err = client.Resource(deploymentRes).Namespace("test").Create(deployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	time.Sleep(5 * time.Minute)

	qr := `label_replace(label_replace(container_cpu_allocation{container='web',namespace='test'}, "container_name", "$1", "container","(.+)"), "pod_name", "$1", "pod","(.+)")`

	end := time.Now()
	start := end.Add(-1 * time.Duration(3*time.Minute))
	step := time.Duration(time.Minute)

	res, err := costModel.QueryRange(promCli, qr, start, end, step)
	if err != nil {
		panic(err)
	}

	vectors, err := costModel.GetContainerMetricVectors(res, false, 0)
	if err != nil {
		panic(err)
	}

	assert.Check(t, len(vectors) > 0)
	for _, values := range vectors {
		assert.Check(t, len(values) > 0)
		for _, vector := range values {
			assert.Check(t, vector.Value == 0.25 || vector.Value == 0.125) // It's halved for fractional minute normalization.
		}
	}

	deletePolicy := metav1.DeletePropagationForeground
	deleteOptions := &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}
	if err := client.Resource(deploymentRes).Namespace("test").Delete("demo-deployment", deleteOptions); err != nil {
		panic(err)
	}

	time.Sleep(5 * time.Minute)

	res, err = costModel.Query(promCli, qr)
	if err != nil {
		panic(err)
	}

	vectors, err = costModel.GetContainerMetricVector(res, false, 0)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, len(vectors), 0)
	provider := &cloud.CustomProvider{
		Clientset: rclient,
	}
	loc, _ := time.LoadLocation("UTC")
	endTime := time.Now().In(loc)
	d, _ := time.ParseDuration("10m")
	startTime := endTime.Add(-1 * d)
	layout := "2006-01-02T15:04:05.000Z"
	startStr := startTime.Format(layout)
	endStr := endTime.Format(layout)
	log.Printf("Starting at %s \n", startStr)
	log.Printf("Ending at %s \n", endStr)
	provider.DownloadPricingData()
	data, err := cm.ComputeCostDataRange(promCli, rclient, provider, startStr, endStr, "1m", "")
	if err != nil {
		panic(err)
	}
	agg := costModel.AggregateCostModel(data, 0.0, "namespace", "")
	_, ok := agg["test"]
	assert.Assert(t, ok)

	data2, err := cm.ComputeCostData(promCli, rclient, provider, "10m", "", "")
	if err != nil {
		panic(err)
	}
	agg2 := costModel.AggregateCostModel(data2, 0.0, "namespace", "")
	_, ok2 := agg2["test"]
	assert.Assert(t, ok2)

	agg3 := costModel.AggregateCostModel(data, 0.0, "label", "testaggregation")
	_, ok3 := agg3["foo"]
	assert.Assert(t, ok3)
}
