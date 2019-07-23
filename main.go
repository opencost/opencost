package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"

	"github.com/julienschmidt/httprouter"
	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	costModel "github.com/kubecost/cost-model/costmodel"
	prometheusClient "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	prometheusServerEndpointEnvVar = "PROMETHEUS_SERVER_ENDPOINT"
	prometheusTroubleshootingEp    = "http://docs.kubecost.com/custom-prom#troubleshoot"
)

var (
	// gitCommit is set by the build system
	gitCommit string
)

type Accesses struct {
	PrometheusClient              prometheusClient.Client
	KubeClientSet                 kubernetes.Interface
	Cloud                         costAnalyzerCloud.Provider
	CPUPriceRecorder              *prometheus.GaugeVec
	RAMPriceRecorder              *prometheus.GaugeVec
	PersistentVolumePriceRecorder *prometheus.GaugeVec
	GPUPriceRecorder              *prometheus.GaugeVec
	NodeTotalPriceRecorder        *prometheus.GaugeVec
	RAMAllocationRecorder         *prometheus.GaugeVec
	CPUAllocationRecorder         *prometheus.GaugeVec
	GPUAllocationRecorder         *prometheus.GaugeVec
}

type DataEnvelope struct {
	Code    int         `json:"code"`
	Status  string      `json:"status"`
	Data    interface{} `json:"data"`
	Message string      `json:"message,omitempty"`
}

func wrapData(data interface{}, err error) []byte {
	var resp []byte
	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&DataEnvelope{
			Code:    500,
			Status:  "error",
			Message: err.Error(),
			Data:    data,
		})
	} else {
		resp, _ = json.Marshal(&DataEnvelope{
			Code:   200,
			Status: "success",
			Data:   data,
		})

	}
	return resp
}

// RefreshPricingData needs to be called when a new node joins the fleet, since we cache the relevant subsets of pricing data to avoid storing the whole thing.
func (a *Accesses) RefreshPricingData(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	err := a.Cloud.DownloadPricingData()

	w.Write(wrapData(nil, err))
}

func filterFields(fields string, data map[string]*costModel.CostData) map[string]costModel.CostData {
	fs := strings.Split(fields, ",")
	fmap := make(map[string]bool)
	for _, f := range fs {
		fieldNameLower := strings.ToLower(f) // convert to go struct name by uppercasing first letter
		klog.V(1).Infof("to delete: %s", fieldNameLower)
		fmap[fieldNameLower] = true
	}
	filteredData := make(map[string]costModel.CostData)
	for cname, costdata := range data {
		s := reflect.TypeOf(*costdata)
		val := reflect.ValueOf(*costdata)
		costdata2 := costModel.CostData{}
		cd2 := reflect.New(reflect.Indirect(reflect.ValueOf(costdata2)).Type()).Elem()
		n := s.NumField()
		for i := 0; i < n; i++ {
			field := s.Field(i)
			value := val.Field(i)
			value2 := cd2.Field(i)
			if _, ok := fmap[strings.ToLower(field.Name)]; !ok {
				value2.Set(reflect.Value(value))
			}
		}
		filteredData[cname] = cd2.Interface().(costModel.CostData)
	}
	return filteredData
}

func (a *Accesses) CostDataModel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("timeWindow")
	offset := r.URL.Query().Get("offset")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")

	if offset != "" {
		offset = "offset " + offset
	}

	data, err := costModel.ComputeCostData(a.PrometheusClient, a.KubeClientSet, a.Cloud, window, offset, namespace)
	if fields != "" {
		filteredData := filterFields(fields, data)
		w.Write(wrapData(filteredData, err))
	} else {
		w.Write(wrapData(data, err))
	}
}

func (a *Accesses) ClusterCosts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	if offset != "" {
		offset = "offset " + offset
	}

	data, err := costModel.ClusterCosts(a.PrometheusClient, a.Cloud, window, offset)
	w.Write(wrapData(data, err))
}

func (a *Accesses) ClusterCostsOverTime(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")
	offset := r.URL.Query().Get("offset")

	if offset != "" {
		offset = "offset " + offset
	}

	data, err := costModel.ClusterCostsOverTime(a.PrometheusClient, a.Cloud, start, end, window, offset)
	w.Write(wrapData(data, err))
}

func (a *Accesses) CostDataModelRange(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")
	fields := r.URL.Query().Get("filterFields")
	namespace := r.URL.Query().Get("namespace")

	data, err := costModel.ComputeCostDataRange(a.PrometheusClient, a.KubeClientSet, a.Cloud, start, end, window, namespace)
	if fields != "" {
		filteredData := filterFields(fields, data)
		w.Write(wrapData(filteredData, err))
	} else {
		w.Write(wrapData(data, err))
	}
}

func (a *Accesses) OutofClusterCosts(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	aggregator := r.URL.Query().Get("aggregator")

	data, err := a.Cloud.ExternalAllocations(start, end, aggregator)
	w.Write(wrapData(data, err))
}

func (p *Accesses) GetAllNodePricing(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := p.Cloud.AllNodePricing()
	w.Write(wrapData(data, err))
}

func (p *Accesses) GetConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.GetConfig()
	w.Write(wrapData(data, err))
}

func (p *Accesses) UpdateSpotInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.SpotInfoUpdateType)
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	err = p.Cloud.DownloadPricingData()
	if err != nil {
		klog.V(1).Infof("Error redownloading data on config update: %s", err.Error())
	}
	return
}

func (p *Accesses) UpdateAthenaInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.AthenaInfoUpdateType)
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	return
}

func (p *Accesses) UpdateBigQueryInfoConfigs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, costAnalyzerCloud.BigqueryUpdateType)
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	return
}

func (p *Accesses) UpdateConfigByKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data, err := p.Cloud.UpdateConfig(r.Body, "")
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	return
}

func (p *Accesses) ManagementPlatform(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := p.Cloud.GetManagementPlatform()
	if err != nil {
		w.Write(wrapData(data, err))
		return
	}
	w.Write(wrapData(data, err))
	return
}

func (p *Accesses) ClusterInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := p.Cloud.ClusterInfo()
	w.Write(wrapData(data, err))

}

func Healthz(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

func (p *Accesses) GetPrometheusMetadata(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(wrapData(costModel.ValidatePrometheus(p.PrometheusClient)))
}

func (a *Accesses) recordPrices() {
	go func() {
		for {
			klog.V(3).Info("Recording prices...")
			data, err := costModel.ComputeCostData(a.PrometheusClient, a.KubeClientSet, a.Cloud, "2m", "", "")
			if err != nil {
				klog.V(1).Info("Error in price recording: " + err.Error())
				// zero the for loop so the time.Sleep will still work
				data = map[string]*costModel.CostData{}
			}
			for _, costs := range data {
				nodeName := costs.NodeName
				node := costs.NodeData
				if node == nil {
					klog.V(3).Infof("Skipping Node \"%s\" due to missing Node Data costs", nodeName)
					continue
				}
				cpuCost, _ := strconv.ParseFloat(node.VCPUCost, 64)
				cpu, _ := strconv.ParseFloat(node.VCPU, 64)
				ramCost, _ := strconv.ParseFloat(node.RAMCost, 64)
				ram, _ := strconv.ParseFloat(node.RAMBytes, 64)
				gpu, _ := strconv.ParseFloat(node.GPU, 64)
				gpuCost, _ := strconv.ParseFloat(node.GPUCost, 64)

				totalCost := cpu*cpuCost + ramCost*(ram/1024/1024/1024) + gpu*gpuCost

				if costs.PVCData != nil {
					for _, pvc := range costs.PVCData {
						pvCost, _ := strconv.ParseFloat(pvc.Volume.Cost, 64)
						a.PersistentVolumePriceRecorder.WithLabelValues(pvc.VolumeName, pvc.VolumeName).Set(pvCost)
					}
				}

				a.CPUPriceRecorder.WithLabelValues(nodeName, nodeName).Set(cpuCost)
				a.RAMPriceRecorder.WithLabelValues(nodeName, nodeName).Set(ramCost)
				a.GPUPriceRecorder.WithLabelValues(nodeName, nodeName).Set(gpuCost)

				a.NodeTotalPriceRecorder.WithLabelValues(nodeName, nodeName).Set(totalCost)

				namespace := costs.Namespace
				podName := costs.PodName
				containerName := costs.Name
				if len(costs.RAMAllocation) > 0 {
					a.RAMAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.RAMAllocation[0].Value)
				}
				if len(costs.CPUAllocation) > 0 {
					a.CPUAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.CPUAllocation[0].Value)
				}
				if len(costs.GPUReq) > 0 {
					// allocation here is set to the request because shared GPU usage not yet supported.
					a.GPUAllocationRecorder.WithLabelValues(namespace, podName, containerName, nodeName, nodeName).Set(costs.GPUReq[0].Value)
				}

				storageClasses, _ := a.KubeClientSet.StorageV1().StorageClasses().List(metav1.ListOptions{})

				storageClassMap := make(map[string]map[string]string)
				for _, storageClass := range storageClasses.Items {
					params := storageClass.Parameters
					storageClassMap[storageClass.ObjectMeta.Name] = params
				}

				pvs, _ := a.KubeClientSet.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
				for _, pv := range pvs.Items {
					parameters, ok := storageClassMap[pv.Spec.StorageClassName]
					if !ok {
						klog.V(4).Infof("Unable to find parameters for storage class \"%s\". Does pv \"%s\" have a storageClassName?", pv.Spec.StorageClassName, pv.Name)
					}
					cacPv := &costAnalyzerCloud.PV{
						Class:      pv.Spec.StorageClassName,
						Region:     pv.Labels[v1.LabelZoneRegion],
						Parameters: parameters,
					}
					costModel.GetPVCost(cacPv, &pv, a.Cloud)
					c, _ := strconv.ParseFloat(cacPv.Cost, 64)
					a.PersistentVolumePriceRecorder.WithLabelValues(pv.Name, pv.Name).Set(c)
				}

			}
			time.Sleep(time.Minute)
		}
	}()
}

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	klog.V(1).Infof("Starting cost-model (git commit \"%s\")", gitCommit)

	address := os.Getenv(prometheusServerEndpointEnvVar)
	if address == "" {
		klog.Fatalf("No address for prometheus set in $%s. Aborting.", prometheusServerEndpointEnvVar)
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
	promCli, _ := prometheusClient.NewClient(pc)

	api := prometheusAPI.NewAPI(promCli)
	_, err := api.Config(context.Background())
	if err != nil {
		klog.Fatalf("No valid prometheus config file at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prometheusTroubleshootingEp)
	}
	klog.V(1).Info("Success: retrieved a prometheus config file from: " + address)

	_, err = costModel.ValidatePrometheus(promCli)
	if err != nil {
		klog.Fatalf("Failed to query prometheus at %s. Error: %s . Troubleshooting help available at: %s", address, err.Error(), prometheusTroubleshootingEp)
	}
	klog.V(1).Info("Success: retrieved the 'up' query against prometheus at: " + address)

	// Kubernetes API setup
	kc, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	kubeClientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		panic(err.Error())
	}

	cloudProviderKey := os.Getenv("CLOUD_PROVIDER_API_KEY")
	cloudProvider, err := costAnalyzerCloud.NewProvider(kubeClientset, cloudProviderKey)
	if err != nil {
		panic(err.Error())
	}

	cpuGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_cpu_hourly_cost",
		Help: "node_cpu_hourly_cost hourly cost for each cpu on this node",
	}, []string{"instance", "node"})

	ramGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_ram_hourly_cost",
		Help: "node_ram_hourly_cost hourly cost for each gb of ram on this node",
	}, []string{"instance", "node"})

	gpuGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_gpu_hourly_cost",
		Help: "node_gpu_hourly_cost hourly cost for each gpu on this node",
	}, []string{"instance", "node"})

	totalGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_total_hourly_cost",
		Help: "node_total_hourly_cost Total node cost per hour",
	}, []string{"instance", "node"})

	pvGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pv_hourly_cost",
		Help: "pv_hourly_cost Cost per GB per hour on a persistent disk",
	}, []string{"volumename", "persistentvolume"})

	RAMAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_memory_allocation_bytes",
		Help: "container_memory_allocation_bytes Bytes of RAM used",
	}, []string{"namespace", "pod", "container", "instance", "node"})

	CPUAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_cpu_allocation",
		Help: "container_cpu_allocation Percent of a single CPU used in a minute",
	}, []string{"namespace", "pod", "container", "instance", "node"})

	GPUAllocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "container_gpu_allocation",
		Help: "container_gpu_allocation GPU used",
	}, []string{"namespace", "pod", "container", "instance", "node"})

	prometheus.MustRegister(cpuGv)
	prometheus.MustRegister(ramGv)
	prometheus.MustRegister(gpuGv)
	prometheus.MustRegister(totalGv)
	prometheus.MustRegister(pvGv)
	prometheus.MustRegister(RAMAllocation)
	prometheus.MustRegister(CPUAllocation)

	a := Accesses{
		PrometheusClient:              promCli,
		KubeClientSet:                 kubeClientset,
		Cloud:                         cloudProvider,
		CPUPriceRecorder:              cpuGv,
		RAMPriceRecorder:              ramGv,
		GPUPriceRecorder:              gpuGv,
		NodeTotalPriceRecorder:        totalGv,
		RAMAllocationRecorder:         RAMAllocation,
		CPUAllocationRecorder:         CPUAllocation,
		GPUAllocationRecorder:         GPUAllocation,
		PersistentVolumePriceRecorder: pvGv,
	}

	err = a.Cloud.DownloadPricingData()
	if err != nil {
		klog.V(1).Info("Failed to download pricing data: " + err.Error())
	}

	a.recordPrices()

	router := httprouter.New()
	router.GET("/costDataModel", a.CostDataModel)
	router.GET("/costDataModelRange", a.CostDataModelRange)
	router.GET("/outOfClusterCosts", a.OutofClusterCosts)
	router.GET("/allNodePricing", a.GetAllNodePricing)
	router.GET("/healthz", Healthz)
	router.GET("/getConfigs", a.GetConfigs)
	router.POST("/refreshPricing", a.RefreshPricingData)
	router.POST("/updateSpotInfoConfigs", a.UpdateSpotInfoConfigs)
	router.POST("/updateAthenaInfoConfigs", a.UpdateAthenaInfoConfigs)
	router.POST("/updateBigQueryInfoConfigs", a.UpdateBigQueryInfoConfigs)
	router.POST("/updateConfigByKey", a.UpdateConfigByKey)
	router.GET("/clusterCostsOverTime", a.ClusterCostsOverTime)
	router.GET("/clusterCosts", a.ClusterCosts)
	router.GET("/validatePrometheus", a.GetPrometheusMetadata)
	router.GET("/managementPlatform", a.ManagementPlatform)
	router.GET("/clusterInfo", a.ClusterInfo)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", router)
	rootMux.Handle("/metrics", promhttp.Handler())

	klog.Fatal(http.ListenAndServe(":9003", rootMux))
}
