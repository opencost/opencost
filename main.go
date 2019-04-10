package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/julienschmidt/httprouter"
	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	costModel "github.com/kubecost/cost-model/costmodel"
	prometheusClient "github.com/prometheus/client_golang/api"
	prometheusAPI "github.com/prometheus/client_golang/api/prometheus/v1"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Accesses struct {
	PrometheusClient       prometheusClient.Client
	KubeClientSet          *kubernetes.Clientset
	Cloud                  costAnalyzerCloud.Provider
	CPUPriceRecorder       *prometheus.GaugeVec
	RAMPriceRecorder       *prometheus.GaugeVec
	NodeTotalPriceRecorder *prometheus.GaugeVec
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

func (a *Accesses) CostDataModel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	window := r.URL.Query().Get("timeWindow")

	data, err := costModel.ComputeCostData(a.PrometheusClient, a.KubeClientSet, a.Cloud, window)
	w.Write(wrapData(data, err))
}

func (a *Accesses) CostDataModelRange(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	window := r.URL.Query().Get("window")

	data, err := costModel.ComputeCostDataRange(a.PrometheusClient, a.KubeClientSet, a.Cloud, start, end, window)
	w.Write(wrapData(data, err))
}

func Healthz(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(200)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain")
}

func (a *Accesses) recordPrices() {
	go func() {
		for {
			log.Print("Recording prices...")
			data, err := costModel.ComputeCostData(a.PrometheusClient, a.KubeClientSet, a.Cloud, "1h")
			if err != nil {
				log.Printf("Error in price recording: " + err.Error())
				// zero the for loop so the time.Sleep will still work
				data = map[string]*costModel.CostData{}
			}
			for _, costs := range data {
				nodeName := costs.NodeName
				node := costs.NodeData
				if node == nil {
					log.Printf("Skipping Node \"%s\" due to missing Node Data costs", nodeName)
					continue
				}
				cpuCost, _ := strconv.ParseFloat(node.VCPUCost, 64)
				cpu, _ := strconv.ParseFloat(node.VCPU, 64)
				ramCost, _ := strconv.ParseFloat(node.RAMCost, 64)
				ram, _ := strconv.ParseFloat(node.RAMBytes, 64)

				totalCost := cpu*cpuCost + ramCost*(ram/1024/1024/1024)

				a.CPUPriceRecorder.WithLabelValues(nodeName).Set(cpuCost)
				a.RAMPriceRecorder.WithLabelValues(nodeName).Set(ramCost)
				a.NodeTotalPriceRecorder.WithLabelValues(nodeName).Set(totalCost)
			}
			time.Sleep(time.Minute)
		}
	}()
}

func main() {

	address := os.Getenv("PROMETHEUS_SERVER_ENDPOINT")
	if address == "" {
		log.Fatal("No address for prometheus set. Aborting.")
	}

	pc := prometheusClient.Config{
		Address: address,
	}
	promCli, _ := prometheusClient.NewClient(pc)

	api := prometheusAPI.NewAPI(promCli)
	_, err := api.Config(context.Background())
	if err != nil {
		log.Fatal("Failed to use Prometheus at " + address + " Error: " + err.Error())
	}
	log.Printf("Checked prometheus endpoint: " + address)

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
		Help: "node_cpu_hourly_cost cost for each cpu on this node",
	}, []string{"instance"})

	ramGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_ram_hourly_cost",
		Help: "node_ram_hourly_cost cost for each gb of ram on this node",
	}, []string{"instance"})

	totalGv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "node_total_hourly_cost",
		Help: "node_total_hourly_cost Total node cost per hour",
	}, []string{"instance"})

	prometheus.MustRegister(cpuGv)
	prometheus.MustRegister(ramGv)
	prometheus.MustRegister(totalGv)

	a := Accesses{
		PrometheusClient:       promCli,
		KubeClientSet:          kubeClientset,
		Cloud:                  cloudProvider,
		CPUPriceRecorder:       cpuGv,
		RAMPriceRecorder:       ramGv,
		NodeTotalPriceRecorder: totalGv,
	}

	err = a.Cloud.DownloadPricingData()
	if err != nil {
		log.Printf("Failed to download pricing data: " + err.Error())
	}

	a.recordPrices()

	router := httprouter.New()
	router.GET("/costDataModel", a.CostDataModel)
	router.GET("/costDataModelRange", a.CostDataModelRange)
	router.GET("/healthz", Healthz)
	router.POST("/refreshPricing", a.RefreshPricingData)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", router)
	rootMux.Handle("/metrics", promhttp.Handler())

	log.Fatal(http.ListenAndServe(":9003", rootMux))
}
