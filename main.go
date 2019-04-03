package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/julienschmidt/httprouter"
	costAnalyzerCloud "github.com/kubecost/cost-model/cloud"
	costModel "github.com/kubecost/cost-model/costmodel"
	prometheusClient "github.com/prometheus/client_golang/api"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Accesses struct {
	PrometheusClient prometheusClient.Client
	KubeClientSet    *kubernetes.Clientset
	Cloud            costAnalyzerCloud.Provider
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

func main() {
	address := os.Getenv("PROMETHEUS_SERVER_ENDPOINT")
	if address == "" {
		log.Fatal("No address for prometheus set. Aborting.")
	}
	pc := prometheusClient.Config{
		Address: address,
	}
	promCli, _ := prometheusClient.NewClient(pc)

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

	a := Accesses{
		PrometheusClient: promCli,
		KubeClientSet:    kubeClientset,
		Cloud:            cloudProvider,
	}

	err = a.Cloud.DownloadPricingData()
	if err != nil {
		log.Printf("Failed to download pricing data: " + err.Error())
	}

	router := httprouter.New()
	router.GET("/costDataModel", a.CostDataModel)
	router.GET("/costDataModelRange", a.CostDataModelRange)
	router.POST("/refreshPricing", a.RefreshPricingData)

	log.Fatal(http.ListenAndServe(":9003", router))
}
