package clustermanager

import (
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"k8s.io/klog"
	"github.com/kubecost/cost-model/pkg/util/json"
)

// DataEnvelope is a generic wrapper struct for http response data
type DataEnvelope struct {
	Code   int         `json:"code"`
	Status string      `json:"status"`
	Data   interface{} `json:"data"`
}

type ClusterManagerEndpoints struct {
	manager *ClusterManager
}

func NewClusterManagerEndpoints(manager *ClusterManager) *ClusterManagerEndpoints {
	return &ClusterManagerEndpoints{
		manager: manager,
	}
}

func (cme *ClusterManagerEndpoints) GetAllClusters(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clusters := cme.manager.GetAll()
	w.Write(wrapData(clusters, nil))
}

func (cme *ClusterManagerEndpoints) PutCluster(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	var clusterDef ClusterDefinition
	err = json.Unmarshal(data, &clusterDef)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	cd, err := cme.manager.AddOrUpdate(clusterDef)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	w.Write(wrapData(cd, nil))
}

func (cme *ClusterManagerEndpoints) DeleteCluster(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clusterID := ps.ByName("id")
	if clusterID == "" {
		w.Write(wrapData(nil, errors.New("Failed to locate cluster with empty id.")))
		return
	}

	err := cme.manager.Remove(clusterID)
	if err != nil {
		w.Write(wrapData(nil, err))
		return
	}

	w.Write(wrapData("success", nil))
}

func wrapData(data interface{}, err error) []byte {
	var resp []byte

	if err != nil {
		klog.V(1).Infof("Error returned to client: %s", err.Error())
		resp, _ = json.Marshal(&DataEnvelope{
			Code:   http.StatusInternalServerError,
			Status: "error",
			Data:   err.Error(),
		})
	} else {
		resp, _ = json.Marshal(&DataEnvelope{
			Code:   http.StatusOK,
			Status: "success",
			Data:   data,
		})

	}

	return resp
}
