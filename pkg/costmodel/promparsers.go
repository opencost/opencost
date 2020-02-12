package costmodel

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	costAnalyzerCloud "github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/util"
	"k8s.io/klog"
)

// PromQueryResult contains a single result from a prometheus query
type PromQueryResult struct {
	Metric map[string]interface{}
	Values []*util.Vector
}

func (pqr *PromQueryResult) GetString(field string) (string, error) {
	f, ok := pqr.Metric[field]
	if !ok {
		return "", fmt.Errorf("%s field does not exist in data result vector", field)
	}

	strField, ok := f.(string)
	if !ok {
		return "", fmt.Errorf("%s field is improperly formatted", field)
	}

	return strField, nil
}

func (pqr *PromQueryResult) GetLabels() map[string]string {
	result := make(map[string]string)

	// Find All keys with prefix label_, remove prefix, add to labels
	for k, v := range pqr.Metric {
		if !strings.HasPrefix(k, "label_") {
			continue
		}

		label := k[6:]
		value, ok := v.(string)
		if !ok {
			klog.V(3).Infof("Failed to parse label value for label: %s", label)
			continue
		}

		result[label] = value
	}

	return result
}

// NewQueryResults accepts the raw prometheus query result and returns an array of
// PromQueryResult objects
func NewQueryResults(queryResult interface{}) ([]*PromQueryResult, error) {
	var result []*PromQueryResult

	data, ok := queryResult.(map[string]interface{})["data"]
	if !ok {
		e, err := wrapPrometheusError(queryResult)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(e)
	}

	// Deep Check for proper formatting
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Data field improperly formatted in prometheus repsonse")
	}
	resultData, ok := d["result"]
	if !ok {
		return nil, fmt.Errorf("Result field not present in prometheus response")
	}
	resultsData, ok := resultData.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Result field improperly formatted in prometheus response")
	}

	// Scan Results
	for _, val := range resultsData {
		resultInterface, ok := val.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Result is improperly formatted")
		}

		metricInterface, ok := resultInterface["metric"]
		if !ok {
			return nil, fmt.Errorf("Metric field does not exist in data result vector")
		}
		metricMap, ok := metricInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Metric field is improperly formatted")
		}

		// Wrap execution of this lazily in case the data is not used
		labels := func() string { return labelsForMetric(metricMap) }

		// Determine if the result is a ranged data set or single value
		_, isRange := resultInterface["values"]

		var vectors []*util.Vector
		if !isRange {
			dataPoint, ok := resultInterface["value"]
			if !ok {
				return nil, fmt.Errorf("Value field does not exist in data result vector")
			}

			v, err := parseDataPoint(dataPoint, labels)
			if err != nil {
				return nil, err
			}
			vectors = append(vectors, v)
		} else {
			values, ok := resultInterface["values"].([]interface{})
			if !ok {
				return nil, fmt.Errorf("Values field is improperly formatted")
			}

			for _, value := range values {
				v, err := parseDataPoint(value, labels)
				if err != nil {
					return nil, err
				}

				vectors = append(vectors, v)
			}
		}

		result = append(result, &PromQueryResult{
			Metric: metricMap,
			Values: vectors,
		})
	}

	return result, nil
}

func parseDataPoint(dataPoint interface{}, labels func() string) (*util.Vector, error) {
	value, ok := dataPoint.([]interface{})
	if !ok || len(value) != 2 {
		return nil, fmt.Errorf("Improperly formatted datapoint from Prometheus")
	}

	strVal := value[1].(string)
	v, err := strconv.ParseFloat(strVal, 64)
	if err != nil {
		return nil, err
	}

	// Test for +Inf and -Inf (sign: 0), Test for NaN
	if math.IsInf(v, 0) {
		klog.V(1).Infof("[Warning] Found Inf value parsing vector data point for metric: %s", labels())
		v = 0.0
	} else if math.IsNaN(v) {
		klog.V(1).Infof("[Warning] Found NaN value parsing vector data point for metric: %s", labels())
		v = 0.0
	}

	return &util.Vector{
		Timestamp: math.Round(value[0].(float64)/10) * 10,
		Value:     v,
	}, nil
}

func GetPVInfo(qr interface{}, defaultClusterID string) (map[string]*PersistentVolumeClaimData, error) {
	toReturn := make(map[string]*PersistentVolumeClaimData)
	result, err := NewQueryResults(qr)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		pvcName, err := val.GetString("persistentvolumeclaim")
		if err != nil {
			return toReturn, err
		}

		volumeName, err := val.GetString("volumename")
		if err != nil {
			klog.V(4).Infof("[Warning] Unfulfilled claim %s: volumename field does not exist in data result vector", pvcName)
			volumeName = ""
		}

		pvClass, err := val.GetString("storageclass")
		if err != nil {
			// TODO: We need to look up the actual PV and PV capacity. For now just proceed with "".
			klog.V(2).Infof("[Warning] Storage Class not found for claim \"%s/%s\".", ns, pvcName)
			pvClass = ""
		}

		key := fmt.Sprintf("%s,%s,%s", ns, pvcName, clusterID)
		toReturn[key] = &PersistentVolumeClaimData{
			Class:      pvClass,
			Claim:      pvcName,
			Namespace:  ns,
			ClusterID:  clusterID,
			VolumeName: volumeName,
			Values:     val.Values,
		}
	}

	return toReturn, nil
}

func GetPVAllocationMetrics(queryResult interface{}, defaultClusterID string) (map[string][]*PersistentVolumeClaimData, error) {
	toReturn := make(map[string][]*PersistentVolumeClaimData)
	result, err := NewQueryResults(queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		pod, err := val.GetString("pod")
		if err != nil {
			return toReturn, err
		}

		pvcName, err := val.GetString("persistentvolumeclaim")
		if err != nil {
			return toReturn, err
		}

		pvName, err := val.GetString("persistentvolume")
		if err != nil {
			return toReturn, err
		}

		key := fmt.Sprintf("%s,%s,%s", ns, pod, clusterID)
		pvcData := &PersistentVolumeClaimData{
			Class:      "",
			Claim:      pvcName,
			Namespace:  ns,
			ClusterID:  clusterID,
			VolumeName: pvName,
			Values:     val.Values,
		}

		toReturn[key] = append(toReturn[key], pvcData)
	}

	return toReturn, nil
}

func GetPVCostMetrics(queryResult interface{}, defaultClusterID string) (map[string]*costAnalyzerCloud.PV, error) {
	toReturn := make(map[string]*costAnalyzerCloud.PV)
	result, err := NewQueryResults(queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		volumeName, err := val.GetString("volumename")
		if err != nil {
			return toReturn, err
		}

		key := fmt.Sprintf("%s,%s", volumeName, clusterID)
		toReturn[key] = &costAnalyzerCloud.PV{
			Cost: fmt.Sprintf("%f", val.Values[0].Value),
		}
	}

	return toReturn, nil
}

func GetNamespaceLabelsMetrics(queryResult interface{}, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)
	result, err := NewQueryResults(queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		// We want Namespace and ClusterID for key generation purposes
		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + clusterID
		toReturn[nsKey] = val.GetLabels()
	}

	return toReturn, nil
}

func GetPodLabelsMetrics(queryResult interface{}, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)
	result, err := NewQueryResults(queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		// We want Pod, Namespace and ClusterID for key generation purposes
		pod, err := val.GetString("pod")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + pod + "," + clusterID
		toReturn[nsKey] = val.GetLabels()
	}

	return toReturn, nil
}

func GetStatefulsetMatchLabelsMetrics(queryResult interface{}, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)
	result, err := NewQueryResults(queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		// We want Statefulset, Namespace and ClusterID for key generation purposes
		ss, err := val.GetString("statefulSet")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + ss + "," + clusterID
		toReturn[nsKey] = val.GetLabels()
	}

	return toReturn, nil
}

func GetPodDaemonsetsWithMetrics(queryResult interface{}, defaultClusterID string) (map[string]string, error) {
	toReturn := make(map[string]string)
	result, err := NewQueryResults(queryResult)
	if err != nil {
		return toReturn, err
	}
	for _, val := range result {
		ds, err := val.GetString("owner_name")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		pod, err := val.GetString("pod")
		if err != nil {
			return toReturn, err
		}

		nsKey := ns + "," + pod + "," + clusterID
		toReturn[nsKey] = ds
	}

	return toReturn, nil
}

func GetDeploymentMatchLabelsMetrics(queryResult interface{}, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)
	result, err := NewQueryResults(queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		// We want Deployment, Namespace and ClusterID for key generation purposes
		deployment, err := val.GetString("deployment")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + deployment + "," + clusterID
		toReturn[nsKey] = val.GetLabels()
	}

	return toReturn, nil
}

func GetServiceSelectorLabelsMetrics(queryResult interface{}, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)
	result, err := NewQueryResults(queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result {
		// We want Service, Namespace and ClusterID for key generation purposes
		service, err := val.GetString("service")
		if err != nil {
			return toReturn, err
		}

		ns, err := val.GetString("namespace")
		if err != nil {
			return toReturn, err
		}

		clusterID, err := val.GetString("cluster_id")
		if clusterID == "" {
			clusterID = defaultClusterID
		}

		nsKey := ns + "," + service + "," + clusterID
		toReturn[nsKey] = val.GetLabels()
	}

	return toReturn, nil
}

func labelsForMetric(metricMap map[string]interface{}) string {
	var pairs []string
	for k, v := range metricMap {
		pairs = append(pairs, fmt.Sprintf("%s: %+v", k, v))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ", "))
}
