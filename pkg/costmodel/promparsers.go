package costmodel

import (
	"fmt"

	costAnalyzerCloud "github.com/kubecost/cost-model/pkg/cloud"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
)

func GetPVInfo(qr interface{}, defaultClusterID string) (map[string]*PersistentVolumeClaimData, error) {
	toReturn := make(map[string]*PersistentVolumeClaimData)

	// TODO: Pass actual query instead of PVInfo
	result, err := prom.NewQueryResults("PVInfo", qr)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result.Results {
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
			log.Debugf("Unfulfilled claim %s: volumename field does not exist in data result vector", pvcName)
			volumeName = ""
		}

		pvClass, err := val.GetString("storageclass")
		if err != nil {
			// TODO: We need to look up the actual PV and PV capacity. For now just proceed with "".
			log.Warningf("Storage Class not found for claim \"%s/%s\".", ns, pvcName)
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

	// TODO: Pass actual query instead of PVAllocationMetrics
	result, err := prom.NewQueryResults("PVAllocationMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result.Results {
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
			log.Warningf("persistentvolume field does not exist for pv %s", pvcName) // This is possible for an unfulfilled claim
			continue
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

	// TODO: Pass actual query instead of PVCostMetrics
	result, err := prom.NewQueryResults("PVCostMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result.Results {
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

	// TODO: Pass actual query instead of NamespaceLabelsMetrics
	result, err := prom.NewQueryResults("NamespaceLabelsMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result.Results {
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
		if nsLabels, ok := toReturn[nsKey]; ok {
			for k, v := range val.GetLabels() {
				nsLabels[k] = v // override with more recently assigned if we changed labels within the window.
			}
		} else {
			toReturn[nsKey] = val.GetLabels()
		}
	}
	return toReturn, nil
}

func GetPodLabelsMetrics(queryResult interface{}, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	// TODO: Pass actual query instead of PodLabelsMetrics
	result, err := prom.NewQueryResults("PodLabelsMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result.Results {
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
		if labels, ok := toReturn[nsKey]; ok {
			newlabels := val.GetLabels()
			for k, v := range newlabels {
				labels[k] = v
			}
		} else {
			toReturn[nsKey] = val.GetLabels()
		}
	}

	return toReturn, nil
}

func GetStatefulsetMatchLabelsMetrics(queryResult interface{}, defaultClusterID string) (map[string]map[string]string, error) {
	toReturn := make(map[string]map[string]string)

	// TODO: Pass actual query instead of StatefulsetMatchLabelsMetrics
	result, err := prom.NewQueryResults("StatefulsetMatchLabelsMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result.Results {
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

	// TODO: Pass actual query instead of PodDaemonsetsWithMetrics
	result, err := prom.NewQueryResults("PodDaemonsetsWithMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}
	for _, val := range result.Results {
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

func GetPodJobsWithMetrics(queryResult interface{}, defaultClusterID string) (map[string]string, error) {
	toReturn := make(map[string]string)

	// TODO: Pass actual query instead of PodJobsWithMetrics
	result, err := prom.NewQueryResults("PodJobsWithMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}
	for _, val := range result.Results {
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

	// TODO: Pass actual query instead of DeploymentMatchLabelsMetrics
	result, err := prom.NewQueryResults("DeploymentMatchLabelsMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result.Results {
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

	// TODO: Pass actual query instead of ServiceSelectorLabelsMetrics
	result, err := prom.NewQueryResults("ServiceSelectorLabelsMetrics", queryResult)
	if err != nil {
		return toReturn, err
	}

	for _, val := range result.Results {
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
