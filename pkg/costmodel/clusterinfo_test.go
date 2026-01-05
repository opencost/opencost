package costmodel
import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/promutil"
)

func TestClusterInfoLabels(t *testing.T) {
	expected := map[string]bool{"clusterprofile": true, "errorreporting": true, "id": true, "logcollection": true, "name": true, "productanalytics": true, "provider": true, "provisioner": true, "remotereadenabled": true, "thanosenabled": true, "valuesreporting": true, "version": true}
	clusterInfo := `{"clusterProfile":"production","errorReporting":"true","id":"cluster-one","logCollection":"true","name":"bolt-3","productAnalytics":"true","provider":"GCP","provisioner":"GKE","remoteReadEnabled":"false","thanosEnabled":"false","valuesReporting":"true","version":"1.14+"}`

	var m map[string]interface{}
	err := json.Unmarshal([]byte(clusterInfo), &m)
	if err != nil {
		t.Errorf("Error: %s", err)
		return
	}

	labels := promutil.MapToLabels(m)
	for k := range expected {
		if _, ok := labels[k]; !ok {
			t.Errorf("Failed to locate key: \"%s\" in labels.", k)
			return
		}
	}
}

func TestWriteReportingFlags(t *testing.T) {
	clusterInfo := make(map[string]string)
	writeReportingFlags(clusterInfo)

	expectedKeys := []string{
		clusters.ClusterInfoLogCollectionKey,
		clusters.ClusterInfoProductAnalyticsKey,
		clusters.ClusterInfoErrorReportingKey,
		clusters.ClusterInfoValuesReportingKey,
	}

	for _, key := range expectedKeys {
		if _, ok := clusterInfo[key]; !ok {
			t.Errorf("Missing key: %s", key)
		}
	}
}

func TestWriteClusterProfile(t *testing.T) {
	clusterInfo := make(map[string]string)
	writeClusterProfile(clusterInfo)

	if _, ok := clusterInfo[clusters.ClusterInfoProfileKey]; !ok {
		t.Errorf("Expected profile key %s to be present", clusters.ClusterInfoProfileKey)
	}
}
