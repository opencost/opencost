package prom

import (
	"strings"
	"testing"
)

func TestNewOpenCostPrometheusConfigFromEnv_KubeRbacProxyOutsideCluster(t *testing.T) {
	t.Setenv("PROMETHEUS_SERVER_ENDPOINT", "nowhere")
	t.Setenv("KUBE_RBAC_PROXY_ENABLED", "true")
	// Without these, rest.InClusterConfig returns ErrNotInCluster.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	config, err := NewOpenCostPrometheusConfigFromEnv()
	if err == nil {
		t.Fatalf("expected an error outside a cluster, got config %+v", config)
	}
	if !strings.Contains(err.Error(), "KUBE_RBAC_PROXY_ENABLED") {
		t.Errorf("error %q does not mention KUBE_RBAC_PROXY_ENABLED", err)
	}
}
