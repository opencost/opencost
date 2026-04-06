package target

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"k8s.io/client-go/kubernetes"
)

// K8sProxyTarget implements automatic fallback from direct HTTP to Kubernetes API proxy.
// It first tries direct HTTP scraping (optimal), and if that fails, automatically retries
// via K8s API proxy. This handles environments where direct pod-to-pod communication is
// restricted (e.g., OpenShift with OVN-Kubernetes CNI and hostNetwork=true).
type K8sProxyTarget struct {
	directTarget *UrlTarget
	clientset    kubernetes.Interface
	namespace    string
	podName      string
	port         int
	path         string
}

// NewK8sProxyTarget creates a target that tries direct HTTP first, then K8s API proxy on failure.
func NewK8sProxyTarget(url string, clientset kubernetes.Interface, namespace, podName string, port int, path string) *K8sProxyTarget {
	return &K8sProxyTarget{
		directTarget: NewUrlTarget(url),
		clientset:    clientset,
		namespace:    namespace,
		podName:      podName,
		port:         port,
		path:         path,
	}
}

// Load tries direct HTTP first, then falls back to K8s API proxy on failure.
func (t *K8sProxyTarget) Load() (io.Reader, error) {
	// Try direct HTTP first (optimal performance)
	reader, err := t.directTarget.Load()
	if err == nil {
		return reader, nil
	}

	// Direct HTTP failed, try K8s API proxy
	log.Debugf("Direct HTTP failed for %s/%s, trying K8s API proxy", t.namespace, t.podName)

	// Build the proxy request path
	proxyPath := t.path
	if !strings.HasPrefix(proxyPath, "/") {
		proxyPath = "/" + proxyPath
	}

	// Use the Kubernetes client to make a proxy request
	// Format: /api/v1/namespaces/{namespace}/pods/{pod}:{port}/proxy/{path}
	req := t.clientset.CoreV1().
		RESTClient().
		Get().
		Namespace(t.namespace).
		Resource("pods").
		SubResource("proxy").
		Name(fmt.Sprintf("%s:%d", t.podName, t.port)).
		Suffix(proxyPath)

	// Execute the request
	data, proxyErr := req.DoRaw(context.Background())
	if proxyErr != nil {
		return nil, fmt.Errorf("both direct HTTP and K8s proxy failed - direct: %v, proxy: %v", err, proxyErr)
	}

	log.Infof("Successfully scraped %s/%s via K8s API proxy fallback", t.namespace, t.podName)
	return strings.NewReader(string(data)), nil
}
