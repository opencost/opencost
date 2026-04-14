package target

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// PodProxyGetter makes requests to pods via the Kubernetes API server's proxy endpoint.
// The K8s API server proxies the request to the target pod on our behalf.
type PodProxyGetter interface {
	Get(ctx context.Context, namespace, podName string, port int, path string) (io.Reader, error)
}

// PodProxyClient implements PodProxyGetter using the Kubernetes API.
type PodProxyClient struct {
	clientset kubernetes.Interface
}

// NewPodProxyClient creates a PodProxyClient from a Kubernetes REST config.
// Returns nil if the client cannot be created.
func NewPodProxyClient(config *rest.Config) *PodProxyClient {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil
	}

	return &PodProxyClient{clientset: clientset}
}

// Get makes a GET request to a pod via the Kubernetes API server's proxy endpoint.
// The API server proxies the request to the specified pod.
func (c *PodProxyClient) Get(ctx context.Context, namespace, podName string, port int, path string) (io.Reader, error) {
	// Normalize path by removing leading slash to avoid double slashes in proxy URL
	normalizedPath := strings.TrimPrefix(path, "/")

	// Build the proxy request
	// Format: /api/v1/namespaces/{namespace}/pods/{pod}:{port}/proxy/{path}
	req := c.clientset.CoreV1().
		RESTClient().
		Get().
		Namespace(namespace).
		Resource("pods").
		SubResource("proxy").
		Name(fmt.Sprintf("%s:%d", podName, port)).
		Suffix(normalizedPath)

	// Execute the request - the K8s API server will proxy it to the pod
	data, err := req.DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("proxy request failed: %w", err)
	}

	return &bytesReader{data: data}, nil
}

// bytesReader wraps a byte slice to implement io.Reader
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// K8sProxyTarget implements automatic fallback from direct HTTP to Kubernetes API server proxy.
// It first tries direct HTTP scraping (optimal), and if that fails, automatically retries
// by requesting the K8s API server to proxy the request to the pod. This handles environments
// where direct pod-to-pod communication is restricted (e.g., OpenShift with OVN-Kubernetes CNI
// and hostNetwork=true).
type K8sProxyTarget struct {
	directTarget *UrlTarget
	proxyGetter  PodProxyGetter
	namespace    string
	podName      string
	port         int
	path         string
}

// NewK8sProxyTarget creates a target that tries direct HTTP first, then K8s API server proxy on failure.
func NewK8sProxyTarget(url string, proxyGetter PodProxyGetter, namespace, podName string, port int, path string) *K8sProxyTarget {
	return &K8sProxyTarget{
		directTarget: NewUrlTarget(url),
		proxyGetter:  proxyGetter,
		namespace:    namespace,
		podName:      podName,
		port:         port,
		path:         path,
	}
}

// Load tries direct HTTP first, then falls back to K8s API server proxy on failure.
func (t *K8sProxyTarget) Load() (io.Reader, error) {
	// Try direct HTTP first (optimal performance)
	reader, err := t.directTarget.Load()
	if err == nil {
		return reader, nil
	}

	// Direct HTTP failed, check if proxy getter is available
	if t.proxyGetter == nil {
		return nil, fmt.Errorf("direct HTTP failed and no proxy getter available: %w", err)
	}

	// Direct HTTP failed, request K8s API server to proxy to the pod
	log.Debugf("Direct HTTP failed for %s/%s, requesting K8s API server to proxy", t.namespace, t.podName)

	// Use timeout to prevent hung requests
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use the proxy getter to make the request via K8s API server
	reader, proxyErr := t.proxyGetter.Get(ctx, t.namespace, t.podName, t.port, t.path)
	if proxyErr != nil {
		return nil, fmt.Errorf("both direct HTTP and K8s API proxy failed - direct: %v, proxy: %v", err, proxyErr)
	}

	log.Debugf("Successfully scraped %s/%s via K8s API server proxy fallback", t.namespace, t.podName)
	return reader, nil
}
