package env

import "fmt"

func AddK8sApiNoProxy() error {
	// get the k8s api server host
	// https://github.com/kubernetes/kubernetes/blob/691d4c3989f18e0be22c4499d22eff95d516d32b/pkg/kubelet/kubelet_pods.go#L586
	k8sIP := Get("KUBERNETES_SERVICE_HOST", "")
	// get any existing no proxy and append to it
	noProxy := Get("no_proxy", "")
	if noProxy != "" {
		return Set("no_proxy", fmt.Sprintf("%s,%s", noProxy, k8sIP))
	} else {
		return Set("no_proxy", k8sIP)
	}
}
