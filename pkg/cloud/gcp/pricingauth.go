package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"golang.org/x/oauth2/google"
)

// workloadIdentityNodeLabel marks GKE nodes that run the GKE metadata server (Workload Identity).
const workloadIdentityNodeLabel = "iam.gke.io/gke-metadata-server-enabled"

var (
	// defaultCredentialsLookupTimeout keeps the identity lookup from delaying pricing.
	defaultCredentialsLookupTimeout = 2 * time.Second
	// findDefaultCredentials is replaced in tests.
	findDefaultCredentials = google.FindDefaultCredentials
)

// defaultCredentialsDescription reports where Google default credentials come from and which identity they use.
func (gcp *GCP) defaultCredentialsDescription() string {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCredentialsLookupTimeout)
	defer cancel()

	creds, err := findDefaultCredentials(ctx, GCPCloudOAuthScope)
	if err != nil {
		return "source: none found: " + err.Error()
	}
	if creds.JSON != nil {
		return describeCredentialsFile(creds.JSON)
	}
	return fmt.Sprintf("source: metadata server, %s, identity: %s", gcp.workloadIdentityStatus(), gcp.metadataIdentity(ctx))
}

// workloadIdentityStatus reports whether this pod's node runs the GKE metadata server.
func (gcp *GCP) workloadIdentityStatus() string {
	if gcp.Clientset == nil {
		return "Workload Identity: unknown (no cluster cache)"
	}
	podName, _ := os.Hostname()
	return workloadIdentityStatus(podName, os.Getenv("POD_NAMESPACE"), gcp.Clientset.GetAllPods(), gcp.Clientset.GetAllNodes())
}

// workloadIdentityStatus reads the Workload Identity label of the pod's node.
// Without the GKE metadata server, the metadata server hands out the node's service account.
func workloadIdentityStatus(podName, namespace string, pods []*clustercache.Pod, nodes []*clustercache.Node) string {
	nodeName := ""
	for _, p := range pods {
		if p.Name == podName && (namespace == "" || p.Namespace == namespace) {
			nodeName = p.Spec.NodeName
			break
		}
	}
	if nodeName == "" {
		return fmt.Sprintf("Workload Identity: unknown (pod %q not found)", podName)
	}
	for _, n := range nodes {
		if n.Name != nodeName {
			continue
		}
		if n.Labels[workloadIdentityNodeLabel] == "true" {
			return "Workload Identity: enabled on node " + nodeName
		}
		return "Workload Identity: not enabled on node " + nodeName + ", using the node's service account"
	}
	return fmt.Sprintf("Workload Identity: unknown (node %q not found)", nodeName)
}

// describeCredentialsFile reports the file's type and only a service account email, never a user account.
func describeCredentialsFile(b []byte) string {
	var f struct {
		Type        string `json:"type"`
		ClientEmail string `json:"client_email"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return "source: credentials file, type: unknown: " + err.Error()
	}
	if f.ClientEmail == "" {
		return "source: credentials file, type: " + f.Type
	}
	return fmt.Sprintf("source: credentials file, type: %s, identity: %s", f.Type, f.ClientEmail)
}

// metadataIdentity returns the metadata server's service account email, or why it is unknown.
func (gcp *GCP) metadataIdentity(ctx context.Context) string {
	if gcp.MetadataClient == nil {
		return "unknown: no metadata client"
	}
	email, err := gcp.MetadataClient.EmailWithContext(ctx, "default")
	switch {
	case err != nil:
		return "unknown: " + err.Error()
	case email == "":
		return "unknown: empty email"
	}
	return email
}

// reportDefaultCredentials logs the credentials used on success and adds them to the error on failure.
func reportDefaultCredentials(pages map[string]*GCPPricing, err error, describe func() string) (map[string]*GCPPricing, error) {
	auth := describe()
	if err != nil {
		return nil, fmt.Errorf("GCP Billing API: Google default credentials (%s): %w", auth, err)
	}
	log.Infof("GCP Billing API: loaded pricing using Google default credentials (%s)", auth)
	return pages, nil
}
