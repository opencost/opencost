package gcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2/google"
)

// fakeMetadataServer serves the default service account email like the GCE/GKE metadata server.
func fakeMetadataServer(t *testing.T, email string, delay time.Duration) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		if r.URL.Path != "/computeMetadata/v1/instance/service-accounts/default/email" || r.Header.Get("Metadata-Flavor") != "Google" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, email)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(srv.URL, "http://"))
}

func stubDefaultCredentials(t *testing.T, creds *google.Credentials, err error) {
	t.Helper()
	old := findDefaultCredentials
	findDefaultCredentials = func(context.Context, ...string) (*google.Credentials, error) { return creds, err }
	t.Cleanup(func() { findDefaultCredentials = old })
}

func TestGCP_describeCredentialsFile(t *testing.T) {
	tests := []struct {
		name string
		json string
		want string
	}{
		{"service account key logs its email", `{"type": "service_account", "client_email": "sa@proj.iam.gserviceaccount.com", "private_key": "x"}`,
			"source: credentials file, type: service_account, identity: sa@proj.iam.gserviceaccount.com"},
		{"user credentials never log the person", `{"type": "authorized_user", "client_id": "id", "refresh_token": "t", "account": "person@example.com"}`,
			"source: credentials file, type: authorized_user"},
		{"external account logs the type only", `{"type": "external_account", "audience": "aud"}`,
			"source: credentials file, type: external_account"},
		{"unreadable file", `not json`, "source: credentials file, type: unknown: "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := describeCredentialsFile([]byte(tt.json))
			assert.True(t, strings.HasPrefix(got, tt.want), "got %q, want %q", got, tt.want)
			assert.NotContains(t, got, "person@example.com")
			assert.NotContains(t, got, "private_key")
		})
	}
}

func TestGCP_defaultCredentialsDescription(t *testing.T) {
	host, _ := os.Hostname()
	t.Setenv("POD_NAMESPACE", "")
	cacheOn := func(wi bool) fakeCache {
		n := &clustercache.Node{Name: "node-1", Labels: map[string]string{}}
		if wi {
			n.Labels[workloadIdentityNodeLabel] = "true"
		}
		return fakeCache{pods: []*clustercache.Pod{{Name: host, Spec: clustercache.PodSpec{NodeName: "node-1"}}}, nodes: []*clustercache.Node{n}}
	}

	t.Run("metadata server on a Workload Identity node", func(t *testing.T) {
		stubDefaultCredentials(t, &google.Credentials{}, nil)
		fakeMetadataServer(t, "opencost@proj.iam.gserviceaccount.com", 0)
		gcp := &GCP{MetadataClient: metadata.NewClient(&http.Client{}), Clientset: cacheOn(true)}
		assert.Equal(t, "source: metadata server, Workload Identity: enabled on node node-1, identity: opencost@proj.iam.gserviceaccount.com", gcp.defaultCredentialsDescription())
	})

	t.Run("metadata server on a node without Workload Identity", func(t *testing.T) {
		stubDefaultCredentials(t, &google.Credentials{}, nil)
		fakeMetadataServer(t, "123-compute@developer.gserviceaccount.com", 0)
		gcp := &GCP{MetadataClient: metadata.NewClient(&http.Client{}), Clientset: cacheOn(false)}
		assert.Equal(t, "source: metadata server, Workload Identity: not enabled on node node-1, using the node's service account, identity: 123-compute@developer.gserviceaccount.com", gcp.defaultCredentialsDescription())
	})

	t.Run("credentials file", func(t *testing.T) {
		stubDefaultCredentials(t, &google.Credentials{JSON: []byte(`{"type": "service_account", "client_email": "sa@proj.iam.gserviceaccount.com"}`)}, nil)
		assert.Equal(t, "source: credentials file, type: service_account, identity: sa@proj.iam.gserviceaccount.com", (&GCP{}).defaultCredentialsDescription())
	})

	t.Run("no credentials", func(t *testing.T) {
		stubDefaultCredentials(t, nil, errors.New("google: could not find default credentials"))
		assert.Equal(t, "source: none found: google: could not find default credentials", (&GCP{}).defaultCredentialsDescription())
	})

	t.Run("no metadata client and no cluster cache", func(t *testing.T) {
		stubDefaultCredentials(t, &google.Credentials{}, nil)
		assert.Equal(t, "source: metadata server, Workload Identity: unknown (no cluster cache), identity: unknown: no metadata client", (&GCP{}).defaultCredentialsDescription())
	})

	t.Run("slow metadata server stays within the timeout", func(t *testing.T) {
		stubDefaultCredentials(t, &google.Credentials{}, nil)
		fakeMetadataServer(t, "x@proj.iam.gserviceaccount.com", 300*time.Millisecond)
		old := defaultCredentialsLookupTimeout
		defaultCredentialsLookupTimeout = 50 * time.Millisecond
		t.Cleanup(func() { defaultCredentialsLookupTimeout = old })

		gcp := &GCP{MetadataClient: metadata.NewClient(&http.Client{})}
		start := time.Now()
		got := gcp.defaultCredentialsDescription()
		assert.Contains(t, got, "identity: unknown: ")
		assert.Less(t, time.Since(start), 250*time.Millisecond)
	})
}

// fakeCache serves only pods and nodes.
type fakeCache struct {
	clustercache.ClusterCache
	pods  []*clustercache.Pod
	nodes []*clustercache.Node
}

func (f fakeCache) GetAllPods() []*clustercache.Pod   { return f.pods }
func (f fakeCache) GetAllNodes() []*clustercache.Node { return f.nodes }

func TestGCP_workloadIdentityStatus(t *testing.T) {
	pod := func(name, ns, node string) *clustercache.Pod {
		return &clustercache.Pod{Name: name, Namespace: ns, Spec: clustercache.PodSpec{NodeName: node}}
	}
	node := func(name string, wi bool) *clustercache.Node {
		n := &clustercache.Node{Name: name, Labels: map[string]string{}}
		if wi {
			n.Labels[workloadIdentityNodeLabel] = "true"
		}
		return n
	}
	nodes := []*clustercache.Node{node("wi-node", true), node("plain-node", false)}

	tests := []struct {
		name, pod, ns string
		pods          []*clustercache.Pod
		want          string
	}{
		{"Workload Identity node", "oc", "ns", []*clustercache.Pod{pod("oc", "ns", "wi-node")}, "Workload Identity: enabled on node wi-node"},
		{"plain node", "oc", "ns", []*clustercache.Pod{pod("oc", "ns", "plain-node")}, "Workload Identity: not enabled on node plain-node, using the node's service account"},
		{"same pod name in another namespace is ignored", "oc", "ns", []*clustercache.Pod{pod("oc", "other", "wi-node"), pod("oc", "ns", "plain-node")}, "Workload Identity: not enabled on node plain-node, using the node's service account"},
		{"namespace unknown matches by name", "oc", "", []*clustercache.Pod{pod("oc", "ns", "wi-node")}, "Workload Identity: enabled on node wi-node"},
		{"pod not in cache", "oc", "ns", nil, `Workload Identity: unknown (pod "oc" not found)`},
		{"node not in cache", "oc", "ns", []*clustercache.Pod{pod("oc", "ns", "gone")}, `Workload Identity: unknown (node "gone" not found)`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, workloadIdentityStatus(tt.pod, tt.ns, tt.pods, nodes))
		})
	}
}

func TestGCP_reportDefaultCredentials(t *testing.T) {
	describe := func() string { return "source: metadata server, identity: opencost@proj.iam.gserviceaccount.com" }

	t.Run("success returns the pages", func(t *testing.T) {
		pages := map[string]*GCPPricing{"k": {}}
		got, err := reportDefaultCredentials(pages, nil, describe)
		assert.NoError(t, err)
		assert.Equal(t, pages, got)
	})

	t.Run("failure names the credentials and keeps the cause", func(t *testing.T) {
		cause := errors.New("401 UNAUTHENTICATED")
		got, err := reportDefaultCredentials(nil, cause, describe)
		assert.Nil(t, got)
		assert.ErrorIs(t, err, cause)
		assert.Contains(t, err.Error(), "Google default credentials (source: metadata server, identity: opencost@proj.iam.gserviceaccount.com)")
	})
}
