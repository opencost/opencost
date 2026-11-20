package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
)

func testMetadataClient(t *testing.T) *metadata.Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "test-project")
	}))
	t.Cleanup(server.Close)

	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(server.URL, "http://"))
	return metadata.NewClient(server.Client())
}

func gcpWithComputeHandler(t *testing.T, handler http.HandlerFunc) *GCP {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	svc, err := compute.NewService(context.Background(),
		option.WithEndpoint(server.URL),
		option.WithoutAuthentication(),
		option.WithHTTPClient(server.Client()))
	require.NoError(t, err)

	return &GCP{computeService: svc, MetadataClient: testMetadataClient(t)}
}

// gcpFromDiskPages serves pages in order, failing any request that does not
// carry the previous page's token. A nil page is served as an error.
func gcpFromDiskPages(t *testing.T, pages ...*compute.DiskAggregatedList) (*GCP, *atomic.Int64) {
	t.Helper()

	var requests atomic.Int64
	gcp := gcpWithComputeHandler(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(requests.Add(1)) - 1
		if n >= len(pages) || pages[n] == nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		want := ""
		if n > 0 {
			want = pages[n-1].NextPageToken
		}
		if got := r.URL.Query().Get("pageToken"); got != want {
			http.Error(w, fmt.Sprintf("pageToken %q, want %q", got, want), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(pages[n]))
	})
	return gcp, &requests
}

func scopedDisks(names ...string) compute.DisksScopedList {
	var scoped compute.DisksScopedList
	for _, name := range names {
		scoped.Disks = append(scoped.Disks, &compute.Disk{Name: name, SizeGb: 100})
	}
	return scoped
}

func noResults() compute.DisksScopedList {
	return compute.DisksScopedList{Warning: &compute.DisksScopedListWarning{Code: "NO_RESULTS_ON_PAGE", Message: "no results"}}
}

func diskNames(list *compute.DiskAggregatedList, scope string) []string {
	var names []string
	for _, d := range list.Items[scope].Disks {
		names = append(names, d.Name)
	}
	return names
}

func TestGCP_getAllDisks_FollowsEveryPage(t *testing.T) {
	gcp, requests := gcpFromDiskPages(t,
		&compute.DiskAggregatedList{Items: map[string]compute.DisksScopedList{"zones/us-west1-a": scopedDisks("disk-1", "disk-2")}, NextPageToken: "page2"},
		&compute.DiskAggregatedList{Items: map[string]compute.DisksScopedList{"zones/us-west1-b": scopedDisks("disk-3")}, NextPageToken: "page3"},
		&compute.DiskAggregatedList{Items: map[string]compute.DisksScopedList{"zones/us-west1-a": scopedDisks("disk-4")}},
	)

	list, err := gcp.getAllDisks(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(3), requests.Load(), "every page token must be followed")

	assert.ElementsMatch(t, []string{"disk-1", "disk-2", "disk-4"}, diskNames(list, "zones/us-west1-a"))
	assert.ElementsMatch(t, []string{"disk-3"}, diskNames(list, "zones/us-west1-b"))
}

func TestGCP_getAllDisks_LaterPageErrorIsReported(t *testing.T) {
	gcp, _ := gcpFromDiskPages(t,
		&compute.DiskAggregatedList{Items: map[string]compute.DisksScopedList{"zones/us-west1-a": scopedDisks("disk-1")}, NextPageToken: "page2"},
		nil,
	)

	list, err := gcp.getAllDisks(context.Background())
	assert.ErrorContains(t, err, "listing GCP disks", "a partial listing must not be returned as a complete one")
	assert.Nil(t, list)
}

func TestGCP_getAllDisks_RespectsDeadline(t *testing.T) {
	gcp := gcpWithComputeHandler(t, func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := gcp.getAllDisks(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "listing GCP disks")
	assert.Less(t, time.Since(started), 5*time.Second, "the listing must give up when its context does")
}

func TestGCP_getAllDisks_PreservesResponseMetadata(t *testing.T) {
	gcp, _ := gcpFromDiskPages(t,
		&compute.DiskAggregatedList{
			Kind: "compute#diskAggregatedList",
			Id:   "project/disks",
			Items: map[string]compute.DisksScopedList{
				"zones/us-west1-a": scopedDisks("disk-1"),
				"zones/us-west1-c": noResults(),
			},
			Warning:       &compute.DiskAggregatedListWarning{Code: "PARTIAL_SUCCESS", Message: "some scopes failed"},
			Unreachables:  []string{"zones/us-west1-e", "zones/us-west1-e"},
			NextPageToken: "page2",
		},
		&compute.DiskAggregatedList{
			Kind:         "compute#diskAggregatedList",
			Id:           "project/disks",
			Items:        map[string]compute.DisksScopedList{"zones/us-west1-b": scopedDisks("disk-2")},
			SelfLink:     "https://example/disks",
			Unreachables: []string{"zones/us-west1-d"},
		},
	)

	list, err := gcp.getAllDisks(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "compute#diskAggregatedList", list.Kind)
	assert.Equal(t, "https://example/disks", list.SelfLink)
	require.NotNil(t, list.Warning)
	assert.Equal(t, "PARTIAL_SUCCESS", list.Warning.Code)
	assert.ElementsMatch(t, []string{"zones/us-west1-d", "zones/us-west1-e"}, list.Unreachables)

	warned, ok := list.Items["zones/us-west1-c"]
	require.True(t, ok, "a scope with only a warning must survive the merge")
	require.NotNil(t, warned.Warning)
	assert.Equal(t, "NO_RESULTS_ON_PAGE", warned.Warning.Code)

	assert.ElementsMatch(t, []string{"disk-1"}, diskNames(list, "zones/us-west1-a"))
	assert.ElementsMatch(t, []string{"disk-2"}, diskNames(list, "zones/us-west1-b"))

	assert.Empty(t, list.NextPageToken)
}

func TestGCP_getAllDisks_DropsNoResultsWarningOnceDisksArrive(t *testing.T) {
	tests := []struct {
		name         string
		first, again compute.DisksScopedList
	}{
		{name: "warning then disks", first: noResults(), again: scopedDisks("disk-1")},
		{name: "disks then warning", first: scopedDisks("disk-1"), again: noResults()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gcp, _ := gcpFromDiskPages(t,
				&compute.DiskAggregatedList{Items: map[string]compute.DisksScopedList{"zones/us-west1-a": tt.first}, NextPageToken: "page2"},
				&compute.DiskAggregatedList{Items: map[string]compute.DisksScopedList{"zones/us-west1-a": tt.again}},
			)

			list, err := gcp.getAllDisks(context.Background())
			require.NoError(t, err)

			assert.ElementsMatch(t, []string{"disk-1"}, diskNames(list, "zones/us-west1-a"))
			assert.Nil(t, list.Items["zones/us-west1-a"].Warning)
		})
	}
}
