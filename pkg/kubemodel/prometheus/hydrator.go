package prometheus

import (
	"context"
	"fmt"
	"time"

	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/kubemodel"
)

// ============================================================================
// Hydrator Factory
// ============================================================================

// NewBasicHydrator creates a ModelHydrator that populates basic node, namespace, and pod metadata
// from Prometheus labels and annotations.
//
// The hydrator executes queries in parallel for optimal performance and populates:
//   - Nodes with ID, name, cluster, and labels
//   - Namespaces with ID, name, cluster, and labels
//   - Pods with ID, name, namespace reference, labels, and annotations
//
// The clusterID parameter is used as a fallback when cluster information is not
// present in the Prometheus metrics.
func NewBasicHydrator(clusterID string) kubemodel.ModelHydrator {
	return func(ctx context.Context, model *kubemodel.Model, ds source.OpenCostDataSource, start, end time.Time) error {
		// Check if context was cancelled before starting
		if err := ctx.Err(); err != nil {
			return err
		}

		// Step 1: Fetch all required metrics in parallel
		nodeLabels, namespaceLabels, podLabels, podAnnotations, err := fetchMetricsInParallel(ctx, ds, start, end)
		if err != nil {
			return err
		}

		// Step 2: Populate model resources in dependency order
		// Order matters: namespaces must be created before pods can reference them
		populateNodes(model, clusterID, nodeLabels)
		namespaceIndex := populateNamespaces(model, clusterID, namespaceLabels)
		populatePods(model, clusterID, namespaceIndex, podLabels, podAnnotations)

		return nil
	}
}

// fetchMetricsInParallel executes all Prometheus queries concurrently and waits for results.
// Returns an error if any query fails.
func fetchMetricsInParallel(
	ctx context.Context,
	ds source.OpenCostDataSource,
	start, end time.Time,
) ([]*source.NodeLabelsResult, []*source.NamespaceLabelsResult, []*source.PodLabelsResult, []*source.PodAnnotationsResult, error) {
	metrics := ds.Metrics()

	// Create query group for parallel execution
	grp := source.NewQueryGroup()

	// Launch all queries in parallel
	nodeLabelsFuture := source.WithGroup(grp, metrics.QueryNodeLabels(start, end))
	namespaceLabelsFuture := source.WithGroup(grp, metrics.QueryNamespaceLabels(start, end))
	podLabelsFuture := source.WithGroup(grp, metrics.QueryPodLabels(start, end))
	podAnnotationsFuture := source.WithGroup(grp, metrics.QueryPodAnnotations(start, end))

	// Wait for all queries to complete
	nodeLabels, errNodes := nodeLabelsFuture.Await()
	namespaceLabels, errNamespaces := namespaceLabelsFuture.Await()
	podLabels, errPodLabels := podLabelsFuture.Await()
	podAnnotations, errPodAnnotations := podAnnotationsFuture.Await()

	// Check for individual query errors
	if err := firstError(errNodes, errNamespaces, errPodLabels, errPodAnnotations); err != nil {
		return nil, nil, nil, nil, err
	}

	// Check for group-level errors
	if err := grp.Error(); err != nil {
		return nil, nil, nil, nil, err
	}

	return nodeLabels, namespaceLabels, podLabels, podAnnotations, nil
}

// ============================================================================
// Resource Population Functions
// ============================================================================

// populateNodes transforms Prometheus node label results into protobuf Node messages.
// Skips nodes without a valid ID (UID or name).
func populateNodes(model *kubemodel.Model, clusterID string, results []*source.NodeLabelsResult) {
	for _, res := range results {
		// Use UID if available, fallback to node name
		id := nonEmpty(res.UID, res.Node)
		if id == "" {
			// Skip nodes without any identifier
			continue
		}

		// Use cluster from result if available, otherwise use provided clusterID
		cluster := nonEmpty(res.Cluster, clusterID)

		model.Nodes[id] = &kubepb.Node{
			ID:        id,
			ClusterID: cluster,
			Name:      res.Node,
			Labels:    copyStringMap(res.Labels),
		}
	}
}

// populateNamespaces transforms Prometheus namespace label results into protobuf Namespace messages.
// Returns a lookup index mapping "cluster/namespace" keys to namespace UIDs for pod linkage.
func populateNamespaces(model *kubemodel.Model, clusterID string, results []*source.NamespaceLabelsResult) map[string]string {
	// Index maps "cluster/namespace" to UID for fast pod->namespace lookups
	index := make(map[string]string)

	for _, res := range results {
		cluster := nonEmpty(res.Cluster, clusterID)

		// Prefer UID, but generate a synthetic ID if not available
		id := nonEmpty(res.UID, namespaceKey(cluster, res.Namespace))
		if id == "" {
			// Skip namespaces without name
			continue
		}

		model.Namespaces[id] = &kubepb.Namespace{
			ID:        id,
			ClusterID: cluster,
			Name:      res.Namespace,
			Labels:    copyStringMap(res.Labels),
		}

		// Build lookup index for pod population
		key := namespaceKey(cluster, res.Namespace)
		index[key] = id
	}

	return index
}

// populatePods transforms Prometheus pod label and annotation results into protobuf Pod messages.
// This is a three-step process:
//  1. Merge label results into temporary pod records
//  2. Merge annotation results into the same records
//  3. Convert records to protobuf and link to namespaces
//
// The function handles cases where labels and annotations come from separate queries
// and may contain different information for the same pod.
func populatePods(
	model *kubemodel.Model,
	clusterID string,
	namespaces map[string]string,
	labels []*source.PodLabelsResult,
	annotations []*source.PodAnnotationsResult,
) {
	// Step 1: Build intermediate pod records from both data sources
	pods := buildPodRecords(clusterID, labels, annotations)

	// Step 2: Convert records to protobuf and link to namespaces
	for id, rec := range pods {
		nsID := resolveNamespaceID(model, namespaces, rec, clusterID)

		model.Pods[id] = &kubepb.Pod{
			ID:          rec.uid,
			NamespaceID: nsID,
			Name:        rec.name,
			Labels:      copyStringMap(rec.labels),
			Annotations: copyStringMap(rec.annotations),
		}
	}
}

// buildPodRecords merges label and annotation data into intermediate pod records.
// Returns a map of pod UID -> podRecord.
func buildPodRecords(
	clusterID string,
	labels []*source.PodLabelsResult,
	annotations []*source.PodAnnotationsResult,
) map[string]*podRecord {
	pods := make(map[string]*podRecord)

	// First pass: populate from labels
	for _, res := range labels {
		id := nonEmpty(res.UID, res.Pod)
		if id == "" {
			continue
		}

		cluster := nonEmpty(res.Cluster, clusterID)
		rec := getOrCreatePodRecord(pods, id)
		rec.uid = id
		rec.cluster = cluster
		rec.namespace = res.Namespace
		rec.name = res.Pod
		rec.labels = copyStringMap(res.Labels)
	}

	// Second pass: merge annotations (labels take precedence for conflicting fields)
	for _, res := range annotations {
		id := nonEmpty(res.UID, res.Pod)
		if id == "" {
			continue
		}

		rec := getOrCreatePodRecord(pods, id)

		// Only fill in missing fields (labels query takes precedence)
		if rec.cluster == "" {
			rec.cluster = nonEmpty(res.Cluster, clusterID)
		}
		if rec.namespace == "" {
			rec.namespace = res.Namespace
		}
		if rec.name == "" {
			rec.name = res.Pod
		}

		// Merge annotations
		if rec.annotations == nil {
			rec.annotations = copyStringMap(res.Annotations)
		} else {
			for k, v := range res.Annotations {
				rec.annotations[k] = v
			}
		}
	}

	return pods
}

// resolveNamespaceID finds the namespace UID for a pod, creating a synthetic namespace if needed.
// This handles cases where a pod references a namespace that wasn't in the namespace query results.
func resolveNamespaceID(
	model *kubemodel.Model,
	namespaces map[string]string,
	pod *podRecord,
	clusterID string,
) string {
	cluster := nonEmpty(pod.cluster, clusterID)
	nsKey := namespaceKey(cluster, pod.namespace)

	// Try to find existing namespace by cluster/name key
	nsID, found := namespaces[nsKey]
	if found {
		return nsID
	}

	// Namespace not found in index - create a synthetic one
	// Use the key as the ID (format: "cluster/namespace")
	nsID = nsKey

	// Only create if it doesn't already exist in the model
	if _, exists := model.Namespaces[nsID]; !exists {
		model.Namespaces[nsID] = &kubepb.Namespace{
			ID:        nsID,
			ClusterID: cluster,
			Name:      pod.namespace,
		}
	}

	return nsID
}

// ============================================================================
// Helper Functions
// ============================================================================

// firstError returns the first non-nil error from the list, or nil if all are nil.
// Useful for checking multiple errors in sequence.
func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// nonEmpty returns the first non-empty string from the list.
// Returns empty string if all values are empty.
// Useful for providing fallback values.
func nonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// copyStringMap creates a deep copy of a string map.
// Returns nil if the input map is empty.
func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// namespaceKey creates a unique key for a namespace in the format "cluster/namespace".
// Used for lookups when linking pods to namespaces.
func namespaceKey(cluster, namespace string) string {
	return fmt.Sprintf("%s/%s", cluster, namespace)
}

// getOrCreatePodRecord retrieves an existing pod record or creates a new one.
// This enables merging data from multiple sources (labels and annotations).
func getOrCreatePodRecord(pods map[string]*podRecord, id string) *podRecord {
	rec, ok := pods[id]
	if !ok {
		rec = &podRecord{uid: id}
		pods[id] = rec
	}
	return rec
}

// ============================================================================
// Internal Types
// ============================================================================

// podRecord is an intermediate structure for merging pod data from multiple sources
// (labels and annotations) before converting to the final protobuf message.
type podRecord struct {
	uid         string
	cluster     string
	namespace   string
	name        string
	labels      map[string]string
	annotations map[string]string
}
