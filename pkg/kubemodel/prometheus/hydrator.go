package prometheus

import (
	"context"
	"fmt"
	"time"

	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/kubemodel"
)

// NewBasicHydrator creates a ModelHydrator that populates basic node, namespace, and pod metadata
// from Prometheus labels and annotations.
func NewBasicHydrator(clusterID string) kubemodel.ModelHydrator {
	return func(ctx context.Context, model *kubemodel.Model, ds source.OpenCostDataSource, start, end time.Time) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		metrics := ds.Metrics()

		grp := source.NewQueryGroup()
		nodeLabelsFuture := source.WithGroup(grp, metrics.QueryNodeLabels(start, end))
		namespaceLabelsFuture := source.WithGroup(grp, metrics.QueryNamespaceLabels(start, end))
		podLabelsFuture := source.WithGroup(grp, metrics.QueryPodLabels(start, end))
		podAnnotationsFuture := source.WithGroup(grp, metrics.QueryPodAnnotations(start, end))

		nodeLabels, errNodes := nodeLabelsFuture.Await()
		namespaceLabels, errNamespaces := namespaceLabelsFuture.Await()
		podLabels, errPodLabels := podLabelsFuture.Await()
		podAnnotations, errPodAnnotations := podAnnotationsFuture.Await()

		if err := firstError(errNodes, errNamespaces, errPodLabels, errPodAnnotations); err != nil {
			return err
		}

		if err := grp.Error(); err != nil {
			return err
		}

		populateNodes(model, clusterID, nodeLabels)
		namespaceIndex := populateNamespaces(model, clusterID, namespaceLabels)
		populatePods(model, clusterID, namespaceIndex, podLabels, podAnnotations)

		return nil
	}
}

func populateNodes(model *kubemodel.Model, clusterID string, results []*source.NodeLabelsResult) {
	for _, res := range results {
		id := nonEmpty(res.UID, res.Node)
		if id == "" {
			continue
		}

		cluster := nonEmpty(res.Cluster, clusterID)

		model.Nodes[id] = &kubepb.Node{
			ID:        id,
			ClusterID: cluster,
			Name:      res.Node,
			Labels:    copyStringMap(res.Labels),
		}
	}
}

func populateNamespaces(model *kubemodel.Model, clusterID string, results []*source.NamespaceLabelsResult) map[string]string {
	index := make(map[string]string)

	for _, res := range results {
		cluster := nonEmpty(res.Cluster, clusterID)
		id := nonEmpty(res.UID, namespaceKey(cluster, res.Namespace))
		if id == "" {
			continue
		}

		model.Namespaces[id] = &kubepb.Namespace{
			ID:        id,
			ClusterID: cluster,
			Name:      res.Namespace,
			Labels:    copyStringMap(res.Labels),
		}

		key := namespaceKey(cluster, res.Namespace)
		index[key] = id
	}

	return index
}

func populatePods(model *kubemodel.Model, clusterID string, namespaces map[string]string, labels []*source.PodLabelsResult, annotations []*source.PodAnnotationsResult) {
	pods := make(map[string]*podRecord)

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

	for _, res := range annotations {
		id := nonEmpty(res.UID, res.Pod)
		if id == "" {
			continue
		}

		rec := getOrCreatePodRecord(pods, id)
		if rec.cluster == "" {
			rec.cluster = nonEmpty(res.Cluster, clusterID)
		}
		if rec.namespace == "" {
			rec.namespace = res.Namespace
		}
		if rec.name == "" {
			rec.name = res.Pod
		}
		if rec.annotations == nil {
			rec.annotations = copyStringMap(res.Annotations)
		} else {
			for k, v := range res.Annotations {
				rec.annotations[k] = v
			}
		}
	}

	for id, rec := range pods {
		cluster := nonEmpty(rec.cluster, clusterID)
		nsKey := namespaceKey(cluster, rec.namespace)
		nsID, ok := namespaces[nsKey]
		if !ok {
			nsID = nsKey
			if _, exists := model.Namespaces[nsID]; !exists {
				model.Namespaces[nsID] = &kubepb.Namespace{
					ID:        nsID,
					ClusterID: cluster,
					Name:      rec.namespace,
				}
			}
		}

		model.Pods[id] = &kubepb.Pod{
			ID:          rec.uid,
			NamespaceID: nsID,
			Name:        rec.name,
			Labels:      copyStringMap(rec.labels),
			Annotations: copyStringMap(rec.annotations),
		}
	}
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func nonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

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

func namespaceKey(cluster, namespace string) string {
	return fmt.Sprintf("%s/%s", cluster, namespace)
}

func getOrCreatePodRecord(pods map[string]*podRecord, id string) *podRecord {
	rec, ok := pods[id]
	if !ok {
		rec = &podRecord{uid: id}
		pods[id] = rec
	}
	return rec
}

type podRecord struct {
	uid         string
	cluster     string
	namespace   string
	name        string
	labels      map[string]string
	annotations map[string]string
}
