package prometheus

import (
	"context"
	"fmt"
	"time"

	modelpb "github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/kubemodel"

	"google.golang.org/protobuf/proto"
)

// MetricsClient captures the subset of Prometheus queries required to hydrate
// the kubemodel protos. This indirection keeps tests lightweight while still
// allowing production code to plug in the full MetricsQuerier.
type MetricsClient interface {
	QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult]
	QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult]
	QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult]
	QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult]
}

// MetricsFromQuerier adapts a source.MetricsQuerier into a MetricsClient.
func MetricsFromQuerier(mq source.MetricsQuerier) MetricsClient {
	return &metricsAdapter{mq: mq}
}

type metricsAdapter struct {
	mq source.MetricsQuerier
}

func (m *metricsAdapter) QueryNodeLabels(start, end time.Time) *source.Future[source.NodeLabelsResult] {
	return m.mq.QueryNodeLabels(start, end)
}

func (m *metricsAdapter) QueryNamespaceLabels(start, end time.Time) *source.Future[source.NamespaceLabelsResult] {
	return m.mq.QueryNamespaceLabels(start, end)
}

func (m *metricsAdapter) QueryPodLabels(start, end time.Time) *source.Future[source.PodLabelsResult] {
	return m.mq.QueryPodLabels(start, end)
}

func (m *metricsAdapter) QueryPodAnnotations(start, end time.Time) *source.Future[source.PodAnnotationsResult] {
	return m.mq.QueryPodAnnotations(start, end)
}

// Config drives how the Prometheus source is initialised.
type Config struct {
	Metrics MetricsClient

	ClusterID   string
	ClusterName string
	Account     string
	Provider    kubepb.Provider
}

// Source implements kubemodel.Source backed by Prometheus queries.
type Source struct {
	metrics MetricsClient

	clusterID   string
	clusterName string
	account     string
	provider    kubepb.Provider
}

// NewSource wires the Prometheus metrics client into a kubemodel Source.
func NewSource(cfg Config) (*Source, error) {
	if cfg.Metrics == nil {
		return nil, fmt.Errorf("prometheus: metrics client must be provided")
	}
	if cfg.ClusterID == "" {
		return nil, fmt.Errorf("prometheus: cluster ID must be provided")
	}
	if cfg.ClusterName == "" {
		return nil, fmt.Errorf("prometheus: cluster name must be provided")
	}

	return &Source{
		metrics:     cfg.Metrics,
		clusterID:   cfg.ClusterID,
		clusterName: cfg.ClusterName,
		account:     cfg.Account,
		provider:    cfg.Provider,
	}, nil
}

// ComputeModel collects cluster metadata, provisioned resources, and allocated
// resources for the provided window.
func (s *Source) ComputeModel(ctx context.Context, window *modelpb.Window) (*kubemodel.Model, error) {
	if window == nil {
		return nil, fmt.Errorf("prometheus: window must be provided")
	}
	if window.GetStart() == nil {
		return nil, fmt.Errorf("prometheus: window start must be provided")
	}

	duration, err := kubemodel.ResolutionToDuration(window.GetResolution())
	if err != nil {
		return nil, err
	}

	start := window.GetStart().AsTime().UTC()
	end := start.Add(duration)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	model := kubemodel.NewModel()
	model.Window = proto.Clone(window).(*modelpb.Window)
	model.Cluster = &kubepb.Cluster{
		ID:       s.clusterID,
		Provider: s.provider,
		Account:  s.account,
		Name:     s.clusterName,
		Window:   proto.Clone(window).(*modelpb.Window),
	}

	grp := source.NewQueryGroup()
	nodeLabelsFuture := source.WithGroup(grp, s.metrics.QueryNodeLabels(start, end))
	namespaceLabelsFuture := source.WithGroup(grp, s.metrics.QueryNamespaceLabels(start, end))
	podLabelsFuture := source.WithGroup(grp, s.metrics.QueryPodLabels(start, end))
	podAnnotationsFuture := source.WithGroup(grp, s.metrics.QueryPodAnnotations(start, end))

	nodeLabels, errNodes := nodeLabelsFuture.Await()
	namespaceLabels, errNamespaces := namespaceLabelsFuture.Await()
	podLabels, errPodLabels := podLabelsFuture.Await()
	podAnnotations, errPodAnnotations := podAnnotationsFuture.Await()

	if err := firstError(errNodes, errNamespaces, errPodLabels, errPodAnnotations); err != nil {
		return nil, err
	}

	if err := grp.Error(); err != nil {
		return nil, err
	}

	s.populateNodes(model, nodeLabels)
	namespaceIndex := s.populateNamespaces(model, namespaceLabels)
	s.populatePods(model, namespaceIndex, podLabels, podAnnotations)

	return model, nil
}

func (s *Source) populateNodes(model *kubemodel.Model, results []*source.NodeLabelsResult) {
	for _, res := range results {
		id := nonEmpty(res.UID, res.Node)
		if id == "" {
			continue
		}

		cluster := nonEmpty(res.Cluster, s.clusterID)

		model.Nodes[id] = &kubepb.Node{
			ID:        id,
			ClusterID: cluster,
			Name:      res.Node,
			Labels:    copyStringMap(res.Labels),
		}
	}
}

func (s *Source) populateNamespaces(model *kubemodel.Model, results []*source.NamespaceLabelsResult) map[string]string {
	index := make(map[string]string)

	for _, res := range results {
		cluster := nonEmpty(res.Cluster, s.clusterID)
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

func (s *Source) populatePods(model *kubemodel.Model, namespaces map[string]string, labels []*source.PodLabelsResult, annotations []*source.PodAnnotationsResult) {
	pods := make(map[string]*podRecord)

	for _, res := range labels {
		id := nonEmpty(res.UID, res.Pod)
		if id == "" {
			continue
		}

		cluster := nonEmpty(res.Cluster, s.clusterID)
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
			rec.cluster = nonEmpty(res.Cluster, s.clusterID)
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
		cluster := nonEmpty(rec.cluster, s.clusterID)
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

// Ensure we satisfy the kubemodel.Source interface at compile time.
var _ kubemodel.Source = (*Source)(nil)
