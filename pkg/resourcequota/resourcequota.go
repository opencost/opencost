package resourcequota

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/log"
	proto "github.com/opencost/opencost/core/pkg/protocol"

	"github.com/opencost/opencost/pkg/env"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var protocol = proto.HTTP()

type QuotaTracker struct {
	clientset *kubernetes.Clientset
}

type QuotaMetrics struct {
	ClusterID    string            `json:"clusterID"`
	Namespace    string            `json:"namespace"`
	QuotaName    string            `json:"quotaName"`
	ResourceType string            `json:"resourceType"`
	Hard         resource.Quantity `json:"hard"`
	Used         resource.Quantity `json:"used"`
	Percentage   float64           `json:"percentage"`
	Timestame    time.Time         `json:"timestamp"`
	Labels       map[string]string `json:"labels"`
}

func NewQuotaTracker(clientset *kubernetes.Clientset) *QuotaTracker {
	return &QuotaTracker{
		clientset: clientset,
	}
}

func (qt *QuotaTracker) GetAllQuotaMetrics() (map[string]map[string][]QuotaMetrics, error) {
	allMetrics := make(map[string]map[string][]QuotaMetrics)
	namespaces, err := qt.clientset.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}
	for _, ns := range namespaces.Items {
		metrics, err := qt.GetNamespaceQuotaMetrics(ns.Name)
		if err != nil {
			log.Errorf("Warning: failed to get quota metrics for namespace %s: %v\n", ns.Name, err)
			continue
		}
		if allMetrics[env.GetClusterID()] == nil {
			allMetrics[env.GetClusterID()] = make(map[string][]QuotaMetrics)
		}
		allMetrics[env.GetClusterID()][ns.Name] = metrics
	}
	return allMetrics, nil
}

func (qt *QuotaTracker) GetNamespaceQuotaMetrics(namespace string) ([]QuotaMetrics, error) {
	quotas, err := qt.clientset.CoreV1().ResourceQuotas(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list resource quotas in namespace %s: %w", namespace, err)
	}
	var metrics []QuotaMetrics
	timestamp := time.Now()
	for _, quota := range quotas.Items {
		for resourceName, hardLimit := range quota.Status.Hard {
			usedQuantity := quota.Status.Used[resourceName]
			var percentage float64
			if hardLimit.Value() > 0 {
				percentage = usedQuantity.AsApproximateFloat64() / hardLimit.AsApproximateFloat64()
			}
			metric := QuotaMetrics{
				ClusterID:    env.GetClusterID(),
				Namespace:    namespace,
				QuotaName:    quota.Name,
				ResourceType: string(resourceName),
				Hard:         hardLimit,
				Used:         usedQuantity,
				Percentage:   percentage,
				Timestame:    timestamp,
				Labels:       quota.Labels,
			}
			metrics = append(metrics, metric)
		}

	}
	return metrics, nil
}

func GetQuotaMetricsHandler(clientset *kubernetes.Clientset) func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")
		qtr := NewQuotaTracker(clientset)
		quotametrics, err := qtr.GetAllQuotaMetrics()
		if err != nil {
			protocol.InternalServerError(err.Error())
		} else {
			protocol.WriteData(w, quotametrics)
		}

	}
}
