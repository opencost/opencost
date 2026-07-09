package scrape

import (
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/core/pkg/clustercache"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/parser"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
	v1 "k8s.io/api/core/v1"
)

const (
	// inferenceModelLabelEnv names the pod label whose presence identifies a
	// model-server pod and whose value is the served model name. The default
	// matches the label used by the inference cost feature and llm-d.
	inferenceModelLabelEnv     = "INFERENCE_MODEL_LABEL"
	inferenceModelLabelDefault = "llm-d.ai/model"

	// inferenceScrapePortEnv overrides the default metrics port used when a
	// model-server pod does not carry a prometheus.io/port annotation. The
	// default matches the vLLM OpenAI-compatible server port.
	inferenceScrapePortEnv     = "INFERENCE_SCRAPE_PORT"
	inferenceScrapePortDefault = 8000

	// prometheusPortAnnotation is the conventional pod annotation naming the
	// port that serves Prometheus metrics.
	prometheusPortAnnotation = "prometheus.io/port"
)

// inferenceMetricNames are the model-server scheduler gauges standardized by
// the Gateway API Inference Extension Model Server Protocol: KV-cache
// utilization, queue depth (requests waiting), and running requests.
var inferenceMetricNames = map[string]struct{}{
	metric.VLLMKVCacheUsagePerc:   {},
	metric.VLLMNumRequestsWaiting: {},
	metric.VLLMNumRequestsRunning: {},
}

// inferenceTarget is a scrape target for a single model-server pod. The pod's
// Kubernetes identity is carried alongside the URL because serving engines
// emit model_name on their metrics but, unlike the DCGM exporter, do not
// self-report the namespace and pod they run in.
type inferenceTarget struct {
	target    target.ScrapeTarget
	namespace string
	pod       string
}

// InferenceScraper discovers model-server pods by pod label and scrapes their
// Prometheus metrics endpoints for the Model Server Protocol gauges. Pods are
// selected by the presence of the model label (INFERENCE_MODEL_LABEL, default
// "llm-d.ai/model"); clusters with no labelled pods produce no targets and no
// scrape traffic.
type InferenceScraper struct {
	clusterCache clustercache.ClusterCache
	modelLabel   string
	defaultPort  int
}

func newInferenceScraper(clusterCache clustercache.ClusterCache) *InferenceScraper {
	return &InferenceScraper{
		clusterCache: clusterCache,
		modelLabel:   coreenv.Get(inferenceModelLabelEnv, inferenceModelLabelDefault),
		defaultPort:  coreenv.GetInt(inferenceScrapePortEnv, inferenceScrapePortDefault),
	}
}

func (s *InferenceScraper) getTargets() []inferenceTarget {
	pods := s.clusterCache.GetAllPods()

	var targets []inferenceTarget
	for _, pod := range pods {
		if pod.Status.Phase != v1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		if _, ok := pod.Labels[s.modelLabel]; !ok {
			continue
		}

		port := s.defaultPort
		if p, ok := pod.Annotations[prometheusPortAnnotation]; ok {
			if parsed, err := strconv.Atoi(p); err == nil {
				port = parsed
			}
		}

		url := fmt.Sprintf("http://%s:%d/metrics", pod.Status.PodIP, port)
		log.Debugf("Inference: found target: %s", url)

		targets = append(targets, inferenceTarget{
			target:    target.NewUrlTarget(url),
			namespace: pod.Namespace,
			pod:       pod.Name,
		})
	}

	return targets
}

func (s *InferenceScraper) Scrape() []metric.Update {
	targets := s.getTargets()

	var errLock sync.Mutex
	var errors []error

	var scrapeFuncs []ScrapeFunc
	for i := range targets {
		t := targets[i]

		fn := func() []metric.Update {
			var scrapeResults []metric.Update
			f, err := t.target.Load()
			if err != nil {
				errLock.Lock()
				errors = append(errors, err)
				errLock.Unlock()

				log.Errorf("failed to scrape inference target: %s", err.Error())
				return scrapeResults
			}
			if closer, ok := f.(io.ReadCloser); ok {
				defer closer.Close()
			}
			results, err := parser.Parse(f)
			if err != nil {
				errLock.Lock()
				errors = append(errors, err)
				errLock.Unlock()

				log.Errorf("failed to parse inference target: %s", err.Error())
				return scrapeResults
			}
			for _, result := range results {
				if _, ok := inferenceMetricNames[result.Name]; !ok {
					continue
				}
				labels := result.Labels
				if labels == nil {
					labels = map[string]string{}
				}
				// Attach the pod's Kubernetes identity from discovery; the
				// serving engine only knows its model_name.
				labels[source.NamespaceLabel] = t.namespace
				labels[source.PodLabel] = t.pod
				scrapeResults = append(scrapeResults, metric.Update{
					Name:   result.Name,
					Labels: labels,
					Value:  result.Value,
				})
			}
			return scrapeResults
		}
		scrapeFuncs = append(scrapeFuncs, fn)
	}

	updates := concurrentScrape(scrapeFuncs...)

	// dispatch a scrape event for this specific scrape
	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.InferenceScraperName,
		Targets:     len(targets),
		Errors:      errors,
	})

	return updates
}
