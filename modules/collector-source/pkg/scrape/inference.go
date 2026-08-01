package scrape

import (
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/modules/collector-source/pkg/env"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/parser"
	"github.com/opencost/opencost/modules/collector-source/pkg/scrape/target"
	v1 "k8s.io/api/core/v1"
)

// prometheusPortAnnotation is the conventional pod annotation naming the port
// that serves Prometheus metrics.
const prometheusPortAnnotation = "prometheus.io/port"

// inferenceMetricNames are the model-server series kept from each scrape.
//
// The first group is the scheduler gauges standardized by the Gateway API
// Inference Extension Model Server Protocol: KV-cache utilization, queue
// depth (requests waiting), and running requests, plus preemptions.
//
// The second group is the token and timing counters the inference cost
// feature reads, which the Prometheus source has always collected; keeping
// them here costs no extra scrape traffic (it is the same /metrics response)
// and lets the collector source serve the whole inference querier surface
// rather than half of it. Only the _sum child of the timing histograms is
// read, never the per-bucket series.
var inferenceMetricNames = map[string]struct{}{
	metric.VLLMKVCacheUsagePerc:    {},
	metric.VLLMNumRequestsWaiting:  {},
	metric.VLLMNumRequestsRunning:  {},
	metric.VLLMNumPreemptionsTotal: {},

	metric.VLLMPromptTokensTotal:                   {},
	metric.VLLMGenerationTokensTotal:               {},
	metric.VLLMRequestPrefillTimeSecondsSum:        {},
	metric.VLLMRequestTimePerOutputTokenSecondsSum: {},
	metric.VLLMPrefixCacheHitsTotal:                {},
	metric.VLLMCacheConfigInfo:                     {},
}

// inferenceTarget is a scrape target for a single model-server pod. The pod's
// Kubernetes identity is carried alongside the URL because serving engines
// emit model_name on their metrics but, unlike the DCGM exporter, do not
// self-report the namespace and pod they run in.
//
// The UIDs are what let these metrics join the rest of the KubeModel: names
// are ambiguous across a pod's lifetime (a recreated pod reuses its name),
// whereas pod_uid is the identity every other kubemodel entity is keyed on.
// Names are kept alongside them for human-readable output.
type inferenceTarget struct {
	target       target.ScrapeTarget
	namespace    string
	namespaceUID string
	pod          string
	podUID       string
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
		modelLabel:   env.GetInferenceModelLabel(),
		defaultPort:  env.GetInferenceScrapePort(),
	}
}

func (s *InferenceScraper) getTargets() []inferenceTarget {
	pods := s.clusterCache.GetAllPods()
	namespaceIndex := buildNamespaceIndex(s.clusterCache.GetAllNamespaces())

	var targets []inferenceTarget
	for _, pod := range pods {
		if pod.Status.Phase != v1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		if _, ok := pod.Labels[s.modelLabel]; !ok {
			continue
		}

		nsUID, ok := namespaceIndex[pod.Namespace]
		if !ok {
			log.Debugf("Inference: namespaceUID missing from index for namespace name '%s'", pod.Namespace)
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
			target:       target.NewUrlTarget(url),
			namespace:    pod.Namespace,
			namespaceUID: string(nsUID),
			pod:          pod.Name,
			podUID:       string(pod.UID),
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
				// serving engine only knows its model_name. pod_uid is the
				// key the rest of the KubeModel joins on, so it rides along
				// with the names rather than being reconstructed later.
				labels[source.NamespaceLabel] = t.namespace
				labels[source.NamespaceUIDLabel] = t.namespaceUID
				labels[source.PodLabel] = t.pod
				labels[source.PodUIDLabel] = t.podUID

				update := metric.Update{
					Name:   result.Name,
					Labels: labels,
					Value:  result.Value,
				}
				// cache_config_info is an info metric: its value is a constant
				// 1 and the payload rides on labels such as
				// enable_prefix_caching. The Info aggregator reads that off
				// AdditionalInfo, so the labels have to be carried there too.
				if result.Name == metric.VLLMCacheConfigInfo {
					update.AdditionalInfo = labels
				}
				scrapeResults = append(scrapeResults, update)
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
