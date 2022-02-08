package metrics

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util/watcher"
	"k8s.io/klog"
)

var metricsConfigLock = new(sync.Mutex)

type MetricsConfig struct {
	DisabledMetrics []string `json:"disabledMetrics"`
}

func (mc MetricsConfig) GetDisabledMetricsMap() map[string]struct{} {
	disabledMetricsMap := make(map[string]struct{})

	for i := range mc.DisabledMetrics {
		disabledMetricsMap[mc.DisabledMetrics[i]] = struct{}{}
		log.Infof("Adding disabled metric %s", mc.DisabledMetrics[i])
	}

	return disabledMetricsMap
}

func GetMetricsConfig() (*MetricsConfig, error) {
	metricsConfigLock.Lock()
	defer metricsConfigLock.Unlock()
	mc := &MetricsConfig{}
	body, err := ioutil.ReadFile("/var/configs/metrics.json")
	if os.IsNotExist(err) {

		return mc, nil
	} else if err != nil {
		return mc, err
	}

	err = json.Unmarshal(body, mc)
	if err != nil {
		return mc, err
	}

	return mc, nil
}

func UpdateMetricsConfig(mc *MetricsConfig) (*MetricsConfig, error) {
	metricsConfigLock.Lock()
	defer metricsConfigLock.Unlock()

	mcb, err := json.Marshal(mc)
	if err != nil {
		return nil, fmt.Errorf("error decoding metrics config struct: %s", err)
	}

	err = ioutil.WriteFile("/var/configs/metrics.json", mcb, 0644)
	if err != nil {
		return nil, fmt.Errorf("error writing to metrics config file: %s", err)
	}

	return mc, nil
}

func UpdateMetricsConfigFromConfigmap(data map[string]string) error {

	mc := &MetricsConfig{}
	key := "metrics.json"

	cdata, ok := data[key]
	if !ok {
		return fmt.Errorf("error finding metrics config data")
	}

	err := json.Unmarshal([]byte(cdata), &mc)
	if err != nil {
		return fmt.Errorf("failed to unmarshal metrics configs: %s", err)
	}

	_, err = UpdateMetricsConfig(mc)
	if err != nil {
		return err
	}

	return nil

}

func GetMetricsConfigWatcher() *watcher.ConfigMapWatcher {
	return &watcher.ConfigMapWatcher{
		ConfigMapName: "metrics-config", // temporary, use env
		WatchFunc: func(name string, data map[string]string) error {
			klog.Infof("--CONFIGMAP DATA--")
			for key, val := range data {
				klog.Infof("%s : %s", key, val)
			}
			klog.Infof("------------------")
			err := UpdateMetricsConfigFromConfigmap(data)
			return err
		},
	}
}
