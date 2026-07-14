package external

import "fmt"

type LabelSource interface {
	ExtractNodeLabels(map[string]string) (map[string]string, error)
}

func NewLabelSource(cfg *Config) (LabelSource, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}

	if !cfg.HasNodeLabelConfig() {
		return nil, fmt.Errorf("no supported external label config")
	}

	nlConfig := cfg.NodeLabelConfig()

	if nlConfig.ConfigMapName() != "" {
		return &ConfigMapSource{
			cfg: cfg,
		}, nil
	}

	return nil, fmt.Errorf("no label source configured")
}

// WatchFunc bridges a LabelSource and a LabelProvider as a watcher callback.
// It returns a func(string, map[string]string) error that passes the raw source
// data through src.Extract and forwards the resulting labels to provider.Update.
func WatchFunc(src LabelSource, provider LabelProvider) func(string, map[string]string) error {
	return func(name string, data map[string]string) error {
		labels, err := src.ExtractNodeLabels(data)
		if err != nil {
			return err
		}
		return provider.Update(name, labels)
	}
}
