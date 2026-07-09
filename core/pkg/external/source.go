package external

import "fmt"

type LabelSource interface {
	Extract(map[string]string) (map[string]string, error)
}

func NewLabelSource(cfg *Config) (LabelSource, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	if cfg.ConfigMapName != "" {
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
		labels, err := src.Extract(data)
		if err != nil {
			return err
		}
		return provider.Update(name, labels)
	}
}
