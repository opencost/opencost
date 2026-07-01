package externallabels

import "context"

// TO-DO: Implement this
type ConfigMapProvider struct {
}

// TO-DO: Implement this
func NewConfigMapProvider() (*ConfigMapProvider, error) {
	return &ConfigMapProvider{}, nil
}

// TO-DO: Implement this
func (a *ConfigMapProvider) Start(ctx context.Context) error {
	return nil
}

// TO-DO: Implement this
func (a *ConfigMapProvider) Labels(ctx context.Context) (map[string]string, error) {
	return nil, nil
}
