package externallabels

import "context"

type Provider interface {
	Update(name string, data map[string]string) error
	Labels(ctx context.Context) (map[string]string, error)
}
