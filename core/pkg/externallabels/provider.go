package externallabels

import "context"

type Provider interface {
	Start(ctx context.Context) error
	Labels(ctx context.Context) (map[string]string, error)
}
