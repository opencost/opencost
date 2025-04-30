package target

import (
	"fmt"
	"io"
	"net/http"
)

type UrlTarget struct {
	url string
}

func NewUrlTarget(url string) *UrlTarget {
	return &UrlTarget{
		url: url,
	}
}

func (t *UrlTarget) Load() (io.Reader, error) {
	resp, err := http.Get(t.url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch URL: %w", err)
	}

	return resp.Body, nil
}
