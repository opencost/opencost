package target

import (
	"io"
	"strings"
)

type StringTarget struct {
	raw string
}

func NewStringTarget(raw string) *StringTarget {
	return &StringTarget{
		raw: raw,
	}
}

func (t *StringTarget) Load() (io.Reader, error) {
	return strings.NewReader(t.raw), nil
}
