package prom

import "github.com/opencost/opencost/core/pkg/source"

type PrometheusQueryFormatter struct {
	fieldMapper source.FieldMapper
}

func NewPrometheusQueryFormatter(fieldMapper source.FieldMapper) *PrometheusQueryFormatter {
	return &PrometheusQueryFormatter{
		fieldMapper: fieldMapper,
	}
}
