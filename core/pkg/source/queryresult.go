package source

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util"
)

// QueryResultsChan is a channel of query results
type QueryResultsChan chan *QueryResults

// Await returns query results, blocking until they are made available, and
// deferring the closure of the underlying channel
func (qrc QueryResultsChan) Await() ([]*QueryResult, error) {
	defer close(qrc)

	results := <-qrc
	if results.Error != nil {
		return nil, results.Error
	}

	return results.Results, nil
}

// QueryResults contains all of the query results and the source query string.
type QueryResults struct {
	Query   string
	Error   error
	Results []*QueryResult
}

func NewQueryResults(query string) *QueryResults {
	return &QueryResults{
		Query: query,
	}
}

// QueryResult contains a single result from a prometheus query. It's common
// to refer to query results as a slice of QueryResult
type QueryResult struct {
	Metric map[string]interface{} `json:"metric"`
	Values []*util.Vector         `json:"values"`

	// field mapper resolves the lookup keys for a specific field
	fieldMapper FieldMapper
}

func NewQueryResult(metrics map[string]any, values []*util.Vector, keys FieldMapper) *QueryResult {
	if keys == nil {
		keys = NewNoOpFieldMapper()
	}

	return &QueryResult{
		Metric:      metrics,
		Values:      values,
		fieldMapper: keys,
	}
}

func (qr *QueryResult) GetCluster() (string, error) {
	labels := qr.fieldMapper.LabelsFor(ClusterIDLabel)
	return qr.firstOf(labels...)
}

func (qr *QueryResult) GetNamespace() (string, error) {
	labels := qr.fieldMapper.LabelsFor(NamespaceLabel)
	return qr.firstOf(labels...)
}

func (qr *QueryResult) GetNode() (string, error) {
	labels := qr.fieldMapper.LabelsFor(NodeLabel)
	return qr.firstOf(labels...)
}

func (qr *QueryResult) GetInstance() (string, error) {
	labels := qr.fieldMapper.LabelsFor(InstanceLabel)
	return qr.firstOf(labels...)
}

func (qr *QueryResult) GetInstanceType() (string, error) {
	labels := qr.fieldMapper.LabelsFor(InstanceTypeLabel)
	return qr.firstOf(labels...)
}

func (qr *QueryResult) GetContainer() (string, error) {
	labels := qr.fieldMapper.LabelsFor(ContainerLabel)
	return qr.firstOf(labels...)
}

func (qr *QueryResult) GetPod() (string, error) {
	labels := qr.fieldMapper.LabelsFor(PodLabel)
	return qr.firstOf(labels...)
}

func (qr *QueryResult) GetProviderID() (string, error) {
	labels := qr.fieldMapper.LabelsFor(ProviderIDLabel)
	return qr.firstOf(labels...)
}

func (qr *QueryResult) GetDevice() (string, error) {
	labels := qr.fieldMapper.LabelsFor(DeviceLabel)
	return qr.firstOf(labels...)
}

// firstOf returns the first non-empty getResolvedString result from the provided list of fields
func (qr *QueryResult) firstOf(fields ...string) (string, error) {
	for _, field := range fields {
		value, err := qr.getResolvedString(field)
		if value != "" && err == nil {
			return value, nil
		}
	}
	return "", fmt.Errorf("none of the fields %v exist in data result vector", fields)
}

// getResolvedString returns the requested field, or an error if it does not exist
func (qr *QueryResult) getResolvedString(field string) (string, error) {
	f, ok := qr.Metric[field]
	if !ok {
		return "", fmt.Errorf("'%s' field does not exist in data result vector", field)
	}

	strField, ok := f.(string)
	if !ok {
		return "", fmt.Errorf("'%s' field is improperly formatted and cannot be converted to string", field)
	}

	return strField, nil
}

// GetString returns the requested field, or an error if it does not exist
func (qr *QueryResult) GetString(field string) (string, error) {
	// attempt to resolve the provided field. if we fail, assume the field is resolved!
	fields, err := qr.fieldMapper.Resolve(field)
	if err != nil {
		return qr.getResolvedString(field)
	}

	// otherwise, return the first resolved field
	return qr.firstOf(fields...)
}

// GetStrings returns the requested fields, or an error if it does not exist
func (qr *QueryResult) GetStrings(fields ...string) (map[string]string, error) {
	values := map[string]string{}

	for _, field := range fields {
		value, err := qr.GetString(field)
		if err != nil {
			return nil, err
		}

		values[field] = value
	}

	return values, nil
}

// GetLabels returns all labels and their values from the query result
func (qr *QueryResult) GetLabels() map[string]string {
	result := make(map[string]string)

	// Find All keys with prefix label_, remove prefix, add to labels
	for k, v := range qr.Metric {
		if !strings.HasPrefix(k, "label_") {
			continue
		}

		label := strings.TrimPrefix(k, "label_")
		value, ok := v.(string)
		if !ok {
			log.Warnf("Failed to parse label value for label: '%s'", label)
			continue
		}

		result[label] = value
	}

	return result
}

// GetAnnotations returns all annotations and their values from the query result
func (qr *QueryResult) GetAnnotations() map[string]string {
	result := make(map[string]string)

	// Find All keys with prefix annotation_, remove prefix, add to annotations
	for k, v := range qr.Metric {
		if !strings.HasPrefix(k, "annotation_") {
			continue
		}

		annotations := strings.TrimPrefix(k, "annotation_")
		value, ok := v.(string)
		if !ok {
			log.Warnf("Failed to parse label value for label: '%s'", annotations)
			continue
		}

		result[annotations] = value
	}

	return result
}
