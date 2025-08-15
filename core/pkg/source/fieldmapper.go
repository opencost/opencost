package source

import (
	"fmt"
	"maps"

	"github.com/opencost/opencost/core/pkg/util/sliceutil"
)

// A FieldMapper maps the source name for the field to a corresponding set of
// potential labels representing that source field. It also maintains a reverse mapping
// which allows finding a source field by a label.
//
// By design, there are NO overlapping labels permitted between source fields and labels. That is,
// labels for a specific field cannot overlap with labels for another field.
type FieldMapper interface {
	// LabelsFor returns all of the labels associated with that specific field.
	LabelsFor(field string) []string

	// FieldFor returns the field the provided label is associated with
	FieldFor(label string) string

	// Resolve attempts to find all labels associated with a given value. If the value
	// is a field, then all of the labels associated with that field are returned. If
	// the value is a label, then all the labels (including the provided value) are
	// returned. Lastly, if neither of the previous conditions are true, an error is
	// returned.
	Resolve(value string) ([]string, error)
}

// NoOpFieldMapper is a no-op implementation of the FieldMapper interface, which provides a direct
// passthrough of lookups. Useful when all field names are deterministic!
type NoOpFieldMapper struct{}

// NewNoOpFieldMapper creates a new NoOpFieldMapper instance for use with query results where the
// field names are deterministic.
func NewNoOpFieldMapper() *NoOpFieldMapper {
	return new(NoOpFieldMapper)
}

// LabelsFor returns all of the labels associated with that specific field.
func (nofm *NoOpFieldMapper) LabelsFor(field string) []string {
	return []string{field}
}

// FieldFor returns the field the provided label is associated with
func (nofm *NoOpFieldMapper) FieldFor(label string) string {
	return label
}

// Resolve returns the value passed into the function
func (nofm *NoOpFieldMapper) Resolve(value string) ([]string, error) {
	return []string{value}, nil
}

// ReverseFieldMapper maps the source name for the field to a corresponding set of
// potential labels representing that source field. It also maintains a reverse mapping
// which allows finding a source field by a label.
//
// By design, there are NO overlapping labels permitted between source fields and labels. That is,
// labels for a specific field cannot overlap with labels for another field.
type ReverseFieldMapper struct {
	fieldToLabel map[string][]string
	labelToField map[string]string
}

// NewReverseFieldMapper creates a new ReverseFieldMapper instance.
func NewReverseFieldMapper() *ReverseFieldMapper {
	return &ReverseFieldMapper{
		fieldToLabel: make(map[string][]string),
		labelToField: make(map[string]string),
	}
}

// LabelsFor returns all of the labels associated with that specific field.
func (rfm *ReverseFieldMapper) LabelsFor(field string) []string {
	return rfm.fieldToLabel[field]
}

// FieldFor returns the field the provided label is associated with
func (rfm *ReverseFieldMapper) FieldFor(label string) string {
	return rfm.labelToField[label]
}

// Resolve attempts to find all labels associated with a given value. If the value
// is a field, then all of the labels associated with that field are returned. If
// the value is a label, then all the labels (including the provided value) are
// returned. Lastly, if neither of the previous conditions are true, an error is
// returned.
func (rfm *ReverseFieldMapper) Resolve(value string) ([]string, error) {
	labels := rfm.LabelsFor(value)
	if len(labels) > 0 {
		return labels, nil
	}

	field := rfm.FieldFor(value)
	if field != "" {
		return rfm.LabelsFor(field), nil
	}

	return nil, fmt.Errorf("no labels found for value %s", value)
}

// Set appends a field -> labels mapping and returns an error if any keys or fields
// overlap
func (rfm *ReverseFieldMapper) Set(source string, labels ...string) error {
	if rfm.fieldToLabel == nil {
		rfm.fieldToLabel = make(map[string][]string)
	}

	if _, ok := rfm.fieldToLabel[source]; ok {
		return fmt.Errorf("source: %s is already mapped to a set of labels", source)
	}

	// ensure all labels are unique, and ensure non-zero length
	labels = toUnique(source, labels)
	rfm.fieldToLabel[source] = labels

	if rfm.labelToField == nil {
		rfm.labelToField = make(map[string]string)
	}
	for _, label := range labels {
		// overlap check -- clear out previously written mappings, return an error with conflict
		if l, ok := rfm.labelToField[label]; ok {
			for _, ll := range labels {
				delete(rfm.labelToField, ll)
			}

			return fmt.Errorf("label %s is already mapped to source %s", label, l)
		}

		rfm.labelToField[label] = source
	}
	return nil
}

// toUnique ensures that all labels are unique within the provided slices, and will add the source
// string to the list if the provided labels are empty
func toUnique(source string, labels []string) []string {
	set := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if label != "" {
			set[label] = struct{}{}
		}
	}
	result := sliceutil.SeqToSlice(maps.Keys(set))
	if len(result) == 0 {
		result = append(result, source)
	}

	return result
}
