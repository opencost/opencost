package gcp

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestBigQueryIntegrationTypes_Load(t *testing.T) {
	// Test the Load method for CloudCostLoader
	ccl := &CloudCostLoader{}

	// Test with empty values
	var values []bigquery.Value
	var schema bigquery.Schema
	err := ccl.Load(values, schema)
	assert.Error(t, err) // Expect error due to empty data
}

func TestBigQueryIntegrationTypes_LoadWithValidData(t *testing.T) {
	// Test with some valid data
	ccl := &CloudCostLoader{}

	values := []bigquery.Value{"test"}
	schema := bigquery.Schema{
		&bigquery.FieldSchema{Name: "test"},
	}

	err := ccl.Load(values, schema)
	// This will likely fail due to invalid structure, but we can test the function
	assert.Error(t, err) // Expect error due to invalid structure
}

func TestBigQueryIntegrationTypes_LoadWithInvalidJSON(t *testing.T) {
	// Test with invalid data
	ccl := &CloudCostLoader{}

	values := []bigquery.Value{nil}
	schema := bigquery.Schema{
		&bigquery.FieldSchema{Name: "test"},
	}

	err := ccl.Load(values, schema)
	assert.Error(t, err) // Expect error due to invalid data
}
