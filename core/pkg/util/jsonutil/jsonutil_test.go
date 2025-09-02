package jsonutil

import (
	"bytes"
	"math"
	"testing"
)

func TestEncodeFloat64(t *testing.T) {
	buffer := &bytes.Buffer{}
	
	// Test normal float value
	buffer.Reset()
	EncodeFloat64(buffer, "test", 3.14, ",")
	expected := `"test":3.140000,`
	if buffer.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buffer.String())
	}
	
	// Test NaN value
	buffer.Reset()
	EncodeFloat64(buffer, "test", math.NaN(), ",")
	expected = `"test":null,`
	if buffer.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buffer.String())
	}
	
	// Test positive infinity
	buffer.Reset()
	EncodeFloat64(buffer, "test", math.Inf(1), ",")
	expected = `"test":null,`
	if buffer.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buffer.String())
	}
	
	// Test negative infinity
	buffer.Reset()
	EncodeFloat64(buffer, "test", math.Inf(-1), ",")
	expected = `"test":null,`
	if buffer.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buffer.String())
	}
}

func TestEncodeString(t *testing.T) {
	buffer := &bytes.Buffer{}
	EncodeString(buffer, "key", "value", ",")
	expected := `"key":"value",`
	if buffer.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buffer.String())
	}
}

func TestEncode(t *testing.T) {
	buffer := &bytes.Buffer{}
	
	// Test with a simple struct
	type testStruct struct {
		Field string `json:"field"`
	}
	
	testObj := testStruct{Field: "test"}
	Encode(buffer, "key", testObj, ",")
	expected := `"key":{"field":"test"},`
	if buffer.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buffer.String())
	}
	
	// Test with nil (should produce null)
	buffer.Reset()
	Encode(buffer, "key", nil, ",")
	expected = `"key":null,`
	if buffer.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buffer.String())
	}
}