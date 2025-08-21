package opencost

import (
	"bytes"

	"github.com/opencost/opencost/core/pkg/util/jsonutil"
)

// jsonEncodeFloat64 encodes a float64 value to JSON, handling NaN and infinity values
func jsonEncodeFloat64(buffer *bytes.Buffer, name string, val float64, comma string) {
	jsonutil.EncodeFloat64(buffer, name, val, comma)
}

// jsonEncodeString encodes a string value to JSON
func jsonEncodeString(buffer *bytes.Buffer, name, val, comma string) {
	jsonutil.EncodeString(buffer, name, val, comma)
}

// jsonEncode encodes any object to JSON
func jsonEncode(buffer *bytes.Buffer, name string, obj interface{}, comma string) {
	jsonutil.Encode(buffer, name, obj, comma)
}
