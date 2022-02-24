package json

import (
	"encoding/json"

	gojson "github.com/goccy/go-json"
)

var Marshal = gojson.Marshal
var Unmarshal = gojson.Unmarshal

type Marshaler json.Marshaler
type Unmarshaler json.Unmarshaler

type RawMessage json.RawMessage

var NewDecoder = json.NewDecoder
