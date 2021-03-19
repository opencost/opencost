package json

import (
    "encoding/json"

    jsoniter "github.com/json-iterator/go"
)

var Marshal = jsoniter.ConfigCompatibleWithStandardLibrary.Marshal
var Unmarshal = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal
type Marshaler json.Marshaler
var NewDecoder = json.NewDecoder
