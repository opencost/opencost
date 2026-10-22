package reader

import (
	"encoding/json"
	"io"
)

// NewJSONLinesReader returns a Reader[T] that streams a sequence of JSON values
// read from an underlying io.Reader. It handles JSON Lines (one JSON value per
// line), which can be whitespace/newline-separated (or not) and consolidated
// to a single line each (or not, e.g. pretty-printed). Stops on io.EOF from
// the underlying reader. If the underlying reader is also an io.Closer, it
// will close it on Close().
func NewJSONLinesReader[T any](r io.Reader) *FuncReader[T] {
	dec := json.NewDecoder(r)

	next := func() (T, error) {
		var item T
		err := dec.Decode(&item)
		return item, err
	}

	closer, _ := r.(io.Closer)

	return NewFuncReader(next, closer)
}
