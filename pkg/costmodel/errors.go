package costmodel

import "errors"

// ErrBadRequest marks errors caused by invalid client input (e.g. a malformed
// filter or an incompatible combination of query parameters). HTTP handlers
// should check for it with errors.Is and respond with 400 Bad Request instead
// of 500 Internal Server Error.
//
// Wrap it with fmt.Errorf("%w - <details>", ErrBadRequest) so the resulting
// message keeps the "bad request - <details>" format.
var ErrBadRequest = errors.New("bad request")
