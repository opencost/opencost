package autocomplete

import "errors"

// ErrBadRequest indicates a client validation error for autocomplete requests.
var ErrBadRequest = errors.New("autocomplete bad request")

// IsBadRequest reports whether err is a client validation error.
func IsBadRequest(err error) bool {
	return errors.Is(err, ErrBadRequest)
}
