package protocol

import (
	"net/http"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// HTTPProtocol is a struct used as a selector for request/response protocol utility methods
type HTTPProtocol struct{}

// HTTPError represents an http error response
type HTTPError struct {
	StatusCode int
	Body       string
}

// Error returns the error string
func (he HTTPError) Error() string {
	return string(he.Body)
}

// BadRequest creates a BadRequest HTTPError
func (hp HTTPProtocol) BadRequest(message string) HTTPError {
	return HTTPError{
		StatusCode: http.StatusBadRequest,
		Body:       message,
	}
}

// UnprocessableEntity creates an UnprocessableEntity HTTPError
func (hp HTTPProtocol) UnprocessableEntity(message string) HTTPError {
	if message == "" {
		message = "Unprocessable Entity"
	}
	return HTTPError{
		StatusCode: http.StatusUnprocessableEntity,
		Body:       message,
	}
}

// InternalServerError creates an InternalServerError HTTPError
func (hp HTTPProtocol) InternalServerError(message string) HTTPError {
	if message == "" {
		message = "Internal Server Error"
	}
	return HTTPError{
		StatusCode: http.StatusInternalServerError,
		Body:       message,
	}
}

func (hp HTTPProtocol) NotImplemented(message string) HTTPError {
	if message == "" {
		message = "Not Implemented"
	}
	return HTTPError{
		StatusCode: http.StatusNotImplemented,
		Body:       message,
	}
}
func (hp HTTPProtocol) Forbidden(message string) HTTPError {
	if message == "" {
		message = "Forbidden"
	}
	return HTTPError{
		StatusCode: http.StatusForbidden,
		Body:       message,
	}
}

// NotFound creates a NotFound HTTPError
func (hp HTTPProtocol) NotFound() HTTPError {
	return HTTPError{
		StatusCode: http.StatusNotFound,
		Body:       "Not Found",
	}
}

// HTTPResponse represents a data envelope for our HTTP messaging
type HTTPResponse struct {
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
	Message string      `json:"message,omitempty"`
	Warning string      `json:"warning,omitempty"`
}

// ToResponse accepts a data payload and/or error to encode into a new HTTPResponse instance. Responses
// which should not contain an error should pass nil for err.
func (hp HTTPProtocol) ToResponse(data interface{}, err error) *HTTPResponse {
	if err != nil {
		return &HTTPResponse{
			Code:    http.StatusInternalServerError,
			Data:    data,
			Message: err.Error(),
		}
	}

	return &HTTPResponse{
		Code: http.StatusOK,
		Data: data,
	}
}
func (hp HTTPProtocol) WriteRawOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func (hp HTTPProtocol) WriteRawNoContent(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}

// WriteJSONData uses json content-type and json encoder with no data envelope allowing to remove
// xss CWE as well as backwards compatibility to exisitng FE expectations
func (hp HTTPProtocol) WriteJSONData(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Error("Failed to encode JSON response: " + err.Error())
	}
}

// WriteRawError uses json content-type and outputs raw error message for backwards compatibility to existing
// frontend expectations.
func (hp HTTPProtocol) WriteRawError(w http.ResponseWriter, httpStatusCode int, err string) {
	// I know this isn't json, but its what we've done and don't want to break frontned while we fix CWE
	w.Header().Set("Content-Type", "application/json")
	http.Error(w, err, httpStatusCode)
}

// WriteEncodedError writes an error response in the format of HTTPResponse
func (hp HTTPProtocol) WriteEncodedError(w http.ResponseWriter, httpStatusCode int, errorResponse interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatusCode)
	if err := json.NewEncoder(w).Encode(errorResponse); err != nil {
		log.Error("Failed to encode error response: " + err.Error())
	}
}

// WriteData wraps the data payload in an HTTPResponse and writes the resulting response using the
// http.ResponseWriter
func (hp HTTPProtocol) WriteData(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Error("Failed to encode response: " + err.Error())
	}
}

// WriteDataWithWarning writes the data payload similiar to WriteData except it provides an additional warning message.
func (hp HTTPProtocol) WriteDataWithWarning(w http.ResponseWriter, data interface{}, warning string) {
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	resp := &HTTPResponse{
		Code:    status,
		Data:    data,
		Warning: warning,
	}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Error("Failed to encode response with warning: " + err.Error())
	}
}

// WriteDataWithMessage writes the data payload similiar to WriteData except it provides an additional string message.
func (hp HTTPProtocol) WriteDataWithMessage(w http.ResponseWriter, data interface{}, message string) {
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	resp := &HTTPResponse{
		Code:    status,
		Data:    data,
		Message: message,
	}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Error("Failed to encode response with message: " + err.Error())
	}
}

// WriteProtoWithMessage uses the protojson package to convert proto3 response to json response and
// return it to the requester. Proto3 drops messages with default values but overriding the param
// EmitUnpopulated to true it returns default values in the Json response payload. If error is
// encountered it sent InternalServerError and the error why the json conversion failed.
func (hp HTTPProtocol) WriteProtoWithMessage(w http.ResponseWriter, data proto.Message) {
	w.Header().Set("Content-Type", "application/json")
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	status := http.StatusOK
	w.WriteHeader(status)
	b, err := m.Marshal(data)
	if err != nil {
		hp.WriteError(w, hp.InternalServerError(err.Error()))
		log.Error("Failed to marshal proto to json: " + err.Error())
		return
	}

	w.Write(b)
}

// WriteDataWithMessageAndWarning writes the data payload similiar to WriteData except it provides a warning and additional message string.
func (hp HTTPProtocol) WriteDataWithMessageAndWarning(w http.ResponseWriter, data interface{}, message string, warning string) {
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	resp := &HTTPResponse{
		Code:    status,
		Data:    data,
		Message: message,
		Warning: warning,
	}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Error("Failed to encode response with message and warning: " + err.Error())
	}
}

// WriteError wraps the HTTPError in a HTTPResponse and writes it via http.ResponseWriter
func (hp HTTPProtocol) WriteError(w http.ResponseWriter, err HTTPError) {
	w.Header().Set("Content-Type", "application/json")
	status := err.StatusCode
	if status == 0 {
		status = http.StatusInternalServerError
	}
	w.WriteHeader(status)

	resp, _ := json.Marshal(&HTTPResponse{
		Code:    status,
		Message: err.Body,
	})
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Error("Failed to encode error response: " + err.Error())
	}
}

// WriteResponse writes the provided HTTPResponse instance via http.ResponseWriter
func (hp HTTPProtocol) WriteResponse(w http.ResponseWriter, r *HTTPResponse) {
	w.Header().Set("Content-Type", "application/json")
	status := r.Code
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(r); err != nil {
		log.Error("Failed to encode response: " + err.Error())
	}
}
