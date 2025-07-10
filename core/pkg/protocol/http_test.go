package protocol

import (
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTPError_Error(t *testing.T) {
	err := HTTPError{StatusCode: 400, Body: "bad request"}
	assert.Equal(t, "bad request", err.Error())
}

func TestHTTPProtocol_BadRequest(t *testing.T) {
	hp := HTTPProtocol{}
	err := hp.BadRequest("bad req")
	assert.Equal(t, http.StatusBadRequest, err.StatusCode)
	assert.Equal(t, "bad req", err.Body)
}

func TestHTTPProtocol_UnprocessableEntity(t *testing.T) {
	hp := HTTPProtocol{}
	err := hp.UnprocessableEntity("")
	assert.Equal(t, http.StatusUnprocessableEntity, err.StatusCode)
	assert.Equal(t, "Unprocessable Entity", err.Body)
	err2 := hp.UnprocessableEntity("custom")
	assert.Equal(t, "custom", err2.Body)
}

func TestHTTPProtocol_InternalServerError(t *testing.T) {
	hp := HTTPProtocol{}
	err := hp.InternalServerError("")
	assert.Equal(t, http.StatusInternalServerError, err.StatusCode)
	assert.Equal(t, "Internal Server Error", err.Body)
	err2 := hp.InternalServerError("custom")
	assert.Equal(t, "custom", err2.Body)
}

func TestHTTPProtocol_NotImplemented(t *testing.T) {
	hp := HTTPProtocol{}
	err := hp.NotImplemented("")
	assert.Equal(t, http.StatusNotImplemented, err.StatusCode)
	assert.Equal(t, "Not Implemented", err.Body)
	err2 := hp.NotImplemented("custom")
	assert.Equal(t, "custom", err2.Body)
}

func TestHTTPProtocol_Forbidden(t *testing.T) {
	hp := HTTPProtocol{}
	err := hp.Forbidden("")
	assert.Equal(t, http.StatusForbidden, err.StatusCode)
	assert.Equal(t, "Forbidden", err.Body)
	err2 := hp.Forbidden("custom")
	assert.Equal(t, "custom", err2.Body)
}

func TestHTTPProtocol_NotFound(t *testing.T) {
	hp := HTTPProtocol{}
	err := hp.NotFound()
	assert.Equal(t, http.StatusNotFound, err.StatusCode)
	assert.Equal(t, "Not Found", err.Body)
}

func TestHTTPProtocol_ToResponse(t *testing.T) {
	hp := HTTPProtocol{}
	resp := hp.ToResponse("data", nil)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "data", resp.Data)
	resp2 := hp.ToResponse("data", errors.New("fail"))
	assert.Equal(t, http.StatusInternalServerError, resp2.Code)
	assert.Equal(t, "fail", resp2.Message)
}

func TestHTTPProtocol_WriteRawOK(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteRawOK(rw)
	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Equal(t, "application/json", rw.Header().Get("Content-Type"))
	assert.Equal(t, "0", rw.Header().Get("Content-Length"))
}

func TestHTTPProtocol_WriteRawNoContent(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteRawNoContent(rw)
	assert.Equal(t, http.StatusNoContent, rw.Code)
	assert.Equal(t, "application/json", rw.Header().Get("Content-Type"))
}

func TestHTTPProtocol_WriteJSONData(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteJSONData(rw, map[string]string{"foo": "bar"})
	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Contains(t, rw.Body.String(), "foo")
}

func TestHTTPProtocol_WriteRawError(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteRawError(rw, http.StatusBadRequest, "bad")
	assert.Equal(t, http.StatusBadRequest, rw.Code)
	assert.Equal(t, "text/plain; charset=utf-8", rw.Header().Get("Content-Type"))
	assert.Contains(t, rw.Body.String(), "bad")
}

func TestHTTPProtocol_WriteEncodedError(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteEncodedError(rw, http.StatusBadRequest, map[string]string{"err": "bad"})
	assert.Equal(t, http.StatusBadRequest, rw.Code)
	assert.Contains(t, rw.Body.String(), "bad")
}

func TestHTTPProtocol_WriteData(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteData(rw, map[string]string{"foo": "bar"})
	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Contains(t, rw.Body.String(), "foo")
}

func TestHTTPProtocol_WriteDataWithWarning(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteDataWithWarning(rw, map[string]string{"foo": "bar"}, "warn")
	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Contains(t, rw.Body.String(), "warn")
}

func TestHTTPProtocol_WriteDataWithMessage(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteDataWithMessage(rw, map[string]string{"foo": "bar"}, "msg")
	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Contains(t, rw.Body.String(), "msg")
}

func TestHTTPProtocol_WriteDataWithMessageAndWarning(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteDataWithMessageAndWarning(rw, map[string]string{"foo": "bar"}, "msg", "warn")
	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Contains(t, rw.Body.String(), "msg")
	assert.Contains(t, rw.Body.String(), "warn")
}

func TestHTTPProtocol_WriteError(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	hp.WriteError(rw, HTTPError{StatusCode: 400, Body: "fail"})
	assert.Equal(t, 400, rw.Code)
	body := rw.Body.String()
	log.Println("body: " + body)
	assert.Contains(t, body, "fail")
}

func TestHTTPProtocol_WriteResponse(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	resp := &HTTPResponse{Code: 200, Data: "foo"}
	hp.WriteResponse(rw, resp)
	assert.Equal(t, 200, rw.Code)
	assert.Contains(t, rw.Body.String(), "foo")
}

func TestHTTPProtocol_WriteData_Structure(t *testing.T) {
	hp := HTTPProtocol{}
	rw := httptest.NewRecorder()
	data := map[string]string{"foo": "bar"}
	hp.WriteData(rw, data)
	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Equal(t, "application/json", rw.Header().Get("Content-Type"))

	// Check the structure of the JSON response
	body := rw.Body.String()
	assert.Contains(t, body, "\"code\":200")
	assert.Contains(t, body, "\"data\":{\"foo\":\"bar\"}")
	assert.NotContains(t, body, "message")
	assert.NotContains(t, body, "warning")
}
 