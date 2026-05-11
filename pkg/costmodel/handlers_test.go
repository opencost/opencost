package costmodel

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	coremodel "github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/stretchr/testify/require"
)

// mockKubeModelQuerier implements kubeModelQuerier for handler tests.
type mockKubeModelQuerier struct {
	results []*coremodel.KubeModelSet
	err     error
}

func (m *mockKubeModelQuerier) Query(_ opencost.Window) ([]*coremodel.KubeModelSet, error) {
	return m.results, m.err
}

// newKubeModelRequest builds a GET request to /kubemodel with the given window query param.
func newKubeModelRequest(window string) *http.Request {
	url := "/kubemodel"
	if window != "" {
		url += "?window=" + window
	}
	r, _ := http.NewRequest(http.MethodGet, url, nil)
	return r
}

func TestKubeModelHandler_NilQuerier_Returns503(t *testing.T) {
	a := &Accesses{KubeModelQuerier: nil}

	w := httptest.NewRecorder()
	a.KubeModelHandler(w, newKubeModelRequest("1d"), httprouter.Params{})

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestKubeModelHandler_MissingWindow_Returns400(t *testing.T) {
	a := &Accesses{KubeModelQuerier: &mockKubeModelQuerier{}}

	w := httptest.NewRecorder()
	a.KubeModelHandler(w, newKubeModelRequest(""), httprouter.Params{})

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestKubeModelHandler_InvalidWindow_Returns400(t *testing.T) {
	a := &Accesses{KubeModelQuerier: &mockKubeModelQuerier{}}

	w := httptest.NewRecorder()
	a.KubeModelHandler(w, newKubeModelRequest("notawindow"), httprouter.Params{})

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestKubeModelHandler_QuerierError_Returns500(t *testing.T) {
	a := &Accesses{
		KubeModelQuerier: &mockKubeModelQuerier{err: fmt.Errorf("storage unavailable")},
	}

	w := httptest.NewRecorder()
	a.KubeModelHandler(w, newKubeModelRequest("1d"), httprouter.Params{})

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestKubeModelHandler_Success_Returns200WithData(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	kms := coremodel.NewMockKubeModelSet(now.Add(-time.Hour), now)
	a := &Accesses{
		KubeModelQuerier: &mockKubeModelQuerier{results: []*coremodel.KubeModelSet{kms}},
	}

	w := httptest.NewRecorder()
	a.KubeModelHandler(w, newKubeModelRequest("1d"), httprouter.Params{})

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	// Response must be valid JSON and contain a non-empty data payload.
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	_, hasData := body["data"]
	require.True(t, hasData, "response body should contain a 'data' field")
}

func TestKubeModelHandler_EmptyResults_Returns200(t *testing.T) {
	a := &Accesses{
		KubeModelQuerier: &mockKubeModelQuerier{results: []*coremodel.KubeModelSet{}},
	}

	w := httptest.NewRecorder()
	a.KubeModelHandler(w, newKubeModelRequest("1d"), httprouter.Params{})

	require.Equal(t, http.StatusOK, w.Code)
}