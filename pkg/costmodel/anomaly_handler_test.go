package costmodel

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
)

func TestGetAnomalyHandler_InvalidWindow_Returns400(t *testing.T) {
	a := &Accesses{}

	r, _ := http.NewRequest(http.MethodGet, "/anomaly?window=notawindow", nil)
	w := httptest.NewRecorder()
	a.GetAnomalyHandler(w, r, httprouter.Params{})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
	if !strings.Contains(w.Body.String(), "Invalid 'window' parameter") {
		t.Fatalf("expected invalid window error in body, got: %q", w.Body.String())
	}
}

func TestGetAnomalyHandler_InvalidStep_Returns400(t *testing.T) {
	a := &Accesses{}

	r, _ := http.NewRequest(http.MethodGet, "/anomaly?window=30d&step=notaduration", nil)
	w := httptest.NewRecorder()
	a.GetAnomalyHandler(w, r, httprouter.Params{})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
	if !strings.Contains(w.Body.String(), "Invalid 'step' parameter") {
		t.Fatalf("expected invalid step error in body, got: %q", w.Body.String())
	}
}

func TestGetAnomalyHandler_InvalidLookback_Returns400(t *testing.T) {
	a := &Accesses{}

	r, _ := http.NewRequest(http.MethodGet, "/anomaly?window=30d&step=1d&lookback=notaduration", nil)
	w := httptest.NewRecorder()
	a.GetAnomalyHandler(w, r, httprouter.Params{})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
	if !strings.Contains(w.Body.String(), "Invalid 'lookback' parameter") {
		t.Fatalf("expected invalid lookback error in body, got: %q", w.Body.String())
	}
}

func TestGetAnomalyHandler_InvalidAlgorithm_Returns400(t *testing.T) {
	a := &Accesses{}

	r, _ := http.NewRequest(http.MethodGet, "/anomaly?window=30d&step=1d&lookback=7d&algorithm=invalid", nil)
	w := httptest.NewRecorder()
	a.GetAnomalyHandler(w, r, httprouter.Params{})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
	if !strings.Contains(w.Body.String(), "Invalid 'algorithm' parameter") {
		t.Fatalf("expected invalid algorithm error in body, got: %q", w.Body.String())
	}
}
