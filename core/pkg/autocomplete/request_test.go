package autocomplete

import (
	"errors"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func validateTestField(field string) (string, error) {
	if field == "" {
		return "", ErrBadRequest
	}
	return field, nil
}

func TestNormalizeRequest(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	req := &Request{
		TenantID: "t1",
		Field:    "cluster",
		Search:   " x ",
		Limit:    0,
		Window:   opencost.NewClosedWindow(start, start.Add(24*time.Hour)),
	}
	field, err := NormalizeRequest(req, validateTestField, NormalizeOptions{RequireTenantID: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if field != "cluster" || req.Search != "x" || req.Limit != DefaultResultLimit {
		t.Fatalf("unexpected normalized request: %+v", req)
	}

	_, err = NormalizeRequest(req, validateTestField, NormalizeOptions{RequireTenantID: true, EnsureLabelConfig: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.LabelConfig == nil {
		t.Fatal("expected default label config")
	}

	nilFilterReq := &Request{
		TenantID: "t1",
		Field:    "label",
		Window:   opencost.NewClosedWindow(start, start.Add(24*time.Hour)),
	}
	_, err = NormalizeRequest(nilFilterReq, validateTestField, NormalizeOptions{RequireTenantID: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nilFilterReq.Filter == nil || nilFilterReq.Filter.Op() != ast.FilterOpVoid {
		t.Fatalf("expected nil filter normalized to void op, got %+v", nilFilterReq.Filter)
	}

	_, err = NormalizeRequest(nil, validateTestField, NormalizeOptions{})
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected nil request error, got %v", err)
	}

	openReq := &Request{Field: "cluster", Window: opencost.NewWindow(&start, nil)}
	_, err = NormalizeRequest(openReq, validateTestField, NormalizeOptions{})
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected open window error, got %v", err)
	}

	_, err = NormalizeRequest(&Request{Field: "cluster", Window: req.Window}, validateTestField, NormalizeOptions{RequireTenantID: true})
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected tenant ID error, got %v", err)
	}

	limitReq := &Request{
		TenantID: "t1",
		Field:    "cluster",
		Window:   opencost.NewClosedWindow(start, start.Add(24*time.Hour)),
		Limit:    MaxResultLimit + 1,
	}
	_, err = NormalizeRequest(limitReq, validateTestField, NormalizeOptions{RequireTenantID: true})
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected limit error, got %v", err)
	}
}

func TestDefaultWindowValidator(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	if err := DefaultWindowValidator(opencost.NewClosedWindow(start, end)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := DefaultWindowValidator(opencost.NewWindow(&start, nil)); err == nil {
		t.Fatal("expected open window error")
	}
}

func TestParseRequest(t *testing.T) {
	windowStr := "2023-01-01T00:00:00Z,2023-01-02T00:00:00Z"
	qp := httputil.NewQueryParams(map[string][]string{
		"window":   {windowStr},
		"field":    {"cluster"},
		"search":   {" ns "},
		"tenantId": {"t1"},
	})
	got, err := ParseRequest(qp, ParseOptions{}, validateTestField, nil)
	if err != nil {
		t.Fatalf("ParseRequest() error = %v", err)
	}
	if got.Field != "cluster" || got.Search != "ns" || got.TenantID != "t1" {
		t.Fatalf("unexpected request: %+v", got)
	}
	if got.Filter == nil || got.Filter.Op() != ast.FilterOpVoid {
		t.Fatalf("expected void filter when filter param omitted, got %+v", got.Filter)
	}

	_, err = ParseRequest(httputil.NewQueryParams(map[string][]string{"field": {"cluster"}}), ParseOptions{}, validateTestField, nil)
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected missing window error, got %v", err)
	}

	_, err = ParseRequest(httputil.NewQueryParams(map[string][]string{
		"window": {"bad"},
		"field":  {"cluster"},
	}), ParseOptions{}, validateTestField, nil)
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected invalid window error, got %v", err)
	}

	offset := 5 * time.Hour
	_, err = ParseRequest(qp, ParseOptions{UTCOffset: &offset}, validateTestField, nil)
	if err != nil {
		t.Fatalf("ParseRequest with offset error = %v", err)
	}

	noFilterQP := httputil.NewQueryParams(map[string][]string{
		"window": {windowStr},
		"field":  {"cluster"},
		"filter": {"cluster:\"prod\""},
	})
	_, err = ParseRequest(noFilterQP, ParseOptions{}, validateTestField, nil)
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected filter parser required error, got %v", err)
	}

	parseFilter := func(filterString string) (filter.Filter, error) {
		if filterString == "bad" {
			return nil, errors.New("bad filter")
		}
		return nil, nil
	}

	badFilterQP := httputil.NewQueryParams(map[string][]string{
		"window": {windowStr},
		"field":  {"cluster"},
		"filter": {"bad"},
	})
	_, err = ParseRequest(badFilterQP, ParseOptions{}, validateTestField, parseFilter)
	if err == nil || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected filter parse error, got %v", err)
	}

	okFilterQP := httputil.NewQueryParams(map[string][]string{
		"window": {windowStr},
		"field":  {"cluster"},
		"filter": {"ok"},
	})
	got, err = ParseRequest(okFilterQP, ParseOptions{}, validateTestField, parseFilter)
	if err != nil {
		t.Fatalf("ParseRequest with filter error = %v", err)
	}
	if got.Field != "cluster" {
		t.Fatalf("unexpected request: %+v", got)
	}
	if got.Filter == nil || got.Filter.Op() != ast.FilterOpVoid {
		t.Fatalf("expected nil parse result normalized to void filter, got %+v", got.Filter)
	}
}
