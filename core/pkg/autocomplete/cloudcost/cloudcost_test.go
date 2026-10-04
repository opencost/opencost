package cloudcost

import (
	"errors"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/httputil"
)

func TestValidateField(t *testing.T) {
	got, err := ValidateField("accountID")
	if err != nil || got != "accountID" {
		t.Fatalf("ValidateField(accountID) = %q, %v", got, err)
	}

	got, err = ValidateField("label:App")
	if err != nil || got != "label:App" {
		t.Fatalf("ValidateField(label:App) = %q, %v", got, err)
	}

	got, err = ValidateField("label")
	if err != nil || got != "label" {
		t.Fatalf("ValidateField(label) = %q, %v", got, err)
	}

	_, err = ValidateField("")
	if err == nil {
		t.Fatal("expected error for empty field")
	}

	_, err = ValidateField("bad")
	if err == nil {
		t.Fatal("expected error for unrecognized field")
	}
}

func TestNormalizeRequest(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	req := &autocomplete.Request{
		Field:  "accountID",
		Search: " x ",
		Limit:  0,
		Window: opencost.NewClosedWindow(start, start.Add(24*time.Hour)),
	}
	field, err := autocomplete.NormalizeRequest(req, ValidateField, autocomplete.NormalizeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if field != "accountID" || req.Search != "x" {
		t.Fatalf("unexpected normalized request: %+v", req)
	}

	openReq := &autocomplete.Request{Field: "accountID", Window: opencost.NewWindow(&start, nil)}
	_, err = autocomplete.NormalizeRequest(openReq, ValidateField, autocomplete.NormalizeOptions{})
	if err == nil || !errors.Is(err, autocomplete.ErrBadRequest) {
		t.Fatalf("expected open window error, got %v", err)
	}
}

func TestParseRequest(t *testing.T) {
	windowStr := "2023-01-01T00:00:00Z,2023-01-02T00:00:00Z"
	qp := httputil.NewQueryParams(map[string][]string{
		"window": {windowStr},
		"field":  {"accountID"},
		"search": {" aws "},
	})
	got, err := ParseRequest(qp, autocomplete.ParseOptions{})
	if err != nil {
		t.Fatalf("ParseRequest() error = %v", err)
	}
	if got.Field != "accountID" || got.Search != "aws" {
		t.Fatalf("unexpected request: %+v", got)
	}

	_, err = ParseRequest(httputil.NewQueryParams(map[string][]string{"field": {"accountID"}}), autocomplete.ParseOptions{})
	if err == nil || !errors.Is(err, autocomplete.ErrBadRequest) {
		t.Fatalf("expected missing window error, got %v", err)
	}
}
