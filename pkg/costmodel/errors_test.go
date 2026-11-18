package costmodel

import (
	"errors"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestQueryAllocation_PARCWithoutIdle_IsBadRequest(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Hour)
	window := opencost.NewClosedWindow(end.Add(-24*time.Hour), end)

	cm := &CostModel{}
	_, err := cm.QueryAllocation(window, time.Hour, nil, false, false, true, false, false, opencost.AccumulateOptionNone, false, "")
	if err == nil {
		t.Fatal("expected error when includeProportionalAssetResourceCosts is set without includeIdle")
	}
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("expected error to wrap ErrBadRequest, got: %v", err)
	}

	want := "bad request - includeIdle must be set true if includeProportionalAssetResourceCosts is true"
	if err.Error() != want {
		t.Fatalf("error message changed:\n got: %q\nwant: %q", err.Error(), want)
	}
}
