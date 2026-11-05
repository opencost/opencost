package scaleway

import (
	"testing"
)

func TestScalewayRefreshCustomPricing(t *testing.T) {
	c := &Scaleway{}
	if err := c.RefreshCustomPricing(); err != nil {
		t.Fatalf("Scaleway.RefreshCustomPricing() unexpected error: %v", err)
	}
}
