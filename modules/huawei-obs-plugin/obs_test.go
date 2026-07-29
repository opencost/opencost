package main

import "testing"

func TestObsEndpoint(t *testing.T) {
	expected := "https://obs.la-south-2.myhuaweicloud.com"
	if got := obsEndpoint("la-south-2"); got != expected {
		t.Fatalf("expected %s, got %s", expected, got)
	}
}
