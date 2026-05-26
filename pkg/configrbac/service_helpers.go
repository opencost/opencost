package configrbac

import (
	"errors"
	"fmt"
	"strings"
)

var errUserIDRequired = errors.New("userId is required")

func trimID(id string) string {
	return strings.TrimSpace(id)
}

func normalizeScopedView(view *ScopedView) {
	view.ID = trimID(view.ID)
	view.Name = trimID(view.Name)
	normalizeUserBuckets(&view.Users)
}

func normalizeUserBuckets(b *ScopedViewUserBuckets) {
	if b.AvailableFor == nil {
		b.AvailableFor = []string{}
	}
	if b.EnforcedFor == nil {
		b.EnforcedFor = []string{}
	}
	if b.EnabledByDefaultFor == nil {
		b.EnabledByDefaultFor = []string{}
	}
	if b.StrictlyEnabledFor == nil {
		b.StrictlyEnabledFor = []string{}
	}
}

func validateScopedView(view ScopedView) error {
	if view.ID == "" {
		return fmt.Errorf("id is required")
	}
	if view.Name == "" {
		return fmt.Errorf("name is required")
	}
	return nil
}
