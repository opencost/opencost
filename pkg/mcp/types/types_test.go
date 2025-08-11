package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWindowString(t *testing.T) {
	testCases := []struct {
		name     string
		window   Window
		expected string
	}{
		{
			name:     "Empty window",
			window:   Window{},
			expected: "today",
		},
		{
			name: "Valid window",
			window: Window{
				Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC),
			},
			expected: "2025-01-01T00:00:00Z,2025-01-07T00:00:00Z",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.window.String()
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSessionStructure(t *testing.T) {
	session := Session{
		ID:                 "test-123",
		StartTime:          time.Now(),
		LastActivity:       time.Now(),
		QueryHistory:       []QueryHistoryItem{},
		ActiveFilters:      make(map[string]interface{}),
		PreferredUnits:     "USD",
		UserExpertiseLevel: "intermediate",
	}

	assert.NotEmpty(t, session.ID)
	assert.Equal(t, "USD", session.PreferredUnits)
	assert.NotNil(t, session.ActiveFilters)
}

func TestInsightStructure(t *testing.T) {
	insight := Insight{
		Type:        "optimization",
		Severity:    "high",
		Title:       "High Cost Detected",
		Description: "Namespace 'production' costs increased by 150%",
		Confidence:  0.85,
		ActionItems: []string{"Review resource allocation", "Check for unused resources"},
	}

	assert.Equal(t, "optimization", insight.Type)
	assert.Equal(t, "high", insight.Severity)
	assert.Len(t, insight.ActionItems, 2)
	assert.InDelta(t, 0.85, insight.Confidence, 0.01)
}