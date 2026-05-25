package configrbac

// ScopedViewFilterRow matches the UI scoped view filter shape.
type ScopedViewFilterRow struct {
	ID       string `json:"id"`
	Dataset  string `json:"dataset"`
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

// ScopedViewUserBuckets assigns users to scoped view modes.
type ScopedViewUserBuckets struct {
	AvailableFor        []string `json:"availableFor"`
	EnforcedFor         []string `json:"enforcedFor"`
	EnabledByDefaultFor []string `json:"enabledByDefaultFor"`
	StrictlyEnabledFor  []string `json:"strictlyEnabledFor"`
}

// ScopedViewApplyNewUsers controls default assignment for new users.
type ScopedViewApplyNewUsers struct {
	AvailableFor        bool `json:"availableFor"`
	EnforcedFor         bool `json:"enforcedFor"`
	EnabledByDefaultFor bool `json:"enabledByDefaultFor"`
	StrictlyEnabledFor  bool `json:"strictlyEnabledFor"`
}

// ScopedView is the persisted scoped view object exchanged with the API.
type ScopedView struct {
	ID              string                  `json:"id"`
	Name            string                  `json:"name"`
	Filters         []ScopedViewFilterRow   `json:"filters"`
	Users           ScopedViewUserBuckets   `json:"users"`
	ApplyToNewUsers ScopedViewApplyNewUsers `json:"applyToNewUsers"`
	CreatedAt       string                  `json:"createdAt"`
	UpdatedAt       string                  `json:"updatedAt,omitempty"`
}

// PolicyViewMode is how a scoped view applies to a user.
type PolicyViewMode string

const (
	PolicyModeAvailable        PolicyViewMode = "available"
	PolicyModeEnabledByDefault PolicyViewMode = "enabledByDefault"
	PolicyModeEnforced         PolicyViewMode = "enforced"
	PolicyModeStrictlyEnabled  PolicyViewMode = "strictlyEnabled"
)

// PolicyResolvedView is a scoped view with its effective mode for a user.
type PolicyResolvedView struct {
	ID      string                `json:"id"`
	Name    string                `json:"name"`
	Mode    PolicyViewMode        `json:"mode"`
	Filters []ScopedViewFilterRow `json:"filters"`
}

// PolicyResponse is returned by GET /config/rbac/policy/users/:userId.
type PolicyResponse struct {
	UserID string               `json:"userId"`
	Views  []PolicyResolvedView `json:"views"`
}
