package configrbac

// ResolvePolicy computes effective scoped views for a Clerk user sub.
func (s *Service) ResolvePolicy(userID string) (PolicyResponse, error) {
	if err := s.requireEnabled(); err != nil {
		return PolicyResponse{}, err
	}
	userID = trimID(userID)
	if userID == "" {
		return PolicyResponse{}, errUserIDRequired
	}

	views, err := s.store.List()
	if err != nil {
		return PolicyResponse{}, err
	}

	hasUserBucket, err := userAppearsInAnyScopedViewBucket(s.store, userID)
	if err != nil {
		return PolicyResponse{}, err
	}
	isNewUser := !hasUserBucket

	resp := PolicyResponse{
		UserID: userID,
		Views:  []PolicyResolvedView{},
	}

	for _, view := range views {
		mode := resolveViewMode(view, userID, isNewUser)
		if mode == "" {
			continue
		}
		filters := view.Filters
		if filters == nil {
			filters = []ScopedViewFilterRow{}
		}
		resp.Views = append(resp.Views, PolicyResolvedView{
			ID:      view.ID,
			Name:    view.Name,
			Mode:    mode,
			Filters: filters,
		})
	}
	return resp, nil
}

func resolveViewMode(view ScopedView, userID string, isNewUser bool) PolicyViewMode {
	if contains(view.Users.StrictlyEnabledFor, userID) {
		return PolicyModeStrictlyEnabled
	}
	if contains(view.Users.EnforcedFor, userID) {
		return PolicyModeEnforced
	}
	if contains(view.Users.EnabledByDefaultFor, userID) {
		return PolicyModeEnabledByDefault
	}
	if contains(view.Users.AvailableFor, userID) {
		return PolicyModeAvailable
	}
	if isNewUser {
		if view.ApplyToNewUsers.StrictlyEnabledFor {
			return PolicyModeStrictlyEnabled
		}
		if view.ApplyToNewUsers.EnforcedFor {
			return PolicyModeEnforced
		}
		if view.ApplyToNewUsers.EnabledByDefaultFor {
			return PolicyModeEnabledByDefault
		}
		if view.ApplyToNewUsers.AvailableFor {
			return PolicyModeAvailable
		}
	}
	return ""
}

func contains(list []string, id string) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}

func userAppearsInAnyScopedViewBucket(store *Store, userID string) (bool, error) {
	views, err := store.List()
	if err != nil {
		return false, err
	}
	for _, v := range views {
		if contains(v.Users.AvailableFor, userID) ||
			contains(v.Users.EnforcedFor, userID) ||
			contains(v.Users.EnabledByDefaultFor, userID) ||
			contains(v.Users.StrictlyEnabledFor, userID) {
			return true, nil
		}
	}
	return false, nil
}
