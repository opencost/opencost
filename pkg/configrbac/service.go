package configrbac

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Service implements RBAC CRUD with config-gated storage.
type Service struct {
	loader *ConfigLoader
	store  *Store
}

// NewService wires config loading and SQLite persistence.
func NewService(loader *ConfigLoader, store *Store) *Service {
	if loader == nil {
		loader = NewConfigLoader()
	}
	if store == nil {
		store = NewStore()
	}
	return &Service{loader: loader, store: store}
}

func (s *Service) requireEnabled() error {
	enabled, err := s.loader.ScopedViewsEnabled()
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	if !enabled {
		return ErrScopedViewsDisabled
	}
	return nil
}

// List returns all scoped views when the API is enabled.
func (s *Service) List() ([]ScopedView, error) {
	if err := s.requireEnabled(); err != nil {
		return nil, err
	}
	return s.store.List()
}

// Get returns one scoped view by id when the API is enabled.
func (s *Service) Get(id string) (ScopedView, error) {
	if err := s.requireEnabled(); err != nil {
		return ScopedView{}, err
	}
	id = trimID(id)
	if id == "" {
		return ScopedView{}, fmt.Errorf("id is required")
	}
	return s.store.Get(id)
}

// Create stores a new scoped view when the API is enabled.
func (s *Service) Create(view ScopedView) (ScopedView, error) {
	if err := s.requireEnabled(); err != nil {
		return ScopedView{}, err
	}
	normalizeScopedView(&view)
	if err := validateScopedView(view); err != nil {
		return ScopedView{}, err
	}
	if view.Filters == nil {
		view.Filters = []ScopedViewFilterRow{}
	}

	exists, err := s.store.Exists(view.ID)
	if err != nil {
		return ScopedView{}, err
	}
	if exists {
		return ScopedView{}, fmt.Errorf("%w: scoped view %q", ErrDuplicateID, view.ID)
	}

	now := nowRFC3339()
	if strings.TrimSpace(view.CreatedAt) == "" {
		view.CreatedAt = now
	}
	view.UpdatedAt = now

	if err := s.store.Create(view); err != nil {
		return ScopedView{}, err
	}
	return view, nil
}

// Update replaces an existing scoped view when the API is enabled.
func (s *Service) Update(id string, view ScopedView) (ScopedView, error) {
	if err := s.requireEnabled(); err != nil {
		return ScopedView{}, err
	}
	id = trimID(id)
	if id == "" {
		return ScopedView{}, fmt.Errorf("id is required")
	}
	if strings.TrimSpace(view.ID) != "" && view.ID != id {
		return ScopedView{}, fmt.Errorf("id in body does not match path")
	}
	view.ID = id
	normalizeScopedView(&view)

	if err := validateScopedView(view); err != nil {
		return ScopedView{}, err
	}
	if view.Filters == nil {
		view.Filters = []ScopedViewFilterRow{}
	}

	existing, err := s.store.Get(id)
	if err != nil {
		return ScopedView{}, err
	}

	view.CreatedAt = existing.CreatedAt
	view.UpdatedAt = nowRFC3339()

	if err := s.store.Update(view); err != nil {
		return ScopedView{}, err
	}
	return view, nil
}

// Delete removes a scoped view when the API is enabled.
func (s *Service) Delete(id string) error {
	if err := s.requireEnabled(); err != nil {
		return err
	}
	id = trimID(id)
	if id == "" {
		return fmt.Errorf("id is required")
	}
	return s.store.Delete(id)
}

// IsNotFound reports whether err is a missing row.
func IsNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
