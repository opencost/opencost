package collections

import (
	"errors"
	"fmt"
	"iter"
)

var (
	// ErrEmptyID is returned when the provided entry into an IdNameMap returns an empty string
	// for ID
	ErrEmptyID error = errors.New("id must be non-empty")

	// ErrEmptyName is returned when the provided entry into an IdNameMap returns an empty string
	// for Name
	ErrEmptyName error = errors.New("name must be non-empty")
)

// WithIdName is a generic constraint required for elements added to a `IdNameMap`
type WithIdName interface {
	Id() string
	Name() string
}

// IdNameMap contains two maps which alias the same element by id and name. It provides O(1) lookups
// by identifier or by name, both a required constraint on the `T` type.
type IdNameMap[T WithIdName] struct {
	m map[string]T
	r map[string]T
}

func NewIdNameMap[T WithIdName]() *IdNameMap[T] {
	return &IdNameMap[T]{
		m: make(map[string]T),
		r: make(map[string]T),
	}
}

// Insert inserts a `T` instance into the map successfully under the following requirements:
//
// Insertion of new Entry:
//  1. IDs and Name for the `T` instance must be non-empty.
//  2. ID and Name must not partially overlap with an existing entry. This would happen if
//     you attempted to insert a `T` with a unique ID, but a conflicting Name. Likewise,
//     a unique name, but conflicting ID will also fail.
//
// Replacing an existing Entry:
//  1. If there exists an old entry with the id of the new entry, then the name for the new
//     entry must also point to the old entry.
//  2. If there exists an old entry with the name of the new entry, then the id for the new
//     entry must also point to the old entry.
//
// To summarize, you can replace an existing item as long as the id/name lookups for the entry
// being replaced are the same.
func (rm *IdNameMap[T]) Insert(item T) error {
	id := item.Id()
	if id == "" {
		return ErrEmptyID
	}

	name := item.Name()
	if name == "" {
		return ErrEmptyName
	}

	oldForId, idExists := rm.m[id]
	oldForName, nameExists := rm.r[name]

	// check partial insertion of id
	if idExists && !nameExists {
		return fmt.Errorf(
			"insertion of new entry: [id: %s, name: %s] would partially overwrite existing entry: [id: %s, name: %s]",
			id,
			name,
			oldForId.Id(),
			oldForId.Name(),
		)
	}

	// check partial insertion of name
	if !idExists && nameExists {
		return fmt.Errorf(
			"insertion of new entry: [id: %s, name: %s] would partially overwrite existing entry: [id: %s, name: %s]",
			id,
			name,
			oldForName.Id(),
			oldForName.Name(),
		)
	}

	// if we are overwriting, check to ensure that the entities from each map have identical mappings
	if idExists && nameExists {
		if oldForId.Id() != oldForName.Id() || oldForId.Name() != oldForName.Name() {
			return fmt.Errorf(
				"attempting to overwrite entries [id: %s, name: %s] and [id: %s, name: %s] with new entry [id: %s, name: %s] creating a multi-entry conflict",
				oldForId.Id(),
				oldForId.Name(),
				oldForName.Id(),
				oldForName.Name(),
				id,
				name,
			)
		}
	}

	rm.m[id] = item
	rm.r[name] = item

	return nil
}

func (rm *IdNameMap[T]) ById(id string) (T, bool) {
	item, ok := rm.m[id]
	return item, ok
}

func (rm *IdNameMap[T]) ByName(name string) (T, bool) {
	item, ok := rm.r[name]
	return item, ok
}

func (rm *IdNameMap[T]) RemoveById(id string) bool {
	item, ok := rm.ById(id)
	if !ok {
		return false
	}

	name := item.Name()
	delete(rm.m, id)
	delete(rm.r, name)

	return true
}

func (rm *IdNameMap[T]) RemoveByName(name string) bool {
	item, ok := rm.ByName(name)
	if !ok {
		return false
	}

	id := item.Id()
	delete(rm.m, id)
	delete(rm.r, name)

	return true
}

func (rm *IdNameMap[T]) Keys() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for id, value := range rm.m {
			name := value.Name()
			if !yield(id, name) {
				return
			}
		}
	}
}

func (rm *IdNameMap[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, value := range rm.m {
			if !yield(value) {
				return
			}
		}
	}
}
