package collections

import "iter"

// WithIdName is a generic constraint required for elements added to a `ReverseMap`
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

func (rm *IdNameMap[T]) Insert(item T) {
	id := item.Id()
	name := item.Name()

	rm.m[id] = item
	rm.r[name] = item
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
	delete(rm.m, name)

	return true
}

func (rm *IdNameMap[T]) RemoveByName(name string) bool {
	item, ok := rm.ByName(name)
	if !ok {
		return false
	}

	id := item.Id()
	delete(rm.m, id)
	delete(rm.m, name)

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
