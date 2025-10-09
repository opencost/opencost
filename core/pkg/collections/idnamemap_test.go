package collections

import (
	"errors"
	"testing"
)

type testItem struct {
	id    string
	name  string
	value int
}

func (t testItem) Id() string {
	return t.id
}

func (t testItem) Name() string {
	return t.name
}

func TestNewIdNameMap(t *testing.T) {
	m := NewIdNameMap[testItem]()
	if m == nil {
		t.Fatal("NewIdNameMap returned nil")
	}
	if m.m == nil {
		t.Fatal("internal id map is nil")
	}
	if m.r == nil {
		t.Fatal("internal name map is nil")
	}
}

func TestIdNameMap_Insert(t *testing.T) {
	m := NewIdNameMap[testItem]()
	item := testItem{id: "id1", name: "name1"}

	m.Insert(item)

	// Verify item can be retrieved by id
	retrieved, ok := m.ById("id1")
	if !ok {
		t.Fatal("item not found by id after insert")
	}
	if retrieved.Id() != "id1" || retrieved.Name() != "name1" {
		t.Errorf("retrieved item mismatch: got %+v, want %+v", retrieved, item)
	}

	// Verify item can be retrieved by name
	retrieved, ok = m.ByName("name1")
	if !ok {
		t.Fatal("item not found by name after insert")
	}
	if retrieved.Id() != "id1" || retrieved.Name() != "name1" {
		t.Errorf("retrieved item mismatch: got %+v, want %+v", retrieved, item)
	}
}

func TestIdNameMap_InsertEmptyIdAndName(t *testing.T) {
	m := NewIdNameMap[testItem]()

	item1 := testItem{id: "", name: "name1"}
	item2 := testItem{id: "id1", name: ""}

	err := m.Insert(item1)
	if err == nil {
		t.Fatalf("Expected insertion failure, but succeeded!")
	}
	if !errors.Is(err, ErrEmptyID) {
		t.Fatalf("Expected ErrEmptyID, but instead got: %s", err.Error())
	}

	err = m.Insert(item2)
	if err == nil {
		t.Fatalf("Expected insertion failure, but succeeded!")
	}
	if !errors.Is(err, ErrEmptyName) {
		t.Fatalf("Expected ErrEmptyName, but instead got: %s", err.Error())
	}
}

func TestIdNameMap_InsertDuplicateId(t *testing.T) {
	m := NewIdNameMap[testItem]()
	item1 := testItem{id: "id1", name: "name1"}
	item2 := testItem{id: "id1", name: "name2"}
	item3 := testItem{id: "id1", name: "name1", value: 10}

	err := m.Insert(item1)
	if err != nil {
		t.Fatalf("%s", err.Error())
		return
	}

	// should fail
	err = m.Insert(item2)
	if err == nil {
		t.Fatalf("Expected insertion to fail, but it succeeded!")
	} else {
		t.Logf("%s", err.Error())
	}

	err = m.Insert(item3)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	// Verify second item overwrote the first
	retrieved, ok := m.ById("id1")
	if !ok {
		t.Fatal("item not found by id")
	}
	if retrieved.Name() != "name1" {
		t.Errorf("expected name1, got %s", retrieved.Name())
	}

	// Failed name should not exist
	_, ok = m.ByName("name2")
	if ok {
		t.Error("old name should not exist after overwrite")
	}

	// Second name should exist
	retrieved, ok = m.ByName("name1")
	if !ok {
		t.Fatal("new name not found")
	}
	if retrieved.Id() != "id1" {
		t.Errorf("expected id1, got %s", retrieved.Id())
	}
	if retrieved.value != 10 {
		t.Errorf("expected 10, got: %d", retrieved.value)
	}
}

func TestIdNameMap_InsertDuplicateName(t *testing.T) {
	m := NewIdNameMap[testItem]()
	item1 := testItem{id: "id1", name: "name1"}
	item2 := testItem{id: "id2", name: "name1"}
	item3 := testItem{id: "id1", name: "name1", value: 10}

	err := m.Insert(item1)
	if err != nil {
		t.Fatalf("%s", err.Error())
		return
	}

	// expect error
	err = m.Insert(item2)
	if err == nil {
		t.Fatalf("Expected insertion to fail, but it succeeded!")
	} else {
		t.Log(err)
	}

	// overwrite
	err = m.Insert(item3)
	if err != nil {
		t.Fatal(err)
	}

	// Verify second item overwrote the first by name
	retrieved, ok := m.ByName("name1")
	if !ok {
		t.Fatal("item not found by name")
	}
	if retrieved.Id() != "id1" {
		t.Errorf("expected id1, got %s", retrieved.Id())
	}

	// id2 shouldn't exist
	_, ok = m.ById("id2")
	if ok {
		t.Error("id2 failed to insert, so should not exist")
	}

	retrieved, ok = m.ById("id1")
	if !ok {
		t.Fatal("id1 not found")
	}
	if retrieved.Name() != "name1" {
		t.Errorf("expected name1, got %s", retrieved.Name())
	}
	if retrieved.value != 10 {
		t.Errorf("expected value: 10, got %d", retrieved.value)
	}
}

func TestIdNameMap_InsertTrickyPartialOverwrites(t *testing.T) {
	m := NewIdNameMap[testItem]()

	// 2 unique items
	item1 := testItem{id: "id1", name: "foo"}
	item2 := testItem{id: "id2", name: "bar"}

	// item overlaps id from previous entry, and name from another previous entry
	item3 := testItem{id: "id1", name: "bar"}

	err := m.Insert(item1)
	if err != nil {
		t.Fatal(err)
	}

	err = m.Insert(item2)
	if err != nil {
		t.Fatal(err)
	}

	err = m.Insert(item3)
	if err == nil {
		t.Fatalf("Expected to fail, but insert succeeded!")
	} else {
		t.Log(err)
	}

	if !m.RemoveById("id1") {
		t.Fatalf("Expected to remove entry with id: 'id1', but failed")
	}

	// this will _still_ be a partial insert, so expect failure
	err = m.Insert(item3)
	if err == nil {
		t.Fatalf("Expected to fail, but insert succeeded!")
	} else {
		t.Log(err)
	}

	if !m.RemoveByName("bar") {
		t.Fatalf("Expected to remove entry with name: 'bar', but failed")
	}

	// this should now succeed!
	err = m.Insert(item3)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIdNameMap_ByIdNotFound(t *testing.T) {
	m := NewIdNameMap[testItem]()

	_, ok := m.ById("nonexistent")
	if ok {
		t.Error("expected false for nonexistent id")
	}
}

func TestIdNameMap_ByNameNotFound(t *testing.T) {
	m := NewIdNameMap[testItem]()

	_, ok := m.ByName("nonexistent")
	if ok {
		t.Error("expected false for nonexistent name")
	}
}

func TestIdNameMap_RemoveById(t *testing.T) {
	m := NewIdNameMap[testItem]()
	item := testItem{id: "id1", name: "name1"}

	m.Insert(item)

	// Verify item exists
	_, ok := m.ById("id1")
	if !ok {
		t.Fatal("item should exist before removal")
	}
	_, ok = m.ByName("name1")
	if !ok {
		t.Fatal("item should exist before removal")
	}

	// Remove by id
	removed := m.RemoveById("id1")
	if !removed {
		t.Error("RemoveById should return true for existing item")
	}

	// Verify item no longer exists
	_, ok = m.ById("id1")
	if ok {
		t.Error("item should not exist after removal by id")
	}
	_, ok = m.ByName("name1")
	if ok {
		t.Error("item should not exist after removal by id")
	}
}

func TestIdNameMap_RemoveByIdNotFound(t *testing.T) {
	m := NewIdNameMap[testItem]()

	removed := m.RemoveById("nonexistent")
	if removed {
		t.Error("RemoveById should return false for nonexistent item")
	}
}

func TestIdNameMap_RemoveByName(t *testing.T) {
	m := NewIdNameMap[testItem]()
	item := testItem{id: "id1", name: "name1"}

	m.Insert(item)

	// Verify item exists
	_, ok := m.ById("id1")
	if !ok {
		t.Fatal("item should exist before removal")
	}
	_, ok = m.ByName("name1")
	if !ok {
		t.Fatal("item should exist before removal")
	}

	// Remove by name
	removed := m.RemoveByName("name1")
	if !removed {
		t.Error("RemoveByName should return true for existing item")
	}

	// Verify item no longer exists
	_, ok = m.ById("id1")
	if ok {
		t.Error("item should not exist after removal by name")
	}
	_, ok = m.ByName("name1")
	if ok {
		t.Error("item should not exist after removal by name")
	}
}

func TestIdNameMap_RemoveByNameNotFound(t *testing.T) {
	m := NewIdNameMap[testItem]()

	removed := m.RemoveByName("nonexistent")
	if removed {
		t.Error("RemoveByName should return false for nonexistent item")
	}
}

func TestIdNameMap_Keys(t *testing.T) {
	m := NewIdNameMap[testItem]()
	items := []testItem{
		{id: "id1", name: "name1"},
		{id: "id2", name: "name2"},
		{id: "id3", name: "name3"},
	}

	for _, item := range items {
		m.Insert(item)
	}

	keys := make(map[string]string)
	for id, name := range m.Keys() {
		keys[id] = name
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	for _, item := range items {
		name, ok := keys[item.id]
		if !ok {
			t.Errorf("missing key for id %s", item.id)
		}
		if name != item.name {
			t.Errorf("wrong name for id %s: got %s, want %s", item.id, name, item.name)
		}
	}
}

func TestIdNameMap_Values(t *testing.T) {
	m := NewIdNameMap[testItem]()
	items := []testItem{
		{id: "id1", name: "name1"},
		{id: "id2", name: "name2"},
		{id: "id3", name: "name3"},
	}

	for _, item := range items {
		m.Insert(item)
	}

	values := make(map[string]testItem)
	for value := range m.Values() {
		values[value.Id()] = value
	}

	if len(values) != 3 {
		t.Errorf("expected 3 values, got %d", len(values))
	}

	for _, item := range items {
		value, ok := values[item.id]
		if !ok {
			t.Errorf("missing value for id %s", item.id)
		}
		if value.Id() != item.id || value.Name() != item.name {
			t.Errorf("wrong value for id %s: got %+v, want %+v", item.id, value, item)
		}
	}
}

func TestIdNameMap_EmptyIterators(t *testing.T) {
	m := NewIdNameMap[testItem]()

	// Test empty Keys iterator
	count := 0
	for range m.Keys() {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 keys in empty map, got %d", count)
	}

	// Test empty Values iterator
	count = 0
	for range m.Values() {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 values in empty map, got %d", count)
	}
}

func TestIdNameMap_IteratorEarlyBreak(t *testing.T) {
	m := NewIdNameMap[testItem]()
	items := []testItem{
		{id: "id1", name: "name1"},
		{id: "id2", name: "name2"},
		{id: "id3", name: "name3"},
	}

	for _, item := range items {
		m.Insert(item)
	}

	// Test early break in Keys iterator
	count := 0
	for range m.Keys() {
		count++
		if count == 1 {
			break
		}
	}
	if count != 1 {
		t.Errorf("expected early break to work in Keys iterator, got count %d", count)
	}

	// Test early break in Values iterator
	count = 0
	for range m.Values() {
		count++
		if count == 1 {
			break
		}
	}
	if count != 1 {
		t.Errorf("expected early break to work in Values iterator, got count %d", count)
	}
}
