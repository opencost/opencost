package stringutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileBank_LoadOrStore_MissAndHit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strings.dat")
	bank, err := NewFileStringBank(path, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bank.(*fileStringBank).Close() }()

	v, loaded := bank.LoadOrStore("hello", "hello")
	if loaded {
		t.Errorf("first LoadOrStore: expected loaded=false, got true")
	}
	if v != "hello" {
		t.Errorf("first LoadOrStore: expected value %q, got %q", "hello", v)
	}

	v, loaded = bank.LoadOrStore("hello", "world")
	if !loaded {
		t.Errorf("second LoadOrStore: expected loaded=true, got false")
	}
	if v != "hello" {
		t.Errorf("second LoadOrStore: expected cached value %q, got %q", "hello", v)
	}
}

func TestFileBank_EvictionPersistsAndReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strings.dat")
	maxBytes := 4 // 2 entries of size len(key)+len(value)==2 each
	bank, err := NewFileStringBank(path, maxBytes, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bank.(*fileStringBank).Close() }()

	bank.LoadOrStore("a", "a")
	time.Sleep(50 * time.Millisecond)
	bank.LoadOrStore("b", "b")
	time.Sleep(50 * time.Millisecond)
	bank.LoadOrStore("c", "c")
	time.Sleep(50 * time.Millisecond)
	// force eviction over capacity
	bank.LoadOrStore("d", "d")

	time.Sleep(400 * time.Millisecond)

	fb := bank.(*fileStringBank)
	fb.lock.Lock()
	inMemBytes := fb.currentSize
	fb.lock.Unlock()
	if inMemBytes > maxBytes {
		t.Fatalf("expected in-memory bytes <= %d after eviction, got %d", maxBytes, inMemBytes)
	}

	// Oldest entries should be recoverable from file-backed spill.
	v, loaded := bank.LoadOrStore("a", "a")
	if !loaded {
		t.Error("expected hit for 'a' via spill after eviction")
	}
	if v != "a" {
		t.Errorf("expected value %q from spill, got %q", "a", v)
	}

	fb.lock.Lock()
	_, stillSpill := fb.spill["a"]
	fb.lock.Unlock()
	if stillSpill {
		t.Error("after promoting 'a' into cache, it should be removed from spill index")
	}
}

func TestFileBank_EvictionUsesByteSizeNotEntryCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strings.dat")
	maxBytes := 10
	bank, err := NewFileStringBank(path, maxBytes, 150*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bank.(*fileStringBank).Close() }()

	bank.LoadOrStore("k1", "aaaaaa") // size 8
	time.Sleep(30 * time.Millisecond)
	bank.LoadOrStore("k2", "bb") // size 4, total 12 > 10

	time.Sleep(250 * time.Millisecond)

	fb := bank.(*fileStringBank)
	fb.lock.Lock()
	defer fb.lock.Unlock()
	if fb.currentSize > maxBytes {
		t.Fatalf("expected in-memory bytes <= %d, got %d", maxBytes, fb.currentSize)
	}
}

func TestFileBank_LoadOrStoreFunc_FactoryCalledOnMissOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strings.dat")
	bank, err := NewFileStringBank(path, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bank.(*fileStringBank).Close() }()

	calls := 0
	factory := func() string {
		calls++
		return "k"
	}

	bank.LoadOrStoreFunc("k", factory)
	bank.LoadOrStoreFunc("k", factory)

	if calls != 1 {
		t.Errorf("factory should be called exactly once, got %d calls", calls)
	}
}

func TestFileBank_Clear(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strings.dat")
	bank, err := NewFileStringBank(path, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bank.(*fileStringBank).Close() }()

	bank.LoadOrStore("x", "x")
	bank.Clear()

	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 0 {
		t.Errorf("expected truncated file after Clear, size=%d", st.Size())
	}

	_, loaded := bank.LoadOrStore("x", "y")
	if loaded {
		t.Error("expected miss after Clear")
	}
}

func TestFileBank_NewOpenError(t *testing.T) {
	// Non-directory path that cannot be created as a file parent.
	_, err := NewFileStringBank("/nonexistent/dir/bank.dat", 3, time.Minute)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}
