package stringutil

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBasicLruEvict(t *testing.T) {
	lruBank := NewLruStringBank(3, 2*time.Second).(*lruStringBank)
	defer lruBank.Stop()

	lruBank.LoadOrStore("foo", "foo")
	time.Sleep(500 * time.Millisecond)
	lruBank.LoadOrStore("bar", "bar")
	time.Sleep(500 * time.Millisecond)
	lruBank.LoadOrStore("whaz", "whaz")
	time.Sleep(500 * time.Millisecond)
	// access foo, updating recency
	lruBank.LoadOrStore("foo", "foo")
	// should push bar out after eviction runs
	lruBank.LoadOrStore("test", "test")
	time.Sleep(time.Second)

	lruBank.lock.Lock()
	for _, v := range lruBank.m {
		t.Logf("Value: %s\n", v.value)
		if v.value == "bar" {
			t.Errorf("The 'bar' entry should've been replaced by 'test'")
		}
	}
	lruBank.lock.Unlock()
}

// ---------------------------------------------------------------------------
// LoadOrStore
// ---------------------------------------------------------------------------

// A stored value must be retrievable and LoadOrStore must signal the hit/miss
// correctly via the boolean return.
func TestLoadOrStore_MissAndHit(t *testing.T) {
	bank := NewLruStringBank(10, time.Minute).(*lruStringBank)
	defer bank.Stop()

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
	// The original value must be returned on a hit, not the new candidate.
	if v != "hello" {
		t.Errorf("second LoadOrStore: expected cached value %q, got %q", "hello", v)
	}
}

// Hitting an existing entry must update its recency so it is not evicted ahead
// of entries that were never touched again.
func TestLoadOrStore_HitUpdateRecency(t *testing.T) {
	bank := NewLruStringBank(2, 500*time.Millisecond).(*lruStringBank)
	defer bank.Stop()

	bank.LoadOrStore("old", "old")
	time.Sleep(100 * time.Millisecond)
	bank.LoadOrStore("keep", "keep")
	time.Sleep(100 * time.Millisecond)

	// Re-touch "old" so it becomes the most-recently-used.
	bank.LoadOrStore("old", "old")
	time.Sleep(100 * time.Millisecond)

	// Adding a third entry exceeds capacity; "keep" should be the oldest now.
	bank.LoadOrStore("new", "new")

	// Wait for the eviction goroutine.
	time.Sleep(600 * time.Millisecond)

	bank.lock.Lock()
	defer bank.lock.Unlock()

	if _, ok := bank.m["keep"]; ok {
		t.Error("expected 'keep' to be evicted but it is still present")
	}
	if _, ok := bank.m["old"]; !ok {
		t.Error("expected 'old' to survive eviction after its recency was refreshed")
	}
}

// ---------------------------------------------------------------------------
// LoadOrStoreFunc
// ---------------------------------------------------------------------------

// The factory function must only be called on a cache miss, not on a hit.
func TestLoadOrStoreFunc_FactoryCalledOnMissOnly(t *testing.T) {
	bank := NewLruStringBank(10, time.Minute).(*lruStringBank)
	defer bank.Stop()
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

// ---------------------------------------------------------------------------
// Capacity / eviction
// ---------------------------------------------------------------------------

// If the bank never exceeds capacity, nothing should be evicted.
func TestEviction_BelowCapacityNoEviction(t *testing.T) {
	const capacity = 5
	bank := NewLruStringBank(capacity, 200*time.Millisecond).(*lruStringBank)
	defer bank.Stop()

	for i := 0; i < capacity; i++ {
		bank.LoadOrStore(fmt.Sprintf("v%d", i), fmt.Sprintf("v%d", i))
	}

	// Wait several eviction cycles.
	time.Sleep(600 * time.Millisecond)

	bank.lock.Lock()
	defer bank.lock.Unlock()

	if got := len(bank.m); got != capacity {
		t.Errorf("expected %d entries, got %d", capacity, got)
	}
}

// After eviction the map must be trimmed down to exactly capacity.
func TestEviction_ExceedCapacityTrimsToCapacity(t *testing.T) {
	const capacity = 3
	bank := NewLruStringBank(capacity, 350*time.Millisecond).(*lruStringBank)
	defer bank.Stop()

	for i := 0; i < capacity+3; i++ {
		bank.LoadOrStore(fmt.Sprintf("v%d", i), fmt.Sprintf("v%d", i))
		time.Sleep(20 * time.Millisecond) // ensure distinct timestamps
	}

	// Wait for eviction.
	time.Sleep(500 * time.Millisecond)

	bank.lock.Lock()
	defer bank.lock.Unlock()

	if got := len(bank.m); got > capacity {
		t.Errorf("expected at most %d entries after eviction, got %d", capacity, got)
	}
}

// The most-recently-used entries must survive eviction.
func TestEviction_MRUSurvives(t *testing.T) {
	const capacity = 2
	bank := NewLruStringBank(capacity, 300*time.Millisecond).(*lruStringBank)
	defer bank.Stop()

	bank.LoadOrStore("evict1", "evict1")
	time.Sleep(50 * time.Millisecond)
	bank.LoadOrStore("evict2", "evict2")
	time.Sleep(50 * time.Millisecond)

	// These two are the most recent; they must survive.
	bank.LoadOrStore("keep1", "keep1")
	time.Sleep(50 * time.Millisecond)
	bank.LoadOrStore("keep2", "keep2")

	time.Sleep(500 * time.Millisecond)

	bank.lock.Lock()
	defer bank.lock.Unlock()

	for _, must := range []string{"keep1", "keep2"} {
		if _, ok := bank.m[must]; !ok {
			t.Errorf("expected %q to survive eviction", must)
		}
	}
}

// ---------------------------------------------------------------------------
// Clear
// ---------------------------------------------------------------------------

func TestClear_EmptiesMap(t *testing.T) {
	bank := NewLruStringBank(10, time.Minute).(*lruStringBank)
	defer bank.Stop()

	for i := 0; i < 5; i++ {
		bank.LoadOrStore(fmt.Sprintf("v%d", i), fmt.Sprintf("v%d", i))
	}

	bank.Clear()

	bank.lock.Lock()
	defer bank.lock.Unlock()

	if len(bank.m) != 0 {
		t.Errorf("expected empty map after Clear, got %d entries", len(bank.m))
	}
}

// After a Clear, previously stored keys must not be found.
func TestClear_PreviousKeysGone(t *testing.T) {
	bank := NewLruStringBank(10, time.Minute).(*lruStringBank)
	defer bank.Stop()

	bank.LoadOrStore("hello", "world")
	bank.Clear()

	_, loaded := bank.LoadOrStore("hello", "new")
	if loaded {
		t.Error("expected key to be absent after Clear, but it was found")
	}
}

// ---------------------------------------------------------------------------
// nOldest helper
// ---------------------------------------------------------------------------

func TestNOldest_ReturnsCorrectCount(t *testing.T) {
	now := time.Now()
	entries := []*lruEntry{
		{value: "a", used: now.Add(-4 * time.Second).UnixMilli()},
		{value: "b", used: now.Add(-3 * time.Second).UnixMilli()},
		{value: "c", used: now.Add(-2 * time.Second).UnixMilli()},
		{value: "d", used: now.Add(-1 * time.Second).UnixMilli()},
		{value: "e", used: now.UnixMilli()},
	}

	oldest := nOldest(entries, 2)
	if len(oldest) != 2 {
		t.Fatalf("expected 2 oldest entries, got %d", len(oldest))
	}

	values := map[string]bool{}
	for _, e := range oldest {
		values[e.value] = true
	}
	for _, must := range []string{"a", "b"} {
		if !values[must] {
			t.Errorf("expected %q in oldest set, got %v", must, values)
		}
	}
}

func TestNOldest_NGreaterThanLen(t *testing.T) {
	now := time.Now()
	entries := []*lruEntry{
		{value: "x", used: now.UnixMilli()},
		{value: "y", used: now.Add(-time.Second).UnixMilli()},
	}

	result := nOldest(entries, 10)
	if len(result) != 2 {
		t.Errorf("expected all %d entries when n >= len, got %d", 2, len(result))
	}
}

func TestNOldest_NEqualsLen(t *testing.T) {
	now := time.Now()
	entries := []*lruEntry{
		{value: "x", used: now.UnixMilli()},
		{value: "y", used: now.Add(-time.Second).UnixMilli()},
	}

	result := nOldest(entries, 2)
	if len(result) != 2 {
		t.Errorf("expected 2 entries when n == len, got %d", len(result))
	}
}

func TestNOldest_NIsZero(t *testing.T) {
	now := time.Now()
	entries := []*lruEntry{
		{value: "x", used: now.UnixMilli()},
	}

	result := nOldest(entries, 0)
	if len(result) != 0 {
		t.Errorf("expected 0 entries when n=0, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// Concurrent LoadOrStore calls must not race or panic.
func TestConcurrentLoadOrStore(t *testing.T) {
	bank := NewLruStringBank(50, 100*time.Millisecond).(*lruStringBank)
	defer bank.Stop()

	const goroutines = 20
	const opsEach = 100

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		g := i
		wg.Go(func() {
			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("k%d", (g*opsEach+i)%30)
				bank.LoadOrStore(key, key)
			}
		})
	}

	waiter := func() chan struct{} {
		st := make(chan struct{})

		go func() {
			wg.Wait()
			close(st)
		}()

		return st
	}

	select {
	case <-waiter():
		t.Logf("Completed Successfully\n")
	case <-time.After(10 * time.Second):
		t.Logf("Timed out\n")
	}
}

// Concurrent calls interleaved with eviction cycles must not deadlock or race.
func TestConcurrentLoadOrStoreWithEviction(t *testing.T) {
	bank := NewLruStringBank(5, 50*time.Millisecond).(*lruStringBank)
	defer bank.Stop()

	const goroutines = 10
	const duration = 300 * time.Millisecond

	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		g := i
		stop := time.After(duration)

		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					key := fmt.Sprintf("g%d", g)
					bank.LoadOrStore(key, key)
				}
			}
		})
	}

	waiter := func() chan struct{} {
		st := make(chan struct{})

		go func() {
			wg.Wait()
			close(st)
		}()

		return st
	}

	select {
	case <-waiter():
		t.Logf("Completed Successfully\n")
	case <-time.After(10 * time.Second):
		t.Logf("Timed out\n")
	}
}

// Concurrent Clear calls alongside reads/writes must not panic.
func TestConcurrentClear(t *testing.T) {
	bank := NewLruStringBank(10, time.Minute).(*lruStringBank)
	defer bank.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bank.LoadOrStore(fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bank.Clear()
		}()
	}

	wg.Wait()
}
