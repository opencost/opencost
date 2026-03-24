package stringutil

import (
	"container/heap"
	"sync"
	"time"
)

type lruEntry struct {
	value string
	used  time.Time
}

type heapEntry struct {
	*lruEntry
	key string
}

type maxHeap []*heapEntry

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return h[i].used.After(h[j].used) } // newer = "larger"
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *maxHeap) Push(x any) {
	*h = append(*h, x.(*heapEntry))
}

func (h *maxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func nOldest(arr []*heapEntry, n int) []*heapEntry {
	if n <= 0 {
		return []*heapEntry{}
	}

	if n >= len(arr) {
		return arr
	}

	h := maxHeap(arr[:n])
	heap.Init(&h)

	for _, entry := range arr[n:] {
		// swap in oldest, re-heapify
		if entry.used.Before(h[0].used) {
			h[0] = entry
			heap.Fix(&h, 0)
		}
	}

	return []*heapEntry(h)
}

type lruStringBank struct {
	lock sync.Mutex
	stop chan struct{}
	m    map[string]*lruEntry
}

func NewLruStringBank(capacity int, evictionInterval time.Duration) StringBank {
	stop := make(chan struct{})
	bank := &lruStringBank{
		m: make(map[string]*lruEntry),
	}

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(evictionInterval):
			}

			// need to take the lock during eviction
			bank.lock.Lock()
			if len(bank.m) <= capacity {
				bank.lock.Unlock()
				continue
			}

			// we collect a list of all lru entries so we can max heap the first n elements
			arr := make([]*heapEntry, 0, len(bank.m))
			for k, v := range bank.m {
				arr = append(arr, &heapEntry{key: k, lruEntry: v})
			}

			oldest := nOldest(arr, len(bank.m)-capacity)
			for _, old := range oldest {
				delete(bank.m, old.key)
			}
			bank.lock.Unlock()
		}
	}()

	return bank
}

func (sb *lruStringBank) Stop() {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.stop != nil {
		close(sb.stop)
		sb.stop = nil
	}
}

func (sb *lruStringBank) LoadOrStore(key, value string) (string, bool) {
	sb.lock.Lock()

	if v, ok := sb.m[key]; ok {
		v.used = time.Now().UTC()
		sb.lock.Unlock()
		return v.value, ok
	}

	sb.m[key] = &lruEntry{
		value: value,
		used:  time.Now().UTC(),
	}
	sb.lock.Unlock()
	return value, false
}

func (sb *lruStringBank) LoadOrStoreFunc(key string, f func() string) (string, bool) {
	sb.lock.Lock()

	if v, ok := sb.m[key]; ok {
		v.used = time.Now().UTC()
		sb.lock.Unlock()
		return v.value, ok
	}

	// create the key and value using the func (the key could be deallocated later)
	value := f()
	sb.m[value] = &lruEntry{
		value: value,
		used:  time.Now().UTC(),
	}
	sb.lock.Unlock()
	return value, false
}

func (sb *lruStringBank) Clear() {
	sb.lock.Lock()
	sb.m = make(map[string]*lruEntry)
	sb.lock.Unlock()
}
