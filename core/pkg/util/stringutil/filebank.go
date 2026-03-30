package stringutil

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// fileLruEntry tracks the map key (lookup key) separately from the canonical value
// so eviction can persist and rehydrate correctly when key != value.
type fileLruEntry struct {
	key   string
	value string
	used  int64
}

type fileStringBank struct {
	lock     sync.Mutex
	stop     chan struct{}
	capacity int
	f        *os.File
	m        map[string]*fileLruEntry
	// spill maps a lookup key to a byte offset in f for strings evicted from m.
	spill map[string]int64
}

// NewFileStringBank returns a StringBank that keeps an LRU in memory and persists
// evicted strings in a length-prefixed record file at path. Lookups consult memory,
// then the spill index, then allocate and cache.
func NewFileStringBank(path string, capacity int, evictionInterval time.Duration) (StringBank, error) {
	ff, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("stringutil: open file string bank: %w", err)
	}
	stop := make(chan struct{})
	bank := &fileStringBank{
		f:        ff,
		m:        make(map[string]*fileLruEntry),
		spill:    make(map[string]int64),
		capacity: capacity,
		stop:     stop,
	}

	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(evictionInterval):
			}
			bank.lock.Lock()
			evictFileBank(bank, capacity)
			bank.lock.Unlock()
		}
	}()

	return bank, nil
}

func evictFileBank(bank *fileStringBank, capacity int) {
	if len(bank.m) <= capacity {
		return
	}

	arr := make([]*fileLruEntry, 0, len(bank.m))
	for _, v := range bank.m {
		arr = append(arr, v)
	}

	oldest := nOldestFileEntries(arr, len(bank.m)-capacity)
	for _, old := range oldest {
		if err := bank.persistSpill(old); err != nil {
			// Best effort: leave entry in memory on I/O failure to avoid losing data.
			continue
		}
		delete(bank.m, old.key)
	}
}

func (bank *fileStringBank) persistSpill(e *fileLruEntry) error {
	off, err := bank.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	payload := []byte(e.value)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := bank.f.Write(hdr[:]); err != nil {
		return err
	}

	if _, err := bank.f.Write(payload); err != nil {
		return err
	}
	// if err := bank.f.Sync(); err != nil {
	// 	return err
	// }
	bank.spill[e.key] = off
	return nil
}

func (bank *fileStringBank) readSpill(offset int64) (string, error) {
	if _, err := bank.f.Seek(offset, io.SeekStart); err != nil {
		return "", err
	}
	var hdr [4]byte
	if _, err := io.ReadFull(bank.f, hdr[:]); err != nil {
		return "", err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(bank.f, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func (bank *fileStringBank) loadFromSpill(key string) (string, bool) {
	off, ok := bank.spill[key]
	if !ok {
		return "", false
	}
	s, err := bank.readSpill(off)
	if err != nil {
		return "", false
	}
	return s, true
}

func nOldestFileEntries(arr []*fileLruEntry, n int) []*fileLruEntry {
	if n <= 0 {
		return []*fileLruEntry{}
	}
	if n >= len(arr) {
		return arr
	}
	adapted := make([]*lruEntry, len(arr))
	for i := range arr {
		adapted[i] = &lruEntry{used: arr[i].used}
	}
	oldestIdx := make(map[*lruEntry]int, len(arr))
	for i := range arr {
		oldestIdx[adapted[i]] = i
	}
	oldest := nOldest(adapted, n)
	out := make([]*fileLruEntry, 0, len(oldest))
	for _, o := range oldest {
		out = append(out, arr[oldestIdx[o]])
	}
	return out
}

// Stop ends the background eviction goroutine. Close closes the backing file.
func (bank *fileStringBank) Stop() {
	bank.lock.Lock()
	defer bank.lock.Unlock()
	if bank.stop != nil {
		close(bank.stop)
		bank.stop = nil
	}
}

// Close releases the file handle; Stop is implied for the eviction loop.
func (bank *fileStringBank) Close() error {
	bank.Stop()
	bank.lock.Lock()
	defer bank.lock.Unlock()
	var err error
	if bank.f != nil {
		err = bank.f.Close()
		bank.f = nil
	}
	return err
}

func (bank *fileStringBank) LoadOrStore(key, value string) (string, bool) {
	bank.lock.Lock()
	defer bank.lock.Unlock()

	if v, ok := bank.m[key]; ok {
		v.used = time.Now().UnixMilli()
		return v.value, true
	}
	if s, ok := bank.loadFromSpill(key); ok {
		bank.m[key] = &fileLruEntry{
			key:   key,
			value: s,
			used:  time.Now().UnixMilli(),
		}
		delete(bank.spill, key)
		if len(bank.m) > (bank.capacity + (bank.capacity / 2)) {
			evictFileBank(bank, bank.capacity)
		}
		return s, true
	}

	bank.m[key] = &fileLruEntry{
		key:   key,
		value: value,
		used:  time.Now().UnixMilli(),
	}
	if len(bank.m) > (bank.capacity + (bank.capacity / 2)) {
		evictFileBank(bank, bank.capacity)
	}
	return value, false
}

func (bank *fileStringBank) LoadOrStoreFunc(key string, f func() string) (string, bool) {
	bank.lock.Lock()
	defer bank.lock.Unlock()

	if v, ok := bank.m[key]; ok {
		v.used = time.Now().UnixMilli()
		return v.value, true
	}
	if s, ok := bank.loadFromSpill(key); ok {
		bank.m[key] = &fileLruEntry{
			key:   key,
			value: s,
			used:  time.Now().UnixMilli(),
		}
		delete(bank.spill, key)
		if len(bank.m) > (bank.capacity + (bank.capacity / 2)) {
			evictFileBank(bank, bank.capacity)
		}
		return s, true
	}

	value := f()
	bank.m[value] = &fileLruEntry{
		key:   value,
		value: value,
		used:  time.Now().UnixMilli(),
	}
	if len(bank.m) > (bank.capacity + (bank.capacity / 2)) {
		evictFileBank(bank, bank.capacity)
	}
	return value, false
}

func (bank *fileStringBank) Clear() {
	bank.lock.Lock()
	defer bank.lock.Unlock()

	bank.m = make(map[string]*fileLruEntry)
	bank.spill = make(map[string]int64)
	if bank.f != nil {
		if err := bank.f.Truncate(0); err == nil {
			_, _ = bank.f.Seek(0, io.SeekStart)
		}
	}
}
