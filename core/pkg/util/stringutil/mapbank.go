package stringutil

import "sync"

type stringBank struct {
	lock sync.Mutex
	m    map[string]string
}

func NewStringBank() StringBank {
	return &stringBank{
		m: make(map[string]string),
	}
}

func (sb *stringBank) LoadOrStore(key, value string) (string, bool) {
	sb.lock.Lock()

	if v, ok := sb.m[key]; ok {
		sb.lock.Unlock()
		return v, ok
	}

	sb.m[value] = value
	sb.lock.Unlock()
	return value, false
}

func (sb *stringBank) LoadOrStoreFunc(key string, f func() string) (string, bool) {
	sb.lock.Lock()

	if v, ok := sb.m[key]; ok {
		sb.lock.Unlock()
		return v, ok
	}

	// create the key and value using the func (the key could be deallocated later)
	value := f()
	sb.m[value] = value
	sb.lock.Unlock()
	return value, false
}

func (sb *stringBank) Clear() {
	sb.lock.Lock()
	sb.m = make(map[string]string)
	sb.lock.Unlock()
}
