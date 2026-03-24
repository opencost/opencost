package stringutil

type noOpStringBank struct{}

func NewNoOpStringBank() StringBank {
	return new(noOpStringBank)
}

func (nsb *noOpStringBank) LoadOrStore(key, value string) (string, bool) {
	return value, true
}

func (nsb *noOpStringBank) LoadOrStoreFunc(key string, f func() string) (string, bool) {
	return f(), true
}

func (nsb *noOpStringBank) Clear() {}
