package external

type LabelProvider interface {
	Update(name string, data map[string]string) error
	Labels() (map[string]string, error)
}
