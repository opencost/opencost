package file

// BasePath for many fileStores in kubecost
const BasePath = "/var/configs/"

// FileStore is an interface for storing objects fileName directory
type File[T any] interface {
	Get() (T, error)
	Save(T) error
}
