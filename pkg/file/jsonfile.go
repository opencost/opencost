package file

import (
	"fmt"
	"github.com/kubecost/cost-model/pkg/storage"
	"github.com/kubecost/cost-model/pkg/util/json"
	"sync"
)

// JSONFileStore generic struct for saving and loading data from fileName system in json format
type jsonFile[T any] struct {
	sync.RWMutex
	fileName string
	storage  storage.Storage
}

// NewJSONFileStore creates a singleton FileStore per provided path to ensure that fileName access is thread safe
func NewJSONFileStore[T any](fileName string, storage storage.Storage) (*jsonFile[T], error) {

	newJSONFileStore := func(m *manager) any {
		return &jsonFile[T]{
			fileName: fileName,
			storage:  m.store,
		}
	}

	file := _m.newFile(fileName, newJSONFileStore)

	jf, ok := file.(*jsonFile[T])
	if !ok {
		return nil, fmt.Errorf("NewJSONFileStore: existing fileName store has a differnt type")
	}

	return jf, nil
}

// Get returns the object of type T stored at the path
func (jf *jsonFile[T]) Get() (T, error) {
	var storedObject T
	if jf == nil {
		return storedObject, fmt.Errorf("JSONFileStore: JSONFileStore is nil")
	}

	jf.RLock()
	defer jf.RUnlock()

	raw, err := jf.storage.Read(jf.fileName)
	if err != nil {
		return storedObject, fmt.Errorf("JSONFileStore: read fileName: %s", err)
	}

	err = json.Unmarshal(raw, &storedObject)
	if err != nil {
		return storedObject, fmt.Errorf("JSONFileStore: unmarshal: %s", err)
	}

	return storedObject, nil
}

// Save saves give object in JSON format on path
func (jf *jsonFile[T]) Save(storedObject T) error {
	if jf == nil {
		return fmt.Errorf("JSONFileStore: JSONFileStore is nil")
	}

	jf.Lock()
	defer jf.Unlock()

	raw, err := json.Marshal(storedObject)
	if err != nil {
		return fmt.Errorf("JSONFileStore: error marshaling data: %s", err)
	}

	err = jf.storage.Write(jf.fileName, raw)
	if err != nil {
		return fmt.Errorf("JSONFileStore: error writing data: %s", err)
	}
	return nil
}
