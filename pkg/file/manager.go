package file

import (
	"io/ioutil"
	"sync"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/storage"
)

//--------------------------------------------------------------------------
//  ManagerOpts
//--------------------------------------------------------------------------

// ManagerOpts describes how to configure the Manager for
// serving configuration fileStores
type ManagerOpts struct {
	// BucketStoreConfig is the local fileName location for the configuration used to
	// write and read configuration data to/from the bucket. The format of this
	// configuration fileName should be compatible with storage.NewBucketStorage
	BucketStoreConfig string

	// LocalConfigPath provides a backup location for storing the configuration
	// fileStores
	LocalConfigPath string
}

// IsBucketStorageEnabled returns true if bucket storage is enabled.
func (cfmo *ManagerOpts) IsBucketStorageEnabled() bool {
	return cfmo.BucketStoreConfig != ""
}

// DefaultManagerOpts returns the default configuration options for the
// config fileName manager
func DefaultManagerOpts() *ManagerOpts {
	return &ManagerOpts{
		BucketStoreConfig: "",
		LocalConfigPath:   "/",
	}
}

//--------------------------------------------------------------------------
//  manager
//--------------------------------------------------------------------------

var _m *manager

// manager is a fascade for a central API used to create and watch
// config fileStores.
type manager struct {
	lock  *sync.RWMutex
	store storage.Storage
	files map[string]any
}

// NewManager creates a new backing storage and configuration fileName manager
func InitManager(opts *ManagerOpts) {
	if _m != nil {
		log.Warningf("fileName store manager is already initialized")
	}

	if opts == nil {
		opts = DefaultManagerOpts()
	}

	var configStore storage.Storage
	if opts.IsBucketStorageEnabled() {
		bucketConfig, err := ioutil.ReadFile(opts.BucketStoreConfig)
		if err != nil {
			log.Warningf("Failed to initialize config bucket storage: %s", err)
		} else {
			bucketStore, err := storage.NewBucketStorage(bucketConfig)
			if err != nil {
				log.Warningf("Failed to create config bucket storage: %s", err)
			} else {
				configStore = bucketStore
			}
		}
	} else {
		configStore = storage.NewFileStorage(opts.LocalConfigPath)
	}

	_m = &manager{
		lock:  new(sync.RWMutex),
		store: configStore,
		files: make(map[string]any),
	}
}

func (m *manager) newFile(key string, newFn func(*manager) any) any {
	m.lock.Lock()
	defer m.lock.Unlock()

	file, ok := m.files[key]
	if !ok {
		return newFn(m)
	}

	return file
}
