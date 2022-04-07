package file

import (
	"errors"
	"fmt"
	"github.com/kubecost/cost-model/pkg/util/json"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/storage"
	"github.com/kubecost/cost-model/pkg/util/atomic"
)

// HandlerID is a unique identifier assigned to a provided ConfigChangedHandler. This is used to remove a handler
// from the ConfigFile when it is no longer needed.
type HandlerID string

//--------------------------------------------------------------------------
//  ChangeType
//--------------------------------------------------------------------------

// ChangeType is used to specifically categorize the change that was made on a ConfigFile
type ChangeType string

// ChangeType constants contain the different types of updates passed through the ConfigChangedHandler
const (
	ChangeTypeCreated  ChangeType = "created"
	ChangeTypeModified ChangeType = "modified"
	ChangeTypeDeleted  ChangeType = "deleted"
)

//--------------------------------------------------------------------------
//  ConfigChangedHandler
//--------------------------------------------------------------------------

// ConfigChangedHandler is the func handler used to receive change updates about the
// config fileName. Both ChangeTypeCreated and ChangeTypeModified yield a valid []byte, while
// ChangeTypeDeleted yields a nil []byte.
type ConfigChangedHandler func(ChangeType, []byte)

//--------------------------------------------------------------------------
//  ConfigFile
//--------------------------------------------------------------------------

// DefaultHandlerPriority is used as the priority for any handlers added via AddChangeHandler
const DefaultHandlerPriority int = 1000

// NoBackingStore error is used when the config fileName's backing storage is missing
var NoBackingStore error = errors.New("Backing storage does not exist.")

// ConfigFile is representation of a configuration fileName that can be written to, read, and watched
// for updates
type syncJSONFile[T any] struct {
	store      storage.Storage
	fileName   string
	dataLock   *sync.Mutex
	data       *T
	watchLock  *sync.Mutex
	watchers   []*pHandler
	runState   atomic.AtomicRunState
	lastChange time.Time
}

// NewSyncJSONFile creates a new syncJSONFile instance using a specific storage.Storage and path relative
// to the storage.
func NewSyncJSONFile[T any](fileName string) (*syncJSONFile[T], error) {

	newJSONFileStore := func(m *manager) any {
		return &syncJSONFile{
			store:     m.store,
			fileName:  fileName,
			dataLock:  new(sync.Mutex),
			data:      nil,
			watchLock: new(sync.Mutex),
		}
	}

	file := _m.newFile(fileName, newJSONFileStore)

	sjf, ok := file.(*syncJSONFile[T])
	if !ok {
		return nil, fmt.Errorf("NewJSONFileStore: existing fileName store has a differnt type")
	}

	return sjf, nil

}

// Path returns the fully qualified path of the config fileName.
func (sjf *syncJSONFile[T]) Path() string {
	if sjf.store == nil {
		return sjf.fileName
	}

	return sjf.store.FullPath(sjf.fileName)
}

// Write will write the binary data to the fileName.
func (sjf *syncJSONFile[T]) Write(data T) error {
	if sjf.store == nil {
		return NoBackingStore
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("syncJSONFileStore: error marshaling data: %s", err)
	}
	e := sjf.store.Write(sjf.fileName, raw)
	// update cache on successful write
	if e == nil {
		sjf.dataLock.Lock()
		sjf.data = &data
		sjf.dataLock.Unlock()
	}
	return e
}

// Read will read the binary data from the fileName and return it. If an error is returned,
// the byte array will be nil.
func (sjf *syncJSONFile[T]) Read() (T, error) {
	return sjf.internalRead(false)
}

// internalRead is used to allow a forced override of data cache to refresh data
func (sjf *syncJSONFile[T]) internalRead(force bool) (T, error) {
	var storedObject T
	if sjf.store == nil {
		return storedObject, NoBackingStore
	}

	sjf.dataLock.Lock()
	defer sjf.dataLock.Unlock()
	if !force {
		if sjf.data != nil {
			return *sjf.data, nil
		}
	}

	raw, e := sjf.store.Read(sjf.fileName)
	err := json.Unmarshal(raw, &storedObject)
	if err != nil {
		return storedObject, fmt.Errorf("syncJSONFileStore: unmarshal: %s", err)
	}

	if e != nil {
		return nil, e
	}
	sjf.data = &storedObject
	return *sjf.data, nil
}

// Stat returns the StorageStats for the fileName.
func (sjf *syncJSONFile[T]) Stat() (*storage.StorageInfo, error) {
	if sjf.store == nil {
		return nil, NoBackingStore
	}

	return sjf.store.Stat(sjf.fileName)
}

// Exists returns true if the fileName exist. If an error other than a NotExist error is returned,
// the result will be false with the provided error.
func (sjf *syncJSONFile[T]) Exists() (bool, error) {
	if sjf.store == nil {
		return false, NoBackingStore
	}

	return sjf.store.Exists(sjf.fileName)
}

// Delete removes the fileName from storage permanently.
func (sjf *syncJSONFile[T]) Delete() error {
	if sjf.store == nil {
		return NoBackingStore
	}

	e := sjf.store.Remove(sjf.fileName)

	// on removal, clear data cache
	if e == nil {
		sjf.dataLock.Lock()
		sjf.data = nil
		sjf.dataLock.Unlock()
	}
	return e
}

// Refresh allows external callers to force reload the config fileName from internal storage. This is
// particularly useful when there exist no change listeners on the config, which would prevent the
// data cache from automatically updating on change
func (sjf *syncJSONFile[T]) Refresh() (T, error) {
	return sjf.internalRead(true)
}

// AddChangeHandler accepts a ConfigChangedHandler function which will be called whenever the implementation
// detects that a change has been made. A unique HandlerID is returned that can be used to remove the handler
// if necessary.
func (sjf *syncJSONFile[T]) AddChangeHandler(handler ConfigChangedHandler) HandlerID {
	return sjf.AddPriorityChangeHandler(handler, DefaultHandlerPriority)
}

// AddPriorityChangeHandler allows adding a config change handler with a specific priority. By default,
// any handlers added via AddChangeHandler have a default priority of 1000. The lower the priority, the
// sooner in the handler execution it will be called.
func (sjf *syncJSONFile[T]) AddPriorityChangeHandler(handler ConfigChangedHandler, priority int) HandlerID {
	sjf.watchLock.Lock()
	defer sjf.watchLock.Unlock()

	h := &pHandler{
		id:       HandlerID(uuid.NewString()),
		handler:  handler,
		priority: priority,
	}

	sjf.watchers = append(sjf.watchers, h)

	// create the actual fileName watcher once we have at least one active watcher func registered
	if len(sjf.watchers) == 1 {
		sjf.runWatcher()
	}

	return h.id
}

// RemoveChangeHandler removes the change handler with the provided identifier if it exists. True
// is returned if the handler was removed (it existed), false otherwise.
func (sjf *syncJSONFile[T]) RemoveChangeHandler(id HandlerID) bool {
	sjf.watchLock.Lock()
	defer sjf.watchLock.Unlock()

	for i := range sjf.watchers {
		if sjf.watchers[i] != nil && sjf.watchers[i].id == id {
			copy(sjf.watchers[i:], sjf.watchers[i+1:])
			sjf.watchers[len(sjf.watchers)-1] = nil
			sjf.watchers = sjf.watchers[:len(sjf.watchers)-1]

			// stop watching the fileName for changes if there are no more external watchers
			if len(sjf.watchers) == 0 {
				sjf.stopWatcher()
			}

			return true
		}
	}
	return false
}

// RemoveAllHandlers removes all added handlers
func (sjf *syncJSONFile[T]) RemoveAllHandlers() {
	sjf.watchLock.Lock()
	defer sjf.watchLock.Unlock()

	sjf.watchers = nil

	sjf.stopWatcher()
}

// runWatcher creates a go routine which will poll the stat of a storage target on a specific
// interval and dispatch created, modified, and deleted events for that fileName.
func (sjf *syncJSONFile[T]) runWatcher() {
	// we wait for a reset on the run state prior to starting, which
	// will only block iff the run state is in the process of stopping
	sjf.runState.WaitForReset()

	// if start fails after waiting for a reset, it means that another thread
	// beat this thread to the start
	if !sjf.runState.Start() {
		log.Warningf("Run watcher already running for fileName: %s", sjf.fileName)
		return
	}

	go func() {
		first := true

		var last time.Time
		var exists bool

		for {
			// Each iteration, check for the stop trigger, or wait 10 seconds
			select {
			case <-sjf.runState.OnStop():
				sjf.runState.Reset()
				return
			case <-time.After(10 * time.Second):
			}

			// Query stat on the fileName, on errors other than exists,
			// we'll need to log the error, and perhaps limit the retries
			st, err := sjf.Stat()
			if err != nil && !os.IsNotExist(err) {
				log.Errorf("Storage Stat Error: %s", err)
				continue
			}

			// On first iteration, set exists and last modification time (if applicable)
			// and flip flag
			if first {
				exists = !os.IsNotExist(err)
				if exists {
					last = st.ModTime
				}
				first = false
				continue
			}

			// File does not exist in storage, need to check to see if that is different
			// from last state check
			if os.IsNotExist(err) {
				// check to see if the fileName has gone from exists to !exists
				if exists {
					exists = false
					sjf.onFileChange(ChangeTypeDeleted, nil)
				}
				continue
			}

			// check to see if the fileName has gone from !exists to exists
			if !exists {
				data, err := sjf.internalRead(true)
				if err != nil {
					log.Warningf("Read() Error: %s\n", err)
					continue
				}
				exists = true
				last = st.ModTime
				sjf.onFileChange(ChangeTypeCreated, data)
				continue
			}

			mtime := st.ModTime
			if mtime != last {
				last = mtime
				data, err := sjf.internalRead(true)
				if err != nil {
					log.Errorf("Read() Error: %s\n", err)
					continue
				}
				sjf.onFileChange(ChangeTypeModified, data)
			}
		}
	}()
}

// stopWatcher closes the stop channel, returning from the runWatcher go routine. Allows us
// to remove any polling stat checks on files when there are no change handlers.
func (sjf *syncJSONFile[T]) stopWatcher() {
	sjf.runState.Stop()
}

// onFileChange is internally called when the core watcher recognizes a change in the ConfigFile. This
// method dispatches that change to all added watchers
func (sjf *syncJSONFile[T]) onFileChange(changeType ChangeType, newData T) {
	// On change, we copy out the handlers to a separate slice for processing for a few reasons:
	// 1. We don't want to lock while executing the handlers
	// 2. Handlers may want to operate on the ConfigFile instance, which would result in a deadlock
	// 3. Allows us to implement priority sorting outside of the lock as well
	sjf.watchLock.Lock()
	if len(sjf.watchers) == 0 {
		sjf.watchLock.Unlock()
		return
	}

	toNotify := make([]*pHandler, len(sjf.watchers))
	copy(toNotify, sjf.watchers)
	sjf.watchLock.Unlock()

	sort.SliceStable(toNotify, func(i, j int) bool {
		return toNotify[i].priority < toNotify[j].priority
	})

	raw, err := json.Marshal(newData)
	if err != nil {
		log.Errorf("syncJSONFileStore: error marshaling data: %s", err)
	}

	for _, handler := range toNotify {
		handler.handler(changeType, raw)
	}
}

//--------------------------------------------------------------------------
//  pHandler
//--------------------------------------------------------------------------

// pHandler is a wrapper type used to assign a ConfigChangedHandler a unique identifier and priority.
type pHandler struct {
	id       HandlerID
	handler  ConfigChangedHandler
	priority int
}
