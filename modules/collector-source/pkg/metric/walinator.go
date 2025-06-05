package metric

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

const ControllerEventName = "controller"

type fileInfo struct {
	name      string
	timestamp time.Time
	ext       string
}

type Walinator struct {
	storage         storage.Storage
	paths           pathing.StoragePathFormatter[time.Time]
	exporter        exporter.EventExporter[UpdateSet]
	limitResolution *util.Resolution
	repo            *MetricRepository
}

func NewWalinator(
	clusterID string,
	store storage.Storage,
	resolutions []*util.Resolution,
	repo *MetricRepository,
) (*Walinator, error) {
	var limitResolution *util.Resolution
	for _, resolution := range resolutions {
		if limitResolution == nil || resolution.Limit().Before(limitResolution.Limit()) {
			limitResolution = resolution
		}
	}
	pathFormatter, err := pathing.NewEventStoragePathFormatter("", clusterID, ControllerEventName)
	if err != nil {
		return nil, fmt.Errorf("filed to create path formatter for scrape controller: %s", err.Error())
	}
	encoder := exporter.NewJSONEncoder[UpdateSet]()
	exp := exporter.NewEventStorageExporter(
		pathFormatter,
		encoder,
		store,
	)

	return &Walinator{
		storage:         store,
		paths:           pathFormatter,
		exporter:        exp,
		limitResolution: limitResolution,
		repo:            repo,
	}, nil
}

func (w *Walinator) Start() {
	w.restore()

	// Start cleaning function
	go func() {
		time.Sleep(w.limitResolution.Next().Sub(time.Now().UTC()))
		w.clean()
	}()
}

// restore applies updates from wal files to restore the state of the repo
func (w *Walinator) restore() {
	fileInfos, err := w.getFileInfos()
	if err != nil {
		log.Errorf("failed to retrieve updates files: %s", err.Error())
	}
	limit := w.limitResolution.Limit()
	for _, fi := range fileInfos {
		if fi.timestamp.Before(limit) {
			continue
		}

		b, err := w.storage.Read(fi.name)
		if err != nil {
			log.Errorf("failed to load file contents for '%s': %s", fi.name, err.Error())
			continue
		}
		updateSet := UpdateSet{}
		err = json.Unmarshal(b, &updateSet)
		if err != nil {
			log.Errorf("failed to unmarshal file %s: %s", fi.name, err.Error())
			continue
		}
		w.repo.Update(updateSet.Updates, fi.timestamp)
	}
}

// Update calls update on the repo and then exports the update to storage
func (w *Walinator) Update(
	updates []Update,
	timestamp time.Time,
) {
	// run update
	w.repo.Update(updates, timestamp)

	err := w.exporter.Export(timestamp, &UpdateSet{
		Updates: updates,
	})
	if err != nil {
		log.Errorf("failed to export update results: %s", err.Error())
	}
}

// getFileInfos returns a sorted slice of fileInfo
func (w *Walinator) getFileInfos() ([]fileInfo, error) {
	dirPath := w.paths.Dir()
	files, err := w.storage.List(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to list files in scrape controller: %w", err)
	}
	var fileInfos []fileInfo
	for _, file := range files {
		fileName := path.Base(file.Name)
		fileNameComponents := strings.Split(fileName, ".")
		if len(fileNameComponents) != 2 {
			log.Errorf("file has invalid name: %s", fileName)
			continue
		}
		timeString := fileNameComponents[0]
		timestamp, err := time.Parse(pathing.EventStorageTimeFormat, timeString)
		if err != nil {
			log.Errorf("failed to parse fileName %s: %s", fileName, err.Error())
			continue
		}
		ext := fileNameComponents[1]
		fileInfos = append(fileInfos, fileInfo{
			name:      w.paths.ToFullPath("", timestamp, ext),
			timestamp: timestamp,
			ext:       ext,
		})
	}
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].timestamp.Before(fileInfos[j].timestamp)
	})
	return fileInfos, nil
}

// clean removes files that are older than the limit resolution from the storage
func (w *Walinator) clean() {
	fileInfos, err := w.getFileInfos()
	if err != nil {
		log.Errorf("failed to retrieve file info for cleaning: %s", err.Error())
	}
	limit := w.limitResolution.Limit()
	for _, fi := range fileInfos {
		if !limit.After(fi.timestamp) {
			continue
		}
		err = w.storage.Remove(fi.name)
		if err != nil {
			log.Errorf("failed to remove file '%s': %s", fi.name, err.Error())
		}
	}
}
