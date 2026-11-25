package metric

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/core/pkg/util/stringutil"
	"github.com/opencost/opencost/core/pkg/util/worker"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

const CollectorEventName = "collector"

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
	updater         Updater

	statusLock     sync.Mutex
	status         source.WALStatus
	lastFailureLog time.Time
}

const (
	// failureLogInterval is the minimum interval between error logs while wal writes keep failing
	failureLogInterval = 10 * time.Minute

	// maxStatusErrorLength truncates errors recorded in the wal status
	maxStatusErrorLength = 512
)

func NewWalinator(
	clusterID string,
	applicationName string,
	store storage.Storage,
	resolutions []*util.Resolution,
	updater Updater,
) (*Walinator, error) {
	var limitResolution *util.Resolution
	for _, resolution := range resolutions {
		if limitResolution == nil || resolution.Limit().Before(limitResolution.Limit()) {
			limitResolution = resolution
		}
	}
	pathFormatter, err := pathing.NewEventStoragePathFormatter(applicationName, clusterID, CollectorEventName)
	if err != nil {
		return nil, fmt.Errorf("failed to create path formatter for scrape controller: %s", err.Error())
	}

	encoder := exporter.NewBingenFileEncoder[UpdateSet]()
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
		updater:         updater,
		status:          source.WALStatus{Enabled: true},
	}, nil
}

func (w *Walinator) Start() {
	w.clean()
	w.restore()

	// Start cleaning function
	go func() {
		for {
			time.Sleep(w.limitResolution.Next().Sub(time.Now().UTC()))
			w.clean()
		}
	}()
}

// restoreResult is the outcome of reading a single wal file during restore
type restoreResult struct {
	fi        fileInfo
	updateSet *UpdateSet
}

// restoreStats accumulates the outcome of a restore as files are processed in order
type restoreStats struct {
	seen, applied, errs int
	oldest, newest      time.Time
	largestGap          time.Duration
	largestGapStart     time.Time
}

// record notes an applied file with the given timestamp, tracking the restored range and the largest
// gap between consecutive applied files
func (rs *restoreStats) record(ts time.Time) {
	rs.applied++
	if rs.oldest.IsZero() {
		rs.oldest = ts
	} else if gap := ts.Sub(rs.newest); gap > rs.largestGap {
		rs.largestGap = gap
		rs.largestGapStart = rs.newest
	}
	rs.newest = ts
}

// restore applies updates from wal files to restore the state of the previous updater(repo)
func (w *Walinator) restore() {
	startTime := time.Now().UTC()
	var listErr string

	fileInfos, err := w.getFileInfos()
	if err != nil {
		listErr = stringutil.RedactURLs(err.Error())
		log.Errorf("failed to retrieve updates files: %s", listErr)
	}
	limit := w.limitResolution.Limit()

	var inRange []fileInfo
	for _, fi := range fileInfos {
		if !fi.timestamp.Before(limit) {
			inRange = append(inRange, fi)
		}
	}

	// processFn is called in file order from a single goroutine
	stats := restoreStats{seen: len(inRange)}
	processFn := func(res restoreResult) {
		if res.updateSet == nil {
			stats.errs++
			return
		}
		w.updater.Update(res.updateSet)
		stats.record(res.fi.timestamp)
	}
	worker.ConcurrentOrderedProcessWith(worker.OptimalWorkerCount(), w.readRestoreFile, inRange, processFn)

	w.finishRestore(startTime, limit, listErr, stats)
}

// readRestoreFile reads and decodes a single wal file, returning a result with a nil update set if
// the file could not be read or decoded
func (w *Walinator) readRestoreFile(fi fileInfo) restoreResult {
	b, err := w.storage.Read(fi.name)
	if err != nil {
		log.Errorf("failed to load file contents for '%s': %s", fi.name, stringutil.RedactURLs(err.Error()))
		return restoreResult{fi: fi}
	}

	updateSet, err := deserializeUpdateSet(fi.ext, b)
	if err != nil {
		log.Errorf("failed to deserialize file contents for '%s': %s", fi.name, err.Error())
		return restoreResult{fi: fi}
	}

	if updateSet.Timestamp.IsZero() {
		updateSet.Timestamp = fi.timestamp
	}

	return restoreResult{fi: fi, updateSet: updateSet}
}

// finishRestore logs the restore outcome and records it in the wal status
func (w *Walinator) finishRestore(startTime, limit time.Time, listErr string, stats restoreStats) {
	duration := time.Since(startTime)
	tailFrom := stats.newest
	if tailFrom.IsZero() {
		tailFrom = limit
	}
	tailGap := max(startTime.Sub(tailFrom), 0)

	if listErr != "" || stats.errs > 0 {
		log.Errorf("wal restore incomplete: %d of %d objects applied, %d errors, list error: %q", stats.applied, stats.seen, stats.errs, listErr)
	} else {
		log.Infof("wal restore complete: %d objects applied in %s, largest gap %s, %s since newest object", stats.applied, duration, stats.largestGap, tailGap)
	}

	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	w.status.RestoreCompleted = true
	w.status.RestoreStartedAt = startTime
	w.status.RestoreListError = listErr
	w.status.RestoreObjectsSeen = stats.seen
	w.status.RestoreObjectsApplied = stats.applied
	w.status.RestoreErrors = stats.errs
	w.status.RestoreDuration = duration
	w.status.RestoreOldest = stats.oldest
	w.status.RestoreNewest = stats.newest
	w.status.RestoreLargestGap = stats.largestGap
	w.status.RestoreLargestGapStart = stats.largestGapStart
	w.status.RestoreTailGap = tailGap
}

// Status returns the current export and restore status of the wal
func (w *Walinator) Status() source.WALStatus {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	return w.status
}

func deserializeUpdateSet(ext string, b []byte) (*UpdateSet, error) {
	extSplit := strings.Split(ext, ".")
	lastElem := extSplit[len(extSplit)-1]
	switch lastElem {
	case "json":
		updateSet := &UpdateSet{}
		err := json.Unmarshal(b, updateSet)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %w", err)
		}
		return updateSet, nil
	case "gz":
		buf := bytes.NewBuffer(b)
		reader, err := gzip.NewReader(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress gzip: %w", err)

		}
		defer reader.Close()
		decompressed, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read decompressed gzip: %w", err)
		}

		return deserializeUpdateSet(strings.TrimSuffix(ext, ".gz"), decompressed)
	case "bingen":
		updateSet := new(UpdateSet)
		err := updateSet.UnmarshalBinary(b)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal bingen: %w", err)
		}
		return updateSet, nil
	}

	return nil, fmt.Errorf("unrecognized extension: '%s'", ext)
}

// Update calls update on the previous updater(repo) and then exports the update to storage
func (w *Walinator) Update(
	updateSet *UpdateSet,
) {
	if updateSet == nil {
		return
	}

	// run update
	w.updater.Update(updateSet)

	err := w.exporter.Export(updateSet.Timestamp, updateSet)
	w.recordExport(err)
}

// recordExport updates the export status, logging only when the export state changes so that a
// long outage does not produce an error log per scrape
func (w *Walinator) recordExport(err error) {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()

	now := time.Now().UTC()
	if err == nil {
		if w.status.ConsecutiveExportFailures > 0 {
			log.Infof("wal export recovered after %d failed writes", w.status.ConsecutiveExportFailures)
		}
		w.status.LastExportSuccess = now
		w.status.ConsecutiveExportFailures = 0
		return
	}

	msg := stringutil.RedactURLs(err.Error())
	if len(msg) > maxStatusErrorLength {
		msg = msg[:maxStatusErrorLength] + "..."
	}

	// log at error level when writes start failing and periodically while they keep failing, not on
	// every scrape
	if w.status.ConsecutiveExportFailures == 0 || now.Sub(w.lastFailureLog) >= failureLogInterval {
		log.Errorf("failed to export update results (%d consecutive failures): %s", w.status.ConsecutiveExportFailures+1, msg)
		w.lastFailureLog = now
	} else {
		log.Debugf("failed to export update results: %s", msg)
	}
	w.status.LastExportError = msg
	w.status.LastExportErrorAt = now
	w.status.ConsecutiveExportFailures++
	w.status.ExportFailuresTotal++
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
		fileNameComponents := strings.SplitN(fileName, ".", 2)
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
