package metric

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric/aggregator"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
)

const TestActiveMinutesID = "TestActiveMinutes"
const TestAverageID = "TestAverage"
const TestMetric = "test_metric"

func testMetricCollector() MetricStore {
	memStore := NewInMemoryMetricStore()

	memStore.Register(NewMetricCollector(
		TestActiveMinutesID,
		TestMetric,
		[]string{
			"test",
		},
		aggregator.ActiveMinutes,
		nil,
	))

	memStore.Register(NewMetricCollector(
		TestAverageID,
		TestMetric,
		[]string{
			"test",
		},
		aggregator.AverageOverTime,
		nil,
	))

	return memStore
}

func TestWalinator_Update(t *testing.T) {
	time2 := time.Now().UTC().Truncate(timeutil.Day)
	time1 := time2.Add(-timeutil.Day)
	store := storage.NewMemoryStorage()
	res1d, _ := util.NewResolution(util.ResolutionConfiguration{
		Interval:  "1d",
		Retention: 3,
	})
	resolutions := []*util.Resolution{
		res1d,
	}
	repo := NewMetricRepository(
		resolutions,
		testMetricCollector,
	)
	wal, _ := NewWalinator(
		"test",
		store,
		resolutions,
		repo,
	)
	inputUpdates1 := []Update{
		{
			Name: TestMetric,
			Labels: map[string]string{
				"test": "test",
			},
			Value:          1,
			AdditionalInfo: nil,
		},
	}

	wal.Update(&UpdateSet{
		Timestamp: time1,
		Updates:   inputUpdates1,
	})

	// check that the repo has a collector
	if len(repo.resolutionStores["1d"].collectors) != 1 {
		t.Error("call to Update did not update repository correctly")
	}
	files, _ := store.List(wal.paths.Dir())
	// check storage
	if len(files) != 1 {
		t.Error("Update did not update storage")
	}
}

func TestWalinator_restore(t *testing.T) {
	time3 := time.Now().UTC().Truncate(timeutil.Day)
	time2 := time3.Add(-12 * time.Hour)
	time1 := time3.Add(-timeutil.Day)
	store := storage.NewMemoryStorage()
	res1d, _ := util.NewResolution(util.ResolutionConfiguration{
		Interval:  "1d",
		Retention: 3,
	})
	resolutions := []*util.Resolution{
		res1d,
	}
	repo := NewMetricRepository(
		resolutions,
		testMetricCollector,
	)
	wal, _ := NewWalinator(
		"test",
		store,
		resolutions,
		repo,
	)
	inputUpdates1 := []Update{
		{
			Name: TestMetric,
			Labels: map[string]string{
				"test": "test",
			},
			Value:          1,
			AdditionalInfo: nil,
		},
	}

	inputUpdates2 := []Update{
		{
			Name: TestMetric,
			Labels: map[string]string{
				"test": "test",
			},
			Value:          2,
			AdditionalInfo: nil,
		},
	}

	inputUpdates3 := []Update{
		{
			Name: TestMetric,
			Labels: map[string]string{
				"test": "test",
			},
			Value:          3,
			AdditionalInfo: nil,
		},
	}

	wal.Update(&UpdateSet{
		Timestamp: time1,
		Updates:   inputUpdates1,
	})
	wal.Update(&UpdateSet{
		Timestamp: time2,
		Updates:   inputUpdates2,
	})
	wal.Update(&UpdateSet{
		Timestamp: time3,
		Updates:   inputUpdates3,
	})

	repo2 := NewMetricRepository(
		resolutions,
		testMetricCollector,
	)

	// replace the repo in the walinator
	wal.repo = repo2

	wal.restore()

	collector1, err := repo.GetCollector("1d", time3)
	if err != nil {
		t.Fatalf("failed to get collector from repo1: %s", err.Error())
	}
	activeMinutesRes1, err := collector1.Query(TestActiveMinutesID)
	if err != nil {
		t.Fatalf("failed to query %s from repo1: %s", TestActiveMinutesID, err.Error())
	}
	averageRes1, err := collector1.Query(TestAverageID)
	if err != nil {
		t.Fatalf("failed to query %s from repo1: %s", TestAverageID, err.Error())
	}

	collector2, err := repo2.GetCollector("1d", time3)
	if err != nil {
		t.Fatalf("failed to get collector from repo2: %s", err.Error())
	}
	activeMinutesRes2, err := collector2.Query(TestActiveMinutesID)
	if err != nil {
		t.Fatalf("failed to query %s from repo2: %s", TestActiveMinutesID, err.Error())
	}
	averageRes2, err := collector2.Query(TestAverageID)
	if err != nil {
		t.Fatalf("failed to query %s from repo2: %s", TestAverageID, err.Error())
	}

	if !reflect.DeepEqual(activeMinutesRes1, activeMinutesRes2) {
		t.Errorf("active minute query results did not match 1: %v, 2: %v", activeMinutesRes1, activeMinutesRes2)
	}
	if !reflect.DeepEqual(averageRes1, averageRes2) {
		t.Errorf("average query results did not match 1: %v, 2: %v", averageRes1, averageRes2)
	}
}

func TestWalinator_clean(t *testing.T) {
	time3 := time.Now().UTC().Truncate(timeutil.Day)
	time2 := time3.Add(-timeutil.Day)
	time1 := time2.Add(-timeutil.Day)
	store := storage.NewMemoryStorage()
	res1d, _ := util.NewResolution(util.ResolutionConfiguration{
		Interval:  "1d",
		Retention: 2,
	})
	resolutions := []*util.Resolution{
		res1d,
	}
	repo := NewMetricRepository(
		resolutions,
		testMetricCollector,
	)
	wal, _ := NewWalinator(
		"test",
		store,
		resolutions,
		repo,
	)
	inputUpdates1 := []Update{
		{
			Name: TestMetric,
			Labels: map[string]string{
				"test": "test",
			},
			Value:          1,
			AdditionalInfo: nil,
		},
	}

	wal.Update(&UpdateSet{
		Timestamp: time1,
		Updates:   inputUpdates1,
	})
	wal.Update(&UpdateSet{
		Timestamp: time2,
		Updates:   inputUpdates1,
	})
	wal.Update(&UpdateSet{
		Timestamp: time3,
		Updates:   inputUpdates1,
	})

	files, err := wal.getFileInfos()
	if err != nil {
		t.Errorf("failed to retrieve file info: %s", err.Error())
	}
	if len(files) != 3 {
		t.Errorf("incorrect number of files after updates: wanted %d, got %d", 3, len(files))
	}

	wal.clean()

	files, err = wal.getFileInfos()
	if err != nil {
		t.Errorf("failed to retrieve file info: %s", err.Error())
	}
	if len(files) != 2 {
		t.Errorf("incorrect number of files after clean: wanted %d, got %d", 2, len(files))
	}
}

func Test_deserializeUpdateSet(t *testing.T) {

	inputUpdateSet1 := &UpdateSet{
		Updates: []Update{
			{
				Name: TestMetric,
				Labels: map[string]string{
					"test": "test",
				},
				Value:          1,
				AdditionalInfo: nil,
			},
		},
	}

	jsonEncoder := exporter.NewJSONEncoder[UpdateSet]()
	gZipJsonEncoder := exporter.NewGZipEncoder(exporter.NewJSONEncoder[UpdateSet]())

	invalidBytes := []byte("invalid")
	jsonBytes1, _ := jsonEncoder.Encode(inputUpdateSet1)
	gZipJsonBytes1, _ := gZipJsonEncoder.Encode(inputUpdateSet1)

	tests := map[string]struct {
		ext     string
		b       []byte
		want    *UpdateSet
		wantErr bool
	}{
		"json with invalid": {
			ext:     "json",
			b:       invalidBytes,
			want:    nil,
			wantErr: true,
		},
		"json with json": {
			ext:     "json",
			b:       jsonBytes1,
			want:    inputUpdateSet1,
			wantErr: false,
		},
		"json with gzipjson": {
			ext:     "json",
			b:       gZipJsonBytes1,
			want:    nil,
			wantErr: true,
		},
		"json.gz with invalid": {
			ext:     "json.gz",
			b:       invalidBytes,
			want:    nil,
			wantErr: true,
		},
		"json.gz with json": {
			ext:     "json.gz",
			b:       jsonBytes1,
			want:    nil,
			wantErr: true,
		},
		"json.gz with gzipjson": {
			ext:     "json.gz",
			b:       gZipJsonBytes1,
			want:    inputUpdateSet1,
			wantErr: false,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := deserializeUpdateSet(tt.ext, tt.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("deserializeUpdateSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deserializeUpdateSet() got = %v, want %v", got, tt.want)
			}
		})
	}
}
