package costmodel

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/pkg/kubecost"
)

//go:generate moq -out moq_cloud_storage_test.go . CloudStorage:CloudStorageMock
//go:generate moq -out moq_allocation_model_test.go . AllocationModel:AllocationModelMock

func Test_UpdateCSV(t *testing.T) {
	t.Run("previous data doesn't exist, upload new data", func(t *testing.T) {
		var csv string
		storage := &CloudStorageMock{
			FileExistsFunc: func(ctx context.Context, path string) (bool, error) {
				return false, nil
			},
			FileReplaceFunc: func(ctx context.Context, f *os.File, path string) error {
				data, err := io.ReadAll(f)
				csv = string(data)
				require.NoError(t, err)
				return nil
			},
		}
		model := &AllocationModelMock{
			ComputeAllocationFunc: func(start time.Time, end time.Time, resolution time.Duration) (*kubecost.AllocationSet, error) {
				return &kubecost.AllocationSet{
					Allocations: map[string]*kubecost.Allocation{
						"test": {
							Name:                   "test",
							CPUCoreUsageAverage:    0.1,
							CPUCoreRequestAverage:  0.2,
							CPUCost:                0.3,
							RAMBytesUsageAverage:   0.4,
							RAMBytesRequestAverage: 0.5,
							RAMCost:                0.6,
						},
					},
				}, nil
			},
		}
		err := UpdateCSV(context.TODO(), storage, model, "/test.csv", time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		// uploaded a single file with the data
		require.Len(t, storage.FileReplaceCalls(), 1)
		require.Equal(t, `Date,Name,CPUCoreUsageAverage,CPUCoreRequestAverage,CPUCost,RAMBytesUsageAverage,RAMBytesRequestAverage,RAMCost
2021-01-01,test,0.1,0.2,0.3,0.4,0.5,0.6
`, csv)
	})

	t.Run("merge new data with previous data", func(t *testing.T) {
		var csv string
		storage := &CloudStorageMock{
			FileExistsFunc: func(ctx context.Context, path string) (bool, error) {
				return true, nil
			},
			FileDownloadFunc: func(ctx context.Context, path string) (*os.File, error) {
				f, err := os.CreateTemp("", "")
				require.NoError(t, err)
				_, err = f.WriteString(`Date,Name,CPUCoreUsageAverage,CPUCoreRequestAverage,CPUCost,RAMBytesUsageAverage,RAMBytesRequestAverage,RAMCost
2021-01-01,test,0.1,0.2,0.3,0.4,0.5,0.6
`)
				_, err = f.Seek(0, io.SeekStart)
				require.NoError(t, err)
				return f, err
			},
			FileReplaceFunc: func(ctx context.Context, f *os.File, path string) error {
				data, err := io.ReadAll(f)
				csv = string(data)
				require.NoError(t, err)
				return nil
			},
		}
		model := &AllocationModelMock{
			ComputeAllocationFunc: func(start time.Time, end time.Time, resolution time.Duration) (*kubecost.AllocationSet, error) {
				return &kubecost.AllocationSet{
					Allocations: map[string]*kubecost.Allocation{
						"test": {
							Name:                   "test",
							CPUCoreUsageAverage:    1,
							CPUCoreRequestAverage:  2,
							CPUCost:                3,
							RAMBytesUsageAverage:   4,
							RAMBytesRequestAverage: 5,
							RAMCost:                6,
						},
					},
				}, nil
			},
		}
		err := UpdateCSV(context.TODO(), storage, model, "/test.csv", time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)
		// uploaded a single file with the data
		require.Len(t, storage.FileReplaceCalls(), 1)
		require.Equal(t, `Date,Name,CPUCoreUsageAverage,CPUCoreRequestAverage,CPUCost,RAMBytesUsageAverage,RAMBytesRequestAverage,RAMCost
2021-01-01,test,0.1,0.2,0.3,0.4,0.5,0.6
2021-01-02,test,1,2,3,4,5,6
`, csv)
	})
}
