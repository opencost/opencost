package provider

import (
	"sync"
	"testing"

	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
	mock_storage "github.com/opencost/opencost/pkg/storage/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestUpdateFromMap(t *testing.T) {
	const (
		file        = "test"
		xCPUMonthly = "0.001507" // 1.1/730
		yCPU        = "42.42"
		yCPUMonthly = "0.058110" // 42.42/730
	)

	for _, tc := range []struct {
		name          string
		a             map[string]string
		StorageWrite  func(path string, data []byte) error
		expectedCP    *models.CustomPricing
		expectedError error
	}{
		{
			name:          "CPU input is a valid float64",
			a:             map[string]string{"CPU": yCPU},
			StorageWrite:  func(path string, data []byte) error { return nil },
			expectedCP:    &models.CustomPricing{CPU: yCPUMonthly},
			expectedError: nil,
		},
		{
			name:          "CPU uses default value when input is an empty string",
			a:             map[string]string{"CPU": ""},
			StorageWrite:  func(path string, data []byte) error { return nil },
			expectedCP:    &models.CustomPricing{CPU: xCPUMonthly},
			expectedError: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			s := mock_storage.NewMockStorage(ctrl)

			if tc.StorageWrite != nil {
				s.EXPECT().Write(file, gomock.Any()).DoAndReturn(tc.StorageWrite).Times(1)
			}

			pc := ProviderConfig{
				lock:          new(sync.Mutex),
				configFile:    config.NewConfigFile(s, file),
				customPricing: &models.CustomPricing{CPU: xCPUMonthly},
			}

			cp, err := pc.UpdateFromMap(tc.a)
			if tc.expectedError != nil {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cp)
			require.Equal(t, tc.expectedCP.CPU, cp.CPU)
		})
	}
}
