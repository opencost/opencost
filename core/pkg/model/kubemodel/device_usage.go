package kubemodel

import (
	"errors"
	"fmt"
	"time"
)

// @bingen:generate:DeviceUsage
type DeviceUsage struct {
	ContainerUID          string      `json:"containerUid"`
	DeviceUID             string      `json:"deviceUid"`
	UsageSeconds          Measurement `json:"usageSeconds"`
	UsagePercentageMax    float64     `json:"usagePercentageMax"`
	MemoryByteSecondsUsed Measurement `json:"memoryByteSecondsUsed"`
	DeviceType            string      `json:"deviceType,omitempty"`
	DurationSeconds       Measurement `json:"durationSeconds,omitempty"`
	Start                 time.Time   `json:"start"`
	End                   time.Time   `json:"end"`
}

func (u *DeviceUsage) Validate() error {
	if u.ContainerUID == "" {
		return errors.New("ContainerUID is required")
	}
	if u.DeviceUID == "" {
		return errors.New("DeviceUID is required")
	}
	if u.UsagePercentageMax < 0 || u.UsagePercentageMax > 100 {
		return fmt.Errorf("UsagePercentageMax must be 0-100, got %.2f", u.UsagePercentageMax)
	}
	return nil
}

func (u *DeviceUsage) Clone() *DeviceUsage {
	if u == nil {
		return nil
	}

	cloned := &DeviceUsage{
		ContainerUID:          u.ContainerUID,
		DeviceUID:             u.DeviceUID,
		UsageSeconds:          u.UsageSeconds,
		UsagePercentageMax:    u.UsagePercentageMax,
		MemoryByteSecondsUsed: u.MemoryByteSecondsUsed,
		DeviceType:            u.DeviceType,
		DurationSeconds:       u.DurationSeconds,
		Start:                 u.Start,
		End:                   u.End,
	}

	return cloned
}

func (u *DeviceUsage) UsageAverage() Measurement {
	if u.DurationSeconds == 0 {
		return 0
	}
	return (u.UsageSeconds / u.DurationSeconds) * 100
}

func (u *DeviceUsage) MemoryByteUsageAverage() Measurement {
	if u.DurationSeconds == 0 {
		return 0
	}
	return u.MemoryByteSecondsUsed / u.DurationSeconds
}

func (kms *KubeModelSet) RegisterUsage(id, containerID, deviceId string) error {
	if id == "" {
		err := fmt.Errorf("UID is nil for DeviceUsage")
		kms.Error(err)
		return err
	}

	if _, ok := kms.DeviceUsages[id]; !ok {
		kms.DeviceUsages[id] = &DeviceUsage{
			ContainerUID: containerID,
			DeviceUID:    deviceId,
		}

		kms.Metadata.ObjectCount++
	}

	return nil
}
