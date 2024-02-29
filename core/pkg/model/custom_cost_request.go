package model

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

type CustomCostRequest struct {
	TargetWindow *opencost.Window
	Resolution   time.Duration
}

func (c *CustomCostRequest) GetTargetWindow() *opencost.Window {
	return c.TargetWindow
}

func (c *CustomCostRequest) GetTargetResolution() time.Duration {
	return c.Resolution
}
