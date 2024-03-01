package model

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

type CustomCostRequest struct {
	// specifies the window for data to be
	// retrieved
	TargetWindow *opencost.Window

	// the step size to return
	Resolution time.Duration
}

func (c CustomCostRequest) GetTargetWindow() *opencost.Window {
	return c.TargetWindow
}

func (c CustomCostRequest) GetTargetResolution() time.Duration {
	return c.Resolution
}

type CustomCostRequestInterface interface {
	GetTargetWindow() *opencost.Window
	GetTargetResolution() time.Duration
}
