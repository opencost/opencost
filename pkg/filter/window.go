package filter

import (
	"fmt"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
)

type windowed interface {
	GetWindow() kubecost.Window
}

// WindowOperation are operations that can be performed on types that have windows
type WindowOperation string

const (
	WindowOpContains WindowOperation = "contains"
)

// WindowCondition is a filter can be used on any type that has a window and implements GetWindow()
type WindowCondition[T windowed] struct {
	Window kubecost.Window
	Op     WindowOperation
}

func (wc WindowCondition[T]) String() string {
	return fmt.Sprintf(`(window %s by "%s")`, wc.Op, wc.Window.String())
}

func (wc WindowCondition[T]) Matches(that T) bool {
	thatWindow := that.GetWindow()
	switch wc.Op {
	case WindowOpContains:
		return wc.Window.ContainsWindow(thatWindow)
	default:
		log.Errorf("Filter: Window: Unhandled filter operation. This is a filter implementation error and requires immediate patching. Op: %s", wc.Op)
		return false
	}
}

func (wc WindowCondition[T]) Flattened() Filter[T] {
	return wc
}

func (wc WindowCondition[T]) Equals(that Filter[T]) bool {
	thatWindowFilter, ok := that.(WindowCondition[T])
	if !ok {
		return false
	}

	if !wc.Window.Equal(thatWindowFilter.Window) {
		return false
	}

	if wc.Op != thatWindowFilter.Op {
		return false
	}

	return true
}
