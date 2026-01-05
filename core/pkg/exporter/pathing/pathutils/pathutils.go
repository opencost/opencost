package pathutils

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
)

// FormatEpochRange returns a string representation of the given start and end times in the form:
// <start-epoch>-<end-epoch>
func FormatEpochRange(start, end time.Time) string {
	startStr := strconv.FormatInt(start.Unix(), 10)
	endStr := strconv.FormatInt(end.Unix(), 10)
	return fmt.Sprintf("%s-%s", startStr, endStr)
}

// FormatEpochWindow returns a string representation of the given window in the form:
// <start-epoch>-<end-epoch>
//
// If the window is not closed, an error is returned.
func FormatEpochWindow(window opencost.Window) (string, error) {
	start := window.Start()
	end := window.End()
	if start == nil || end == nil {
		return "", fmt.Errorf("illegal window: %s", window)
	}

	return FormatEpochRange(*start, *end), nil
}

// EpochFormatToWindow converts an epoch formatted file name to a Window.
func EpochFormatToWindow(fileName string) (opencost.Window, error) {
	var window opencost.Window

	tokens := strings.Split(fileName, "-")
	if len(tokens) != 2 {
		return window, fmt.Errorf("invalid path format")
	}

	startUnix, err := strconv.ParseInt(tokens[0], 10, 64)
	if err != nil {
		return window, fmt.Errorf("Failed to Parse start(%s): %s\n", tokens[0], err.Error())
	}
	endUnix, err := strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return window, fmt.Errorf("Failed to Parse end(%s): %s\n", tokens[1], err.Error())
	}

	start := time.Unix(startUnix, 0)
	end := time.Unix(endUnix, 0)

	return opencost.NewWindow(&start, &end), nil
}

// FormatUTFRange returns a string representation of the given start and end times in the form:
// <start-utf>-<end-utf>
func FormatUTFRange(start, end time.Time) string {
	startStr := start.Format(time.RFC3339)
	endStr := end.Format(time.RFC3339)
	return fmt.Sprintf("%s-%s", startStr, endStr)
}

// FormatUTFWindow returns a string representation of the given window in the form:
// <start-epoch>-<end-epoch>
//
// If the window is not closed, an error is returned.
func FormatUTFWindow(window opencost.Window) (string, error) {
	start := window.Start()
	end := window.End()
	if start == nil || end == nil {
		return "", fmt.Errorf("illegal window: %s", window)
	}

	return FormatEpochRange(*start, *end), nil
}

// UTFFormatToWindow converts an epoch UTF file name to a Window.
func UTFFormatToWindow(fileName string) (opencost.Window, error) {
	var window opencost.Window

	tokens := strings.Split(fileName, "-")
	if len(tokens) != 2 {
		return window, fmt.Errorf("invalid path format")
	}

	start, err := time.Parse(time.RFC3339, tokens[0])
	if err != nil {
		return window, fmt.Errorf("Failed to Parse start(%s): %s\n", tokens[0], err.Error())
	}
	end, err := time.Parse(time.RFC3339, tokens[1])
	if err != nil {
		return window, fmt.Errorf("Failed to Parse end(%s): %s\n", tokens[1], err.Error())
	}

	return opencost.NewWindow(&start, &end), nil
}
