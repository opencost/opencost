package log

import (
	"fmt"
	"time"

	"k8s.io/klog"
)

func Errorf(format string, a ...interface{}) {
	klog.Errorf(fmt.Sprintf("[Error] %s", format), a...)
}

func Warningf(format string, a ...interface{}) {
	klog.V(2).Infof(fmt.Sprintf("[Warning] %s", format), a...)
}

func Infof(format string, a ...interface{}) {
	klog.V(3).Infof(fmt.Sprintf("[Info] %s", format), a...)
}

func Profilef(format string, a ...interface{}) {
	klog.V(3).Infof(fmt.Sprintf("[Profiler] %s", format), a...)
}

func Debugf(format string, a ...interface{}) {
	klog.V(4).Infof(fmt.Sprintf("[Debug] %s", format), a...)
}

func Profile(start time.Time, name string) {
	elapsed := time.Since(start)
	Profilef("%s: %s", elapsed, name)
}

func ProfileWithThreshold(start time.Time, threshold time.Duration, name string) {
	elapsed := time.Since(start)
	if elapsed > threshold {
		Profilef("%s: %s", elapsed, name)
	}
}
