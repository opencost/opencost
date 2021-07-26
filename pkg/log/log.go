package log

import (
	"fmt"
	"time"

	"k8s.io/klog"
)

// TODO for deduped functions, if timeLogged > logTypeLimit, should we log once
// every... 100 (?) times so we don't lose track entirely?

// concurrency-safe counter
var ctr = newCounter()

func Errorf(format string, a ...interface{}) {
	klog.Errorf(fmt.Sprintf("[Error] %s", format), a...)
}

func DedupedErrorf(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Errorf(format, a...)
	} else if timesLogged == logTypeLimit {
		Errorf(format, a...)
		Infof("%s logged %d times: suppressing future logs", format, logTypeLimit)
	}
}

func Warningf(format string, a ...interface{}) {
	klog.V(2).Infof(fmt.Sprintf("[Warning] %s", format), a...)
}

func DedupedWarningf(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Warningf(format, a...)
	} else if timesLogged == logTypeLimit {
		Warningf(format, a...)
		Infof("%s logged %d times: suppressing future logs", format, logTypeLimit)
	}
}

func Infof(format string, a ...interface{}) {
	klog.V(3).Infof(fmt.Sprintf("[Info] %s", format), a...)
}

func DedupedInfof(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Infof(format, a...)
	} else if timesLogged == logTypeLimit {
		Infof(format, a...)
		Infof("%s logged %d times: suppressing future logs", format, logTypeLimit)
	}
}

func Profilef(format string, a ...interface{}) {
	klog.V(3).Infof(fmt.Sprintf("[Profiler] %s", format), a...)
}

func Debugf(format string, a ...interface{}) {
	klog.V(5).Infof(fmt.Sprintf("[Debug] %s", format), a...)
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
