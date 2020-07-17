package log

import (
	"fmt"
	"time"

	"k8s.io/klog"
)

var seen = make(map[string]int)

func Errorf(format string, a ...interface{}) {
	klog.Errorf(fmt.Sprintf("[Error] %s", format), a...)
}

func DedupedErrorf(format string, logTypeLimit int, a ...interface{}) {
	timesLogged, ok := seen[format]
	if !ok {
		seen[format] = 1
	} else if timesLogged > logTypeLimit {
		f := fmt.Sprintf("[Error] %s", format)
		klog.Errorf("%s seen more than %d times, suppressing future logs", f, logTypeLimit)
	} else {
		seen[format] += 1
		klog.Errorf(fmt.Sprintf("[Error] %s", format), a...)
	}
}

func Warningf(format string, a ...interface{}) {
	klog.V(2).Infof(fmt.Sprintf("[Warning] %s", format), a...)
}

func DedupedWarningf(format string, logTypeLimit int, a ...interface{}) {
	timesLogged, ok := seen[format]
	if !ok {
		seen[format] = 1
	} else if timesLogged > logTypeLimit {
		f := fmt.Sprintf("[Error] %s", format)
		klog.Errorf("%s seen more than %d times, suppressing future logs", f, logTypeLimit)
	} else {
		seen[format] += 1
		klog.V(2).Infof(fmt.Sprintf("[Warning] %s", format), a...)
	}
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

type Profiler struct {
	profiles map[string]time.Duration
	starts   map[string]time.Time
}

func NewProfiler() *Profiler {
	return &Profiler{
		profiles: map[string]time.Duration{},
		starts:   map[string]time.Time{},
	}
}

func (p *Profiler) Start(name string) {
	if p == nil {
		return
	}
	p.starts[name] = time.Now()
}

func (p *Profiler) Stop(name string) time.Duration {
	if p == nil {
		return 0
	}
	if start, ok := p.starts[name]; ok {
		elapsed := time.Since(start)
		p.profiles[name] += elapsed
		return elapsed
	}
	return 0
}

func (p *Profiler) Log(name string) {
	if p == nil {
		return
	}
	Profilef("%s: %s", p.profiles[name], name)
}

func (p *Profiler) LogAll() {
	if p == nil {
		return
	}

	// Print profiles, largest to smallest. (Inefficienct, but shouldn't matter.)
	print := map[string]time.Duration{}
	for name, value := range p.profiles {
		print[name] = value
	}
	for len(print) > 0 {
		largest := ""
		for name := range print {
			if print[name] > print[largest] {
				largest = name
			}
		}
		Profilef("%s: %s", print[largest], largest)
		delete(print, largest)
	}
}
