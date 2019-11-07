package costmodel

import (
	"time"
)

// Basic job that executes on condition checked on Update(). Condition resettable.
type Job interface {
	Update() (bool, error)
	Reset()
}

// Repeatable job based on time conditions that requires manual updating
type TimedJob struct {
	jobFunc  func() error
	interval time.Duration
	lastRun  time.Time
	nextRun  time.Time
}

// Creates a new timed job to be executed on a specific interval
func NewTimedJob(interval time.Duration, jobFunc func() error) Job {
	now := time.Now()

	return &TimedJob{
		interval: interval,
		lastRun:  now,
		nextRun:  now.Add(interval),
		jobFunc:  jobFunc,
	}
}

func (tj *TimedJob) Update() (bool, error) {
	current := time.Now()
	if current.After(tj.nextRun) {
		tj.Reset()

		err := tj.jobFunc()
		return true, err
	}

	return false, nil
}

func (tj *TimedJob) Reset() {
	tj.lastRun = time.Now()
	tj.nextRun = tj.lastRun.Add(tj.interval)
}
