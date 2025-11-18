package kubemodel

// @bingen:generate:StatType
type StatType string

const (
	StatAvg StatType = "avg"
	StatMax StatType = "max"
	StatMin StatType = "min"
	StatP95 StatType = "p95"
	StatP85 StatType = "p85"
)

// @bingen:generate:Stats
type Stats map[StatType]uint64

func NewStats(capacity ...int) Stats {
	if len(capacity) == 1 {
		s := make(map[StatType]uint64, capacity[0])
		return s
	}

	return map[StatType]uint64{}
}

func (s Stats) Avg() (uint64, bool) {
	if s == nil {
		return 0, false
	}

	val, ok := s[StatAvg]

	return val, ok
}

func (s Stats) Max() (uint64, bool) {
	if s == nil {
		return 0, false
	}

	val, ok := s[StatMax]

	return val, ok
}

func (s Stats) Min() (uint64, bool) {
	if s == nil {
		return 0, false
	}

	val, ok := s[StatMin]

	return val, ok
}

func (s Stats) P95() (uint64, bool) {
	if s == nil {
		return 0, false
	}

	val, ok := s[StatP95]

	return val, ok
}

func (s Stats) P85() (uint64, bool) {
	if s == nil {
		return 0, false
	}

	val, ok := s[StatP85]

	return val, ok
}
