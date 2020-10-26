package test

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util"
)

type VectorGenerator interface {
	New() []*util.Vector
}

type RandomVectorGenerator struct {
	ts []float64
}

func (rvg *RandomVectorGenerator) New() []*util.Vector {
	vs := []*util.Vector{}

	for _, ts := range rvg.ts {
		if rand.Intn(2) == 1 {
			vs = append(vs, newVec(ts, rand.Float64()*100.0))
		}
	}

	return vs
}

type StaticVectorGenerator struct {
	ts      []float64
	current float64
}

func (svg *StaticVectorGenerator) New() []*util.Vector {
	vs := []*util.Vector{}

	for _, ts := range svg.ts {
		vs = append(vs, newVec(ts, svg.current))
	}

	svg.current += 1.0

	return vs
}

func NewRandomGenerator(ts []float64) VectorGenerator {
	return &RandomVectorGenerator{
		ts: ts,
	}
}

func NewStaticGenerator(ts []float64) VectorGenerator {
	return &StaticVectorGenerator{
		ts:      ts,
		current: 1.0,
	}
}

func newVec(ts float64, v float64) *util.Vector {
	return &util.Vector{
		Timestamp: ts,
		Value:     v,
	}
}

func sumOperation(result *util.Vector, values []*float64) bool {
	var sum *float64
	for _, y := range values {
		if sum != nil && y != nil {
			*sum += *y
		} else if y != nil {
			yy := *y
			sum = &yy
		}
	}

	if sum == nil {
		log.Errorf("Sum should never be nil for timestamp.")
		return false
	}

	result.Value = *sum
	return true
}

func binarySumOperation(result *util.Vector, x *float64, y *float64) bool {
	if x != nil && y != nil {
		result.Value = *x + *y
	} else if y != nil {
		result.Value = *y
	} else if x != nil {
		result.Value = *x
	}

	return true
}

func generateTimestamps(start float64, total int) []float64 {
	timestamps := []float64{}
	current := start
	for i := 0; i < total; i++ {
		timestamps = append(timestamps, current)
		current += 60.0
	}
	return timestamps
}

func roundTimestamp(ts float64, precision float64) float64 {
	return math.Round(ts/precision) * precision
}

func runRandom() [][]*util.Vector {
	gen := NewRandomGenerator(generateTimestamps(5000.0, 30))

	v1 := gen.New()
	v2 := gen.New()
	v3 := gen.New()
	v4 := gen.New()
	v5 := gen.New()

	var wg sync.WaitGroup
	wg.Add(10)

	results := [][]*util.Vector{}
	var l sync.Mutex

	for i := 0; i < 10; i++ {
		go func() {
			result := addMulti(v1, v2, v3, v4, v5)
			l.Lock()
			results = append(results, result)
			l.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()

	return results
}

func runStatic() [][]*util.Vector {
	gen := NewStaticGenerator(generateTimestamps(5000.0, 10))

	v1 := gen.New()
	v2 := gen.New()
	v3 := gen.New()
	v4 := gen.New()
	v5 := gen.New()

	var wg sync.WaitGroup
	wg.Add(10)

	results := [][]*util.Vector{}
	var l sync.Mutex

	for i := 0; i < 10; i++ {
		go func() {
			result := addMulti(v1, v2, v3, v4, v5)
			l.Lock()
			results = append(results, result)
			l.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()

	return results
}

func addMulti(vs ...[]*util.Vector) []*util.Vector {
	return util.ApplyMultiVectorOp(sumOperation, vs...)
}

func addBinary(vs ...[]*util.Vector) []*util.Vector {
	result := vs[0]
	for i := 1; i < len(vs); i++ {
		result = util.ApplyVectorOp(result, vs[i], binarySumOperation)
	}
	return result
}

func TestApplyMultiVectorOp(t *testing.T) {
	results := runRandom()

	for i := 1; i < len(results); i++ {
		for idx, v := range results[i] {
			vv := results[0][idx]
			if v.Timestamp != vv.Timestamp && v.Value != vv.Value {
				t.Errorf("Vector not equal at result set: %d, index: %d\n", i, idx)
				return
			}
		}
	}
}

func TestApplyMultiVectorOpDeterministic(t *testing.T) {
	results := runStatic()

	for i := 0; i < len(results); i++ {
		for _, v := range results[i] {
			if v.Value != 15.0 {
				t.Errorf("Incorrect sum: %f\n", v.Value)
			}
		}
	}
}

const (
	BenchVectors    = 5
	BenchDataPoints = 400
)

var benchResults []*util.Vector
var benchMultiResults []*util.Vector

func BenchmarkApplyMultiVectorOp(b *testing.B) {
	var results []*util.Vector

	gen := NewStaticGenerator(generateTimestamps(5000.0, BenchDataPoints))

	var vecs [][]*util.Vector
	for i := 0; i < BenchVectors; i++ {
		vecs = append(vecs, gen.New())
	}

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			results = addMulti(vecs...)
		}
	})

	benchMultiResults = results
}

func BenchmarkApplyVectorOp(b *testing.B) {
	var results []*util.Vector

	gen := NewStaticGenerator(generateTimestamps(5000.0, BenchDataPoints))

	var vecs [][]*util.Vector
	for i := 0; i < BenchVectors; i++ {
		vecs = append(vecs, gen.New())
	}

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			results = addBinary(vecs...)
		}
	})

	benchResults = results
}

func TestBoth(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	var r1 [][]*util.Vector
	var r2 [][]*util.Vector
	go func() {
		r1 = runRandom()
		wg.Done()
	}()

	go func() {
		r2 = runStatic()
		wg.Done()
	}()

	wg.Wait()

	for i := 1; i < len(r1); i++ {
		for idx, v := range r1[i] {
			vv := r1[0][idx]
			if v.Timestamp != vv.Timestamp && v.Value != vv.Value {
				t.Errorf("Vector not equal at result set: %d, index: %d\n", i, idx)
				return
			}
		}
	}

	for i := 0; i < len(r2); i++ {
		for _, v := range r2[i] {
			if v.Value != 5.0 {
				t.Errorf("Incorrect sum: %f\n", v.Value)
			}
		}
	}
}
