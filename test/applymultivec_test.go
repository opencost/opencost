package test

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util"
)

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

func addVectors(vectors ...[]*util.Vector) []*util.Vector {
	return util.ApplyMultiVectorOp(sumOperation, vectors...)
}

func generateTimeStamps(start float64, total int) []float64 {
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

func createRandomVectors(timestamps []float64) []*util.Vector {
	vs := []*util.Vector{}

	for _, ts := range timestamps {
		if rand.Intn(2) == 1 {
			vs = append(vs, newVec(ts, rand.Float64()*100.0))
		}
	}

	return vs
}

func createStaticVectors(timestamps []float64, value float64) []*util.Vector {
	vs := []*util.Vector{}

	for _, ts := range timestamps {
		vs = append(vs, newVec(ts, value))
	}

	return vs
}

func runRandom() [][]*util.Vector {
	ts := generateTimeStamps(5000.0, 30)
	v1 := createRandomVectors(ts)
	v2 := createRandomVectors(ts)
	v3 := createRandomVectors(ts)
	v4 := createRandomVectors(ts)
	v5 := createRandomVectors(ts)

	var wg sync.WaitGroup
	wg.Add(10)

	results := [][]*util.Vector{}
	var l sync.Mutex

	for i := 0; i < 10; i++ {
		go func() {
			result := addVectors(v1, v2, v3, v4, v5)
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
	ts := generateTimeStamps(5000.0, 10)

	v1 := createStaticVectors(ts, 1.0)
	v2 := createStaticVectors(ts, 2.0)
	v3 := createStaticVectors(ts, 3.0)
	v4 := createStaticVectors(ts, 4.0)
	v5 := createStaticVectors(ts, 5.0)

	var wg sync.WaitGroup
	wg.Add(10)

	results := [][]*util.Vector{}
	var l sync.Mutex

	for i := 0; i < 10; i++ {
		go func() {
			result := addVectors(v1, v2, v3, v4, v5)
			l.Lock()
			results = append(results, result)
			l.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()

	return results
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
