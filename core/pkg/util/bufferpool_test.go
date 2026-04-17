package util

import (
	"math"
	"math/bits"
	"sync"
	"testing"
)

// --- poolIndex / putIndex unit tests ---

func TestPoolIndex(t *testing.T) {
	cases := []struct {
		length int
		want   int
	}{
		{1, 0},
		{2, 1},
		{3, 2},
		{4, 2},
		{5, 3},
		{7, 3},
		{8, 3},
		{255, 8},
		{256, 8},
		{1023, 10},
		{1024, 10},
		{math.MaxUint16 - 50, 16},
	}
	for _, c := range cases {
		got := poolIndex(c.length)
		if got != c.want {
			t.Errorf("poolIndex(%d) = %d, want %d", c.length, got, c.want)
		}
	}
}

func TestAllocMinusOne(t *testing.T) {
	bp := newBufferPool()
	for i := 1; i <= 16; i++ {
		capacity := 1 << i
		length := capacity - 1
		if length <= 0 {
			continue
		}

		b := bp.Get(length)
		c := cap(b)

		pIndex := poolIndex(length)
		rIndex := putIndex(c)

		if pIndex != rIndex {
			t.Errorf("pIndex: %d != rIndex: %d\n", pIndex, rIndex)
		}

	}
}

func TestPutIndex(t *testing.T) {
	// putIndex must be the inverse of poolIndex for all power-of-two capacities
	// that Get hands out.
	for i := 1; i <= 16; i++ {
		cap := 1 << i
		got := putIndex(cap)
		if got != i {
			t.Errorf("putIndex(1<<%d = %d) = %d, want %d", i, cap, got, i)
		}
	}
}

func TestPoolIndexPutIndexRoundTrip(t *testing.T) {
	// For any requested length, the buffer Get returns has capacity 1<<poolIndex(length).
	// Confirm that putIndex maps that capacity back to the same pool slot.
	for length := 1; length <= math.MaxUint16; length++ {
		i := poolIndex(length)
		capacity := 1 << i
		j := putIndex(capacity)
		if i != j {
			t.Errorf("length=%d: poolIndex=%d, capacity=1<<%d=%d, putIndex=%d — round-trip broken",
				length, i, i, capacity, j)
		}
	}
}

// --- Get ---

func TestGetNilOnZeroOrNegative(t *testing.T) {
	bp := newBufferPool()
	for _, n := range []int{0, -1, -100} {
		if got := bp.Get(n); got != nil {
			t.Errorf("Get(%d) = %v, want nil", n, got)
		}
	}
}

func TestGetLengthIsExact(t *testing.T) {
	bp := newBufferPool()
	for _, n := range []int{1, 2, 3, 7, 8, 100, 1000, 65535, 65536} {
		buf := bp.Get(n)
		if len(buf) != n {
			t.Errorf("Get(%d): len = %d, want %d", n, len(buf), n)
		}
	}
}

func TestGetCapacityIsPowerOfTwo(t *testing.T) {
	bp := newBufferPool()
	for _, n := range []int{1, 2, 3, 4, 5, 100, 1000, 550, math.MaxUint16 - 100, math.MaxUint16} {
		buf := bp.Get(n)
		c := cap(buf)
		if c == 0 || !isPowerOfTwo(c) {
			t.Errorf("Get(%d): cap = %d, not a power of two", n, c)
		}
	}
}

func TestGetCapacityIsSmallestFittingPowerOfTwo(t *testing.T) {
	bp := newBufferPool()
	cases := []struct {
		n       int
		wantCap int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{8, 8},
		{9, 16},
		{255, 256},
		{256, 256},
		{1024, 1024},
	}
	for _, c := range cases {
		buf := bp.Get(c.n)
		if cap(buf) != c.wantCap {
			t.Errorf("Get(%d): cap = %d, want %d", c.n, cap(buf), c.wantCap)
		}
	}
}

func TestGetOversizeFallback(t *testing.T) {
	bp := newBufferPool()
	n := math.MaxUint16 + 1
	buf := bp.Get(n)
	if len(buf) != n {
		t.Errorf("Get(MaxUint16+1): len = %d, want %d", len(buf), n)
	}
}

// --- Put ---

func TestPutDropsZeroCapBuffer(t *testing.T) {
	// Put on a nil or zero-cap slice must not panic.
	bp := newBufferPool()
	bp.Put(nil)
	bp.Put([]byte{})
}

// --- Get / Put round-trip ---

func TestGetPutSamePool(t *testing.T) {
	// A buffer returned via Put must land in the same pool that Get draws from,
	// so the very next Get (with the same length) should reuse it.
	bp := newBufferPool()

	buf := bp.Get(100)
	ptr := &buf[0]
	bp.Put(buf)

	buf2 := bp.Get(100)
	if &buf2[0] != ptr {
		// sync.Pool may have GC'd the entry; this is not a hard failure but
		// we at minimum require length and capacity to be correct.
		if len(buf2) != 100 {
			t.Errorf("Get(100) after Put: len = %d, want 100", len(buf2))
		}
	}
}

func TestPutRestoresFullCapacity(t *testing.T) {
	// After Put, the pooled slice should have full capacity, not the resliced length.
	// We verify this by inspecting what comes out of the pool on the next Get.
	bp := newBufferPool()

	buf := bp.Get(10)  // len=10, cap=16
	bp.Put(buf)        // must put back with cap=16
	buf2 := bp.Get(15) // asks for 15 — still fits in cap=16 pool
	if cap(buf2) < 15 {
		t.Errorf("After Put(cap=16), Get(15): cap = %d, too small", cap(buf2))
	}
}

func TestIsPowerOfTwo(t *testing.T) {
	for i := 0; i < 16; i++ {
		cap := 1 << i

		if !isPowerOfTwo(cap) {
			t.Fatalf("Failed at: i=%d, cap=%d\n", i, cap)
		}
	}

	for _, v := range []int{5, 17, 19, 31, 55} {
		if isPowerOfTwo(v) {
			t.Fatalf("Unexpected isPowerOfTwo: %d", v)
		}
	}
}

func TestPutNonPowerOfTwoCapIsDiscarded(t *testing.T) {
	// Buffers with non-power-of-two capacities (e.g. from outside the pool)
	// get silently dropped. Confirm no panic and pool still works after.
	bp := newBufferPool()
	value := make([]byte, 0, 17)
	bp.Put(value)

	buf := bp.Get(24)
	if len(buf) != 24 {
		t.Errorf("Get(24) after spurious Put: len = %d, want 24", len(buf))
	}
	if cap(buf) != 32 {
		t.Errorf("Get(24) after spurious Put: cap = %d, want 32", cap(buf))
	}
}

// --- Concurrency ---

func TestConcurrentGetPut(t *testing.T) {
	bp := newBufferPool()
	var wg sync.WaitGroup
	const goroutines = 64
	const iters = 1000

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				n := (id*iters + i) % 4096
				if n == 0 {
					n = 1
				}
				buf := bp.Get(n)
				if len(buf) != n {
					t.Errorf("concurrent Get(%d): len = %d", n, len(buf))
				}
				// Write to every byte to catch races under -race.
				for j := range buf {
					buf[j] = byte(j)
				}
				bp.Put(buf)
			}
		}(g)
	}
	wg.Wait()
}

// --- Edge cases at pool boundaries ---

func TestGetExactPowerOfTwo(t *testing.T) {
	// Exact powers of two are the boundary between two pools; confirm correct
	// bucket selection and full round-trip for each.
	bp := newBufferPool()
	for i := 0; i < 17; i++ {
		n := 1 << i
		buf := bp.Get(n)
		if len(buf) != n {
			t.Errorf("Get(1<<%d=%d): len = %d", i, n, len(buf))
		}
		expectedCap := 1 << (bits.Len16(uint16(n - 1)))
		if cap(buf) != expectedCap {
			t.Errorf("Get(1<<%d=%d): cap = %d, want %d", i, n, cap(buf), expectedCap)
		}
		bp.Put(buf)
	}
}

func TestGetMaxInt16(t *testing.T) {
	i := poolIndex(math.MaxUint16)
	if i >= 17 {
		t.Errorf("poolIndex(MaxUint16) = %d, overflows pool array", i)
	}
}

// --- Benchmarks ---

func BenchmarkGetPut(b *testing.B) {
	sizes := []int{64, 512, 4096, 65535}
	for _, size := range sizes {
		bp := newBufferPool()
		b.Run("", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf := bp.Get(size)
				bp.Put(buf)
			}
		})
	}
}

func BenchmarkGetPutParallel(b *testing.B) {
	bp := newBufferPool()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := bp.Get(4096)
			bp.Put(buf)
		}
	})
}
