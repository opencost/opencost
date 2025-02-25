package stringutil_test

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/opencost/opencost/core/pkg/util/stringutil"
)

var oldBank sync.Map

type bankTest struct {
	Bank     func(string) string
	BankFunc func(string, func() string) string
	Clear    func()
}

var (
	legacyTest = bankTest{
		Bank:     BankLegacy,
		BankFunc: func(s string, f func() string) string { return s },
		Clear:    ClearBankLegacy,
	}

	standardBankTest = bankTest{
		Bank:     stringutil.Bank,
		BankFunc: stringutil.BankFunc,
		Clear:    stringutil.ClearBank,
	}
)

// This is the old implementation of the string bank to use for comparison benchmarks
func BankLegacy(s string) string {
	ss, _ := oldBank.LoadOrStore(s, s)
	return ss.(string)
}

func ClearBankLegacy() {
	oldBank = sync.Map{}
}

func copyString(s string) string {
	return string([]byte(s))
}

func generateBenchData(totalStrings, totalUnique int) []string {
	randStrings := make([]string, 0, totalStrings)
	r := rand.New(rand.NewSource(27644437))

	// create totalUnique unique strings
	for range totalUnique {
		randStrings = append(
			randStrings,
			fmt.Sprintf("%s/%s/%s", stringutil.RandSeqWith(r, 10), stringutil.RandSeqWith(r, 10), stringutil.RandSeqWith(r, 10)),
		)
	}

	// set the seed such that the resulting "remainder" strings are deterministic for each bench
	r = rand.New(rand.NewSource(1523942))

	// append a random selection from 0-totalUnique to the list.
	for range totalStrings - totalUnique {
		randStrings = append(randStrings, strings.Clone(randStrings[r.Intn(totalUnique)]))
	}

	// shuffle the list of strings
	r.Shuffle(totalStrings, func(i, j int) { randStrings[i], randStrings[j] = randStrings[j], randStrings[i] })

	return randStrings
}

func benchmarkStringBank(b *testing.B, bt bankTest, totalStrings, totalUnique int, useBankFunc bool) {
	b.StopTimer()
	randStrings := generateBenchData(totalStrings, totalUnique)

	b.Run(b.Name(), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			for bb := 0; bb < totalStrings; bb++ {
				if useBankFunc {
					bt.BankFunc(randStrings[bb], func() string { return randStrings[bb] })
				} else {
					bt.Bank(randStrings[bb])
				}
			}
			b.StopTimer()
			bt.Clear()
			//runtime.GC()
			//debug.FreeOSMemory()
		}
	})
}

func BenchmarkLegacyStringBank90PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, legacyTest, 1_000_000, 100_000, false)
}

func BenchmarkLegacyStringBank75PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, legacyTest, 1_000_000, 250_000, false)
}

func BenchmarkLegacyStringBank50PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, legacyTest, 1_000_000, 100_000, false)
}

func BenchmarkLegacyStringBank25PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, legacyTest, 1_000_000, 750_000, false)
}

func BenchmarkLegacyStringBankNoDuplicate(b *testing.B) {
	benchmarkStringBank(b, legacyTest, 1_000_000, 1_000_000, false)
}

func BenchmarkStringBank90PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 100_000, false)
}

func BenchmarkStringBank75PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 250_000, false)
}

func BenchmarkStringBank50PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 100_000, false)
}

func BenchmarkStringBank25PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 750_000, false)
}

func BenchmarkStringBankNoDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 1_000_000, false)
}

func BenchmarkStringBankFunc90PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 100_000, true)
}

func BenchmarkStringBankFunc75PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 250_000, true)
}

func BenchmarkStringBankFunc50PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 100_000, true)
}

func BenchmarkStringBankFunc25PercentDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 750_000, true)
}

func BenchmarkStringBankFuncNoDuplicate(b *testing.B) {
	benchmarkStringBank(b, standardBankTest, 1_000_000, 1_000_000, true)
}
