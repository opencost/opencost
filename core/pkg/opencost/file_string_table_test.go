package opencost

import (
	"testing"

	util "github.com/opencost/opencost/core/pkg/util"
)

func TestFileStringTableFromBuffer_RoundTrip(t *testing.T) {
	bw := util.NewBuffer()
	bw.WriteString("hello")
	bw.WriteString("")
	bw.WriteString("world")

	br := util.NewBufferFromBytes(bw.Bytes())
	ft, err := NewFileStringTableFromBuffer(br, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer ft.Close()

	if ft.Len() != 3 {
		t.Fatalf("len: %d", ft.Len())
	}
	s0, err := ft.StringAt(0)
	if err != nil || s0 != "hello" {
		t.Fatalf("0: %q %v", s0, err)
	}
	s1, err := ft.StringAt(1)
	if err != nil || s1 != "" {
		t.Fatalf("1: %q %v", s1, err)
	}
	s2, err := ft.StringAt(2)
	if err != nil || s2 != "world" {
		t.Fatalf("2: %q %v", s2, err)
	}
}
