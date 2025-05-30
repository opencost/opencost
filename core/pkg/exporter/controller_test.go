package exporter

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
)

type TObject struct {
	Window  opencost.Window
	Message string
}

func (tobj *TObject) MarshalBinary() ([]byte, error) {
	ctx := &opencost.EncodingContext{
		Buffer: util.NewBuffer(),
		Table:  nil,
	}

	err := tobj.Window.MarshalBinaryWithContext(ctx)
	if err != nil {
		return nil, err
	}

	ctx.Buffer.WriteString(tobj.Message)
	return ctx.Buffer.Bytes(), nil
}

func (tobj *TObject) UnmarshalBinary(data []byte) error {
	ctx := &opencost.DecodingContext{
		Buffer: util.NewBufferFromBytes(data),
		Table:  nil,
	}

	err := tobj.Window.UnmarshalBinaryWithContext(ctx)
	if err != nil {
		return err
	}

	tobj.Message = ctx.Buffer.ReadString()
	return nil
}

func (tobj *TObject) IsEmpty() bool {
	return tobj.Message == ""
}

type TObjectSource struct {
	count atomic.Uint64
}

func (s *TObjectSource) CanCompute(start, end time.Time) bool {
	return true
}
func (s *TObjectSource) Compute(start, end time.Time, resolution time.Duration) (*TObject, error) {
	c := s.count.Add(1)
	return &TObject{
		Window:  opencost.NewClosedWindow(start, end),
		Message: fmt.Sprintf("Message #%d", c),
	}, nil
}
func (s *TObjectSource) Name() string {
	return "TObjectSource"
}

type TObjectComputeExporter struct {
	encoder Encoder[TObject]
	data    map[string][]byte
}

func NewTObjectComputeExporter() *TObjectComputeExporter {
	return &TObjectComputeExporter{
		encoder: NewBingenEncoder[TObject](),
		data:    make(map[string][]byte),
	}
}

func (t *TObjectComputeExporter) Export(window opencost.Window, data *TObject) error {
	d, err := data.MarshalBinary()
	if err != nil {
		return err
	}

	t.data[window.String()] = d
	return nil
}

func TestTObjectSymmetry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	future := now.Add(time.Hour)

	tobj := &TObject{
		Window:  opencost.NewClosedWindow(now, future),
		Message: "Testing 1 2 3",
	}

	data, err := tobj.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	tobj2 := &TObject{}
	err = tobj2.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if tobj2.Message != tobj.Message && tobj2.Message == "Testing 1 2 3" {
		t.Fatalf("Message mismatch: %s != %s", tobj2.Message, tobj.Message)
	}

	if !tobj2.Window.Start().Equal(*tobj.Window.Start()) || !tobj2.Window.Start().Equal(now) {
		t.Fatalf("Start time mismatch: %s != %s", tobj2.Window.Start(), tobj.Window.Start())
	}
	if !tobj2.Window.End().Equal(*tobj.Window.End()) || !tobj2.Window.End().Equal(future) {
		t.Fatalf("End time mismatch: %s != %s", tobj2.Window.End(), tobj.Window.End())
	}
}
