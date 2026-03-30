package opencost

import (
	"fmt"
	"io"
	"os"

	util "github.com/opencost/opencost/core/pkg/util"
)

// fileStringRef maps a bingen string-table index to a payload stored in a temp file.
type fileStringRef struct {
	off    int64
	length int
}

// FileStringTable holds string-table payloads on disk and resolves indices on demand.
type FileStringTable struct {
	f    *os.File
	refs []fileStringRef
}

// NewFileStringTableFromBuffer reads exactly tl length-prefixed (uint16) string payloads from buff
// and appends each payload to a new temp file. It does not retain full strings in memory.
func NewFileStringTableFromBuffer(buff *util.Buffer, tl int) (*FileStringTable, error) {
	os.MkdirAll("/var/lib/clickhouse/tmp", 0755)
	f, err := os.CreateTemp("/var/lib/clickhouse/tmp", "opencost-bgst-*")
	if err != nil {
		return nil, fmt.Errorf("opencost: create string table file: %w", err)
	}

	t := &FileStringTable{f: f, refs: make([]fileStringRef, tl)}
	var writeErr error
	defer func() {
		if writeErr != nil {
			_ = t.Close()
		}
	}()

	for i := 0; i < tl; i++ {
		payload, err := buff.ReadStringBytes()
		if err != nil {
			writeErr = err
			return nil, fmt.Errorf("opencost: read string table entry %d: %w", i, err)
		}
		var off int64
		if len(payload) > 0 {
			off, err = f.Seek(0, io.SeekEnd)
			if err != nil {
				writeErr = err
				return nil, fmt.Errorf("opencost: seek string table file: %w", err)
			}
			if _, err := f.Write(payload); err != nil {
				writeErr = err
				return nil, fmt.Errorf("opencost: write string table entry %d: %w", i, err)
			}
		}
		t.refs[i] = fileStringRef{off: off, length: len(payload)}
	}

	return t, nil
}

// Len returns the number of strings in the table.
func (t *FileStringTable) Len() int {
	if t == nil {
		return 0
	}
	return len(t.refs)
}

// StringAt returns the string for wire index i, reading from the backing file.
func (t *FileStringTable) StringAt(i int) (string, error) {
	if t == nil || t.f == nil {
		return "", fmt.Errorf("opencost: closed or nil string table")
	}
	if i < 0 || i >= len(t.refs) {
		return "", fmt.Errorf("opencost: string table index %d out of range [0,%d)", i, len(t.refs))
	}
	ref := t.refs[i]
	if ref.length == 0 {
		return "", nil
	}
	buf := make([]byte, ref.length)
	n, err := t.f.ReadAt(buf, ref.off)
	if err != nil {
		return "", err
	}
	if n != ref.length {
		return "", fmt.Errorf("opencost: short read in string table at %d", i)
	}
	return string(buf), nil
}

// Close releases the backing file (and removes the temp file).
func (t *FileStringTable) Close() error {
	if t == nil || t.f == nil {
		return nil
	}
	path := t.f.Name()
	err := t.f.Close()
	t.f = nil
	t.refs = nil
	if path != "" {
		_ = os.Remove(path)
	}
	return err
}

func newDecodingContextFromBytes(data []byte) (*DecodingContext, error) {
	buff := util.NewBufferFromBytes(data)
	if !isBinaryTag(data, BinaryTagStringTable) {
		return &DecodingContext{Buffer: buff}, nil
	}

	buff.ReadBytes(len(BinaryTagStringTable))
	tl := buff.ReadInt()
	if tl <= 0 {
		return &DecodingContext{Buffer: buff}, nil
	}

	ft, err := NewFileStringTableFromBuffer(buff, tl)
	if err != nil {
		return nil, err
	}
	return &DecodingContext{Buffer: buff, FileTable: ft}, nil
}

// IsStringTable returns true if a non-empty string table is present (in-memory and/or file-backed).
func (dc *DecodingContext) IsStringTable() bool {
	return dc != nil && (len(dc.Table) > 0 || (dc.FileTable != nil && dc.FileTable.Len() > 0))
}

// CloseFileTable closes and removes the backing file for the string table, if any.
func (dc *DecodingContext) CloseFileTable() {
	if dc == nil || dc.FileTable == nil {
		return
	}
	_ = dc.FileTable.Close()
	dc.FileTable = nil
}

func (dc *DecodingContext) tableString(i int) string {
	if len(dc.Table) > 0 {
		if i < 0 || i >= len(dc.Table) {
			panic(fmt.Sprintf("opencost: string table index %d out of range [0,%d)", i, len(dc.Table)))
		}
		return dc.Table[i]
	}
	if dc.FileTable == nil {
		panic(fmt.Sprintf("opencost: string table lookup with no file table (index %d)", i))
	}
	s, err := dc.FileTable.StringAt(i)
	if err != nil {
		panic(err)
	}
	return s
}

func newDecodingContextFromReader(reader io.Reader) (*DecodingContext, error) {
	buff := util.NewBufferFromReader(reader)
	if !isReaderBinaryTag(buff, BinaryTagStringTable) {
		return &DecodingContext{Buffer: buff}, nil
	}

	buff.ReadBytes(len(BinaryTagStringTable))
	tl := buff.ReadInt()
	if tl <= 0 {
		return &DecodingContext{Buffer: buff}, nil
	}

	ft, err := NewFileStringTableFromBuffer(buff, tl)
	if err != nil {
		return nil, err
	}
	return &DecodingContext{Buffer: buff, FileTable: ft}, nil
}
