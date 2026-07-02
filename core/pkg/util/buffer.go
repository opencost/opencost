package util

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"unsafe"

	"github.com/opencost/opencost/core/pkg/util/stringutil"
)

var bytePool *bufferPool = newBufferPool()

// NonPrimitiveTypeError represents an error where the user provided a non-primitive data type for reading/writing
var NonPrimitiveTypeError error = errors.New("Type provided to read/write does not fit inside 8 bytes.")

// Mode is used to represent the 3 possible states of the buffer. note there is
// no overlapping between states, as each Mode is handled exclusively.
type Mode uint8

const (
	ReadWrite Mode = iota
	ReadOnly
	WriteOnly
)

// Buffer is a utility type which implements a very basic binary protocol for
// writing core go types. It can run as read-only, write-only, or read-write.
type Buffer struct {
	r  *bufio.Reader
	w  *bufio.Writer
	rw *bytes.Buffer
	m  Mode
}

// NewBuffer creates a new Buffer instance using LittleEndian ByteOrder.
func NewBuffer() *Buffer {
	return &Buffer{
		r:  nil,
		w:  nil,
		rw: new(bytes.Buffer),
		m:  ReadWrite,
	}
}

// NewBufferFromBytes creates a new read/write Buffer instance using the provided byte slice.
// The new buffer assumes ownership of the byte slice.
func NewBufferFromBytes(b []byte) *Buffer {
	return &Buffer{
		r:  nil,
		w:  nil,
		rw: bytes.NewBuffer(b),
		m:  ReadWrite,
	}
}

// NewBufferFrom creates a new Buffer instance using the remaining unread data from the
// provided Buffer instance. The new buffer assumes ownership of the underlying data.
func NewBufferFrom(b *Buffer) *Buffer {
	bb := b.Bytes()
	return &Buffer{
		r:  nil,
		w:  nil,
		rw: bytes.NewBuffer(bb),
		m:  ReadWrite,
	}
}

// NewBufferFromReader creates a new Buffer instance using the provided io.Reader. This
// buffer is set to read-only.
func NewBufferFromReader(reader io.Reader) *Buffer {
	return &Buffer{
		r:  bufio.NewReader(reader),
		w:  nil,
		rw: nil,
		m:  ReadOnly,
	}
}

// NewBufferFromWriter creates a new Buffer instance using the provided io.Writer. This
// buffer is set to write-only.
func NewBufferFromWriter(writer io.Writer) *Buffer {
	return &Buffer{
		r:  nil,
		w:  bufio.NewWriter(writer),
		rw: nil,
		m:  WriteOnly,
	}
}

// WriteBool writes a bool value to the buffer
func (b *Buffer) WriteBool(i bool) {
	b.checkRO()

	if b.rw != nil {
		writeBool(b.rw, i)
		return
	}

	writeBuffBool(b.w, i)
}

// WriteInt writes an int value to the buffer.
func (b *Buffer) WriteInt(i int) {
	b.checkRO()

	if b.rw != nil {
		writeInt(b.rw, i)
		return
	}

	writeBuffInt(b.w, i)
}

// WriteInt8 writes an int8 value to the buffer.
func (b *Buffer) WriteInt8(i int8) {
	b.checkRO()

	if b.rw != nil {
		writeInt8(b.rw, i)
		return
	}

	writeBuffInt8(b.w, i)
}

// WriteInt16 writes an int16 value to the buffer.
func (b *Buffer) WriteInt16(i int16) {
	b.checkRO()

	if b.rw != nil {
		writeInt16(b.rw, i)
		return
	}

	writeBuffInt16(b.w, i)
}

// WriteInt32 writes an int32 value to the buffer.
func (b *Buffer) WriteInt32(i int32) {
	b.checkRO()

	if b.rw != nil {
		writeInt32(b.rw, i)
		return
	}

	writeBuffInt32(b.w, i)
}

// WriteInt64 writes an int64 value to the buffer.
func (b *Buffer) WriteInt64(i int64) {
	b.checkRO()

	if b.rw != nil {
		writeInt64(b.rw, i)
		return
	}

	writeBuffInt64(b.w, i)
}

// WriteUInt writes a uint value to the buffer.
func (b *Buffer) WriteUInt(i uint) {
	b.checkRO()

	if b.rw != nil {
		writeUint(b.rw, i)
		return
	}

	writeBuffUint(b.w, i)
}

// WriteUInt8 writes a uint8 value to the buffer.
func (b *Buffer) WriteUInt8(i uint8) {
	b.checkRO()

	if b.rw != nil {
		writeUint8(b.rw, i)
		return
	}

	writeBuffUint8(b.w, i)
}

// WriteUInt16 writes a uint16 value to the buffer.
func (b *Buffer) WriteUInt16(i uint16) {
	b.checkRO()

	if b.rw != nil {
		writeUint16(b.rw, i)
		return
	}

	writeBuffUint16(b.w, i)
}

// WriteUInt32 writes a uint32 value to the buffer.
func (b *Buffer) WriteUInt32(i uint32) {
	b.checkRO()

	if b.rw != nil {
		writeUint32(b.rw, i)
		return
	}

	writeBuffUint32(b.w, i)
}

// WriteUInt64 writes a uint64 value to the buffer.
func (b *Buffer) WriteUInt64(i uint64) {
	b.checkRO()

	if b.rw != nil {
		writeUint64(b.rw, i)
		return
	}

	writeBuffUint64(b.w, i)
}

// WriteFloat32 writes a float32 value to the buffer.
func (b *Buffer) WriteFloat32(i float32) {
	b.checkRO()

	if b.rw != nil {
		writeFloat32(b.rw, i)
		return
	}

	writeBuffFloat32(b.w, i)
}

// WriteFloat64 writes a float64 value to the buffer.
func (b *Buffer) WriteFloat64(i float64) {
	b.checkRO()

	if b.rw != nil {
		writeFloat64(b.rw, i)
		return
	}

	writeBuffFloat64(b.w, i)
}

// WriteString writes the string's length as a uint16 followed by the string contents.
func (b *Buffer) WriteString(i string) {
	b.checkRO()
	s := stringToBytes(i)

	// string lengths are limited to uint16 - See ReadString()
	if len(s) > math.MaxUint16 {
		s = s[:math.MaxUint16]
	}

	l := uint16(len(s))

	if b.rw != nil {
		writeUint16(b.rw, l)
		b.rw.Write(s)
		return
	}

	writeBuffUint16(b.w, l)
	b.w.Write(s)
}

// WriteBytes writes the contents of the byte slice to the buffer.
func (b *Buffer) WriteBytes(bytes []byte) {
	b.checkRO()

	if b.rw != nil {
		b.rw.Write(bytes)
		return
	}

	b.w.Write(bytes)
}

// Bytes returns the unread portion of the underlying buffer storage. If the buffer was
// created with an `io.Reader`, then the remaining unread bytes are drained into a byte
// slice and returned.
func (b *Buffer) Bytes() []byte {
	b.checkWO()

	if b.rw != nil {
		return b.rw.Bytes()
	}

	bytes, err := io.ReadAll(b.r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read remaining bytes from Buffer: %s\n", err)
	}
	return bytes
}

// Peek will attempt to peek ahead if the buffer is in read-only mode.
func (b *Buffer) Peek(length int) ([]byte, error) {
	b.checkWO()

	if b.rw != nil {
		return nil, fmt.Errorf("unsupported Peek() operation on read/write buffer.")
	}

	return b.r.Peek(length)
}

// Flush will attempt to flush any pending writes if the buffer is in write-only mode.
func (b *Buffer) Flush() {
	if b.IsWriteOnly() {
		if err := b.w.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "Flushing io.Writer failed: %s\n", err)
		}
	}
}

// this should be inlined
func (b *Buffer) checkRO() {
	if b.IsReadOnly() {
		panic("Tried to write to a Buffer that is set to read-only")
	}
}

func (b *Buffer) checkWO() {
	if b.IsWriteOnly() {
		panic("Tried to read from a Buffer that is set to write-only")
	}
}

// IsReadOnly returns true if the buffer is set to only read mode.
func (b *Buffer) IsReadOnly() bool {
	return b.m == ReadOnly
}

// IsWriteOnly returns true if the buffer is set to only write mode.
func (b *Buffer) IsWriteOnly() bool {
	return b.m == WriteOnly
}

// IsReadWrite returns true if the buffer can be written to and read from.
func (b *Buffer) IsReadWrite() bool {
	return b.m == ReadWrite
}

// ReadBool reads a bool value from the buffer.
func (b *Buffer) ReadBool() bool {
	b.checkWO()

	var i bool
	if b.rw != nil {
		readBool(b.rw, &i)
		return i
	}

	readBuffBool(b.r, &i)
	return i
}

// ReadInt reads an int value from the buffer.
func (b *Buffer) ReadInt() int {
	b.checkWO()

	var i int
	if b.rw != nil {
		readInt(b.rw, &i)
		return i
	}

	readBuffInt(b.r, &i)
	return i
}

// ReadInt8 reads an int8 value from the buffer.
func (b *Buffer) ReadInt8() int8 {
	b.checkWO()

	var i int8
	if b.rw != nil {
		readInt8(b.rw, &i)
		return i
	}

	readBuffInt8(b.r, &i)
	return i
}

// ReadInt16 reads an int16 value from the buffer.
func (b *Buffer) ReadInt16() int16 {
	b.checkWO()

	var i int16
	if b.rw != nil {
		readInt16(b.rw, &i)
		return i
	}

	readBuffInt16(b.r, &i)
	return i
}

// ReadInt32 reads an int32 value from the buffer.
func (b *Buffer) ReadInt32() int32 {
	b.checkWO()

	var i int32
	if b.rw != nil {
		readInt32(b.rw, &i)
		return i
	}

	readBuffInt32(b.r, &i)
	return i
}

// ReadInt64 reads an int64 value from the buffer.
func (b *Buffer) ReadInt64() int64 {
	b.checkWO()

	var i int64
	if b.rw != nil {
		readInt64(b.rw, &i)
		return i
	}

	readBuffInt64(b.r, &i)
	return i
}

// ReadUInt reads a uint value from the buffer.
func (b *Buffer) ReadUInt() uint {
	b.checkWO()

	var i uint
	if b.rw != nil {
		readUint(b.rw, &i)
		return i
	}

	readBuffUint(b.r, &i)
	return i
}

// ReadUInt8 reads a uint8 value from the buffer.
func (b *Buffer) ReadUInt8() uint8 {
	b.checkWO()

	var i uint8
	if b.rw != nil {
		readUint8(b.rw, &i)
		return i
	}

	readBuffUint8(b.r, &i)
	return i
}

// ReadUInt16 reads a uint16 value from the buffer.
func (b *Buffer) ReadUInt16() uint16 {
	b.checkWO()

	var i uint16
	if b.rw != nil {
		readUint16(b.rw, &i)
		return i
	}

	readBuffUint16(b.r, &i)
	return i
}

// ReadUInt32 reads a uint32 value from the buffer.
func (b *Buffer) ReadUInt32() uint32 {
	b.checkWO()

	var i uint32
	if b.rw != nil {
		readUint32(b.rw, &i)
		return i
	}

	readBuffUint32(b.r, &i)
	return i
}

// ReadUInt64 reads a uint64 value from the buffer.
func (b *Buffer) ReadUInt64() uint64 {
	b.checkWO()

	var i uint64
	if b.rw != nil {
		readUint64(b.rw, &i)
		return i
	}

	readBuffUint64(b.r, &i)
	return i
}

// ReadFloat32 reads a float32 value from the buffer.
func (b *Buffer) ReadFloat32() float32 {
	b.checkWO()

	var i float32
	if b.rw != nil {
		readFloat32(b.rw, &i)
		return i
	}

	readBuffFloat32(b.r, &i)
	return i
}

// ReadFloat64 reads a float64 value from the buffer.
func (b *Buffer) ReadFloat64() float64 {
	b.checkWO()

	var i float64
	if b.rw != nil {
		readFloat64(b.rw, &i)
		return i
	}

	readBuffFloat64(b.r, &i)
	return i
}

// ReadString reads a uint16 value from the buffer representing the string's length,
// then uses the length to extract the exact length []byte representing the string.
func (b *Buffer) ReadString() string {
	b.checkWO()

	var l uint16
	if b.rw != nil {
		readUint16(b.rw, &l)
		return bytesToString(b.rw.Next(int(l)))
	}

	readBuffUint16(b.r, &l)

	bytes := bytePool.Get(int(l))
	defer bytePool.Put(bytes)

	_, err := readBuffFull(b.r, bytes)
	if err != nil {
		return ""
	}

	return bytesToString(bytes)
}

// ReadBytes reads the specified length from the buffer and returns the byte slice.
func (b *Buffer) ReadBytes(length int) []byte {
	b.checkWO()

	if b.rw != nil {
		return b.rw.Next(length)
	}

	bytes := make([]byte, length)
	_, err := readBuffFull(b.r, bytes)
	if err != nil {
		return bytes
	}

	return bytes
}

// bytesAsString converts a []byte into a string in place. Note that you should use this helper
// when the []byte slice contains _only_ the string data and isn't part of a larger underlying array.
// For example, a case where you should *not* use this helper:
//
//	func parseString(buffer *bytes.Buffer, length int) string {
//	  bytes := buffer.Next(length)   // this extracts a sub-slice of the underlying byte array from pos->pos+length
//
//	  return bytesAsString(bytes)
//	}
//
// Now both the []byte AND the value string are linked and neither can be GC'd until the other one is GC'd.
// This is especially problematic if you drop the references to the byte array, as you're effectively requiring
// 1024 bytes for an 11-byte string.
//
// An example where it _is_ ok, and recommended to drop the underlying []byte reference is the following:
//
//	func parseString(reader io.Reader, length int) string {
//	  bytes := make([]byte, length)
//	  io.ReadFull(reader, bytes)
//
//	  return bytesAsString(bytes)
//	}
//
// In this case, we've created a byte array just big enough for the string, we extract the string data from the reader
// and then cast the byte array in place to the string, and finally drop the byte array reference. This omits an additional
// allocation if you were to use string(bytes)
func bytesAsString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// Conversion from byte slice to string
func bytesToString(b []byte) string {
	// This code will take the passed byte slice and cast it in-place into a string. By doing
	// this, we are pinning the byte slice's underlying array in memory, preventing it from
	// being garbage collected while the string is still in use. If we are using the Bank()
	// functionality to cache new strings, we risk keeping the pinned array alive. To avoid this,
	// we will use the BankFunc() call which uses the casted string to check for existence of a
	// cached string. If it exists, then we drop the pinned reference immediately and use the
	// cached string. If it does _not_ exist, then we use the passed func() string to allocate a new
	// string and cache it. This will prevent us from allocating throw-away strings just to
	// check our cache.
	pinned := bytesAsString(b)

	return stringutil.BankFunc(pinned, func() string {
		return string(b)
	})
}

// Direct string to byte conversion that doesn't allocate.
func stringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
