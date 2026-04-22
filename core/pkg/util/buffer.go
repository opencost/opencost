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

// Buffer is a utility type which implements a very basic binary protocol for
// writing core go types.
type Buffer struct {
	b  *bufio.Reader
	bw *bytes.Buffer
}

// NewBuffer creates a new Buffer instance using LittleEndian ByteOrder.
func NewBuffer() *Buffer {
	var b bytes.Buffer
	return &Buffer{
		b:  nil,
		bw: &b,
	}
}

// NewBufferFromBytes creates a new Buffer instance using the provided byte slice.
// The new buffer assumes ownership of the byte slice.
func NewBufferFromBytes(b []byte) *Buffer {
	return &Buffer{
		bw: bytes.NewBuffer(b),
	}
}

// NewBufferFrom creates a new Buffer instance using the remaining unread data from the
// provided Buffer instance. The new buffer assumes ownership of the underlying data.
func NewBufferFrom(b *Buffer) *Buffer {
	bb := b.Bytes()
	return &Buffer{
		bw: bytes.NewBuffer(bb),
	}
}

// NewBufferFromReader creates a new Buffer instance using the provided io.Reader. This
// buffer is set to read-only.
func NewBufferFromReader(reader io.Reader) *Buffer {
	return &Buffer{
		b:  bufio.NewReader(reader),
		bw: nil,
	}
}

// WriteBool writes a bool value to the buffer
func (b *Buffer) WriteBool(i bool) {
	b.checkRO()
	writeBool(b.bw, i)
}

// WriteInt writes an int value to the buffer.
func (b *Buffer) WriteInt(i int) {
	b.checkRO()
	writeInt(b.bw, i)
}

// WriteInt8 writes an int8 value to the buffer.
func (b *Buffer) WriteInt8(i int8) {
	b.checkRO()
	writeInt8(b.bw, i)
}

// WriteInt16 writes an int16 value to the buffer.
func (b *Buffer) WriteInt16(i int16) {
	b.checkRO()
	writeInt16(b.bw, i)
}

// WriteInt32 writes an int32 value to the buffer.
func (b *Buffer) WriteInt32(i int32) {
	b.checkRO()
	writeInt32(b.bw, i)
}

// WriteInt64 writes an int64 value to the buffer.
func (b *Buffer) WriteInt64(i int64) {
	b.checkRO()
	writeInt64(b.bw, i)
}

// WriteUInt writes a uint value to the buffer.
func (b *Buffer) WriteUInt(i uint) {
	b.checkRO()
	writeUint(b.bw, i)
}

// WriteUInt8 writes a uint8 value to the buffer.
func (b *Buffer) WriteUInt8(i uint8) {
	b.checkRO()
	writeUint8(b.bw, i)
}

// WriteUInt16 writes a uint16 value to the buffer.
func (b *Buffer) WriteUInt16(i uint16) {
	b.checkRO()
	writeUint16(b.bw, i)
}

// WriteUInt32 writes a uint32 value to the buffer.
func (b *Buffer) WriteUInt32(i uint32) {
	b.checkRO()
	writeUint32(b.bw, i)
}

// WriteUInt64 writes a uint64 value to the buffer.
func (b *Buffer) WriteUInt64(i uint64) {
	b.checkRO()
	writeUint64(b.bw, i)
}

// WriteFloat32 writes a float32 value to the buffer.
func (b *Buffer) WriteFloat32(i float32) {
	b.checkRO()
	writeFloat32(b.bw, i)
}

// WriteFloat64 writes a float64 value to the buffer.
func (b *Buffer) WriteFloat64(i float64) {
	b.checkRO()
	writeFloat64(b.bw, i)
}

// WriteString writes the string's length as a uint16 followed by the string contents.
func (b *Buffer) WriteString(i string) {
	b.checkRO()
	s := stringToBytes(i)

	// string lengths are limited to uint16 - See ReadString()
	if len(s) > math.MaxUint16 {
		s = s[:math.MaxUint16]
	}
	writeUint16(b.bw, uint16(len(s)))
	b.bw.Write(s)
}

// WriteBytes writes the contents of the byte slice to the buffer.
func (b *Buffer) WriteBytes(bytes []byte) {
	b.checkRO()
	b.bw.Write(bytes)
}

// Bytes returns the unread portion of the underlying buffer storage. If the buffer was
// created with an `io.Reader`, then the remaining unread bytes are drained into a byte
// slice and returned.
func (b *Buffer) Bytes() []byte {
	if b.bw != nil {
		return b.bw.Bytes()
	}

	bytes, err := io.ReadAll(b.b)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read remaining bytes from Buffer: %s\n", err)
	}
	return bytes
}

func (b *Buffer) Peek(length int) ([]byte, error) {
	if b.bw != nil {
		return nil, fmt.Errorf("unsupported Peek() operation on read/write buffer.")
	}
	return b.b.Peek(length)
}

// this should be inlined
func (b *Buffer) checkRO() {
	if b.bw == nil {
		panic("Buffer is set to read-only")
	}
}

// ReadBool reads a bool value from the buffer.
func (b *Buffer) ReadBool() bool {
	var i bool
	if b.bw != nil {
		readBool(b.bw, &i)
		return i
	}

	readBuffBool(b.b, &i)
	return i
}

// ReadInt reads an int value from the buffer.
func (b *Buffer) ReadInt() int {
	var i int
	if b.bw != nil {
		readInt(b.bw, &i)
		return i
	}

	readBuffInt(b.b, &i)
	return i
}

// ReadInt8 reads an int8 value from the buffer.
func (b *Buffer) ReadInt8() int8 {
	var i int8
	if b.bw != nil {
		readInt8(b.bw, &i)
		return i
	}

	readBuffInt8(b.b, &i)
	return i
}

// ReadInt16 reads an int16 value from the buffer.
func (b *Buffer) ReadInt16() int16 {
	var i int16
	if b.bw != nil {
		readInt16(b.bw, &i)
		return i
	}

	readBuffInt16(b.b, &i)
	return i
}

// ReadInt32 reads an int32 value from the buffer.
func (b *Buffer) ReadInt32() int32 {
	var i int32
	if b.bw != nil {
		readInt32(b.bw, &i)
		return i
	}

	readBuffInt32(b.b, &i)
	return i
}

// ReadInt64 reads an int64 value from the buffer.
func (b *Buffer) ReadInt64() int64 {
	var i int64
	if b.bw != nil {
		readInt64(b.bw, &i)
		return i
	}

	readBuffInt64(b.b, &i)
	return i
}

// ReadUInt reads a uint value from the buffer.
func (b *Buffer) ReadUInt() uint {
	var i uint
	if b.bw != nil {
		readUint(b.bw, &i)
		return i
	}

	readBuffUint(b.b, &i)
	return i
}

// ReadUInt8 reads a uint8 value from the buffer.
func (b *Buffer) ReadUInt8() uint8 {
	var i uint8
	if b.bw != nil {
		readUint8(b.bw, &i)
		return i
	}

	readBuffUint8(b.b, &i)
	return i
}

// ReadUInt16 reads a uint16 value from the buffer.
func (b *Buffer) ReadUInt16() uint16 {
	var i uint16
	if b.bw != nil {
		readUint16(b.bw, &i)
		return i
	}

	readBuffUint16(b.b, &i)
	return i
}

// ReadUInt32 reads a uint32 value from the buffer.
func (b *Buffer) ReadUInt32() uint32 {
	var i uint32
	if b.bw != nil {
		readUint32(b.bw, &i)
		return i
	}

	readBuffUint32(b.b, &i)
	return i
}

// ReadUInt64 reads a uint64 value from the buffer.
func (b *Buffer) ReadUInt64() uint64 {
	var i uint64
	if b.bw != nil {
		readUint64(b.bw, &i)
		return i
	}

	readBuffUint64(b.b, &i)
	return i
}

// ReadFloat32 reads a float32 value from the buffer.
func (b *Buffer) ReadFloat32() float32 {
	var i float32
	if b.bw != nil {
		readFloat32(b.bw, &i)
		return i
	}

	readBuffFloat32(b.b, &i)
	return i
}

// ReadFloat64 reads a float64 value from the buffer.
func (b *Buffer) ReadFloat64() float64 {
	var i float64
	if b.bw != nil {
		readFloat64(b.bw, &i)
		return i
	}

	readBuffFloat64(b.b, &i)
	return i
}

// ReadString reads a uint16 value from the buffer representing the string's length,
// then uses the length to extract the exact length []byte representing the string.
func (b *Buffer) ReadString() string {
	var l uint16
	if b.bw != nil {
		readUint16(b.bw, &l)
		return bytesToString(b.bw.Next(int(l)))
	}

	readBuffUint16(b.b, &l)

	bytes := bytePool.Get(int(l))
	defer bytePool.Put(bytes)

	_, err := readBuffFull(b.b, bytes)
	if err != nil {
		return ""
	}

	return bytesToString(bytes)
}

// ReadBytes reads the specified length from the buffer and returns the byte slice.
func (b *Buffer) ReadBytes(length int) []byte {
	if b.bw != nil {
		return b.bw.Next(length)
	}

	bytes := make([]byte, length)
	_, err := readBuffFull(b.b, bytes)
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
// In this case, we've create a byte array just big enough for the string, we extract the string data from the reader
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
