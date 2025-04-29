package exporter

import (
	"bytes"
	"compress/gzip"
	"encoding"

	"github.com/opencost/opencost/core/pkg/util/json"
)

// Encoder[T] is a generic interface for encoding an instance of a T type into a byte slice.
type Encoder[T any] interface {
	Encode(*T) ([]byte, error)

	// FileExt returns the file extension for the encoded data. This can be used by a pathing strategy
	// to append the file extension when exporting the data. Returning an empty string will typically
	// omit the file extension completely.
	FileExt() string
}

// BinaryMarshalerPtr[T] is a generic constraint to ensure types passed to the encoder implement
// encoding.BinaryMarshaler and are pointers to T.
type BinaryMarshalerPtr[T any] interface {
	encoding.BinaryMarshaler
	*T
}

// BingenEncoder[T, U] is a generic encoder that uses the BinaryMarshaler interface to encode data.
// It supports any type T that implements the encoding.BinaryMarshaler interface.
type BingenEncoder[T any, U BinaryMarshalerPtr[T]] struct{}

// NewBingenEncoder creates an `Encoder[T]` implementation which supports binary encoding for the `T`
// type.
func NewBingenEncoder[T any, U BinaryMarshalerPtr[T]]() Encoder[T] {
	return new(BingenEncoder[T, U])
}

// Encode encodes the provided data of type T into a byte slice using the BinaryMarshaler interface.
func (b *BingenEncoder[T, U]) Encode(data *T) ([]byte, error) {
	var bingenData U = data
	return bingenData.MarshalBinary()
}

// FileExt returns the file extension for the encoded data. In this case, it returns an empty string
// to indicate that there is no specific file extension for the binary encoded data.
func (b *BingenEncoder[T, U]) FileExt() string {
	return ""
}

// JSONEncoder[T] is a generic encoder that uses the JSON encoding format to encode data.
type JSONEncoder[T any] struct{}

// NewJSONEncoder creates an `Encoder[T]` implementation which supports JSON encoding for the `T`
// type.
func NewJSONEncoder[T any]() Encoder[T] {
	return new(JSONEncoder[T])
}

// Encode encodes the provided data of type T into a byte slice using JSON encoding.
func (j *JSONEncoder[T]) Encode(data *T) ([]byte, error) {
	return json.Marshal(data)
}

// FileExt returns the file extension for the encoded data. In this case, it returns "json" to indicate
// that the data is in JSON format.
func (j *JSONEncoder[T]) FileExt() string {
	return "json"
}

type GZipEncoder[T any] struct {
	encoder Encoder[T]
}

// NewGZipEncoder creates a new GZip encoder which wraps the provided encoder.
// The encoder is used to encode the data before compressing it with GZip.
func NewGZipEncoder[T any](encoder Encoder[T]) Encoder[T] {
	return &GZipEncoder[T]{
		encoder: encoder,
	}
}

// Encode encodes the provided data of type T into a byte slice using JSON encoding.
func (gz *GZipEncoder[T]) Encode(data *T) ([]byte, error) {
	encoded, err := gz.encoder.Encode(data)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	gzWriter, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	gzWriter.Write(encoded)
	gzWriter.Close()

	return buf.Bytes(), nil
}

// FileExt returns the file extension for the encoded data. In this case, it returns the wrapped encoder's
// file extension with ".gz" appended to indicate that the data is compressed with GZip.
func (gz *GZipEncoder[T]) FileExt() string {
	return gz.encoder.FileExt() + ".gz"
}
