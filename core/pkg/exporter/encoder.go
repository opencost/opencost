package exporter

import (
	"bytes"
	"compress/gzip"
	"encoding"
	"fmt"
	"io"

	"github.com/opencost/opencost/core/pkg/util/json"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	BingenExt = "bingen"
	JSONExt   = "json"
	GZipExt   = "gz"
	PBExt     = "binpb"
)

// Encoder[T] is a generic interface for encoding an instance of a T type into a byte slice.
type Encoder[T any] interface {
	Encode(*T) ([]byte, error)

	// EncodeTo performs a streaming write to an io.Writer instead of writing and returning the full
	// binary encoding.
	EncodeTo(io.Writer, *T) error

	// FileExt returns the file extension for the encoded data. This can be used by a pathing strategy
	// to append the file extension when exporting the data. Returning an empty string will typically
	// omit the file extension completely.
	FileExt() string
}

// BinaryMarshalerPtr[T] is a generic constraint to ensure types passed to the encoder implement
// encoding.BinaryMarshaler and are pointers to T.
type BinaryMarshalerPtr[T any] interface {
	encoding.BinaryMarshaler
	MarshalBinaryTo(io.Writer) error
	*T
}

// BingenEncoder[T, U] is a generic encoder that uses the BinaryMarshaler interface to encode data.
// It supports any type T that implements the encoding.BinaryMarshaler interface.
type BingenEncoder[T any, U BinaryMarshalerPtr[T]] struct {
	fileExt string
}

// NewBingenEncoder creates an `Encoder[T]` implementation which supports binary encoding for the `T`
// type, and doesn't have a file extension.
func NewBingenEncoder[T any, U BinaryMarshalerPtr[T]]() Encoder[T] {
	return &BingenEncoder[T, U]{
		fileExt: "",
	}
}

// NewBingenFileEncoder creates a new `Encoder[T]` implementation which supports binary encoding for the
// 'T' type with the ".bingen" file extension.
func NewBingenFileEncoder[T any, U BinaryMarshalerPtr[T]]() Encoder[T] {
	return &BingenEncoder[T, U]{
		fileExt: BingenExt,
	}
}

// Encode encodes the provided data of type T into a byte slice using the BinaryMarshaler interface.
func (b *BingenEncoder[T, U]) Encode(data *T) ([]byte, error) {
	var bingenData U = data
	return bingenData.MarshalBinary()
}

// EncodeTo performs a streaming write to an io.Writer instead of writing and returning the full
// binary encoding.
func (b *BingenEncoder[T, U]) EncodeTo(writer io.Writer, data *T) error {
	var bingenData U = data
	return bingenData.MarshalBinaryTo(writer)
}

// FileExt returns the configured file extension for the encoded data. This may be an empty
// string when no file extension is configured, or a non-empty value such as "bingen".
func (b *BingenEncoder[T, U]) FileExt() string {
	return b.fileExt
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

// EncodeTo performs a streaming write to an io.Writer instead of writing and returning the full
// binary encoding.
func (j *JSONEncoder[T]) EncodeTo(writer io.Writer, data *T) error {
	jsonWriter := json.NewEncoder(writer)
	return jsonWriter.Encode(data)
}

// FileExt returns the file extension for the encoded data. In this case, it returns "json" to indicate
// that the data is in JSON format.
func (j *JSONEncoder[T]) FileExt() string {
	return JSONExt
}

type GZipEncoder[T any] struct {
	encoder Encoder[T]
	level   int
}

// NewGZipEncoder creates a new GZip encoder which wraps the provided encoder.
// The encoder is used to encode the data before compressing it with GZip.
func NewGZipEncoder[T any](encoder Encoder[T]) Encoder[T] {
	return NewGZipEncoderWithLevel(encoder, gzip.DefaultCompression)
}

// NewGZipEncoderWithLevel creates a new GZip encoder which wraps the provided encoder,
// and uses the specified encoding level when gzipping.
func NewGZipEncoderWithLevel[T any](encoder Encoder[T], level int) Encoder[T] {
	return &GZipEncoder[T]{
		encoder: encoder,
		level:   level,
	}
}

// Encode encodes the provided data of type T into a byte slice using JSON encoding.
func (gz *GZipEncoder[T]) Encode(data *T) ([]byte, error) {
	encoded, err := gz.encoder.Encode(data)
	if err != nil {
		return nil, fmt.Errorf("GZipEncoder: nested encode failure: %w", err)
	}

	compressed, err := gZipEncode(encoded, gz.level)
	if err != nil {
		return nil, fmt.Errorf("GZipEncoder: failed to compress encoded data: %w", err)
	}
	return compressed, nil
}

// EncodeTo performs a streaming write to an io.Writer instead of writing and returning the full
// binary encoding.
func (gz *GZipEncoder[T]) EncodeTo(writer io.Writer, data *T) error {
	gzWriter, err := gzip.NewWriterLevel(writer, gz.level)
	if err != nil {
		return fmt.Errorf("failed to create gzip writer: %w", err)
	}
	if err := gz.encoder.EncodeTo(gzWriter, data); err != nil {
		_ = gzWriter.Close()
		return fmt.Errorf("failed to encode to gzip writer: %w", err)
	}

	return gzWriter.Close()
}

func gZipEncode(data []byte, level int) ([]byte, error) {
	var buf bytes.Buffer

	gzWriter, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, err
	}

	if _, err := gzWriter.Write(data); err != nil {
		_ = gzWriter.Close()
		return nil, err
	}
	if err := gzWriter.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// FileExt returns the file extension for the encoded data. In this case, it returns the wrapped encoder's
// file extension with ".gz" appended to indicate that the data is compressed with GZip.
func (gz *GZipEncoder[T]) FileExt() string {
	prev := gz.encoder.FileExt()
	if prev == "" {
		return GZipExt
	}
	return fmt.Sprintf("%s.%s", prev, GZipExt)
}

// ProtoMessagePtr [T] is a generic constraint to ensure types passed to the encoder implement
// proto.Message and are pointers to T.
type ProtoMessagePtr[T any] interface {
	proto.Message
	*T
}

// ProtobufEncoder [T, U] is a generic encoder that uses the proto.Message interface to encode data.
// It supports any type T that implements the proto.Message interface.
type ProtobufEncoder[T any, U ProtoMessagePtr[T]] struct{}

// NewProtobufEncoder creates an `Encoder[T]` implementation which supports binary encoding for the `T`
// type.
func NewProtobufEncoder[T any, U ProtoMessagePtr[T]]() Encoder[T] {
	return new(ProtobufEncoder[T, U])
}

// Encode encodes the provided data of type T into a byte slice using the proto.Message interface.
func (p *ProtobufEncoder[T, U]) Encode(data *T) ([]byte, error) {
	var message U = data
	raw, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to encode protobuf message: %w", err)
	}
	return raw, nil
}

// EncodeTo performs a streaming write to an io.Writer instead of writing and returning the full
// binary encoding.
func (p *ProtobufEncoder[T, U]) EncodeTo(writer io.Writer, data *T) error {
	var message U = data
	bytes, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to encode protobuf message: %w", err)
	}
	if _, err = writer.Write(bytes); err != nil {
		return fmt.Errorf("failed to write encoded message to writer: %w", err)
	}

	return nil
}

// FileExt returns the file extension for the encoded data. In this case, it returns an empty string
// to indicate that there is no specific file extension for the binary encoded data.
func (p *ProtobufEncoder[T, U]) FileExt() string {
	return PBExt
}

// ProtoJsonEncoder [T, U] is a generic encoder that uses the proto.Message interface to encode data in json format.
// It supports any type T that implements the proto.Message interface.
type ProtoJsonEncoder[T any, U ProtoMessagePtr[T]] struct{}

// NewProtoJsonEncoder creates an `Encoder[T]` implementation which supports binary encoding for the `T`
// type.
func NewProtoJsonEncoder[T any, U ProtoMessagePtr[T]]() Encoder[T] {
	return new(ProtoJsonEncoder[T, U])
}

// Encode encodes the provided data of type T into a byte slice using the proto.Message interface.
func (p *ProtoJsonEncoder[T, U]) Encode(data *T) ([]byte, error) {
	var message U = data
	raw, err := protojson.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to encode protobuf message to json: %w", err)
	}
	return raw, nil
}

// EncodeTo performs a streaming write to an io.Writer instead of writing and returning the full
// binary encoding.
func (p *ProtoJsonEncoder[T, U]) EncodeTo(writer io.Writer, data *T) error {
	var message U = data
	// protojson doesn't have a way to marshal directly to an io.Writer, so we'll encode as normal,
	// and write the resulting data out to the writer
	bytes, err := protojson.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal protojson: %w", err)
	}
	_, err = writer.Write(bytes)
	if err != nil {
		return fmt.Errorf("failed to write encoded protojson to writer: %w", err)
	}
	return nil
}

// FileExt returns the file extension for the encoded data. In this case, it returns an empty string
// to indicate that there is no specific file extension for the binary encoded data.
func (p *ProtoJsonEncoder[T, U]) FileExt() string {
	return JSONExt
}
