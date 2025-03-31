package exporter

import "encoding"

// Encoder[T] is a generic interface for encoding an instance of a T type into a byte slice.
type Encoder[T any] interface {
	Encode(*T) ([]byte, error)
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
