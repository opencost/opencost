package exporter

import "encoding"

// Encoder[T] is a generic interface for encoding an instance of a T type into a byte slice.
type Encoder[T any] interface {
	Encode(*T) ([]byte, error)
}

type BinaryMarshalerPtr[T any] interface {
	encoding.BinaryMarshaler
	*T
}

type BingenEncoder[T any, U BinaryMarshalerPtr[T]] struct{}

func NewBingenEncoder[T any, U BinaryMarshalerPtr[T]]() Encoder[T] {
	return new(BingenEncoder[T, U])
}

func (b *BingenEncoder[T, U]) Encode(data *T) ([]byte, error) {
	var bingenData U = data
	return bingenData.MarshalBinary()
}
