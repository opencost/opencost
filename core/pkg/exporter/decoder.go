package exporter

import (
	"bytes"
	"compress/gzip"
	"encoding"
	"fmt"
	"io"

	"github.com/opencost/opencost/core/pkg/util/json"
	"google.golang.org/protobuf/proto"
)

type Decoder[T any] func([]byte) (*T, error)

// BinaryMarshalerPtr[T] is a generic constraint to ensure types passed to the encoder implement
// encoding.BinaryMarshaler and are pointers to T.
type BinaryUnmarshalerPtr[T any] interface {
	encoding.BinaryUnmarshaler
	*T
}

func BingenDecoder[T any, U BinaryUnmarshalerPtr[T]](data []byte) (*T, error) {
	var set U = new(T)

	err := set.UnmarshalBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode bingen: %w", err)
	}

	return set, nil
}

func JSONDecoder[T any](data []byte) (*T, error) {
	var instance = new(T)
	err := json.Unmarshal(data, instance)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json: %w", err)
	}

	return instance, nil
}

func ProtobufDecoder[T any, U ProtoMessagePtr[T]](data []byte) (*T, error) {
	var message U = new(T)

	err := proto.Unmarshal(data, message)
	if err != nil {
		return nil, fmt.Errorf("failed to decode protobuf message: %w", err)
	}

	return message, nil
}

func GetGzipDecoder[T any](decoder Decoder[T]) Decoder[T] {
	return func(data []byte) (*T, error) {
		// Check for gzip compression. Ref: https://www.ietf.org/rfc/rfc1952.txt Page 5
		if len(data) > 2 && data[0] == 0x1F && data[1] == 0x8B {
			buf := bytes.NewBuffer(data)
			reader, err := gzip.NewReader(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to decompress gzip: %w", err)

			}
			defer reader.Close()
			decompressed, err := io.ReadAll(reader)
			if err != nil {
				return nil, fmt.Errorf("failed to read decompressed gzip: %w", err)
			}

			data = decompressed
		}

		instance, err := decoder(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode decompress gzip data: %w", err)
		}

		return instance, nil
	}
}
