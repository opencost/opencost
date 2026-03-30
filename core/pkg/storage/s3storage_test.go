package storage

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/minio/minio-go/v7"
)

// TestS3Storage_protocol tests the protocol() method returns correct values based on insecure flag
func TestS3Storage_protocol(t *testing.T) {
	tests := []struct {
		name     string
		insecure bool
		want     string
	}{
		{
			name:     "secure connection returns HTTPS",
			insecure: false,
			want:     "HTTPS",
		},
		{
			name:     "insecure connection returns HTTP",
			insecure: true,
			want:     "HTTP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3 := &S3Storage{
				insecure: tt.insecure,
			}
			got := s3.protocol()
			if got != tt.want {
				t.Errorf("S3Storage.protocol() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetGetObjectRange(t *testing.T) {
	tests := []struct {
		name      string
		off       int64
		length    int64
		expectErr bool
	}{
		{
			name:      "full object range",
			off:       0,
			length:    -1,
			expectErr: false,
		},
		{
			name:      "offset to EOF range",
			off:       100,
			length:    -1,
			expectErr: false,
		},
		{
			name:      "bounded range",
			off:       128,
			length:    4096,
			expectErr: false,
		},
		{
			name:      "negative offset rejected",
			off:       -1,
			length:    -1,
			expectErr: true,
		},
		{
			name:      "zero length rejected",
			off:       0,
			length:    0,
			expectErr: true,
		},
		{
			name:      "invalid negative length rejected",
			off:       0,
			length:    -2,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &minio.GetObjectOptions{}
			err := setGetObjectRange(opts, tt.off, tt.length)
			if tt.expectErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestS3ChunkReader_ReadUsesRanges(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	var calls [][2]int64

	reader := newS3ChunkReader(int64(len(data)), 8, func(off, length int64) ([]byte, error) {
		calls = append(calls, [2]int64{off, length})
		end := off + length
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		return data[off:end], nil
	})
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading chunked reader failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got=%q want=%q", string(got), string(data))
	}

	wantCalls := [][2]int64{
		{0, 8},
		{8, 8},
		{16, 8},
		{24, 2},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("range calls mismatch: got=%v want=%v", calls, wantCalls)
	}
}

func TestS3ChunkReader_Close(t *testing.T) {
	reader := newS3ChunkReader(10, 4, func(off, length int64) ([]byte, error) {
		return []byte("xxxx"), nil
	})
	if err := reader.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	p := make([]byte, 4)
	_, err := reader.Read(p)
	if err == nil {
		t.Fatal("expected read error after close")
	}
}

func TestS3ChunkReader_PropagatesFetchError(t *testing.T) {
	reader := newS3ChunkReader(10, 4, func(off, length int64) ([]byte, error) {
		return nil, errors.New("fetch failed")
	})
	defer reader.Close()

	p := make([]byte, 4)
	_, err := reader.Read(p)
	if err == nil || err.Error() != "fetch failed" {
		t.Fatalf("expected fetch error, got: %v", err)
	}
}
