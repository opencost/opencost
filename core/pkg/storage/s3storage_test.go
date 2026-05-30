package storage

import (
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
