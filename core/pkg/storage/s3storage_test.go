package storage

import (
	"testing"
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
