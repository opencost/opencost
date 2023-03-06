package aws

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/pricing/types"
	"net/http"
	"testing"
)

func TestBlobProvider_DownloadPricingFile(t *testing.T) {
	type fields struct {
		client *http.Client
	}
	type args struct {
		region  string
		profile string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []types.PriceList
		wantErr bool
	}{
		{
			name: "Test DownloadPricingFile",
			fields: fields{
				client: &http.Client{},
			},
			args: args{
				region:  "us-east-1",
				profile: "",
			},
			want:    []types.PriceList{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewBlobProvider(tt.args.region, tt.args.profile)
			if err != nil {
				t.Errorf("DownloadPricingFile() error = %v", err)
				return
			}
			got, err := client.DownloadPricingIndexFile(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("DownloadPricingFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) < 1 {
				{
					t.Errorf("DownloadPricingFile() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
