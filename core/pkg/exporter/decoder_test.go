package exporter

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/model"
	"github.com/opencost/opencost/core/pkg/model/pb"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/json"
	"google.golang.org/protobuf/proto"
)

type decoderTestCase[T any] struct {
	name    string
	data    []byte
	want    *T
	wantErr bool
}

func generateBadBytes() []byte {
	buff := util.NewBuffer()
	for i := 0; i < 10; i++ {
		buff.WriteUInt64(9999999)
	}

	return buff.Bytes()
}

func TestBingenDecoder(t *testing.T) {
	badBytes := generateBadBytes()

	now := time.Now().UTC().Truncate(24 * time.Hour)
	start := now.Add(-(24 * 5) * time.Hour)

	// Define and Run Allocation Tests
	allocSet := opencost.GenerateMockAllocationSet(start)
	allocSetRaw, err := allocSet.MarshalBinary()
	if err != nil {
		t.Errorf("failed to marshal allocation set: %s", err.Error())
	}

	allocTests := []decoderTestCase[opencost.AllocationSet]{
		{
			name:    "allocation valid",
			data:    allocSetRaw,
			want:    allocSet,
			wantErr: false,
		},
		{
			name:    "allocation invalid",
			data:    badBytes,
			want:    nil,
			wantErr: true,
		},
	}

	testDecoder(t, BingenDecoder, allocTests)

	// Define and Run Asset Tests
	assetSet := opencost.GenerateMockAssetSet(start, 24*time.Hour)
	assetSetRaw, err := assetSet.MarshalBinary()
	if err != nil {
		t.Errorf("failed to marshal asset set: %s", err.Error())
	}

	assetTests := []decoderTestCase[opencost.AssetSet]{
		{
			name:    "asset valid",
			data:    assetSetRaw,
			want:    assetSet,
			wantErr: false,
		},
		{
			name:    "asset invalid",
			data:    badBytes,
			want:    nil,
			wantErr: true,
		},
	}

	testDecoder(t, BingenDecoder, assetTests)

	// Define and Run Cloud Cost Tests
	CloudCostSet := opencost.GenerateMockCloudCostSet(start, start.Add(24*time.Hour), "gcp", "gke")
	CloudCostSetRaw, err := CloudCostSet.MarshalBinary()
	if err != nil {
		t.Errorf("failed to marshal cloud cost set: %s", err.Error())
	}

	cloudCostTests := []decoderTestCase[opencost.CloudCostSet]{
		{
			name:    "cloud cost valid",
			data:    CloudCostSetRaw,
			want:    CloudCostSet,
			wantErr: false,
		},
		{
			name:    "cloud cost invalid",
			data:    badBytes,
			want:    nil,
			wantErr: true,
		},
	}

	testDecoder(t, BingenDecoder, cloudCostTests)

	// Define and Run Network Insight Tests
	networkInsightSet := opencost.GenerateMockNetworkInsightSet(start, start.Add(24*time.Hour))
	networkInsightSetRaw, err := networkInsightSet.MarshalBinary()
	if err != nil {
		t.Errorf("failed to marshal network insight set: %s", err.Error())
	}

	networkInsightTests := []decoderTestCase[opencost.NetworkInsightSet]{
		{
			name:    "network insight valid",
			data:    networkInsightSetRaw,
			want:    networkInsightSet,
			wantErr: false,
		},
		{
			name:    "network insight invalid",
			data:    badBytes,
			want:    nil,
			wantErr: true,
		},
	}

	testDecoder(t, BingenDecoder, networkInsightTests)
}

func TestJsonDecoder(t *testing.T) {
	badBytes := generateBadBytes()

	now := time.Now().UTC().Truncate(24 * time.Hour)
	start := now.Add(-(24 * 5) * time.Hour)

	hb := heartbeat.Heartbeat{
		Id:          "heartBeatID",
		Timestamp:   start,
		Uptime:      123,
		Application: "mock",
		Version:     "test",
		Metadata: map[string]any{
			"str": "test",
			"num": 1.0,
		},
	}
	hbraw, err := json.Marshal(hb)
	if err != nil {
		t.Errorf("failed to marshal heartbeat: %s", err.Error())
	}

	heartbeatTests := []decoderTestCase[heartbeat.Heartbeat]{
		{
			name:    "heartbeat valid",
			data:    hbraw,
			want:    &hb,
			wantErr: false,
		},
		{
			name:    "heartbeat invalid",
			data:    badBytes,
			want:    nil,
			wantErr: true,
		},
	}

	testDecoder(t, JSONDecoder, heartbeatTests)
}

func TestGzipDecoder(t *testing.T) {
	badBytes := generateBadBytes()

	now := time.Now().UTC().Truncate(24 * time.Hour)
	start := now.Add(-(24 * 5) * time.Hour)

	diag := diagnostics.DiagnosticResult{
		ID:          "diagnosticID",
		Name:        "diagnisticName",
		Description: "Test Diagnostic",
		Category:    "test",
		Timestamp:   start,
		Error:       "test",
		Details: map[string]any{
			"str": "test",
			"num": 1.0,
		},
	}
	diagRaw, err := json.Marshal(diag)
	if err != nil {
		t.Errorf("failed to marshal diagnostic: %s", err.Error())
	}
	diagCompressed, err := gZipEncode(diagRaw)
	if err != nil {
		t.Errorf("failed to compress diagnostic: %s", err.Error())
	}

	badCompressed, err := gZipEncode(badBytes)
	if err != nil {
		t.Errorf("failed to compress bad bytes: %s", err.Error())
	}

	diagnosticTests := []decoderTestCase[diagnostics.DiagnosticResult]{
		{
			name:    "diagnostic valid",
			data:    diagCompressed,
			want:    &diag,
			wantErr: false,
		},
		{
			name:    "diagnostic invalid",
			data:    badCompressed,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "diagnostic bypass valid",
			data:    diagRaw,
			want:    &diag,
			wantErr: false,
		},
		{
			name:    "diagnostic bypass invalid",
			data:    badBytes,
			want:    nil,
			wantErr: true,
		},
	}

	testDecoder(t, GetGzipDecoder[diagnostics.DiagnosticResult](JSONDecoder), diagnosticTests)
}

func TestProtobufDecoder(t *testing.T) {
	badBytes := generateBadBytes()

	now := time.Now().UTC().Truncate(24 * time.Hour)
	start := now.Add(-(24 * 5) * time.Hour)

	customCostSet := model.GenerateMockCustomCostSet(start, start.Add(24*time.Hour))
	customCostSetRaw, err := proto.Marshal(customCostSet)
	if err != nil {
		t.Errorf("failed to marshal custom cost set: %s", err.Error())
	}

	customCostTests := []decoderTestCase[pb.CustomCostResponse]{
		{
			name:    "custom cost valid",
			data:    customCostSetRaw,
			want:    customCostSet,
			wantErr: false,
		},
		{
			name:    "custom cost invalid",
			data:    badBytes,
			want:    nil,
			wantErr: true,
		},
	}

	testProtoBufDecoder(t, ProtobufDecoder, customCostTests)

	labelsResponse := model.GenerateMockLabelResponse(start, "1d")
	labelsResponseRaw, err := proto.Marshal(labelsResponse)
	if err != nil {
		t.Errorf("failed to marshal custom cost set: %s", err.Error())
	}

	labelsResponseTests := []decoderTestCase[pb.LabelsResponse]{
		{
			name:    "labels response valid",
			data:    labelsResponseRaw,
			want:    labelsResponse,
			wantErr: false,
		},
		{
			name:    "labels response invalid",
			data:    badBytes,
			want:    nil,
			wantErr: true,
		},
	}

	testProtoBufDecoder(t, ProtobufDecoder, labelsResponseTests)
}

func testProtoBufDecoder[T any, U ProtoMessagePtr[T]](t *testing.T, decoder Decoder[T], testCases []decoderTestCase[T]) {
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decoder(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decoder() error = %v, wantErr %v", err, tt.wantErr)
				if err != nil {
					t.Errorf("Error: %s", err.Error())
				}
				return
			}
			if !proto.Equal(U(got), U(tt.want)) {
				t.Errorf("Decoder() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func testDecoder[T any](t *testing.T, decoder Decoder[T], testCases []decoderTestCase[T]) {
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decoder(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decoder() error = %v, wantErr %v", err, tt.wantErr)
				if err != nil {
					t.Errorf("Error: %s", err.Error())
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decoder() got = %v, want %v", got, tt.want)
			}
		})
	}
}
