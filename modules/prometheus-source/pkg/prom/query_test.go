package prom

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/source"
	"github.com/prometheus/client_golang/api"
)

func TestWarningsFrom(t *testing.T) {
	var results any = map[string]interface{}{
		"status": "success",
		"warnings": []string{
			"Warning #1",
			"Warning #2",
		},
	}

	warnings := warningsFrom(results)
	if len(warnings) != 2 {
		t.Errorf("Unexpected warnings length: %d, Expected 2.", len(warnings))
	}

	if warnings[0] != "Warning #1" {
		t.Errorf("Unexpected first warning: %s", warnings[0])
	}
	if warnings[1] != "Warning #2" {
		t.Errorf("Unexpected second warning: %s", warnings[1])
	}
}

func TestContext_isRequestStepAligned(t *testing.T) {
	type fields struct {
		Client         api.Client
		name           string
		errorCollector *source.QueryErrorCollector
	}
	type args struct {
		start time.Time
		end   time.Time
		step  time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "Test with times that are not step aligned to the hour",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 59, 30, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 59, 30, 0, time.UTC),
				step:  time.Hour,
			},
			want: false,
		},
		{
			name:   "Test with times that are step aligned to the hour",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
				step:  time.Hour,
			},
			want: true,
		},
		{
			name:   "Test with times where start is aligned to the hour but end is not",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 59, 0, 0, time.UTC),
				step:  time.Hour,
			},
			want: false,
		},
		{
			name:   "Test with times where end is aligned to the hour but start is not",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 59, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
				step:  time.Hour,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &Context{
				Client:         tt.fields.Client,
				name:           tt.fields.name,
				errorCollector: tt.fields.errorCollector,
			}
			if got := ctx.isRequestStepAligned(tt.args.start, tt.args.end, tt.args.step); got != tt.want {
				t.Errorf("isRequestStepAligned() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContext_alignWindow(t *testing.T) {
	type fields struct {
		Client         api.Client
		name           string
		errorCollector *source.QueryErrorCollector
	}
	type args struct {
		start time.Time
		end   time.Time
		step  time.Duration
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name:   "Do not update the start and end when step-aligned",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
				step:  time.Hour,
			},
			wantStart: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
		},
		{
			name:   "Update start to be step-aligned and leave end the same",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 59, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
				step:  time.Hour,
			},
			wantStart: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
		},
		{
			name:   "Update end to be step-aligned and leave start the same",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 59, 0, 0, time.UTC),
				step:  time.Hour,
			},
			wantStart: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
		},
		{
			name:   "Update start and end to be step-aligned",
			fields: fields{},
			args: args{
				start: time.Date(2022, 11, 7, 4, 59, 0, 0, time.UTC),
				end:   time.Date(2022, 11, 8, 4, 59, 0, 0, time.UTC),
				step:  time.Hour,
			},
			wantStart: time.Date(2022, 11, 7, 4, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2022, 11, 8, 4, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &Context{
				Client:         tt.fields.Client,
				name:           tt.fields.name,
				errorCollector: tt.fields.errorCollector,
			}
			got, got1 := ctx.alignWindow(tt.args.start, tt.args.end, tt.args.step)
			if !reflect.DeepEqual(got, tt.wantStart) {
				t.Errorf("alignWindow() got = %v, want %v", got, tt.wantStart)
			}
			if !reflect.DeepEqual(got1, tt.wantEnd) {
				t.Errorf("alignWindow() got1 = %v, want %v", got1, tt.wantEnd)
			}
		})
	}
}

// TestNewPrometheusTransport_DisableHTTP2 verifies that when DisableHTTP2 is
// set on PrometheusClientConfig, the resulting transport has TLSNextProto set
// to a non-nil empty map — the canonical Go mechanism for disabling HTTP/2
// ALPN negotiation and forcing HTTP/1.1.
func TestNewPrometheusTransport_DisableHTTP2(t *testing.T) {
	cfg := &PrometheusClientConfig{
		Timeout:               5 * time.Second,
		KeepAlive:             30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		TLSInsecureSkipVerify: false,
		DisableHTTP2:          true,
	}

	transport := newPrometheusTransport(cfg)

	// TLSNextProto must be non-nil and empty to disable HTTP/2.
	if transport.TLSNextProto == nil {
		t.Fatal("Expected TLSNextProto to be non-nil when DisableHTTP2=true")
	}
	if len(transport.TLSNextProto) != 0 {
		t.Errorf("Expected TLSNextProto to be empty, got %d entries", len(transport.TLSNextProto))
	}

	// Verify other transport fields are set from config.
	if transport.TLSHandshakeTimeout != cfg.TLSHandshakeTimeout {
		t.Errorf("TLSHandshakeTimeout mismatch: got %v, want %v", transport.TLSHandshakeTimeout, cfg.TLSHandshakeTimeout)
	}
	if transport.TLSClientConfig == nil {
		t.Fatal("Expected TLSClientConfig to be set")
	}
	if transport.TLSClientConfig.InsecureSkipVerify != cfg.TLSInsecureSkipVerify {
		t.Errorf("InsecureSkipVerify mismatch: got %v, want %v", transport.TLSClientConfig.InsecureSkipVerify, cfg.TLSInsecureSkipVerify)
	}
}

// TestNewPrometheusTransport_HTTP2EnabledByDefault verifies that when
// DisableHTTP2 is false (the default), TLSNextProto is NOT set — allowing
// Go's net/http to negotiate HTTP/2 via ALPN as normal.
func TestNewPrometheusTransport_HTTP2EnabledByDefault(t *testing.T) {
	cfg := &PrometheusClientConfig{
		Timeout:      5 * time.Second,
		DisableHTTP2: false,
	}

	transport := newPrometheusTransport(cfg)

	// TLSNextProto must remain nil so Go can auto-configure HTTP/2.
	if transport.TLSNextProto != nil {
		t.Error("Expected TLSNextProto to be nil when DisableHTTP2=false (HTTP/2 should be allowed)")
	}
}

// TestPrometheusClientConfig_DisableHTTP2Field verifies the DisableHTTP2 field
// is correctly stored and retrieved from PrometheusClientConfig.
func TestPrometheusClientConfig_DisableHTTP2Field(t *testing.T) {
	cfg := &PrometheusClientConfig{DisableHTTP2: true}
	if !cfg.DisableHTTP2 {
		t.Error("Expected DisableHTTP2 to be true")
	}

	cfg2 := &PrometheusClientConfig{DisableHTTP2: false}
	if cfg2.DisableHTTP2 {
		t.Error("Expected DisableHTTP2 to be false")
	}
}
