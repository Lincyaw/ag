package telemetry

import "testing"

func TestExporterEnabled(t *testing.T) {
	tests := []struct {
		name      string
		selector  string
		endpoint  bool
		want      bool
		wantError bool
	}{
		{name: "disabled by default", want: false},
		{name: "endpoint enables exporter", endpoint: true, want: true},
		{name: "explicit otlp", selector: "otlp", want: true},
		{name: "explicit none", selector: "none", endpoint: true, want: false},
		{name: "unknown fails", selector: "console", wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := exporterEnabled(test.selector, test.endpoint)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError %v", err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("enabled = %v, want %v", got, test.want)
			}
		})
	}
}

func TestValidateHTTPProtocol(t *testing.T) {
	tests := []struct {
		name           string
		signalSpecific string
		common         string
		wantError      bool
	}{
		{name: "default"},
		{name: "common HTTP", common: "http/protobuf"},
		{
			name:           "signal overrides common",
			signalSpecific: "http/protobuf",
			common:         "grpc",
		},
		{name: "grpc rejected", common: "grpc", wantError: true},
		{name: "HTTP JSON rejected", common: "http/json", wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateHTTPProtocol(test.signalSpecific, test.common)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}
