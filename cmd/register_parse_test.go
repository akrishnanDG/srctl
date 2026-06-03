package cmd

import "testing"

func TestParseReferences(t *testing.T) {
	tests := []struct {
		name      string
		refs      []string
		wantCount int
		wantErr   bool
		// For the first reference, check these fields
		wantName    string
		wantSubject string
		wantVersion int
	}{
		{
			name:        "simple reference",
			refs:        []string{"Address=address-value:1"},
			wantCount:   1,
			wantName:    "Address",
			wantSubject: "address-value",
			wantVersion: 1,
		},
		{
			name:        "context-prefixed subject",
			refs:        []string{"Address=:.mycontext:address-value:3"},
			wantCount:   1,
			wantName:    "Address",
			wantSubject: ":.mycontext:address-value",
			wantVersion: 3,
		},
		{
			name:      "missing equals",
			refs:      []string{"AddressNoEquals"},
			wantCount: 0,
			wantErr:   true,
		},
		{
			name:      "missing version colon",
			refs:      []string{"Address=address-value"},
			wantCount: 0,
			wantErr:   true,
		},
		{
			name:      "invalid version number",
			refs:      []string{"Address=address-value:abc"},
			wantCount: 0,
			wantErr:   true,
		},
		{
			name:        "multiple references",
			refs:        []string{"Addr=address-value:1", "Phone=phone-value:2"},
			wantCount:   2,
			wantName:    "Addr",
			wantSubject: "address-value",
			wantVersion: 1,
		},
		{
			name:        "empty refs",
			refs:        []string{},
			wantCount:   0,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseReferences(tt.refs)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseReferences() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if len(result) != tt.wantCount {
				t.Fatalf("parseReferences() returned %d refs, want %d", len(result), tt.wantCount)
			}
			if tt.wantCount > 0 {
				if result[0].Name != tt.wantName {
					t.Errorf("ref[0].Name = %q, want %q", result[0].Name, tt.wantName)
				}
				if result[0].Subject != tt.wantSubject {
					t.Errorf("ref[0].Subject = %q, want %q", result[0].Subject, tt.wantSubject)
				}
				if result[0].Version != tt.wantVersion {
					t.Errorf("ref[0].Version = %d, want %d", result[0].Version, tt.wantVersion)
				}
			}
		})
	}
}

func TestDetectSchemaType(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		filename string
		want     string
	}{
		{
			name:     "avsc extension",
			content:  `{"type":"record","name":"Test","fields":[]}`,
			filename: "test.avsc",
			want:     "AVRO",
		},
		{
			name:     "proto extension",
			content:  `syntax = "proto3";`,
			filename: "test.proto",
			want:     "PROTOBUF",
		},
		{
			name:     "json schema by extension and content",
			content:  `{"$schema":"http://json-schema.org/draft-07/schema#"}`,
			filename: "test.json",
			want:     "JSON",
		},
		{
			name:     "avro in json extension",
			content:  `{"type":"record","name":"Test","fields":[]}`,
			filename: "test.json",
			want:     "AVRO",
		},
		{
			name:     "protobuf by content no extension",
			content:  `syntax = "proto3"; message User {}`,
			filename: "",
			want:     "PROTOBUF",
		},
		{
			name:     "json schema by content no extension",
			content:  `{"$schema":"http://json-schema.org/draft-07/schema#"}`,
			filename: "",
			want:     "JSON",
		},
		{
			name:     "default to avro",
			content:  `{"type":"record","name":"Test"}`,
			filename: "",
			want:     "AVRO",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectSchemaType(tt.content, tt.filename)
			if got != tt.want {
				t.Errorf("detectSchemaType() = %q, want %q", got, tt.want)
			}
		})
	}
}
