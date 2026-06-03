package output

import "testing"

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected string
	}{
		{"zero", 0, "0 B"},
		{"negative", -100, "0 B"},
		{"small bytes", 500, "500 B"},
		{"one KB", 1024, "1.0 KB"},
		{"one MB", 1024 * 1024, "1.0 MB"},
		{"one GB", 1024 * 1024 * 1024, "1.0 GB"},
		{"1.5 KB", 1536, "1.5 KB"},
		{"just under KB", 1023, "1023 B"},
		{"large MB", 10 * 1024 * 1024, "10.0 MB"},
		{"one TB", 1024 * 1024 * 1024 * 1024, "1.0 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatBytes(tt.input)
			if result != tt.expected {
				t.Errorf("FormatBytes(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
