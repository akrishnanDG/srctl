package cmd

import "testing"

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		pattern string
		want    bool
	}{
		// Basic patterns
		{"exact match", "user-events", "user-events", true},
		{"exact mismatch", "user-events", "order-events", false},
		{"star matches all", "anything", "*", true},
		{"empty star", "", "*", true},

		// Prefix patterns
		{"prefix match", "user-events", "user-*", true},
		{"prefix mismatch", "order-events", "user-*", false},

		// Suffix patterns
		{"suffix match", "user-events", "*-events", true},
		{"suffix mismatch", "user-orders", "*-events", false},

		// Contains patterns
		{"contains match", "user-events-v2", "*events*", true},
		{"contains mismatch", "user-orders-v2", "*events*", false},

		// Multiple wildcards
		{"multi wildcard match", "user-events-value", "*events*value", true},
		{"multi wildcard mismatch", "user-events-key", "*events*value", false},
		{"prefix-mid-suffix", "a-b-c-d", "a*c*d", true},
		{"prefix-mid-suffix mismatch", "a-b-x-d", "a*c*d", false},

		// Edge cases
		{"empty pattern empty string", "", "", true},
		{"empty pattern nonempty string", "abc", "", false},
		{"pattern longer than string", "ab", "abc", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchGlob(tt.s, tt.pattern)
			if got != tt.want {
				t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
			}
		})
	}
}
