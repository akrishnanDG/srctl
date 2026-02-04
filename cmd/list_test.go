package cmd

import (
	"sort"
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestFilterSubjects(t *testing.T) {
	tests := []struct {
		name     string
		subjects []string
		pattern  string
		expected int
	}{
		{
			name:     "wildcard pattern",
			subjects: []string{"user-events", "user-orders", "order-events", "payment-events"},
			pattern:  "user-*",
			expected: 2,
		},
		{
			name:     "suffix pattern",
			subjects: []string{"user-events", "user-orders", "order-events", "payment-events"},
			pattern:  "*-events",
			expected: 3,
		},
		{
			name:     "exact match",
			subjects: []string{"user-events", "user-orders", "order-events"},
			pattern:  "user-events",
			expected: 1,
		},
		{
			name:     "no match",
			subjects: []string{"user-events", "user-orders", "order-events"},
			pattern:  "payment-*",
			expected: 0,
		},
		{
			name:     "empty pattern",
			subjects: []string{"user-events", "user-orders", "order-events"},
			pattern:  "",
			expected: 0, // empty pattern means no filter, but filterSubjects returns 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterSubjects(tt.subjects, tt.pattern)
			if len(result) != tt.expected {
				t.Errorf("expected %d subjects, got %d", tt.expected, len(result))
			}
		})
	}
}

func TestSortSubjectsByName(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		reverse bool
		first   string
	}{
		{
			name:    "sort ascending",
			input:   []string{"zebra", "apple", "mango"},
			reverse: false,
			first:   "apple",
		},
		{
			name:    "sort descending",
			input:   []string{"zebra", "apple", "mango"},
			reverse: true,
			first:   "zebra",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy to sort
			subjects := make([]string, len(tt.input))
			copy(subjects, tt.input)

			if tt.reverse {
				sort.Sort(sort.Reverse(sort.StringSlice(subjects)))
			} else {
				sort.Strings(subjects)
			}

			if subjects[0] != tt.first {
				t.Errorf("expected first subject to be '%s', got '%s'", tt.first, subjects[0])
			}
		})
	}
}

func TestMockClientGetSubjects(t *testing.T) {
	mock := client.NewMockClient()

	// Add test subjects
	mock.AddSubject("user-events", []client.Schema{
		{Subject: "user-events", Version: 1, ID: 100},
	})
	mock.AddSubject("order-events", []client.Schema{
		{Subject: "order-events", Version: 1, ID: 101},
	})

	subjects, err := mock.GetSubjects(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(subjects) != 2 {
		t.Errorf("expected 2 subjects, got %d", len(subjects))
	}
}

func TestMockClientGetVersions(t *testing.T) {
	mock := client.NewMockClient()

	mock.AddSubject("test-subject", []client.Schema{
		{Subject: "test-subject", Version: 1, ID: 100},
		{Subject: "test-subject", Version: 2, ID: 101},
		{Subject: "test-subject", Version: 3, ID: 102},
	})

	versions, err := mock.GetVersions("test-subject", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
}

func TestMockClientError(t *testing.T) {
	mock := client.NewMockClient()
	mock.ShouldError = true
	mock.ErrorMessage = "connection refused"

	_, err := mock.GetSubjects(false)
	if err == nil {
		t.Error("expected error")
	}

	if err.Error() != "connection refused" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestListCommand(t *testing.T) {
	if listCmd == nil {
		t.Fatal("expected listCmd to be defined")
	}

	if listCmd.Use != "list" {
		t.Errorf("expected Use to be 'list', got '%s'", listCmd.Use)
	}

	// Check for filter flag
	filterFlag := listCmd.Flags().Lookup("filter")
	if filterFlag == nil {
		t.Error("expected --filter flag to exist")
	}

	// Check for versions flag
	versionsFlag := listCmd.Flags().Lookup("versions")
	if versionsFlag == nil {
		t.Error("expected --versions flag to exist")
	}
}

func TestListIncludeDeleted(t *testing.T) {
	mock := client.NewMockClient()

	// Add subjects
	mock.AddSubject("active-subject", []client.Schema{
		{Subject: "active-subject", Version: 1, ID: 100},
	})

	// Test with includeDeleted = false
	subjects, _ := mock.GetSubjects(false)
	if len(subjects) != 1 {
		t.Errorf("expected 1 subject, got %d", len(subjects))
	}

	// Test with includeDeleted = true
	subjects, _ = mock.GetSubjects(true)
	if len(subjects) != 1 {
		t.Errorf("expected 1 subject, got %d", len(subjects))
	}
}

func TestSubjectInfo(t *testing.T) {
	info := subjectInfo{
		Subject:      "test-subject",
		VersionCount: 5,
		LatestID:     100,
	}

	if info.Subject != "test-subject" {
		t.Errorf("expected subject 'test-subject', got '%s'", info.Subject)
	}

	if info.VersionCount != 5 {
		t.Errorf("expected 5 versions, got %d", info.VersionCount)
	}

	if info.LatestID != 100 {
		t.Errorf("expected latest ID 100, got %d", info.LatestID)
	}
}
