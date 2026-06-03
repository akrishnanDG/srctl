package cmd

import (
	"testing"

	"github.com/srctl/srctl/internal/client"
)

func TestRewriteSubjectContext(t *testing.T) {
	tests := []struct {
		name          string
		subject       string
		targetContext string
		want          string
	}{
		{
			name:          "plain subject to context",
			subject:       "user-events",
			targetContext: ".production",
			want:          ":.production:user-events",
		},
		{
			name:          "context-prefixed to new context",
			subject:       ":.staging:user-events",
			targetContext: ".production",
			want:          ":.production:user-events",
		},
		{
			name:          "remove context with empty target",
			subject:       ":.staging:user-events",
			targetContext: "",
			want:          "user-events",
		},
		{
			name:          "remove context with dot target",
			subject:       ":.staging:user-events",
			targetContext: ".",
			want:          "user-events",
		},
		{
			name:          "auto-prefix dot on context",
			subject:       "user-events",
			targetContext: "production",
			want:          ":.production:user-events",
		},
		{
			name:          "plain subject no context change",
			subject:       "user-events",
			targetContext: "",
			want:          "user-events",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rewriteSubjectContext(tt.subject, tt.targetContext)
			if got != tt.want {
				t.Errorf("rewriteSubjectContext(%q, %q) = %q, want %q",
					tt.subject, tt.targetContext, got, tt.want)
			}
		})
	}
}

func TestSortSchemasByDependencies(t *testing.T) {
	schemas := []schemaToImport{
		{Subject: "order-events", Version: 1, References: []client.SchemaReference{
			{Name: "Address", Subject: "address-value", Version: 1},
		}},
		{Subject: "address-value", Version: 1},
		{Subject: "user-events", Version: 1, References: []client.SchemaReference{
			{Name: "Address", Subject: "address-value", Version: 1},
		}},
	}

	sortSchemasByDependencies(schemas)

	// address-value should come before order-events and user-events
	addrIdx := -1
	for i, s := range schemas {
		if s.Subject == "address-value" {
			addrIdx = i
			break
		}
	}

	for i, s := range schemas {
		if s.Subject == "order-events" || s.Subject == "user-events" {
			if i < addrIdx {
				t.Errorf("%s (idx %d) appears before dependency address-value (idx %d)",
					s.Subject, i, addrIdx)
			}
		}
	}
}

func TestSortSchemasByDependencies_NoDeps(t *testing.T) {
	schemas := []schemaToImport{
		{Subject: "c-events", Version: 1},
		{Subject: "a-events", Version: 1},
		{Subject: "b-events", Version: 1},
	}

	sortSchemasByDependencies(schemas)

	// With no dependencies, should be sorted alphabetically for determinism
	if schemas[0].Subject != "a-events" {
		t.Errorf("expected first subject to be a-events, got %s", schemas[0].Subject)
	}
}

func TestSortSchemasByDependencies_MultipleVersions(t *testing.T) {
	schemas := []schemaToImport{
		{Subject: "user-events", Version: 2, References: []client.SchemaReference{
			{Name: "Address", Subject: "address-value", Version: 1},
		}},
		{Subject: "user-events", Version: 1, References: []client.SchemaReference{
			{Name: "Address", Subject: "address-value", Version: 1},
		}},
		{Subject: "address-value", Version: 1},
	}

	sortSchemasByDependencies(schemas)

	// address-value should come first
	if schemas[0].Subject != "address-value" {
		t.Errorf("expected first subject to be address-value, got %s", schemas[0].Subject)
	}

	// Within user-events, v1 should come before v2
	var userVersions []int
	for _, s := range schemas {
		if s.Subject == "user-events" {
			userVersions = append(userVersions, s.Version)
		}
	}
	if len(userVersions) == 2 && userVersions[0] > userVersions[1] {
		t.Errorf("versions within subject should be ascending, got %v", userVersions)
	}
}

func TestFilterVersions(t *testing.T) {
	versions := []int{1, 2, 3, 4, 5}

	tests := []struct {
		name     string
		filter   string
		versions []int
		want     []int
	}{
		{"all", "all", versions, versions},
		{"empty", "", versions, versions},
		{"latest", "latest", versions, []int{5}},
		{"latest empty", "latest", []int{}, nil},
		{"specific", "2,4", versions, []int{2, 4}},
		{"specific with spaces", "1, 3, 5", versions, []int{1, 3, 5}},
		{"specific not found", "6,7", versions, nil},
		{"mixed found and not found", "1,6", versions, []int{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterVersions(tt.versions, tt.filter)
			if len(got) != len(tt.want) {
				t.Fatalf("filterVersions(%v, %q) returned %d items, want %d",
					tt.versions, tt.filter, len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("filterVersions[%d] = %d, want %d", i, got[i], tt.want[i])
				}
			}
		})
	}
}
